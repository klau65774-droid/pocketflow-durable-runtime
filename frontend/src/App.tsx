import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  createRun,
  decideHitl,
  evaluateRedaction,
  fetchBenchmarks,
  fetchHealth,
  fetchMeta,
  fetchRunDetail,
  fetchRuns,
  fetchScenarios,
  fetchTrace,
  forkRun,
  resolveEffectiveVerdict,
  resumeRun,
  runCompare,
} from './api';
import { benchmarkGatePassed, benchmarkRows } from './benchmarks';
import { WorkflowGraph } from './components/WorkflowGraph';
import type {
  BenchmarkResponse,
  CompareResponse,
  ForkResponse,
  MetaResponse,
  RedactionResponse,
  RunDetail,
  RunSummary,
  ScenarioSpec,
  TraceResponse,
  Verdict,
} from './types';

type TabKey = 'chaos' | 'durable' | 'trace' | 'benchmarks' | 'redaction';

const TABS: ReadonlyArray<{ key: TabKey; label: string }> = [
  { key: 'chaos', label: 'Chaos Compare' },
  { key: 'durable', label: 'Durable & Time-Travel' },
  { key: 'trace', label: 'OTel Trace & Diff' },
  { key: 'benchmarks', label: '3-Tier Benchmarks' },
  { key: 'redaction', label: 'Redaction & DoS Lab' },
];

function verdictClass(verdict: Verdict): string {
  if (verdict === 'PASSED' || verdict === 'PROTECTED') {
    return 'verdict-badge verdict-pass';
  }
  if (verdict === 'INTERRUPTED') {
    return 'verdict-badge verdict-warn';
  }
  return 'verdict-badge verdict-fail';
}

export const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState<TabKey>('chaos');
  const [healthOk, setHealthOk] = useState<boolean>(false);
  const [meta, setMeta] = useState<MetaResponse | null>(null);
  const [scenarios, setScenarios] = useState<ScenarioSpec[]>([]);
  const [selectedScenarioId, setSelectedScenarioId] = useState<string>('parallel_batch_retry_race');
  const [selectedFaultMode, setSelectedFaultMode] = useState<string>('concurrent_retry_race');
  const [compareResult, setCompareResult] = useState<CompareResponse | null>(null);

  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [activeRun, setActiveRun] = useState<RunDetail | null>(null);
  const [forkResult, setForkResult] = useState<ForkResponse | null>(null);
  const [forkStep, setForkStep] = useState<number>(2);
  const [forkPatchText, setForkPatchText] = useState<string>('{"amount_cents": 1250}');
  const [reviewerNote, setReviewerNote] = useState<string>('Policy exception verified');

  const [traceData, setTraceData] = useState<TraceResponse | null>(null);
  const [benchmarks, setBenchmarks] = useState<BenchmarkResponse | null>(null);

  const [redactionInput, setRedactionInput] = useState<string>(
    JSON.stringify(
      {
        order_id: '91002348712',
        trace_id: '550e8400-e29b-41d4-a716-446655440000',
        note: 'Order #W91002348712 verified by bearer policy',
      },
      null,
      2,
    ),
  );
  const [redactionResult, setRedactionResult] = useState<RedactionResponse | null>(null);
  const [busy, setBusy] = useState<boolean>(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const selectedScenario = useMemo(
    () => scenarios.find((s) => s.id === selectedScenarioId) ?? scenarios[0] ?? null,
    [scenarios, selectedScenarioId],
  );

  const refreshRuns = useCallback(async () => {
    const res = await fetchRuns(25);
    setRuns(res.runs);
  }, []);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const [h, m, sc, rList, bm, red] = await Promise.all([
          fetchHealth(),
          fetchMeta(),
          fetchScenarios(),
          fetchRuns(25),
          fetchBenchmarks(),
          evaluateRedaction({ order_id: '91002348712' }, true),
        ]);
        if (!mounted) {
          return;
        }
        setHealthOk(h.status === 'ok');
        setMeta(m);
        setScenarios(sc.scenarios);
        setRuns(rList.runs);
        setBenchmarks(bm);
        setRedactionResult(red);
        if (sc.scenarios.length > 0) {
          const initial = sc.scenarios.find((s) => s.id === 'parallel_batch_retry_race') ?? sc.scenarios[0];
          setSelectedScenarioId(initial.id);
          setSelectedFaultMode(initial.default_fault);
        }
      } catch (err) {
        if (mounted) {
          setErrorMsg(err instanceof Error ? err.message : 'Failed to load console metadata');
        }
      }
    })();
    return () => {
      mounted = false;
    };
  }, []);

  const handleScenarioChange = (scenarioId: string) => {
    setCompareResult(null);
    setSelectedScenarioId(scenarioId);
    const found = scenarios.find((s) => s.id === scenarioId);
    if (found) {
      setSelectedFaultMode(found.default_fault);
    }
  };

  const handleRunCompare = async () => {
    setBusy(true);
    setErrorMsg(null);
    setCompareResult(null);
    try {
      const res = await runCompare({
        scenario_id: selectedScenarioId,
        fault_mode: selectedFaultMode,
      });
      setCompareResult(res);
      if (res.durable.run_id) {
        const [detail, trace] = await Promise.all([
          fetchRunDetail(res.durable.run_id),
          fetchTrace(res.durable.run_id),
        ]);
        setActiveRun(detail);
        setTraceData(trace);
      }
      await refreshRuns();
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : 'Compare request failed');
    } finally {
      setBusy(false);
    }
  };

  const handleCreateDurableRun = async (faultMode: string, requireHitl: boolean) => {
    setBusy(true);
    setErrorMsg(null);
    setForkResult(null);
    try {
      const detail = await createRun({
        scenario_id: 'retail_refund_durable',
        fault_mode: faultMode,
        crash_step: faultMode === 'crash_at_step' ? 3 : null,
        require_hitl: requireHitl,
      });
      setActiveRun(detail);
      const trace = await fetchTrace(detail.run_id);
      setTraceData(trace);
      await refreshRuns();
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : 'Failed to start run');
    } finally {
      setBusy(false);
    }
  };

  const handleSelectRun = async (runId: string) => {
    setBusy(true);
    setErrorMsg(null);
    setForkResult(null);
    try {
      const [detail, trace] = await Promise.all([fetchRunDetail(runId), fetchTrace(runId)]);
      setActiveRun(detail);
      setTraceData(trace);
      if (detail.checkpoints.length > 0) {
        setForkStep(Math.min(2, detail.checkpoints[detail.checkpoints.length - 1].step_seq));
      }
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : 'Failed to load run');
    } finally {
      setBusy(false);
    }
  };

  const handleResumeRun = async () => {
    if (!activeRun) {
      return;
    }
    setBusy(true);
    setErrorMsg(null);
    try {
      const resumed = await resumeRun(activeRun.run_id, true);
      setActiveRun(resumed);
      setTraceData(await fetchTrace(resumed.run_id));
      await refreshRuns();
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : 'Resume failed');
    } finally {
      setBusy(false);
    }
  };

  const handleHitl = async (decision: 'approve' | 'reject') => {
    if (!activeRun) {
      return;
    }
    setBusy(true);
    setErrorMsg(null);
    try {
      const updated = await decideHitl(activeRun.run_id, decision, reviewerNote);
      setActiveRun(updated);
      setTraceData(await fetchTrace(updated.run_id));
      await refreshRuns();
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : 'HITL action failed');
    } finally {
      setBusy(false);
    }
  };

  const handleForkRun = async () => {
    if (!activeRun) {
      return;
    }
    setBusy(true);
    setErrorMsg(null);
    try {
      const patch = JSON.parse(forkPatchText) as Record<string, unknown>;
      const res = await forkRun(activeRun.run_id, forkStep, patch);
      setForkResult(res);
      setActiveRun(res.forked_run);
      setTraceData(await fetchTrace(res.forked_run.run_id));
      await refreshRuns();
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : 'Time-travel fork failed');
    } finally {
      setBusy(false);
    }
  };

  const handleRunRedaction = async () => {
    setBusy(true);
    setErrorMsg(null);
    setRedactionResult(null);
    try {
      const parsed = JSON.parse(redactionInput) as Record<string, unknown>;
      const res = await evaluateRedaction(parsed, true);
      setRedactionResult(res);
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : 'Invalid JSON payload');
    } finally {
      setBusy(false);
    }
  };

  const handleLoadSampleSecret = () => {
    setRedactionResult(null);
    const sampleToken = ['sk', 'proj', 'DemoSample1234567890abcdef'].join('-');
    setRedactionInput(
      JSON.stringify(
        {
          api_key: sampleToken,
          order_id: '91002348712',
          trace_id: '550e8400-e29b-41d4-a716-446655440000',
          note: 'Bearer policy applies to this voucher.',
        },
        null,
        2,
      ),
    );
  };

  const baselineVerdict = compareResult ? resolveEffectiveVerdict(compareResult.baseline) : null;
  const durableVerdict = compareResult ? resolveEffectiveVerdict(compareResult.durable) : null;
  const benchmarkReport = benchmarks && 'tier1_tau_bench' in benchmarks ? benchmarks : null;

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <h1 className="brand-title">PocketFlow Durable Runtime Console</h1>
          <p className="brand-sub">
            SQLite WAL Checkpointing · Idempotent Replay · Oscillation Guard · OTel Span DAG · CPU-Only
          </p>
        </div>
        <div className="pill-row">
          <span className={`pill ${healthOk ? 'pill-ok' : 'pill-err'}`}>
            API: {healthOk ? 'ONLINE' : 'OFFLINE'}
          </span>
          <span className="pill">
            UPSTREAM: {meta?.upstream_commit.slice(0, 8) ?? '—'} (MIT)
          </span>
          <span className="pill">STORE: {meta?.storage_mode ?? '—'}</span>
        </div>
      </header>

      <nav className="tablist" role="tablist" aria-label="Runtime console views">
        {TABS.map((tab, idx) => (
          <button
            key={`tab-${idx}-${tab.key}`}
            type="button"
            role="tab"
            id={`tab-btn-${tab.key}`}
            className="tab-btn"
            aria-selected={activeTab === tab.key}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      {errorMsg && (
        <div className="error-banner" role="alert">
          {errorMsg}
        </div>
      )}

      {/* Tab 1: Chaos Compare */}
      <section
        role="tabpanel"
        aria-labelledby="tab-btn-chaos"
        tabIndex={activeTab === 'chaos' ? 0 : -1}
        hidden={activeTab !== 'chaos'}
      >
        <div className="card" style={{ marginBottom: 16 }}>
          <div className="controls-row">
            <label htmlFor="scenario-select">Scenario</label>
            <select
              id="scenario-select"
              value={selectedScenarioId}
              onChange={(e) => handleScenarioChange(e.target.value)}
            >
              {scenarios.map((sc, idx) => (
                <option key={`sc-${idx}-${sc.id}`} value={sc.id}>
                  {sc.title}
                </option>
              ))}
            </select>

            <label htmlFor="fault-select">Fault Injection</label>
            <select
              id="fault-select"
              value={selectedFaultMode}
              disabled={busy}
              onChange={(e) => {
                setCompareResult(null);
                setSelectedFaultMode(e.target.value);
              }}
            >
              {(selectedScenario?.fault_modes ?? ['none']).map((fm, idx) => (
                <option key={`fm-${idx}-${fm}`} value={fm}>
                  {fm}
                </option>
              ))}
            </select>

            <button type="button" className="btn" disabled={busy} onClick={handleRunCompare}>
              {busy ? 'Running...' : 'Run Baseline vs Durable'}
            </button>
          </div>

          {selectedScenario && (
            <>
              <p style={{ color: '#94a3b8', marginTop: 0 }}>{selectedScenario.description}</p>
              <WorkflowGraph
                nodes={selectedScenario.graph.nodes}
                edges={selectedScenario.graph.edges}
              />
            </>
          )}
        </div>

        {compareResult && baselineVerdict && durableVerdict && (
          <div className="grid-2">
            <article className="card">
              <div className="controls-row" style={{ justifyContent: 'space-between' }}>
                <h2 style={{ margin: 0 }}>Upstream Baseline (Verbatim)</h2>
                <span className={verdictClass(baselineVerdict)}>{baselineVerdict}</span>
              </div>
              <p style={{ color: '#94a3b8', fontSize: '0.86rem' }}>
                Status: <strong>{compareResult.baseline.status}</strong> · Duplicate Side Effects:{' '}
                <strong>{compareResult.baseline.duplicate_side_effects}</strong> · Leaked Secrets:{' '}
                <strong>{compareResult.baseline.leaked_secrets}</strong> · Elapsed:{' '}
                <strong>{compareResult.baseline.elapsed_ms} ms</strong>
              </p>
              {compareResult.baseline.failure_reason && (
                <p style={{ color: '#fda4af', fontSize: '0.88rem' }}>
                  {compareResult.baseline.failure_reason}
                </p>
              )}
              <pre className="code-block">
                {JSON.stringify(compareResult.baseline.output, null, 2)}
              </pre>
            </article>

            <article className="card">
              <div className="controls-row" style={{ justifyContent: 'space-between' }}>
                <h2 style={{ margin: 0 }}>Durable Runtime (Upgraded)</h2>
                <span className={verdictClass(durableVerdict)}>{durableVerdict}</span>
              </div>
              <p style={{ color: '#94a3b8', fontSize: '0.86rem' }}>
                Status: <strong>{compareResult.durable.status}</strong> · Duplicate Side Effects:{' '}
                <strong>{compareResult.durable.duplicate_side_effects}</strong> · Leaked Secrets:{' '}
                <strong>{compareResult.durable.leaked_secrets}</strong> · Elapsed:{' '}
                <strong>{compareResult.durable.elapsed_ms} ms</strong>
              </p>
              {compareResult.durable.failure_reason && (
                <p style={{ color: '#93c5fd', fontSize: '0.88rem' }}>
                  {compareResult.durable.failure_reason}
                </p>
              )}
              <pre className="code-block">
                {JSON.stringify(compareResult.durable.output, null, 2)}
              </pre>
            </article>
          </div>
        )}
      </section>

      {/* Tab 2: Durable & Time-Travel */}
      <section
        role="tabpanel"
        aria-labelledby="tab-btn-durable"
        tabIndex={activeTab === 'durable' ? 0 : -1}
        hidden={activeTab !== 'durable'}
      >
        <div className="card" style={{ marginBottom: 16 }}>
          <h2>Launch & Recover Durable Workflows</h2>
          <div className="controls-row">
            <button
              type="button"
              className="btn"
              disabled={busy}
              onClick={() => handleCreateDurableRun('crash_at_step', false)}
            >
              Start Retail Refund (Crash @ Step 3)
            </button>
            <button
              type="button"
              className="btn btn-secondary"
              disabled={busy}
              onClick={() => handleCreateDurableRun('hitl_pause', true)}
            >
              Start Retail Refund (HITL Gate)
            </button>
            <button
              type="button"
              className="btn btn-secondary"
              disabled={busy || !activeRun || activeRun.status !== 'CRASHED'}
              onClick={handleResumeRun}
            >
              Resume Crashed Run (Idempotent)
            </button>
          </div>

          {activeRun && (
            <div className="grid-3" style={{ marginBottom: 12 }}>
              <div className="metric-tile">
                <div className="metric-label">Run ID</div>
                <div className="metric-value" style={{ fontSize: '0.95rem' }}>
                  {activeRun.run_id}
                </div>
              </div>
              <div className="metric-tile">
                <div className="metric-label">Status / Verdict</div>
                <div className="metric-value" style={{ fontSize: '0.95rem' }}>
                  {activeRun.status} ({resolveEffectiveVerdict(activeRun)})
                </div>
              </div>
              <div className="metric-tile">
                <div className="metric-label">Duplicate Side Effects</div>
                <div className="metric-value">{activeRun.duplicate_side_effects}</div>
              </div>
            </div>
          )}

          {activeRun?.status === 'PAUSED_HITL' && (
            <div className="card" style={{ background: '#0f172a', marginBottom: 12 }}>
              <h3>Pending HITL Approval Gate</h3>
              <p style={{ color: '#fbbf24' }}>{activeRun.hitl_interrupt?.reason}</p>
              <div className="controls-row">
                <input
                  type="text"
                  aria-label="Reviewer note"
                  maxLength={500}
                  value={reviewerNote}
                  onChange={(e) => setReviewerNote(e.target.value)}
                />
                <button
                  type="button"
                  className="btn"
                  disabled={busy}
                  onClick={() => handleHitl('approve')}
                >
                  Approve & Resume
                </button>
                <button
                  type="button"
                  className="btn btn-danger"
                  disabled={busy}
                  onClick={() => handleHitl('reject')}
                >
                  Reject
                </button>
              </div>
            </div>
          )}

          {activeRun && activeRun.checkpoints.length > 0 && (
            <div className="card" style={{ background: '#0f172a' }}>
              <h3>Time-Travel Fork From Checkpoint</h3>
              <div className="controls-row">
                <label htmlFor="fork-step-input">From Step Seq</label>
                <input
                  id="fork-step-input"
                  type="number"
                  min={1}
                  max={activeRun.checkpoints.at(-1)?.step_seq}
                  value={forkStep}
                  onChange={(e) => setForkStep(Number(e.target.value) || 1)}
                />
                <label htmlFor="fork-patch-input">State Patch JSON</label>
                <input
                  id="fork-patch-input"
                  type="text"
                  style={{ minWidth: 260 }}
                  value={forkPatchText}
                  onChange={(e) => setForkPatchText(e.target.value)}
                />
                <button type="button" className="btn" disabled={busy} onClick={handleForkRun}>
                  Fork & Replay
                </button>
              </div>
              {forkResult && (
                <pre className="code-block">
                  {JSON.stringify(
                    {
                      forked_run_id: forkResult.forked_run.run_id,
                      parent_run_id: forkResult.parent_run_id,
                      state_diff: forkResult.state_diff,
                    },
                    null,
                    2,
                  )}
                </pre>
              )}
            </div>
          )}
        </div>

        <div className="card">
          <h3 id="runs-table-caption">SQLite WAL Run History</h3>
          <div
            tabIndex={0}
            role="region"
            aria-labelledby="runs-table-caption"
            className="table-wrapper"
          >
            <table>
              <thead>
                <tr>
                  <th>Run ID</th>
                  <th>Scenario</th>
                  <th>Status</th>
                  <th>Verdict</th>
                  <th>Steps</th>
                  <th>Dup Effects</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((r, idx) => (
                  <tr key={`run-${idx}-${r.run_id}`}>
                    <th>{r.run_id}</th>
                    <td>{r.scenario_id}</td>
                    <td>{r.status}</td>
                    <td>
                      <span className={verdictClass(resolveEffectiveVerdict(r))}>
                        {resolveEffectiveVerdict(r)}
                      </span>
                    </td>
                    <td>{r.steps_executed}</td>
                    <td>{r.duplicate_side_effects}</td>
                    <td>
                      <button
                        type="button"
                        className="btn btn-secondary"
                        onClick={() => handleSelectRun(r.run_id)}
                      >
                        Inspect
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* Tab 3: OTel Trace & State Diff */}
      <section
        role="tabpanel"
        aria-labelledby="tab-btn-trace"
        tabIndex={activeTab === 'trace' ? 0 : -1}
        hidden={activeTab !== 'trace'}
      >
        <div className="grid-2">
          <div className="card">
            <h2>OTel Span Waterfall</h2>
            {traceData ? (
              <>
                <p style={{ color: '#94a3b8', fontSize: '0.84rem' }}>
                  Trace ID: <code>{traceData.trace_id}</code> ({traceData.spans.length} spans)
                </p>
                {traceData.spans.map((sp, idx) => {
                  const pct = Math.max(8, Math.min(100, sp.duration_ms * 8 + 12));
                  return (
                    <div key={`span-${idx}-${sp.span_id}`} style={{ marginBottom: 10 }}>
                      <div
                        style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          fontSize: '0.82rem',
                          marginBottom: 4,
                        }}
                      >
                        <span>
                          {sp.name} ({sp.status_code})
                        </span>
                        <span>{sp.duration_ms} ms</span>
                      </div>
                      <div className="span-bar-track">
                        <div className="span-bar-fill" style={{ width: `${pct}%` }} />
                      </div>
                    </div>
                  );
                })}
              </>
            ) : (
              <p style={{ color: '#94a3b8' }}>Run any scenario or select a run to view OTel spans.</p>
            )}
          </div>

          <div className="card">
            <h2>Checkpoint State Diff Timeline</h2>
            {activeRun && activeRun.checkpoints.length > 0 ? (
              activeRun.checkpoints.map((ck, idx) => (
                <div
                  key={`ckpt-${idx}-${ck.step_seq}-${ck.node_id}`}
                  style={{ marginBottom: 10 }}
                >
                  <div style={{ fontSize: '0.85rem', fontWeight: 600, marginBottom: 4 }}>
                    Step #{ck.step_seq}: {ck.node_id} → {ck.action ?? 'end'} [{ck.status}]
                  </div>
                  <pre className="code-block">{JSON.stringify(ck.state_diff, null, 2)}</pre>
                </div>
              ))
            ) : (
              <p style={{ color: '#94a3b8' }}>No checkpoints loaded yet.</p>
            )}
          </div>
        </div>
      </section>

      {/* Tab 4: 3-Tier Benchmarks */}
      <section
        role="tabpanel"
        aria-labelledby="tab-btn-benchmarks"
        tabIndex={activeTab === 'benchmarks' ? 0 : -1}
        hidden={activeTab !== 'benchmarks'}
      >
        <div className="card" style={{ marginBottom: 16 }}>
          <h2 id="bench-table-caption">3-Tier Sealed External Blind Benchmarks</h2>
          {benchmarkReport && (
            <>
              <p className={verdictClass(benchmarkGatePassed(benchmarkReport) ? 'PASSED' : 'FAILED')}>
                {benchmarkGatePassed(benchmarkReport) ? 'Gate passed' : 'Gate failed'}
              </p>
              <div
                tabIndex={0}
                role="region"
                aria-labelledby="bench-table-caption"
                className="table-wrapper"
                style={{ marginBottom: 14 }}
              >
                <table>
                  <thead>
                    <tr>
                      <th scope="col">Tier &amp; Dataset</th>
                      <th scope="col">License</th>
                      <th scope="col">Cases</th>
                      <th scope="col">Baseline</th>
                      <th scope="col">Durable runtime</th>
                      <th scope="col">Measured results</th>
                    </tr>
                  </thead>
                  <tbody>
                    {benchmarkRows(benchmarkReport).map((row, idx) => (
                      <tr key={`bm-${idx}-${row.id}`}>
                        <th scope="row">{row.dataset}</th>
                        <td>{row.license}</td>
                        <td>{row.cases}</td>
                        <td>{row.baseline}</td>
                        <td>{row.durable}</td>
                        <td>{row.details}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
          <pre className="code-block" style={{ maxHeight: 360 }}>
            {JSON.stringify(benchmarks ?? { status: 'loading' }, null, 2)}
          </pre>
        </div>
      </section>

      {/* Tab 5: Redaction & DoS Lab */}
      <section
        role="tabpanel"
        aria-labelledby="tab-btn-redaction"
        tabIndex={activeTab === 'redaction' ? 0 : -1}
        hidden={activeTab !== 'redaction'}
      >
        <div className="grid-2">
          <div className="card">
            <h2>Checkpoint & OTel Payload Scanner</h2>
            <div className="controls-row">
              <button type="button" className="btn" disabled={busy} onClick={handleRunRedaction}>
                Scan & Redact Payload
              </button>
              <button
                type="button"
                className="btn btn-secondary"
                disabled={busy}
                onClick={handleLoadSampleSecret}
              >
                Load Synthetic Secret + Decoy Sample
              </button>
            </div>
            <textarea
              rows={9}
              aria-label="Payload JSON to scan"
              value={redactionInput}
              disabled={busy}
              onChange={(e) => {
                setRedactionResult(null);
                setRedactionInput(e.target.value);
              }}
            />
            {redactionResult && (
              <div style={{ marginTop: 12 }}>
                <p style={{ fontSize: '0.86rem' }}>
                  Findings: <strong>{redactionResult.findings_count}</strong> · Rules:{' '}
                  <code>{redactionResult.matched_rules.join(', ') || 'none'}</code>
                </p>
                <pre className="code-block">
                  {JSON.stringify(redactionResult.redacted_payload, null, 2)}
                </pre>
              </div>
            )}
          </div>

          <div className="card">
            <h2>Tier 3 Built-in Recall & Benign FPR Gate</h2>
            {redactionResult?.suite_summary && (
              <div className="grid-2">
                <div className="metric-tile">
                  <div className="metric-label">Synthetic Secret Recall</div>
                  <div className="metric-value">
                    {redactionResult.suite_summary.secrets_redacted}/
                    {redactionResult.suite_summary.secrets_tested} (
                    {(redactionResult.suite_summary.recall * 100).toFixed(1)}%)
                  </div>
                </div>
                <div className="metric-tile">
                  <div className="metric-label">Benign Decoy FPR</div>
                  <div className="metric-value">
                    {redactionResult.suite_summary.decoys_flagged}/
                    {redactionResult.suite_summary.decoys_tested} (
                    {(redactionResult.suite_summary.benign_fpr * 100).toFixed(1)}%)
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </section>
    </div>
  );
};
