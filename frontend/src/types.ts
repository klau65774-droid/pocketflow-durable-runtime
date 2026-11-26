export type Verdict = 'PASSED' | 'PROTECTED' | 'FAILED' | 'VULNERABLE' | 'INTERRUPTED';
export type RunStatus =
  | 'RUNNING'
  | 'COMPLETED'
  | 'PAUSED_HITL'
  | 'CRASHED'
  | 'FAILED'
  | 'TRIPPED_OSCILLATION'
  | 'DEAD_LETTERED';

export interface HealthResponse {
  status: 'ok';
}

export interface MetaResponse {
  service: string;
  version: string;
  upstream_repo: string;
  upstream_commit: string;
  upstream_license: string;
  storage_mode: 'sqlite-wal' | 'sqlite-memory';
  cpu_only: boolean;
}

export interface GraphNode {
  id: string;
  label: string;
  kind: string;
}

export interface GraphEdge {
  source: string;
  target: string;
  action: string;
}

export interface ScenarioSpec {
  id: string;
  title: string;
  description: string;
  fault_modes: string[];
  default_fault: string;
  graph: {
    nodes: GraphNode[];
    edges: GraphEdge[];
  };
}

export interface CheckpointRecord {
  step_seq: number;
  node_id: string;
  action: string | null;
  status: string;
  state: Record<string, unknown>;
  state_diff: {
    added?: Record<string, unknown>;
    removed?: Record<string, unknown>;
    changed?: Record<string, { before: unknown; after: unknown }>;
  };
  checksum: string;
  created_at: number;
}

export interface LedgerEntry {
  idempotency_key: string;
  tool_name: string;
  result: Record<string, unknown>;
  execution_count: number;
  created_at: number;
}

export interface HitlInterrupt {
  interrupt_id: string;
  step_seq: number;
  node_id: string;
  reason: string;
  status: 'PENDING' | 'APPROVED' | 'REJECTED';
  reviewer_note: string | null;
  created_at: number;
  resolved_at: number | null;
}

export interface RunDetail extends RunSummary {
  crash_step: number | null;
  require_hitl: boolean;
  max_steps: number;
  failure_reason: string | null;
  state: Record<string, unknown>;
  metrics: Record<string, unknown>;
  checkpoints: CheckpointRecord[];
  ledger_entries: LedgerEntry[];
  hitl_interrupt: HitlInterrupt | null;
}

export interface RunSummary {
  run_id: string;
  scenario_id: string;
  mode: string;
  status: RunStatus;
  verdict: Verdict;
  fault_mode: string;
  steps_executed: number;
  duplicate_side_effects: number;
  leaked_secrets: number;
  redacted_secrets_count: number;
  parent_run_id: string | null;
  forked_from_step: number | null;
  created_at: number;
  updated_at: number;
}

export interface CompareSideResult {
  mode: string;
  status: RunStatus;
  verdict: Verdict;
  failure_reason: string | null;
  output: Record<string, unknown>;
  duplicate_side_effects: number;
  elapsed_ms: number;
  steps_executed: number;
  leaked_secrets: number;
}

export interface CompareResponse {
  scenario_id: string;
  fault_mode: string;
  seed: number;
  baseline: CompareSideResult;
  durable: CompareSideResult & { run_id: string };
  delta_summary: {
    baseline_failed_or_vulnerable: boolean;
    durable_passed: boolean;
    duplicate_side_effects_prevented: number;
    silent_failures_prevented: number;
  };
}

export interface SpanRecord {
  span_id: string;
  trace_id: string;
  run_id: string;
  parent_span_id: string | null;
  name: string;
  kind: string;
  status_code: string;
  start_time_ns: number;
  end_time_ns: number;
  duration_ms: number;
  attributes: Record<string, unknown>;
}

export interface TraceResponse {
  run_id: string;
  trace_id: string;
  spans: SpanRecord[];
  otel_export: {
    resourceSpans: Array<{
      resource: { attributes: OtlpAttribute[] };
      scopeSpans: Array<{
        scope: { name: string; version: string };
        spans: Array<{
          traceId: string;
          spanId: string;
          parentSpanId: string;
          name: string;
          kind: 0 | 1 | 2 | 3 | 4 | 5;
          startTimeUnixNano: string;
          endTimeUnixNano: string;
          status: { code: 0 | 1 | 2 };
          attributes: OtlpAttribute[];
        }>;
      }>;
    }>;
  };
}

interface OtlpAttribute {
  key: string;
  value: { stringValue: string };
}

export interface RedactionResponse {
  redacted_payload: Record<string, unknown>;
  findings_count: number;
  matched_rules: string[];
  suite_summary: (RedactionSummary & {
    secret_details: Array<{ label: string; redacted: boolean; rules: string[] }>;
    decoy_details: Array<{ label: string; flagged: boolean; rules: string[] }>;
  }) | null;
}

interface RedactionSummary {
  secrets_tested: number;
  secrets_redacted: number;
  recall: number;
  decoys_tested: number;
  decoys_flagged: number;
  benign_fpr: number;
}

export interface ForkResponse {
  parent_run_id: string;
  forked_run: RunDetail;
  state_diff: {
    added: Record<string, unknown>;
    removed: Record<string, unknown>;
    changed: Record<string, unknown>;
  };
}

type ConfidenceInterval = [number, number];

interface ReplayMetrics {
  pass_rate: number;
  wilson_95ci: ConfidenceInterval;
  p95_ms: number;
  p99_ms: number;
}

interface TauMetrics extends ReplayMetrics {
  pass_power_3: number;
  duplicate_side_effects: number;
}

interface ChaosMetrics {
  fault_resilience_rate: number;
  wilson_95ci: ConfidenceInterval;
}

interface BenchmarkTier<T, U = T> {
  dataset: string;
  license: string;
  tasks_evaluated: number;
  upstream_baseline: T;
  durable_runtime: U;
}

export interface BenchmarkReport {
  project: string;
  upstream_repo: string;
  upstream_commit: string;
  hardware: string;
  total_eval_sec: number;
  tier1_tau_bench: BenchmarkTier<TauMetrics> & {
    retail_tasks: number;
    airline_tasks: number;
    tasks_available: number;
    db_records_counted: number;
    trials_per_task: number;
  };
  tier1_bfcl_v4: BenchmarkTier<ReplayMetrics> & {
    files_evaluated: string[];
    skipped_tasks: number;
  };
  tier2_agentdojo_chaos: BenchmarkTier<
    ChaosMetrics,
    ChaosMetrics & { oscillation_median_trip_ms: number }
  > & { suites_evaluated: string[] };
  tier3_redaction_and_dos: {
    redaction: RedactionSummary;
    dos_guards: {
      fanout_1000_bulkhead_cap_16_passed: boolean;
      peak_concurrency: number;
      circular_ref_blocked: boolean;
      snapshot_byte_cap_blocked: boolean;
    };
  };
  ci_regression_gate: {
    gate_passed: boolean;
    baseline_pass_rate: number;
    candidate_pass_rate: number;
    delta: number;
    duplicate_side_effect_rate: number;
    leaked_secrets: number;
    checks: Record<string, boolean>;
  };
}

export type BenchmarkResponse = BenchmarkReport | { status: 'pending'; message: string };
