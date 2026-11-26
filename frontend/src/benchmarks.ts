import type { BenchmarkReport } from './types';

function percent(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

export function benchmarkGatePassed(report: BenchmarkReport): boolean {
  const { ci_regression_gate: gate, tier3_redaction_and_dos: { redaction, dos_guards: dos } } = report;
  const { tier1_tau_bench: tau, tier1_bfcl_v4: bfcl, tier2_agentdojo_chaos: chaos } = report;
  const checks = Object.values(gate.checks);
  return gate.gate_passed && checks.length > 0 && checks.every(Boolean) &&
    tau.tasks_evaluated > 0 && tau.durable_runtime.duplicate_side_effects === 0 &&
    tau.durable_runtime.pass_power_3 === 1 &&
    bfcl.tasks_evaluated > 0 && bfcl.durable_runtime.pass_rate === 1 &&
    chaos.tasks_evaluated > 0 && chaos.durable_runtime.fault_resilience_rate === 1 &&
    redaction.secrets_tested > 0 && redaction.decoys_tested > 0 &&
    redaction.secrets_redacted === redaction.secrets_tested && redaction.decoys_flagged === 0 &&
    dos.fanout_1000_bulkhead_cap_16_passed && dos.peak_concurrency > 0 &&
    dos.peak_concurrency <= 16 && dos.circular_ref_blocked && dos.snapshot_byte_cap_blocked;
}

export function benchmarkRows(report: BenchmarkReport) {
  const { tier1_tau_bench: tau, tier1_bfcl_v4: bfcl, tier2_agentdojo_chaos: chaos } = report;
  const { redaction, dos_guards: dos } = report.tier3_redaction_and_dos;
  return [
    {
      id: 'tau',
      dataset: `Tier 1A · ${tau.dataset}`,
      license: tau.license,
      cases: `${tau.tasks_evaluated} × ${tau.trials_per_task}`,
      baseline: `${percent(tau.upstream_baseline.pass_rate)} (pass³=${tau.upstream_baseline.pass_power_3})`,
      durable: `${percent(tau.durable_runtime.pass_rate)} (pass³=${tau.durable_runtime.pass_power_3})`,
      details: `Duplicate mutations ${tau.upstream_baseline.duplicate_side_effects} → ${tau.durable_runtime.duplicate_side_effects}`,
    },
    {
      id: 'bfcl',
      dataset: `Tier 1B · ${bfcl.dataset}`,
      license: bfcl.license,
      cases: bfcl.tasks_evaluated,
      baseline: percent(bfcl.upstream_baseline.pass_rate),
      durable: percent(bfcl.durable_runtime.pass_rate),
      details: `${bfcl.files_evaluated.length} source files · ${bfcl.skipped_tasks} cases without paired answers`,
    },
    {
      id: 'chaos',
      dataset: 'Tier 2 · AgentDojo-indexed chaos',
      license: chaos.license,
      cases: chaos.tasks_evaluated,
      baseline: percent(chaos.upstream_baseline.fault_resilience_rate),
      durable: percent(chaos.durable_runtime.fault_resilience_rate),
      details: `Median oscillation trip ${chaos.durable_runtime.oscillation_median_trip_ms} ms`,
    },
    {
      id: 'tier3',
      dataset: 'Tier 3 · Redaction & resource limits',
      license: 'MIT',
      cases: `${redaction.secrets_tested + redaction.decoys_tested} redaction + 3 resource`,
      baseline: '—',
      durable: `${percent(redaction.recall)} recall · ${percent(redaction.benign_fpr)} benign FPR`,
      details: [
        `Fan-out ${dos.fanout_1000_bulkhead_cap_16_passed ? 'passed' : 'failed'} (peak ${dos.peak_concurrency})`,
        `Cycle guard ${dos.circular_ref_blocked ? 'passed' : 'failed'}`,
        `Byte cap ${dos.snapshot_byte_cap_blocked ? 'passed' : 'failed'}`,
      ].join(' · '),
    },
  ];
}
