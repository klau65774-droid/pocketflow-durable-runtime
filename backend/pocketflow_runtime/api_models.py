from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

Verdict = Literal["PASSED", "PROTECTED", "FAILED", "VULNERABLE", "INTERRUPTED"]
RunStatus = Literal[
    "RUNNING", "COMPLETED", "PAUSED_HITL", "CRASHED", "FAILED",
    "TRIPPED_OSCILLATION", "DEAD_LETTERED",
]


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, Any]


class ErrorResponse(BaseModel):
    error: ErrorDetail


class HealthResponse(BaseModel):
    status: Literal["ok"]


class MetaResponse(BaseModel):
    service: str
    version: str
    upstream_repo: str
    upstream_commit: str
    upstream_license: str
    storage_mode: Literal["sqlite-wal", "sqlite-memory"]
    cpu_only: bool


class GraphNode(BaseModel):
    id: str
    label: str
    kind: str


class GraphEdge(BaseModel):
    source: str
    target: str
    action: str


class ScenarioGraph(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]


class ScenarioSpec(BaseModel):
    id: str
    title: str
    description: str
    fault_modes: list[str]
    default_fault: str
    graph: ScenarioGraph


class ScenariosResponse(BaseModel):
    scenarios: list[ScenarioSpec]


class StateDiff(BaseModel):
    added: dict[str, Any]
    removed: dict[str, Any]
    changed: dict[str, Any]


class CheckpointRecord(BaseModel):
    step_seq: int
    node_id: str
    action: str | None
    status: str
    state: dict[str, Any]
    state_diff: dict[str, Any]
    checksum: str
    created_at: float


class LedgerEntry(BaseModel):
    idempotency_key: str
    tool_name: str
    result: dict[str, Any]
    execution_count: int
    created_at: float


class HitlInterrupt(BaseModel):
    interrupt_id: str
    step_seq: int
    node_id: str
    reason: str
    status: Literal["PENDING", "APPROVED", "REJECTED"]
    reviewer_note: str | None
    created_at: float
    resolved_at: float | None


class RunSummary(BaseModel):
    run_id: str
    scenario_id: str
    mode: str
    status: RunStatus
    verdict: Verdict
    parent_run_id: str | None
    forked_from_step: int | None
    fault_mode: str
    steps_executed: int
    duplicate_side_effects: int
    leaked_secrets: int
    redacted_secrets_count: int
    created_at: float
    updated_at: float


class RunDetail(RunSummary):
    crash_step: int | None
    require_hitl: bool
    max_steps: int
    failure_reason: str | None
    state: dict[str, Any]
    metrics: dict[str, Any]
    checkpoints: list[CheckpointRecord]
    ledger_entries: list[LedgerEntry]
    hitl_interrupt: HitlInterrupt | None


class RunsResponse(BaseModel):
    runs: list[RunSummary]


class ForkResponse(BaseModel):
    parent_run_id: str
    forked_run: RunDetail
    state_diff: StateDiff


class CompareSide(BaseModel):
    mode: str
    status: RunStatus
    verdict: Verdict
    failure_reason: str | None
    output: dict[str, Any]
    duplicate_side_effects: int
    elapsed_ms: float
    steps_executed: int
    leaked_secrets: int


class DurableCompareSide(CompareSide):
    run_id: str


class CompareDelta(BaseModel):
    baseline_failed_or_vulnerable: bool
    durable_passed: bool
    duplicate_side_effects_prevented: int
    silent_failures_prevented: int


class CompareResponse(BaseModel):
    scenario_id: str
    fault_mode: str
    seed: int
    baseline: CompareSide
    durable: DurableCompareSide
    delta_summary: CompareDelta


class SpanRecord(BaseModel):
    span_id: str
    trace_id: str
    run_id: str
    parent_span_id: str | None
    name: str
    kind: str
    status_code: str
    start_time_ns: int
    end_time_ns: int
    duration_ms: float
    attributes: dict[str, Any]


class OtlpValue(BaseModel):
    stringValue: str


class OtlpAttribute(BaseModel):
    key: str
    value: OtlpValue


class OtlpSpanStatus(BaseModel):
    code: Literal[0, 1, 2]


class OtlpSpan(BaseModel):
    traceId: str
    spanId: str
    parentSpanId: str
    name: str
    kind: Literal[0, 1, 2, 3, 4, 5]
    startTimeUnixNano: str
    endTimeUnixNano: str
    status: OtlpSpanStatus
    attributes: list[OtlpAttribute]


class OtlpResource(BaseModel):
    attributes: list[OtlpAttribute]


class OtlpScope(BaseModel):
    name: str
    version: str


class OtlpScopeSpans(BaseModel):
    scope: OtlpScope
    spans: list[OtlpSpan]


class OtlpResourceSpans(BaseModel):
    resource: OtlpResource
    scopeSpans: list[OtlpScopeSpans]


class OtlpExport(BaseModel):
    resourceSpans: list[OtlpResourceSpans]


class TraceResponse(BaseModel):
    run_id: str
    trace_id: str
    spans: list[SpanRecord]
    otel_export: OtlpExport


class SecretResult(BaseModel):
    label: str
    redacted: bool
    rules: list[str]


class DecoyResult(BaseModel):
    label: str
    flagged: bool
    rules: list[str]


class RedactionSummary(BaseModel):
    secrets_tested: int
    secrets_redacted: int
    recall: float
    decoys_tested: int
    decoys_flagged: int
    benign_fpr: float


class RedactionSuite(RedactionSummary):
    secret_details: list[SecretResult]
    decoy_details: list[DecoyResult]


class RedactionResponse(BaseModel):
    redacted_payload: dict[str, Any]
    findings_count: int
    matched_rules: list[str]
    suite_summary: RedactionSuite | None


class BenchmarkPending(BaseModel):
    status: Literal["pending"]
    message: str


class ReplayMetrics(BaseModel):
    pass_rate: float
    wilson_95ci: tuple[float, float]
    p95_ms: float
    p99_ms: float


class TauReplayMetrics(ReplayMetrics):
    pass_power_3: float
    duplicate_side_effects: int


class ChaosMetrics(BaseModel):
    fault_resilience_rate: float
    wilson_95ci: tuple[float, float]


class DurableChaosMetrics(ChaosMetrics):
    oscillation_median_trip_ms: float


class BenchmarkTier(BaseModel):
    dataset: str
    license: str
    tasks_evaluated: int


class TauBenchReport(BenchmarkTier):
    retail_tasks: int
    airline_tasks: int
    tasks_available: int
    db_records_counted: int
    trials_per_task: int
    upstream_baseline: TauReplayMetrics
    durable_runtime: TauReplayMetrics


class BfclReport(BenchmarkTier):
    files_evaluated: list[str]
    skipped_tasks: int
    upstream_baseline: ReplayMetrics
    durable_runtime: ReplayMetrics


class AgentDojoReport(BenchmarkTier):
    suites_evaluated: list[str]
    upstream_baseline: ChaosMetrics
    durable_runtime: DurableChaosMetrics


class DosGuardsReport(BaseModel):
    fanout_1000_bulkhead_cap_16_passed: bool
    peak_concurrency: int
    circular_ref_blocked: bool
    snapshot_byte_cap_blocked: bool


class RedactionAndDosReport(BaseModel):
    redaction: RedactionSummary
    dos_guards: DosGuardsReport


class BenchmarkGate(BaseModel):
    gate_passed: bool
    baseline_pass_rate: float
    candidate_pass_rate: float
    delta: float
    duplicate_side_effect_rate: float
    leaked_secrets: int
    checks: dict[str, bool]


class BenchmarkReport(BaseModel):
    project: str
    upstream_repo: str
    upstream_commit: str
    hardware: str
    total_eval_sec: float
    tier1_tau_bench: TauBenchReport
    tier1_bfcl_v4: BfclReport
    tier2_agentdojo_chaos: AgentDojoReport
    tier3_redaction_and_dos: RedactionAndDosReport
    ci_regression_gate: BenchmarkGate
