from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    id: str
    title: str
    description: str
    fault_modes: tuple[str, ...]
    default_fault: str
    graph: dict[str, Any]


SCENARIOS: dict[str, ScenarioSpec] = {
    "retail_refund_durable": ScenarioSpec(
        id="retail_refund_durable",
        title="Retail Order Refund (Crash Recovery + HITL + Idempotency)",
        description=(
            "Multi-step retail refund workflow: LookupOrder -> ValidatePolicy -> "
            "HITLGate -> IdempotentRefund. Reproduces duplicate refund corruption in "
            "upstream vs zero-duplicate SQLite WAL checkpoint recovery."
        ),
        fault_modes=("crash_at_step", "hitl_pause", "transient_503", "none"),
        default_fault="crash_at_step",
        graph={
            "nodes": [
                {"id": "lookup_order", "label": "LookupOrder", "kind": "async_node"},
                {"id": "validate_policy", "label": "ValidatePolicy", "kind": "async_node"},
                {"id": "hitl_gate", "label": "HITL_Gate", "kind": "hitl_node"},
                {"id": "execute_refund", "label": "IdempotentRefund", "kind": "side_effect_node"},
                {"id": "finalize_audit", "label": "FinalizeAudit", "kind": "async_node"},
            ],
            "edges": [
                {"source": "lookup_order", "target": "validate_policy", "action": "default"},
                {"source": "validate_policy", "target": "hitl_gate", "action": "approved_or_gate"},
                {"source": "hitl_gate", "target": "execute_refund", "action": "approved"},
                {"source": "execute_refund", "target": "finalize_audit", "action": "default"},
            ],
        },
    ),
    "parallel_batch_retry_race": ScenarioSpec(
        id="parallel_batch_retry_race",
        title="AsyncParallelBatchNode cur_retry Concurrency Race",
        description=(
            "Reproduces upstream PocketFlow AsyncNode._exec shared self.cur_retry race "
            "causing silent None return (skipping both fallback & raise) or premature fallback, "
            "versus ContextVar-isolated retries + Semaphore bulkhead."
        ),
        fault_modes=("concurrent_retry_race", "none_items_crash", "fanout_burst"),
        default_fault="concurrent_retry_race",
        graph={
            "nodes": [
                {"id": "prepare_batch", "label": "PrepareBatchItems", "kind": "async_node"},
                {
                    "id": "parallel_exec",
                    "label": "ParallelToolBatch",
                    "kind": "parallel_batch_node",
                },
                {"id": "aggregate_batch", "label": "AggregateResults", "kind": "async_node"},
            ],
            "edges": [
                {"source": "prepare_batch", "target": "parallel_exec", "action": "default"},
                {"source": "parallel_exec", "target": "aggregate_batch", "action": "default"},
            ],
        },
    ),
    "cyclic_reflection_oscillation": ScenarioSpec(
        id="cyclic_reflection_oscillation",
        title="Cyclic Reflection Loop & Oscillation Circuit Guard",
        description=(
            "Demonstrates Plan <-> ToolError ping-pong cycle hanging upstream while curr: loop "
            "until watchdog kill, versus <5ms canonical-state OscillationDetector circuit trip."
        ),
        fault_modes=("ping_pong_loop", "none"),
        default_fault="ping_pong_loop",
        graph={
            "nodes": [
                {"id": "planner", "label": "PlannerNode", "kind": "async_node"},
                {"id": "tool_exec", "label": "ToolExecNode", "kind": "async_node"},
                {"id": "reflect", "label": "ReflectNode", "kind": "async_node"},
                {"id": "done", "label": "CompleteNode", "kind": "async_node"},
            ],
            "edges": [
                {"source": "planner", "target": "tool_exec", "action": "call_tool"},
                {"source": "tool_exec", "target": "reflect", "action": "tool_error"},
                {"source": "reflect", "target": "planner", "action": "retry_plan"},
                {"source": "tool_exec", "target": "done", "action": "ok"},
            ],
        },
    ),
    "branch_isolation_and_sync_offload": ScenarioSpec(
        id="branch_isolation_and_sync_offload",
        title="Branch Params Isolation & Sync-Node Loop Offloading",
        description=(
            "Reproduces AsyncParallelBatchFlow shallow-copy nested params aliasing and sync Node "
            "event-loop blocking, versus cycle-safe deep clone + asyncio.to_thread offloading."
        ),
        fault_modes=("shallow_alias_and_sync_stall", "none"),
        default_fault="shallow_alias_and_sync_stall",
        graph={
            "nodes": [
                {"id": "branch_prep", "label": "BranchPrepFlow", "kind": "parallel_batch_flow"},
                {"id": "sync_worker", "label": "BlockingSyncNode", "kind": "sync_node"},
                {"id": "async_sibling", "label": "ConcurrentHeartbeat", "kind": "async_node"},
            ],
            "edges": [
                {"source": "branch_prep", "target": "sync_worker", "action": "default"},
                {"source": "sync_worker", "target": "async_sibling", "action": "default"},
            ],
        },
    ),
    "airline_rebook_unknown_action": ScenarioSpec(
        id="airline_rebook_unknown_action",
        title="Hallucinated Action Routing & Dead-Letter Guard",
        description=(
            "Reproduces Flow.get_next_node silently terminating on unknown action strings with "
            "warnings.warn, versus strict/dead-letter transition enforcement + OTel span capture."
        ),
        fault_modes=("unknown_action", "none"),
        default_fault="unknown_action",
        graph={
            "nodes": [
                {"id": "check_flight", "label": "CheckFlight", "kind": "async_node"},
                {"id": "rebook_seat", "label": "RebookSeat", "kind": "async_node"},
                {"id": "dead_letter", "label": "DeadLetterHandler", "kind": "guard_node"},
            ],
            "edges": [
                {"source": "check_flight", "target": "rebook_seat", "action": "available"},
                {"source": "check_flight", "target": "dead_letter", "action": "dead_letter"},
            ],
        },
    ),
    "secret_checkpoint_redaction": ScenarioSpec(
        id="secret_checkpoint_redaction",
        title="Checkpoint & OTel Span Secret/PII Redaction",
        description=(
            "Processes synthetic API keys, JWTs, PII, and benign decoys "
            "(order IDs, UUID4, SHA256); proves zero secret persistence in "
            "SQLite WAL checkpoints and OTel spans with 0% decoy FPR."
        ),
        fault_modes=("inject_secrets_and_decoys", "none"),
        default_fault="inject_secrets_and_decoys",
        graph={
            "nodes": [
                {"id": "ingest_ctx", "label": "IngestContext", "kind": "async_node"},
                {"id": "persist_redact", "label": "RedactingCheckpoint", "kind": "guard_node"},
                {"id": "export_trace", "label": "ExportOTelSpan", "kind": "async_node"},
            ],
            "edges": [
                {"source": "ingest_ctx", "target": "persist_redact", "action": "default"},
                {"source": "persist_redact", "target": "export_trace", "action": "default"},
            ],
        },
    ),
}


def list_scenarios() -> list[dict[str, Any]]:
    return [
        {
            "id": spec.id,
            "title": spec.title,
            "description": spec.description,
            "fault_modes": list(spec.fault_modes),
            "default_fault": spec.default_fault,
            "graph": spec.graph,
        }
        for spec in SCENARIOS.values()
    ]
