from __future__ import annotations

import asyncio
import importlib.util
import sqlite3
import sys
import time
import uuid
import warnings
from contextlib import suppress
from typing import Any

from pocketflow_runtime.compat import (
    AsyncFlow,
    AsyncNode,
    AsyncParallelBatchFlow,
    AsyncParallelBatchNode,
    Node,
)
from pocketflow_runtime.config import get_project_root
from pocketflow_runtime.guards import (
    OscillationDetectedError,
    OscillationDetector,
    SnapshotGuard,
    SnapshotLimitError,
    TransitionMode,
    TransitionPolicy,
    UnknownTransitionError,
    canonical_hash,
    safe_json_clone,
)
from pocketflow_runtime.redaction import redact_payload, redact_text
from pocketflow_runtime.resilience import (
    BudgetConfig,
    BudgetExceededError,
    BudgetTracker,
    RetryPolicy,
)
from pocketflow_runtime.scenarios import SCENARIOS
from pocketflow_runtime.storage import RunClaim, RuntimeStore
from pocketflow_runtime.tracer import SpanTracer, compute_state_diff


class SimulatedCrashError(RuntimeError):
    """Deterministic crash injected to test checkpoint recovery and idempotency."""


class InvalidRunInput(ValueError):
    """Run inputs or a state patch violate the scenario contract."""


_RUNTIME_STATE_FIELDS = frozenset({
    "scenario_id", "fault_mode", "crash_step", "require_hitl", "hitl_approved", "hitl_paused",
    "max_concurrency", "timeout_sec", "transition_policy", "forked_from_step", "seed",
    "tool_calls", "side_effect_executions", "refund_ledger", "order_lookup", "policy_result",
    "refund_result", "audit_finalized", "batch_results", "attempts_by_item", "bulkhead_peak",
    "cycle_count", "resolved", "done", "oscillation_tripped", "branch_outputs",
    "elapsed_concurrent_ms", "heartbeat_ticks", "checked", "rebooked", "dead_lettered",
    "injected_api_token", "benign_order_id", "benign_uuid",
})


class _BatchProbe:
    def __init__(self, fault_mode: str, *, concurrent: bool) -> None:
        self.fault_mode = fault_mode
        self.concurrent = concurrent
        self.attempts: dict[str, int] = {}
        self.active = 0
        self.peak = 0
        self.last_attempt = asyncio.Event()
        self.sibling_failed = asyncio.Event()

    def items(self) -> list[str] | None:
        if self.fault_mode == "none_items_crash":
            return None
        if self.fault_mode == "fanout_burst":
            return [f"task-{i:03d}" for i in range(24)]
        return ["item_fail_always", "item_succeeds_after_delay"]

    async def execute(self, item: str) -> str:
        attempt = self.attempts.get(item, 0) + 1
        self.attempts[item] = attempt
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            if item == "item_fail_always":
                if self.concurrent and attempt == 3:
                    self.last_attempt.set()
                    await self.sibling_failed.wait()
                raise RuntimeError("Simulated tool failure")
            if item == "item_succeeds_after_delay" and attempt == 1:
                if self.concurrent:
                    await self.last_attempt.wait()
                    self.sibling_failed.set()
                raise RuntimeError("Simulated transient sibling failure")
            await asyncio.sleep(0)
            return f"OK:{item}"
        finally:
            self.active -= 1

    def expected(self) -> list[str]:
        if self.fault_mode == "concurrent_retry_race":
            return ["FALLBACK:item_fail_always:retry=2", "OK:item_succeeds_after_delay"]
        return [f"OK:{item}" for item in (self.items() or [])]


def load_upstream_pocketflow() -> Any:
    """Load the unmodified upstream_baseline pocketflow module dynamically."""
    module_name = "_upstream_baseline_pocketflow"
    if module_name in sys.modules:
        return sys.modules[module_name]
    upstream_path = (
        get_project_root() / "backend" / "upstream_baseline" / "pocketflow" / "__init__.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, str(upstream_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load upstream pocketflow from {upstream_path}")
    mod = importlib.util.module_from_spec(spec)
    exec(compile(upstream_path.read_bytes(), str(upstream_path), "exec"), mod.__dict__)
    sys.modules[module_name] = mod
    return mod


def _count_unredacted_secrets(obj: Any) -> int:
    return redact_payload(obj).findings_count


class DurableRuntimeEngine:
    """Executes workflows in upstream_baseline or durable_runtime mode."""

    def __init__(
        self,
        store: RuntimeStore,
        *,
        max_snapshot_bytes: int = 262_144,
        default_timeout_sec: float = 15.0,
    ) -> None:
        self.store = store
        self.snapshot_guard = SnapshotGuard(max_bytes=max_snapshot_bytes)
        self.default_timeout_sec = default_timeout_sec

    def _apply_patch(self, state: dict[str, Any], patch: dict[str, Any] | None) -> dict[str, Any]:
        if patch and _RUNTIME_STATE_FIELDS.intersection(patch):
            raise InvalidRunInput("State patch contains a runtime-managed field")
        if patch and "order_id" in patch:
            order = patch["order_id"]
            if (
                not isinstance(order, str) or not 1 <= len(order) <= 128
                or not order.isprintable() or redact_text(order, field_name="order_id")[1]
            ):
                raise InvalidRunInput("Order identifier is invalid")
        if patch and "amount_cents" in patch:
            amount = patch["amount_cents"]
            if type(amount) is not int or not 1 <= amount <= 100_000_000:
                raise InvalidRunInput("Refund amount must be a positive bounded integer")
        try:
            merged: dict[str, Any] = safe_json_clone(
                {**state, **(patch or {})}, max_bytes=self.snapshot_guard.max_bytes
            )
            self.snapshot_guard.serialize(redact_payload(merged).redacted)
        except SnapshotLimitError as exc:
            raise InvalidRunInput(str(exc)) from exc
        return merged

    @staticmethod
    def _claim(run: dict[str, Any]) -> RunClaim:
        return RunClaim(run["run_id"], run["status"], run["updated_at"])

    @staticmethod
    def _last_checkpoint(run: dict[str, Any]) -> tuple[int, str | None]:
        if not run["checkpoints"]:
            return 0, None
        last = run["checkpoints"][-1]
        return int(last["step_seq"]), str(last["node_id"])

    async def start_run(
        self,
        *,
        scenario_id: str,
        mode: str = "durable_runtime",
        fault_mode: str | None = None,
        crash_step: int | None = None,
        require_hitl: bool = False,
        max_steps: int = 32,
        max_concurrency: int = 4,
        timeout_sec: float | None = None,
        transition_policy: TransitionMode = "strict",
        input_overrides: dict[str, Any] | None = None,
        seed: int = 42,
    ) -> dict[str, Any]:
        if scenario_id not in SCENARIOS:
            raise KeyError(f"Unknown scenario '{scenario_id}'")
        spec = SCENARIOS[scenario_id]
        if require_hitl and scenario_id != "retail_refund_durable":
            raise InvalidRunInput("This workflow has no HITL approval gate")
        effective_fault = fault_mode or spec.default_fault
        if effective_fault not in spec.fault_modes:
            raise InvalidRunInput("Fault mode is invalid for this scenario")
        effective_crash = (crash_step or 3) if effective_fault == "crash_at_step" else None
        if effective_crash is not None and not 1 <= effective_crash <= 4:
            raise InvalidRunInput("Crash step must be between 1 and 4")

        run_id = f"run-{uuid.uuid4().hex[:12]}"
        initial_state = self._apply_patch({
            "scenario_id": scenario_id,
            "order_id": "ORD-91002348712",
            "amount_cents": 2499,
            "fault_mode": effective_fault,
            "crash_step": effective_crash,
            "require_hitl": require_hitl or (effective_fault == "hitl_pause"),
            "hitl_approved": False,
            "max_concurrency": max_concurrency,
            "timeout_sec": timeout_sec if timeout_sec is not None else self.default_timeout_sec,
            "seed": seed,
            "transition_policy": transition_policy,
            "tool_calls": [],
            "side_effect_executions": 0,
            "refund_ledger": [],
        }, input_overrides)

        self.store.create_run(
            run_id=run_id,
            scenario_id=scenario_id,
            mode=mode,
            status="RUNNING",
            verdict="INTERRUPTED",
            fault_mode=effective_fault,
            crash_step=effective_crash,
            require_hitl=bool(initial_state["require_hitl"]),
            max_steps=max_steps,
            initial_state=initial_state,
        )

        return await self._execute_durable(
            run_id=run_id,
            scenario_id=scenario_id,
            state=initial_state,
            resume_from_step=0,
            max_steps=max_steps,
            max_concurrency=max_concurrency,
            timeout_sec=float(initial_state["timeout_sec"]),
            transition_mode=transition_policy,
        )

    async def resume_run(self, run_id: str, *, clear_fault: bool = True) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"Run '{run_id}' not found")
        if run["status"] not in {"CRASHED", "FAILED"}:
            raise ValueError(f"Run '{run_id}' with status '{run['status']}' is not resumable")
        hitl = run.get("hitl_interrupt")
        if hitl and hitl["status"] != "APPROVED":
            raise ValueError("Run requires an approved HITL decision")

        state: dict[str, Any] = safe_json_clone(run["state"])
        if state.get("require_hitl"):
            state["hitl_approved"] = bool(hitl and hitl["status"] == "APPROVED")
        if clear_fault:
            state["fault_mode"] = "none"
            state["crash_step"] = None
        last_step, last_node = self._last_checkpoint(run)
        if not self.store.claim_run(self._claim(run), state):
            raise ValueError("Run has already been claimed")
        return await self._execute_durable(
            run_id=run_id,
            scenario_id=run["scenario_id"],
            state=state,
            resume_from_step=last_step,
            last_node=last_node,
            max_steps=int(run["max_steps"]),
            max_concurrency=int(state.get("max_concurrency", 4)),
            timeout_sec=float(state.get("timeout_sec", self.default_timeout_sec)),
            transition_mode=state.get("transition_policy", "strict"),
        )

    async def resolve_hitl(
        self,
        run_id: str,
        *,
        decision: str,
        reviewer_note: str | None = None,
        state_patch: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"Run '{run_id}' not found")
        hitl = run["hitl_interrupt"]
        if run["status"] != "PAUSED_HITL" or not hitl or hitl["status"] != "PENDING":
            raise ValueError(f"Run '{run_id}' has no pending HITL interrupt")
        if decision not in {"approve", "reject"}:
            raise InvalidRunInput("Invalid HITL decision")

        state = self._apply_patch(run["state"], state_patch)
        state["hitl_approved"] = decision == "approve"
        state["hitl_paused"] = False
        state["fault_mode"] = "none"
        state["crash_step"] = None
        last_step, last_node = self._last_checkpoint(run)
        if state.get("order_id") != run["state"].get("order_id"):
            state.pop("order_lookup", None)
            state.pop("policy_result", None)
            last_node = None
        with self.store.transaction():
            if not self.store.claim_run(self._claim(run), state):
                raise ValueError("Run has already been claimed")
            if not self.store.resolve_hitl_interrupt(run_id, decision, reviewer_note):
                raise ValueError("Run has no pending HITL interrupt")
            if decision == "reject":
                self.store.update_run(
                    run_id,
                    status="FAILED",
                    verdict="INTERRUPTED",
                    steps_executed=last_step,
                    state=state,
                    metrics=run["metrics"],
                    failure_reason=f"Rejected by HITL reviewer: {reviewer_note or 'no reason'}",
                )
        if decision == "reject":
            updated = self.store.get_run(run_id)
            assert updated is not None
            return updated

        return await self._execute_durable(
            run_id=run_id,
            scenario_id=run["scenario_id"],
            state=state,
            resume_from_step=last_step,
            last_node=last_node,
            max_steps=int(run["max_steps"]),
            max_concurrency=int(state.get("max_concurrency", 4)),
            timeout_sec=float(state.get("timeout_sec", self.default_timeout_sec)),
            transition_mode=state.get("transition_policy", "strict"),
        )

    async def fork_run(
        self,
        run_id: str,
        *,
        from_step_seq: int,
        state_patch: dict[str, Any] | None = None,
        clear_fault: bool = True,
    ) -> dict[str, Any]:
        parent = self.store.get_run(run_id)
        if parent is None:
            raise KeyError(f"Run '{run_id}' not found")
        checkpoint = next(
            (c for c in parent["checkpoints"] if c["step_seq"] == from_step_seq), None
        )
        if checkpoint is None:
            raise LookupError(f"Checkpoint step {from_step_seq} not found for run '{run_id}'")

        base_state = checkpoint["state"]
        forked_state = self._apply_patch(base_state, state_patch)
        last_node = checkpoint["node_id"]
        business_changed = any(
            forked_state.get(key) != base_state.get(key) for key in ("order_id", "amount_cents")
        )
        refund_committed = last_node in {"execute_refund", "finalize_audit"}
        if business_changed and refund_committed:
            raise InvalidRunInput("Select a checkpoint before the committed refund to edit it")
        if forked_state.get("order_id") != base_state.get("order_id"):
            last_node = None
            forked_state.pop("order_lookup", None)
            forked_state.pop("policy_result", None)
        if clear_fault:
            forked_state["fault_mode"] = "none"
            forked_state["crash_step"] = None
        forked_state["forked_from_step"] = from_step_seq
        forked_state["require_hitl"] = bool(
            parent["require_hitl"] or base_state.get("require_hitl")
        )
        forked_state["hitl_approved"] = refund_committed and bool(base_state.get("hitl_approved"))
        forked_state["hitl_paused"] = False
        forked_state["side_effect_executions"] = 0

        fork_id = f"run-{uuid.uuid4().hex[:12]}"
        with self.store.transaction():
            self.store.create_run(
                run_id=fork_id,
                scenario_id=parent["scenario_id"],
                mode="durable_runtime",
                status="RUNNING",
                verdict="INTERRUPTED",
                fault_mode=str(forked_state.get("fault_mode", "none")),
                crash_step=forked_state.get("crash_step"),
                require_hitl=forked_state["require_hitl"],
                max_steps=int(parent["max_steps"]),
                initial_state=forked_state,
                parent_run_id=run_id,
                forked_from_step=from_step_seq,
            )
            for prev in parent["checkpoints"]:
                if int(prev["step_seq"]) <= from_step_seq:
                    self.store.save_checkpoint(
                        run_id=fork_id,
                        step_seq=int(prev["step_seq"]),
                        node_id=str(prev["node_id"]),
                        action=prev["action"],
                        status=str(prev["status"]),
                        state=prev["state"],
                        state_diff=prev["state_diff"],
                    )

        forked_run = await self._execute_durable(
            run_id=fork_id,
            scenario_id=parent["scenario_id"],
            state=forked_state,
            resume_from_step=from_step_seq,
            last_node=last_node,
            max_steps=int(parent["max_steps"]),
            max_concurrency=int(forked_state.get("max_concurrency", 4)),
            timeout_sec=float(forked_state.get("timeout_sec", self.default_timeout_sec)),
            transition_mode=forked_state.get("transition_policy", "strict"),
        )
        diff = compute_state_diff(base_state, forked_run["state"])
        return {
            "parent_run_id": run_id,
            "forked_run": forked_run,
            "state_diff": diff,
        }

    async def _execute_durable(
        self,
        *,
        run_id: str,
        scenario_id: str,
        state: dict[str, Any],
        resume_from_step: int,
        max_steps: int,
        max_concurrency: int,
        timeout_sec: float,
        transition_mode: TransitionMode,
        last_node: str | None = None,
    ) -> dict[str, Any]:
        tracer = SpanTracer(run_id=run_id)
        root_span = tracer.start_span(
            name=f"flow.{scenario_id}",
            kind="SERVER",
            attributes={"scenario_id": scenario_id, "resume_from_step": resume_from_step},
        )
        t0 = time.perf_counter()
        step_counter = resume_from_step
        status = "COMPLETED"
        verdict = "PASSED"
        failure_reason: str | None = None
        last_state: dict[str, Any] = safe_json_clone(state)
        pending_pause: tuple[str, str | None, dict[str, Any]] | None = None
        cancelled: asyncio.CancelledError | None = None

        def _record_step(
            node_id: str,
            action: str | None,
            before: dict[str, Any],
            after: dict[str, Any],
            step_status: str = "OK",
        ) -> None:
            nonlocal step_counter, last_state, pending_pause
            if step_counter >= max_steps:
                raise BudgetExceededError("Step budget exceeded")
            self.snapshot_guard.serialize(after)
            safe_after = redact_payload(after).redacted
            self.snapshot_guard.serialize(safe_after)
            diff = compute_state_diff(redact_payload(before).redacted, safe_after)
            if step_status == "PAUSED_HITL":
                pending_pause = (node_id, action, diff)
                return
            self.store.save_checkpoint(
                run_id=run_id,
                step_seq=step_counter + 1,
                node_id=node_id,
                action=action,
                status=step_status,
                state=after,
                state_diff=diff,
                advance_run=True,
            )
            step_counter += 1
            last_state = safe_json_clone(after)

        sp_id = str(root_span["span_id"])
        try:
            remaining_steps = max_steps - resume_from_step
            async with asyncio.timeout(timeout_sec):
                if scenario_id == "retail_refund_durable":
                    await self._run_durable_retail(
                        run_id, state, last_node, remaining_steps, tracer, sp_id, _record_step
                    )
                    if pending_pause is not None:
                        status = "PAUSED_HITL"
                        verdict = "INTERRUPTED"
                elif remaining_steps <= 0:
                    raise BudgetExceededError("Step budget exceeded")
                elif scenario_id == "parallel_batch_retry_race":
                    await self._run_durable_parallel_batch(
                        state, max_concurrency, timeout_sec, tracer, sp_id, _record_step
                    )
                elif scenario_id == "cyclic_reflection_oscillation":
                    await self._run_durable_oscillation(
                        state, remaining_steps, tracer, sp_id, _record_step
                    )
                elif scenario_id == "branch_isolation_and_sync_offload":
                    await self._run_durable_branch_offload(
                        state, max_concurrency, tracer, sp_id, _record_step
                    )
                elif scenario_id == "airline_rebook_unknown_action":
                    await self._run_durable_unknown_action(
                        state, transition_mode, remaining_steps, tracer, sp_id, _record_step
                    )
                    if state.get("dead_lettered"):
                        status = "DEAD_LETTERED"
                        verdict = "PROTECTED"
                elif scenario_id == "secret_checkpoint_redaction":
                    await self._run_durable_redaction(state, tracer, sp_id, _record_step)
                else:
                    raise InvalidRunInput("Unknown workflow scenario")
            self.snapshot_guard.serialize(state)
            self.snapshot_guard.serialize(redact_payload(state).redacted)
            tracer.finish_span(root_span, status_code="OK")
        except asyncio.CancelledError as exc:
            cancelled = exc
            status, verdict = "CRASHED", "INTERRUPTED"
            failure_reason = "Workflow execution was cancelled"
            state = last_state
            tracer.finish_span(root_span, status_code="ERROR")
        except TimeoutError:
            status, verdict = "FAILED", "FAILED"
            failure_reason = "Workflow execution timed out"
            state = last_state
            tracer.finish_span(root_span, status_code="ERROR")
        except SimulatedCrashError as exc:
            status = "CRASHED"
            verdict = "INTERRUPTED"
            failure_reason = str(exc)
            tracer.finish_span(root_span, status_code="ERROR", extra_attributes={"error": str(exc)})
        except OscillationDetectedError as exc:
            status = "TRIPPED_OSCILLATION"
            verdict = "PROTECTED"
            failure_reason = str(exc)
            state["oscillation_tripped"] = True
            tracer.finish_span(
                root_span,
                status_code="ERROR",
                extra_attributes={"guard": "oscillation", "error": str(exc)},
            )
        except (BudgetExceededError, UnknownTransitionError) as exc:
            status = "DEAD_LETTERED" if isinstance(exc, UnknownTransitionError) else "FAILED"
            verdict = "PROTECTED" if isinstance(exc, UnknownTransitionError) else "FAILED"
            failure_reason = str(exc)
            if isinstance(exc, BudgetExceededError):
                state = last_state
            tracer.finish_span(root_span, status_code="ERROR", extra_attributes={"error": str(exc)})
        except Exception as exc:
            status = "FAILED"
            verdict = "FAILED"
            failure_reason = f"{type(exc).__name__}: {exc}"
            state = last_state
            tracer.finish_span(
                root_span, status_code="ERROR", extra_attributes={"error": failure_reason}
            )

        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        redacted_state = redact_payload(state)
        leaked = _count_unredacted_secrets(redacted_state.redacted)
        dups = max(0, int(state.get("side_effect_executions", 0)) - 1)
        if dups or leaked:
            status, verdict = "FAILED", "VULNERABLE"
            failure_reason = "Workflow duplicate-effect or privacy check failed"
        if status == "PAUSED_HITL":
            step_counter += 1
        metrics = {
            "elapsed_ms": elapsed_ms,
            "steps_executed": step_counter,
            "span_count": len(tracer.spans),
            "redacted_secrets_caught": redacted_state.findings_count,
        }
        try:
            with self.store.transaction():
                if status == "PAUSED_HITL":
                    assert pending_pause is not None
                    node_id, action, diff = pending_pause
                    self.store.save_checkpoint(
                        run_id=run_id, step_seq=step_counter, node_id=node_id, action=action,
                        status=status, state=state, state_diff=diff,
                    )
                    self.store.create_hitl_interrupt(
                        interrupt_id=f"hitl-{uuid.uuid4().hex[:8]}",
                        run_id=run_id,
                        step_seq=step_counter,
                        node_id=node_id,
                        reason=(
                            f"Approve refund of {state['amount_cents']} cents "
                            f"for {state['order_id']}"
                        ),
                    )
                self.store.save_spans(run_id, tracer.spans)
                self.store.update_run(
                    run_id,
                    status=status,
                    verdict=verdict,
                    steps_executed=step_counter,
                    state=state,
                    metrics=metrics,
                    failure_reason=failure_reason,
                    duplicate_side_effects=dups,
                    leaked_secrets=leaked,
                )
        except sqlite3.Error:
            with suppress(sqlite3.Error):
                self.store.update_run(
                    run_id, status="CRASHED", verdict="INTERRUPTED",
                    steps_executed=step_counter - int(status == "PAUSED_HITL"),
                    state=last_state, metrics={}, failure_reason="Final SQLite transaction failed",
                )
            raise
        if cancelled is not None:
            raise cancelled
        run_record = self.store.get_run(run_id)
        assert run_record is not None
        return run_record

    async def _run_durable_retail(
        self,
        run_id: str,
        state: dict[str, Any],
        last_node: str | None,
        remaining_steps: int,
        tracer: SpanTracer,
        parent_span_id: str,
        record_step: Any,
    ) -> None:
        store = self.store
        state["hitl_paused"] = False
        crash_step = state.get("crash_step")
        fault_mode = state.get("fault_mode", "none")

        class LookupOrder(AsyncNode):
            async def prep_async(self, shared: dict[str, Any]) -> str:
                return str(shared["order_id"])

            async def exec_async(self, order_id: str) -> dict[str, Any]:
                return {"order_id": order_id, "status": "delivered", "eligible": True}

            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["order_lookup"] = exec_res
                shared["tool_calls"].append("lookup_order")
                return "default"

        class ValidatePolicy(AsyncNode):
            async def exec_async(self, prep_res: Any) -> dict[str, Any]:
                return {"policy_ok": True, "requires_approval": True}

            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["policy_result"] = exec_res
                shared["tool_calls"].append("validate_policy")
                needs_gate = bool(shared.get("require_hitl")) and not shared.get("hitl_approved")
                return "needs_hitl" if needs_gate else "approved"

        class HitlGate(AsyncNode):
            async def post_async(
                self, shared: dict[str, Any], prep_res: Any, exec_res: Any
            ) -> str | None:
                shared["hitl_paused"] = True
                return "paused"

        class IdempotentRefund(AsyncNode):
            async def prep_async(self, shared: dict[str, Any]) -> dict[str, Any]:
                return {
                    "key": f"refund:{shared['order_id']}:{shared['amount_cents']}",
                    "order_id": shared["order_id"],
                    "amount_cents": shared["amount_cents"],
                }

            async def exec_async(self, prep_res: dict[str, Any]) -> dict[str, Any]:
                if fault_mode == "transient_503" and self.cur_retry == 0:
                    raise RuntimeError("Simulated transient tool failure")
                res = {
                    "refund_id": f"rf-{canonical_hash(prep_res['key'])[:16]}",
                    "amount_cents": prep_res["amount_cents"],
                    "executed_fresh": True,
                }
                result, fresh = store.record_idempotent_result(
                    run_id, prep_res["key"], "execute_refund", res
                )
                return {**result, "idempotent_replay": not fresh}

            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["refund_result"] = exec_res
                if exec_res["refund_id"] not in shared["refund_ledger"]:
                    shared["refund_ledger"].append(exec_res["refund_id"])
                shared["side_effect_executions"] = store.count_effects(run_id, "execute_refund")
                if not shared["tool_calls"] or shared["tool_calls"][-1] != "execute_refund":
                    shared["tool_calls"].append("execute_refund")
                if shared.get("fault_mode") == "crash_at_step" and shared.get("crash_step") == 3:
                    raise SimulatedCrashError("Simulated crash right after refund side effect")
                return "default"

        class FinalizeAudit(AsyncNode):
            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["audit_finalized"] = True
                shared["tool_calls"].append("finalize_audit")
                return "done"

        n_lookup = LookupOrder(node_id="lookup_order")
        n_policy = ValidatePolicy(node_id="validate_policy")
        n_hitl = HitlGate(node_id="hitl_gate")
        n_refund = IdempotentRefund(node_id="execute_refund", max_retries=2)
        n_audit = FinalizeAudit(node_id="finalize_audit")

        n_lookup >> n_policy
        n_policy - "needs_hitl" >> n_hitl
        n_policy - "approved" >> n_refund
        n_refund >> n_audit

        start_node: AsyncNode = n_lookup
        if last_node == "finalize_audit":
            return
        if last_node == "execute_refund":
            start_node = n_audit
        elif last_node == "lookup_order":
            start_node = n_policy
        elif last_node in {"validate_policy", "hitl_gate", "hitl_paused"} and state.get(
            "policy_result"
        ) is not None:
            approved = not state.get("require_hitl") or bool(state.get("hitl_approved"))
            start_node = n_refund if approved else n_hitl

        def _obs(ev: Any) -> None:
            sp = tracer.start_span(
                name=f"node.{ev.node_id}",
                parent_span_id=parent_span_id,
                attributes={"action": ev.action, "step_seq": ev.step_seq},
            )
            tracer.finish_span(sp, status_code="OK")
            paused = ev.node_id == "hitl_gate"
            record_step(
                ev.node_id, ev.action, ev.before_state, ev.after_state,
                "PAUSED_HITL" if paused else "OK",
            )
            crash_nodes = {1: "lookup_order", 2: "validate_policy", 4: "finalize_audit"}
            if (
                fault_mode == "crash_at_step" and isinstance(crash_step, int)
                and crash_nodes.get(crash_step) == ev.node_id
            ):
                raise SimulatedCrashError(f"Simulated crash after step {crash_step} checkpoint")

        flow = AsyncFlow(
            start=start_node, step_observer=_obs,
            budget_tracker=BudgetTracker(BudgetConfig(max_steps=remaining_steps)),
            snapshot_max_bytes=self.snapshot_guard.max_bytes,
        )
        await flow.run_async(state)

    async def _run_durable_parallel_batch(
        self,
        state: dict[str, Any],
        max_concurrency: int,
        timeout_sec: float,
        tracer: SpanTracer,
        parent_span_id: str,
        record_step: Any,
    ) -> None:
        probe = _BatchProbe(
            state.get("fault_mode", "concurrent_retry_race"), concurrent=max_concurrency >= 2,
        )

        class SafeBatchNode(AsyncParallelBatchNode):
            async def prep_async(self, shared: dict[str, Any]) -> list[str] | None:
                return probe.items()

            async def exec_async(self, item: str) -> str:
                return await probe.execute(item)

            async def exec_fallback_async(self, item: str, exc: Exception) -> str:
                return f"FALLBACK:{item}:retry={self.cur_retry}"

            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["batch_results"] = exec_res
                shared["attempts_by_item"] = dict(probe.attempts)
                shared["bulkhead_peak"] = self.last_bulkhead.peak_count if self.last_bulkhead else 0
                if exec_res != probe.expected() or shared["bulkhead_peak"] > max_concurrency:
                    raise RuntimeError("Batch result failed its completion or concurrency check")
                return "done"

        node = SafeBatchNode(
            max_retries=3,
            wait=0.001,
            node_id="parallel_exec",
            retry_policy=RetryPolicy(
                max_retries=3, base_delay_sec=0.001, seed=state.get("seed", 42),
            ),
            timeout_sec=timeout_sec,
            max_concurrency=max_concurrency,
        )

        def _obs(ev: Any) -> None:
            sp = tracer.start_span(name=f"node.{ev.node_id}", parent_span_id=parent_span_id)
            tracer.finish_span(sp, status_code="OK")
            record_step(ev.node_id, ev.action, ev.before_state, ev.after_state)

        flow = AsyncFlow(start=node, step_observer=_obs)
        await flow.run_async(state)

    async def _run_durable_oscillation(
        self,
        state: dict[str, Any],
        max_steps: int,
        tracer: SpanTracer,
        parent_span_id: str,
        record_step: Any,
    ) -> None:
        fault_mode = state.get("fault_mode", "ping_pong_loop")

        class Planner(AsyncNode):
            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["cycle_count"] = int(shared.get("cycle_count", 0)) + 1
                return "call_tool"

        class ToolExec(AsyncNode):
            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                if fault_mode == "ping_pong_loop":
                    return "tool_error"
                shared["resolved"] = True
                return "ok"

        class Reflect(AsyncNode):
            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                if fault_mode == "ping_pong_loop":
                    shared["cycle_count"] = 1
                    return "retry_plan"
                return "retry_plan"

        class Done(AsyncNode):
            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["done"] = True
                return "finished"

        p = Planner(node_id="planner")
        t = ToolExec(node_id="tool_exec")
        r = Reflect(node_id="reflect")
        d = Done(node_id="done")
        p - "call_tool" >> t
        t - "tool_error" >> r
        t - "ok" >> d
        r - "retry_plan" >> p

        def _obs(ev: Any) -> None:
            sp = tracer.start_span(name=f"node.{ev.node_id}", parent_span_id=parent_span_id)
            tracer.finish_span(sp, status_code="OK")
            record_step(ev.node_id, ev.action, ev.before_state, ev.after_state)

        flow = AsyncFlow(
            start=p,
            oscillation_detector=OscillationDetector(window_size=12, repeat_threshold=2),
            budget_tracker=BudgetTracker(BudgetConfig(max_steps=max_steps, max_wall_clock_sec=5.0)),
            step_observer=_obs,
        )
        await flow.run_async(state)

    async def _run_durable_branch_offload(
        self,
        state: dict[str, Any],
        max_concurrency: int,
        tracer: SpanTracer,
        parent_span_id: str,
        record_step: Any,
    ) -> None:
        class SyncMutator(Node):
            def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
                if state["fault_mode"] != "none":
                    time.sleep(0.03)
                cfg = self.params["cfg"]
                cfg["tags"].append(self.params["branch"])
                return {"branch": self.params["branch"], "tags": list(cfg["tags"])}

            def post(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared.setdefault("branch_outputs", {})[prep_res["branch"]] = prep_res["tags"]
                return "done"

        class BranchFlow(AsyncParallelBatchFlow):
            async def prep_async(self, shared: dict[str, Any]) -> list[dict[str, Any]]:
                shared_nested = {"tags": ["base"]}
                return [
                    {"branch": "b1", "cfg": shared_nested},
                    {"branch": "b2", "cfg": shared_nested},
                ]

        before = safe_json_clone(state)
        sp = tracer.start_span(name="flow.branch_offload", parent_span_id=parent_span_id)
        bf = BranchFlow(
            start=SyncMutator(node_id="sync_worker"),
            max_concurrency=max_concurrency,
            isolate_branch_params=True,
            offload_sync_nodes=True,
        )
        heartbeat_ticks = 0

        async def _heartbeat() -> None:
            nonlocal heartbeat_ticks
            for _ in range(3):
                await asyncio.sleep(0.005)
                heartbeat_ticks += 1

        t0 = time.perf_counter()
        await asyncio.gather(bf.run_async(state), _heartbeat())
        state["elapsed_concurrent_ms"] = round((time.perf_counter() - t0) * 1000.0, 2)
        state["heartbeat_ticks"] = heartbeat_ticks
        tracer.finish_span(sp, status_code="OK")
        if state.get("branch_outputs") != {"b1": ["base", "b1"], "b2": ["base", "b2"]}:
            raise RuntimeError("Branch did not produce isolated outputs")
        record_step("branch_prep", "done", before, state)

    async def _run_durable_unknown_action(
        self,
        state: dict[str, Any],
        transition_mode: TransitionMode,
        remaining_steps: int,
        tracer: SpanTracer,
        parent_span_id: str,
        record_step: Any,
    ) -> None:
        fault_mode = state.get("fault_mode", "unknown_action")

        class CheckFlight(AsyncNode):
            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["checked"] = True
                if fault_mode == "unknown_action":
                    return "hallucinated_upgrade_cabin"
                return "available"

        class RebookSeat(AsyncNode):
            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["rebooked"] = True
                return "done"

        class DeadLetter(AsyncNode):
            async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
                shared["dead_lettered"] = True
                return "archived"

        c = CheckFlight(node_id="check_flight")
        r = RebookSeat(node_id="rebook_seat")
        dl = DeadLetter(node_id="dead_letter")
        c - "available" >> r
        c - "fallback" >> dl

        def _obs(ev: Any) -> None:
            sp = tracer.start_span(name=f"node.{ev.node_id}", parent_span_id=parent_span_id)
            tracer.finish_span(sp, status_code="OK")
            record_step(ev.node_id, ev.action, ev.before_state, ev.after_state)

        flow = AsyncFlow(
            start=c,
            transition_policy=TransitionPolicy(mode=transition_mode, fallback_action="fallback"),
            budget_tracker=BudgetTracker(BudgetConfig(max_steps=remaining_steps)),
            step_observer=_obs,
        )
        try:
            await flow.run_async(state)
        except UnknownTransitionError:
            state["dead_lettered"] = True
            raise

    async def _run_durable_redaction(
        self,
        state: dict[str, Any],
        tracer: SpanTracer,
        parent_span_id: str,
        record_step: Any,
    ) -> None:
        before = safe_json_clone(state)
        synth_secret = "sk" + "-proj-" + "DemoSecret9988776655443322"
        if state["fault_mode"] != "none":
            state["injected_api_token"] = synth_secret
        state["benign_order_id"] = "91002348712"
        state["benign_uuid"] = "550e8400-e29b-41d4-a716-446655440000"
        sp = tracer.start_span(
            name="node.persist_redact",
            parent_span_id=parent_span_id,
            attributes={
                "token_sample": state.get("injected_api_token"), "order_id": "91002348712",
            },
        )
        tracer.finish_span(sp, status_code="OK")
        record_step("persist_redact", "default", before, state)

    async def compare_baseline_vs_durable(
        self,
        *,
        scenario_id: str,
        fault_mode: str | None = None,
        crash_step: int | None = None,
        seed: int = 42,
    ) -> dict[str, Any]:
        """Run both verbatim upstream_baseline and durable_runtime side by side."""
        if scenario_id not in SCENARIOS:
            raise KeyError(f"Unknown scenario '{scenario_id}'")
        spec = SCENARIOS[scenario_id]
        eff_fault = fault_mode or spec.default_fault
        if eff_fault not in spec.fault_modes:
            raise InvalidRunInput("Fault mode is invalid for this scenario")
        eff_crash = (crash_step or 3) if eff_fault == "crash_at_step" else None
        if eff_crash is not None and not 1 <= eff_crash <= 4:
            raise InvalidRunInput("Crash step must be between 1 and 4")

        baseline = await self._run_upstream_baseline_scenario(
            scenario_id=scenario_id,
            fault_mode=eff_fault,
            crash_step=eff_crash,
        )

        durable_run = await self.start_run(
            scenario_id=scenario_id,
            mode="durable_runtime",
            fault_mode=eff_fault,
            crash_step=eff_crash,
            require_hitl=(eff_fault == "hitl_pause"),
            max_steps=32,
            seed=seed,
        )
        if durable_run["status"] == "CRASHED":
            durable_run = await self.resume_run(durable_run["run_id"], clear_fault=True)

        durable_summary = {
            "mode": "durable_runtime",
            "run_id": durable_run["run_id"],
            "status": durable_run["status"],
            "verdict": durable_run["verdict"],
            "failure_reason": durable_run["failure_reason"],
            "output": durable_run["state"],
            "duplicate_side_effects": durable_run["duplicate_side_effects"],
            "elapsed_ms": durable_run["metrics"].get("elapsed_ms", 0.0),
            "steps_executed": durable_run["steps_executed"],
            "leaked_secrets": durable_run["leaked_secrets"],
        }

        baseline_bad = baseline["verdict"] in {"FAILED", "VULNERABLE"}
        durable_ok = durable_summary["verdict"] in {"PASSED", "PROTECTED"}
        base_dups = int(baseline["duplicate_side_effects"])
        dur_dups = int(durable_summary["duplicate_side_effects"])
        dup_prevented = max(0, base_dups - dur_dups)
        return {
            "scenario_id": scenario_id,
            "fault_mode": eff_fault,
            "seed": seed,
            "baseline": baseline,
            "durable": durable_summary,
            "delta_summary": {
                "baseline_failed_or_vulnerable": baseline_bad,
                "durable_passed": durable_ok,
                "duplicate_side_effects_prevented": dup_prevented,
                "silent_failures_prevented": 1 if baseline_bad and durable_ok else 0,
            },
        }

    async def _run_upstream_baseline_scenario(
        self,
        *,
        scenario_id: str,
        fault_mode: str,
        crash_step: int | None,
    ) -> dict[str, Any]:
        pf = load_upstream_pocketflow()
        t0 = time.perf_counter()

        if scenario_id == "parallel_batch_retry_race":
            return await self._baseline_parallel_batch(pf, fault_mode, t0)
        if scenario_id == "retail_refund_durable":
            return await self._baseline_retail(pf, fault_mode, crash_step, t0)
        if scenario_id == "cyclic_reflection_oscillation":
            return await self._baseline_oscillation(pf, fault_mode, t0)
        if scenario_id == "branch_isolation_and_sync_offload":
            return await self._baseline_branch_offload(pf, fault_mode, t0)
        if scenario_id == "airline_rebook_unknown_action":
            return await self._baseline_unknown_action(pf, fault_mode, t0)
        return await self._baseline_redaction(pf, fault_mode, t0)

    async def _baseline_parallel_batch(
        self, pf: Any, fault_mode: str, t0: float
    ) -> dict[str, Any]:
        if fault_mode == "none_items_crash":
            async def _prep_none(self: Any, shared: dict[str, Any]) -> None:
                return None

            async def _exec_id(self: Any, item: Any) -> Any:
                return item

            none_cls = type(
                "UpNoneBatch",
                (pf.AsyncParallelBatchNode,),
                {"prep_async": _prep_none, "exec_async": _exec_id},
            )
            try:
                await pf.AsyncFlow(start=none_cls()).run_async({})
                return {
                    "mode": "upstream_baseline", "status": "COMPLETED", "verdict": "PASSED",
                    "failure_reason": None, "output": {}, "duplicate_side_effects": 0,
                    "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 3),
                    "steps_executed": 1, "leaked_secrets": 0,
                }
            except TypeError as exc:
                return {
                    "mode": "upstream_baseline", "status": "FAILED", "verdict": "VULNERABLE",
                    "failure_reason": f"Upstream crashed on items=None: {exc}",
                    "output": {}, "duplicate_side_effects": 0,
                    "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 3),
                    "steps_executed": 1, "leaked_secrets": 0,
                }

        probe = _BatchProbe(fault_mode, concurrent=True)

        async def _prep(self: Any, shared: dict[str, Any]) -> list[str] | None:
            return probe.items()

        async def _execute(self: Any, item: str) -> str:
            return await probe.execute(item)

        async def _fallback(self: Any, item: str, exc: Exception) -> str:
            return f"FALLBACK:{item}:retry={self.cur_retry}"

        async def _post(self: Any, shared: dict[str, Any], prep_res: Any, result: Any) -> str:
            shared["batch_results"] = result
            return "done"

        batch = type("UpBatchProbe", (pf.AsyncParallelBatchNode,), {
            "prep_async": _prep, "exec_async": _execute, "exec_fallback_async": _fallback,
            "post_async": _post,
        })
        shared: dict[str, Any] = {}
        await pf.AsyncFlow(start=batch(max_retries=3)).run_async(shared)
        results = shared.get("batch_results")
        failure = None
        if results != probe.expected():
            failure = "Shared retry counter produced an incomplete result or premature fallback"
        elif fault_mode == "fanout_burst" and probe.peak > 4:
            failure = "Fan-out exceeded the comparison concurrency limit of 4"
        return {
            "mode": "upstream_baseline",
            "status": "FAILED" if failure else "COMPLETED",
            "verdict": "VULNERABLE" if failure else "PASSED",
            "failure_reason": failure,
            "output": {
                "batch_results": results, "attempts_by_item": probe.attempts,
                "bulkhead_peak": probe.peak,
            },
            "duplicate_side_effects": 0,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "steps_executed": 1,
            "leaked_secrets": 0,
        }

    async def _baseline_retail(
        self, pf: Any, fault_mode: str, crash_step: int | None, t0: float,
    ) -> dict[str, Any]:
        shared_retail: dict[str, Any] = {"refund_executions": 0, "refund_ledger": []}
        steps = 0
        interrupted_once = False

        def _step(number: int) -> None:
            nonlocal steps, interrupted_once
            steps += 1
            if fault_mode == "crash_at_step" and crash_step == number and not interrupted_once:
                interrupted_once = True
                raise SimulatedCrashError("Simulated interruption at requested retail step")

        async def _p1(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            s["looked_up"] = True
            _step(1)
            return "default"

        async def _p2(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            s["policy_validated"] = True
            _step(2)
            return "default"

        async def _exec_refund(self: Any, prep_res: Any) -> None:
            if fault_mode == "transient_503":
                raise RuntimeError("Simulated transient tool failure")

        async def _p3(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            s["refund_executions"] += 1
            s["refund_ledger"].append("rf-8000")
            _step(3)
            return "default"

        async def _p4(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            s["audit_finalized"] = True
            _step(4)
            return "done"

        a = type("UpLookup", (pf.AsyncNode,), {"post_async": _p1})()
        b = type("UpPolicy", (pf.AsyncNode,), {"post_async": _p2})()
        c = type("UpRefund", (pf.AsyncNode,), {"exec_async": _exec_refund, "post_async": _p3})()
        d = type("UpFinalize", (pf.AsyncNode,), {"post_async": _p4})()
        a >> b >> c >> d
        flow = pf.AsyncFlow(start=a)
        failure = None
        try:
            await flow.run_async(shared_retail)
        except SimulatedCrashError:
            await flow.run_async(shared_retail)
        except RuntimeError:
            failure = "Transient refund failure exhausted the upstream attempt"

        dups = max(0, int(shared_retail["refund_executions"]) - 1)
        if dups:
            failure = "Upstream restart repeated the committed refund"
        elif fault_mode == "hitl_pause" and shared_retail["refund_executions"] > 0:
            failure = "Upstream executed the refund without a persisted approval gate"
        elif not shared_retail.get("audit_finalized") and failure is None:
            failure = "Upstream failed to finish the refund flow"
        return {
            "mode": "upstream_baseline",
            "status": "FAILED" if failure else "COMPLETED",
            "verdict": "VULNERABLE" if failure else "PASSED",
            "failure_reason": failure,
            "output": shared_retail,
            "duplicate_side_effects": dups,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "steps_executed": steps,
            "leaked_secrets": 0,
        }

    async def _baseline_oscillation(self, pf: Any, fault_mode: str, t0: float) -> dict[str, Any]:
        counter = {"steps": 0}

        async def _p_plan(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            counter["steps"] += 1
            await asyncio.sleep(0.002)
            return "call"

        async def _p_err(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            counter["steps"] += 1
            return "retry" if fault_mode == "ping_pong_loop" else "ok"

        up_p = type("UpPlan", (pf.AsyncNode,), {"post_async": _p_plan})()
        up_e = type("UpErr", (pf.AsyncNode,), {"post_async": _p_err})()
        up_done = pf.AsyncNode()
        up_p - "call" >> up_e
        up_e - "retry" >> up_p
        up_e - "ok" >> up_done
        try:
            await asyncio.wait_for(pf.AsyncFlow(start=up_p).run_async({}), timeout=0.06)
            return {
                "mode": "upstream_baseline", "status": "COMPLETED", "verdict": "PASSED",
                "failure_reason": None, "output": counter, "duplicate_side_effects": 0,
                "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 3),
                "steps_executed": counter["steps"], "leaked_secrets": 0,
            }
        except TimeoutError:
            return {
                "mode": "upstream_baseline", "status": "FAILED", "verdict": "VULNERABLE",
                "failure_reason": (
                    f"Bare while curr: hung indefinitely (killed at 60ms, "
                    f"{counter['steps']} steps)."
                ),
                "output": counter, "duplicate_side_effects": 0,
                "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 3),
                "steps_executed": counter["steps"], "leaked_secrets": 0,
            }

    async def _baseline_branch_offload(
        self, pf: Any, fault_mode: str, t0: float,
    ) -> dict[str, Any]:
        def _sync_prep(self: Any, s: dict[str, Any]) -> list[str]:
            if fault_mode != "none":
                time.sleep(0.03)
            self.params["cfg"]["tags"].append(self.params["branch"])
            return list(self.params["cfg"]["tags"])

        def _sync_post(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            s.setdefault("branch_outputs", {})[self.params["branch"]] = p
            return "done"

        async def _bf_prep(self: Any, s: dict[str, Any]) -> list[dict[str, Any]]:
            nested = {"tags": ["base"]}
            other = {"tags": ["base"]} if fault_mode == "none" else nested
            return [{"branch": "b1", "cfg": nested}, {"branch": "b2", "cfg": other}]

        up_sync = type("UpSync", (pf.Node,), {"prep": _sync_prep, "post": _sync_post})()
        up_bf = type("UpBF", (pf.AsyncParallelBatchFlow,), {"prep_async": _bf_prep})(start=up_sync)

        s_branch: dict[str, Any] = {}
        ticks = 0

        async def _hb() -> None:
            nonlocal ticks
            for _ in range(3):
                await asyncio.sleep(0.005)
                ticks += 1

        await asyncio.gather(up_bf.run_async(s_branch), _hb())
        outputs = s_branch.get("branch_outputs", {})
        aliased = outputs != {"b1": ["base", "b1"], "b2": ["base", "b2"]}
        return {
            "mode": "upstream_baseline",
            "status": "FAILED" if aliased else "COMPLETED",
            "verdict": "VULNERABLE" if aliased else "PASSED",
            "failure_reason": (
                f"Shallow-copy params aliased nested list ({outputs}) & sync Node stalled loop."
                if aliased
                else None
            ),
            "output": {"branch_outputs": outputs, "heartbeat_ticks": ticks},
            "duplicate_side_effects": 0,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "steps_executed": 2,
            "leaked_secrets": 0,
        }

    async def _baseline_unknown_action(
        self, pf: Any, fault_mode: str, t0: float,
    ) -> dict[str, Any]:
        async def _p_flight(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            s["checked"] = True
            return "hallucinated_upgrade_cabin" if fault_mode == "unknown_action" else "available"

        async def _p_rebook(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            s["rebooked"] = True
            return "done"

        uf = type("UpFlight", (pf.AsyncNode,), {"post_async": _p_flight})()
        ur = type("UpRebook", (pf.AsyncNode,), {"post_async": _p_rebook})()
        uf - "available" >> ur
        s_air: dict[str, Any] = {}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await pf.AsyncFlow(start=uf).run_async(s_air)
        silent_drop = not s_air.get("rebooked")
        warn_msg = str(caught[0].message) if caught else ""
        return {
            "mode": "upstream_baseline",
            "status": "FAILED" if silent_drop else "COMPLETED",
            "verdict": "VULNERABLE" if silent_drop else "PASSED",
            "failure_reason": (
                f"Flow.get_next_node silently dropped unknown action: {warn_msg}"
                if silent_drop
                else None
            ),
            "output": s_air,
            "duplicate_side_effects": 0,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "steps_executed": 1 if silent_drop else 2,
            "leaked_secrets": 0,
        }

    async def _baseline_redaction(self, pf: Any, fault_mode: str, t0: float) -> dict[str, Any]:
        synth_secret = "sk" + "-proj-" + "DemoSecret9988776655443322"

        async def _p_sec(self: Any, s: dict[str, Any], p: Any, e: Any) -> str:
            if fault_mode != "none":
                s["injected_api_token"] = synth_secret
            return "done"

        node = type("UpSecretNode", (pf.AsyncNode,), {"post_async": _p_sec})()
        s_sec: dict[str, Any] = {}
        await pf.AsyncFlow(start=node).run_async(s_sec)
        leaked = int(s_sec.get("injected_api_token") == synth_secret)
        return {
            "mode": "upstream_baseline",
            "status": "FAILED" if leaked > 0 else "COMPLETED",
            "verdict": "VULNERABLE" if leaked > 0 else "PASSED",
            "failure_reason": (
                f"Upstream persisted {leaked} unredacted secret(s) in workflow state."
                if leaked > 0
                else None
            ),
            "output": {"leaked_count": leaked, "has_raw_secret": leaked > 0},
            "duplicate_side_effects": 0,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            "steps_executed": 1,
            "leaked_secrets": leaked,
        }
