from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import threading
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from pocketflow_runtime.engine import DurableRuntimeEngine, InvalidRunInput
from pocketflow_runtime.storage import CheckpointIntegrityError, RunClaim, RuntimeStore


async def test_crash_resume_guarantees_zero_duplicate_side_effects(tmp_path: Path) -> None:
    db_path = tmp_path / "durable.sqlite3"
    store = RuntimeStore(db_path)
    try:
        engine = DurableRuntimeEngine(store)
        crashed = await engine.start_run(
            scenario_id="retail_refund_durable",
            fault_mode="crash_at_step",
            crash_step=3,
        )
        assert crashed["status"] == "CRASHED"
        assert crashed["verdict"] == "INTERRUPTED"
        assert crashed["state"]["side_effect_executions"] == 1
        assert len(crashed["ledger_entries"]) == 1

        resumed = await engine.resume_run(crashed["run_id"], clear_fault=True)
        assert resumed["status"] == "COMPLETED"
        assert resumed["verdict"] == "PASSED"
        assert resumed["duplicate_side_effects"] == 0
        assert resumed["state"]["side_effect_executions"] == 1
        assert len(resumed["state"]["refund_ledger"]) == 1
        assert resumed["state"]["audit_finalized"] is True
    finally:
        store.close()


async def test_hitl_suspend_persists_across_store_reopen_and_time_travel_fork(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "hitl.sqlite3"
    store1 = RuntimeStore(db_path)
    engine1 = DurableRuntimeEngine(store1)
    paused = await engine1.start_run(
        scenario_id="retail_refund_durable",
        fault_mode="hitl_pause",
        require_hitl=True,
    )
    assert paused["status"] == "PAUSED_HITL"
    assert paused["hitl_interrupt"] is not None
    assert paused["hitl_interrupt"]["status"] == "PENDING"
    run_id = paused["run_id"]
    store1.close()

    store2 = RuntimeStore(db_path)
    try:
        engine2 = DurableRuntimeEngine(store2)
        completed = await engine2.resolve_hitl(
            run_id,
            decision="approve",
            reviewer_note="Approved after restart",
        )
        assert completed["status"] == "COMPLETED"
        assert completed["verdict"] == "PASSED"
        assert completed["hitl_interrupt"]["status"] == "APPROVED"

        fork_res = await engine2.fork_run(
            run_id,
            from_step_seq=2,
            state_patch={"amount_cents": 999},
            clear_fault=True,
        )
        forked = fork_res["forked_run"]
        assert forked["parent_run_id"] == run_id
        assert forked["forked_from_step"] == 2
        assert forked["status"] == "PAUSED_HITL"
        assert forked["ledger_entries"] == []
        assert forked["state"]["amount_cents"] == 999
        assert "changed" in fork_res["state_diff"]
        approved_fork = await engine2.resolve_hitl(forked["run_id"], decision="approve")
        assert approved_fork["status"] == "COMPLETED"
        assert approved_fork["state"]["refund_result"]["amount_cents"] == 999
    finally:
        store2.close()


async def test_compare_baseline_vs_durable_no_false_greens(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "compare.sqlite3")
    try:
        engine = DurableRuntimeEngine(store)
        for scenario_id in (
            "retail_refund_durable",
            "parallel_batch_retry_race",
            "cyclic_reflection_oscillation",
            "branch_isolation_and_sync_offload",
            "airline_rebook_unknown_action",
            "secret_checkpoint_redaction",
        ):
            res = await engine.compare_baseline_vs_durable(scenario_id=scenario_id)
            assert res["baseline"]["verdict"] in {"FAILED", "VULNERABLE"}, scenario_id
            assert res["durable"]["verdict"] in {"PASSED", "PROTECTED"}, scenario_id
            assert res["delta_summary"]["baseline_failed_or_vulnerable"] is True
            assert res["delta_summary"]["durable_passed"] is True
    finally:
        store.close()


@pytest.mark.parametrize("crash_step", [1, 2, 3, 4])
async def test_reopen_resumes_from_exact_checkpoint(crash_step: int, tmp_path: Path) -> None:
    db_path = tmp_path / "reopen.sqlite3"
    with closing(RuntimeStore(db_path)) as store:
        crashed = await DurableRuntimeEngine(store).start_run(
            scenario_id="retail_refund_durable", fault_mode="crash_at_step", crash_step=crash_step,
        )
        assert crashed["verdict"] == "INTERRUPTED"
        assert crashed["steps_executed"] == (2 if crash_step == 3 else crash_step)
        first_checksums = [item["checksum"] for item in crashed["checkpoints"]]

    with closing(RuntimeStore(db_path)) as store:
        resumed = await DurableRuntimeEngine(store).resume_run(crashed["run_id"])
        assert resumed["state"]["tool_calls"] == [
            "lookup_order", "validate_policy", "execute_refund", "finalize_audit",
        ]
        assert resumed["status"] == "COMPLETED"
        assert resumed["steps_executed"] == 4
        assert len(resumed["ledger_entries"]) == 1
        assert [c["checksum"] for c in resumed["checkpoints"]][:len(first_checksums)] == (
            first_checksums
        )


async def test_approval_state_is_protected_and_fork_replays_same_effect(tmp_path: Path) -> None:
    with closing(RuntimeStore(tmp_path / "approvals.sqlite3")) as store:
        engine = DurableRuntimeEngine(store)
        with pytest.raises(InvalidRunInput, match="runtime-managed"):
            await engine.start_run(
                scenario_id="retail_refund_durable", require_hitl=True,
                input_overrides={"hitl_approved": True},
            )
        assert store.list_runs() == []

        paused = await engine.start_run(
            scenario_id="retail_refund_durable", fault_mode="hitl_pause",
        )
        run_id = paused["run_id"]
        with pytest.raises(InvalidRunInput, match="runtime-managed"):
            await engine.resolve_hitl(
                run_id, decision="approve", state_patch={"require_hitl": False},
            )
        assert engine.store.get_run(run_id) == paused
        assert paused["checkpoints"][-1]["step_seq"] == paused["hitl_interrupt"]["step_seq"]

        source_fork = await engine.fork_run(run_id, from_step_seq=2)
        child = source_fork["forked_run"]
        assert child["status"] == "PAUSED_HITL" and child["ledger_entries"] == []
        approved = await engine.resolve_hitl(run_id, decision="approve")
        approved_child = await engine.resolve_hitl(child["run_id"], decision="approve")
        assert approved_child["state"]["refund_result"]["idempotent_replay"] is True
        assert approved_child["state"]["side_effect_executions"] == 0
        assert approved_child["ledger_entries"] == []
        assert len(approved["ledger_entries"]) == 1

        terminal_fork = await engine.fork_run(run_id, from_step_seq=approved["steps_executed"])
        assert terminal_fork["forked_run"]["steps_executed"] == approved["steps_executed"]
        assert terminal_fork["forked_run"]["ledger_entries"] == []


async def test_rejected_run_cannot_resume_and_notes_are_redacted(tmp_path: Path) -> None:
    marker = "-".join(("sk", "proj", "A" * 24))
    path = tmp_path / "review.sqlite3"
    with closing(RuntimeStore(path)) as store:
        engine = DurableRuntimeEngine(store)
        run = await engine.start_run(scenario_id="retail_refund_durable", fault_mode="hitl_pause")
        rejected = await engine.resolve_hitl(run["run_id"], decision="reject", reviewer_note=marker)
        assert rejected["hitl_interrupt"]["status"] == "REJECTED"
        assert marker not in rejected["hitl_interrupt"]["reviewer_note"]
        assert marker not in rejected["failure_reason"]
        with pytest.raises(ValueError, match="approved HITL"):
            await engine.resume_run(run["run_id"])
        assert rejected["ledger_entries"] == []
    assert marker.encode() not in path.read_bytes()


async def test_hitl_decision_and_pause_rollback_together(tmp_path: Path) -> None:
    path = tmp_path / "atomic.sqlite3"
    with closing(RuntimeStore(path)) as store, closing(sqlite3.connect(path)) as db:
        engine = DurableRuntimeEngine(store)
        db.executescript("""
            CREATE TRIGGER fail_pause BEFORE UPDATE OF status ON workflow_runs
            WHEN NEW.status = 'PAUSED_HITL' BEGIN
              SELECT RAISE(ABORT, 'test pause write failure');
            END;
        """)
        with pytest.raises(sqlite3.IntegrityError, match="test pause"):
            await engine.start_run(scenario_id="retail_refund_durable", fault_mode="hitl_pause")
        affected = store.get_run(store.list_runs()[0]["run_id"])
        assert affected is not None
        assert affected["status"] == "CRASHED"
        assert affected["hitl_interrupt"] is None
        assert [c["node_id"] for c in affected["checkpoints"]] == [
            "lookup_order", "validate_policy",
        ]
        db.executescript("DROP TRIGGER fail_pause")
        recovered = await engine.resume_run(affected["run_id"])
        assert recovered["status"] == "PAUSED_HITL"

        paused = await engine.start_run(
            scenario_id="retail_refund_durable", fault_mode="hitl_pause",
        )
        db.executescript("""
            CREATE TRIGGER fail_reject BEFORE UPDATE OF status ON workflow_runs
            WHEN OLD.status = 'RUNNING' AND NEW.status = 'FAILED' BEGIN
              SELECT RAISE(ABORT, 'test reject write failure');
            END;
        """)
        with pytest.raises(sqlite3.IntegrityError, match="test reject"):
            await engine.resolve_hitl(paused["run_id"], decision="reject")
        assert store.get_run(paused["run_id"]) == paused


async def test_checkpoint_integrity_guards_resume_and_legacy_records(tmp_path: Path) -> None:
    path = tmp_path / "integrity.sqlite3"
    with closing(RuntimeStore(path)) as store, closing(sqlite3.connect(path)) as db:
        engine = DurableRuntimeEngine(store)
        crashed = await engine.start_run(scenario_id="retail_refund_durable", crash_step=2)
        run_id = crashed["run_id"]
        checkpoint = store.get_checkpoint(run_id, 1)
        assert checkpoint is not None
        legacy = f"{run_id}:1:{checkpoint.node_id}:{checkpoint.state_json}".encode()
        with db:
            db.execute(
                "UPDATE checkpoints SET checksum_version=1, checksum=? "
                "WHERE run_id=? AND step_seq=1",
                (hashlib.sha256(legacy).hexdigest(), run_id),
            )
        assert store.get_checkpoint(run_id, 1) is not None
        with pytest.raises(sqlite3.IntegrityError):
            store.save_checkpoint(
                run_id=run_id, step_seq=1, node_id="duplicate", action=None, status="OK",
                state={}, state_diff={},
            )
        cursor = db.execute("SELECT state_json FROM workflow_runs WHERE run_id=?", (run_id,))
        previous_state = cursor.fetchone()[0]
        with db:
            db.execute("UPDATE workflow_runs SET state_json='{}' WHERE run_id=?", (run_id,))
        with pytest.raises(CheckpointIntegrityError, match="run state"):
            await engine.resume_run(run_id)
        with db:
            db.execute("UPDATE workflow_runs SET state_json=? WHERE run_id=?",
                       (previous_state, run_id))
            db.execute("UPDATE checkpoints SET action='unexpected' WHERE step_seq=2")
        with pytest.raises(CheckpointIntegrityError):
            await engine.resume_run(run_id)
        with pytest.raises(CheckpointIntegrityError):
            await engine.fork_run(run_id, from_step_seq=1)
        assert store.list_runs()[0]["status"] == "CRASHED"


async def test_ledger_and_run_claim_have_one_winner_across_connections(tmp_path: Path) -> None:
    path = tmp_path / "race.sqlite3"
    with closing(RuntimeStore(path)) as first, closing(RuntimeStore(path)) as second:
        crashed = await DurableRuntimeEngine(first).start_run(
            scenario_id="retail_refund_durable", crash_step=1,
        )
        run_id = crashed["run_id"]
        claim = RunClaim(run_id, crashed["status"], crashed["updated_at"])
        claims = await asyncio.gather(*(
            asyncio.to_thread(store.claim_run, claim, crashed["state"])
            for store in (first, second)
        ))
        assert sorted(claims) == [False, True]

        barrier = threading.Barrier(2)

        def write(store: RuntimeStore, number: int) -> tuple[Any, bool]:
            barrier.wait(timeout=2)
            return store.record_idempotent_result(run_id, "synthetic-effect", "demo", {"n": number})

        results = await asyncio.gather(
            asyncio.to_thread(write, first, 1), asyncio.to_thread(write, second, 2),
        )
        assert results[0][0] == results[1][0]
        assert sorted(fresh for _, fresh in results) == [False, True]
        assert first.count_effects(run_id, "demo") == 1


async def test_run_step_and_snapshot_caps_apply_before_mutation(tmp_path: Path) -> None:
    path = tmp_path / "bounded.sqlite3"
    with closing(RuntimeStore(path)) as store:
        engine = DurableRuntimeEngine(store, max_snapshot_bytes=1024)
        with pytest.raises(InvalidRunInput, match="byte limit"):
            await engine.start_run(
                scenario_id="retail_refund_durable", input_overrides={"context": "x" * 1200},
            )
        assert store.list_runs() == []
        failed_step = await engine.start_run(
            scenario_id="retail_refund_durable", fault_mode="none", max_steps=2,
        )
        assert failed_step["status"] == "FAILED"
        assert failed_step["steps_executed"] == 2
        assert failed_step["ledger_entries"] == []
        expanded = await engine.start_run(
            scenario_id="retail_refund_durable", fault_mode="none",
            input_overrides={"context": "x" * 680},
        )
        assert expanded["status"] == "FAILED"
        assert expanded["steps_executed"] == len(expanded["checkpoints"])
        with closing(sqlite3.connect(path)) as db:
            stored_bytes = db.execute(
                "SELECT length(CAST(state_json AS BLOB)) FROM workflow_runs WHERE run_id=?",
                (expanded["run_id"],),
            ).fetchone()[0]
        assert stored_bytes <= 1024


async def test_compare_honors_fault_selection_and_keeps_review_pending(tmp_path: Path) -> None:
    with closing(RuntimeStore(tmp_path / "compare_forks.sqlite3")) as store:
        engine = DurableRuntimeEngine(store)
        early = await engine.compare_baseline_vs_durable(
            scenario_id="retail_refund_durable", fault_mode="crash_at_step", crash_step=1,
        )
        assert early["baseline"]["duplicate_side_effects"] == 0
        assert early["baseline"]["verdict"] == "PASSED"
        review = await engine.compare_baseline_vs_durable(
            scenario_id="retail_refund_durable", fault_mode="hitl_pause",
        )
        assert review["durable"]["status"] == "PAUSED_HITL"
        assert review["durable"]["verdict"] == "INTERRUPTED"
        assert review["delta_summary"]["durable_passed"] is False
        for scenario in ("airline_rebook_unknown_action", "secret_checkpoint_redaction"):
            clean = await engine.compare_baseline_vs_durable(
                scenario_id=scenario, fault_mode="none",
            )
            assert clean["baseline"]["verdict"] == clean["durable"]["verdict"] == "PASSED"
