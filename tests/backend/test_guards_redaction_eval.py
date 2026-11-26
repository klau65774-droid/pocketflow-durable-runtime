from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest
from pocketflow_runtime.config import get_project_root
from pocketflow_runtime.engine import load_upstream_pocketflow
from pocketflow_runtime.evaluator import (
    compute_pass_at_k,
    compute_pass_power_k,
    evaluate_regression_gate,
    grade_trajectory,
    wilson_ci,
)
from pocketflow_runtime.guards import (
    OscillationDetectedError,
    OscillationDetector,
    SnapshotGuard,
    SnapshotLimitError,
    safe_json_clone,
)
from pocketflow_runtime.redaction import redact_payload, redact_text, run_builtin_redaction_suite


def test_oscillation_detector_catches_ping_pong_in_sub_millisecond() -> None:
    det = OscillationDetector(window_size=8, repeat_threshold=2)
    det.observe("planner", {"step": 1, "q": "a"})
    det.observe("tool_exec", {"step": 1, "q": "a"})
    with pytest.raises(OscillationDetectedError):
        det.observe("planner", {"step": 1, "q": "a"})


def test_snapshot_guard_blocks_circular_ref_and_oversize_dos() -> None:
    guard = SnapshotGuard(max_bytes=512)
    cyc: dict[str, Any] = {}
    cyc["self"] = cyc
    with pytest.raises(SnapshotLimitError):
        safe_json_clone(cyc)
    with pytest.raises(SnapshotLimitError):
        guard.serialize({"blob": "x" * 1024})


def test_builtin_redaction_suite_achieves_100pct_recall_and_0pct_benign_fpr() -> None:
    summary = run_builtin_redaction_suite()
    assert summary["secrets_tested"] == 15
    assert summary["secrets_redacted"] == 15
    assert summary["recall"] == 1.0
    assert summary["decoys_tested"] == 18
    assert summary["decoys_flagged"] == 0, f"False positives: {summary['decoy_details']}"
    assert summary["benign_fpr"] == 0.0


def test_trajectory_grader_and_pass_power_k() -> None:
    grade = grade_trajectory(
        task_id="t-1",
        predicted_tools=["lookup_order", "refund_order"],
        gold_tools=["lookup_order", "refund_order"],
        predicted_db_state={"order_1": "refunded"},
        gold_db_state={"order_1": "refunded"},
        duplicate_side_effects=0,
        recovered_from_fault=True,
    )
    assert grade.passed is True
    assert grade.sequence_f1 == 1.0
    assert compute_pass_at_k([False, True, False]) == 1.0
    assert compute_pass_power_k([False, True, True]) == 0.0
    assert compute_pass_power_k([True, True, True]) == 1.0
    low, high = wilson_ci(10, 10)
    assert 0.7 <= low <= high <= 1.0
    gate = evaluate_regression_gate(
        baseline_pass_rate=0.4,
        candidate_pass_rate=1.0,
        candidate_dup_rate=0.0,
        candidate_leak_count=0,
    )
    assert gate["gate_passed"] is True


def test_snapshot_counts_utf8_bytes_and_stops_expanding_repeated_aliases() -> None:
    state: dict[str, Any] = {"message": "路" * 5, "list": [True, None, "\n"]}
    expected = json.dumps(state, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    size = len(expected.encode("utf-8"))
    assert SnapshotGuard(max_bytes=size).serialize(state) == expected
    with pytest.raises(SnapshotLimitError, match="byte limit"):
        SnapshotGuard(max_bytes=size - 1).serialize(state)
    alias: Any = "leaf"
    for _ in range(25):
        alias = [alias, alias]
    with pytest.raises(SnapshotLimitError, match="byte limit"):
        safe_json_clone(alias, max_bytes=128)
    with pytest.raises(SnapshotLimitError, match="Non-finite"):
        safe_json_clone({"value": float("nan")}, max_bytes=1024)


def test_oscillation_window_evicts_exactly_and_uses_full_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detector = OscillationDetector(window_size=2, repeat_threshold=2)
    detector.observe("one", {"a": 1})
    detector.observe("two", {"a": 1})
    detector.observe("three", {"a": 1})
    detector.observe("one", {"a": 1})
    with pytest.raises(OscillationDetectedError):
        detector.observe("one", {"a": 1})

    def same_prefix(state: dict[str, int]) -> str:
        return "a" * 16 + str(state["value"]).zfill(48)

    monkeypatch.setattr("pocketflow_runtime.guards.canonical_hash", same_prefix)
    detector = OscillationDetector(window_size=2, repeat_threshold=2)
    detector.observe("node", {"value": 1})
    detector.observe("node", {"value": 2})
    with pytest.raises(ValueError, match="window size"):
        OscillationDetector(window_size=1, repeat_threshold=2)


def test_redaction_covers_sensitive_fields_keys_and_varied_token_lengths() -> None:
    token = "-".join(("sk", "proj", "z" * 180))
    other_token = "-".join(("sk", "proj", "y" * 170))
    private_block = "\n".join((
        "-----BEGIN " + "ENCRYPTED PRIVATE " + "KEY-----",
        "a" * 5000,
        "-----END " + "ENCRYPTED PRIVATE " + "KEY-----",
    ))
    synthetic_card = "42" * 8
    mask = "[REDACTED:openai_or_anthropic_key]"
    order_id = "".join(("138", "00138000"))
    trace_id = "550e8400-e29b-41d4-a716-446655440000"
    digest = hashlib.sha256(b"safe audit input").hexdigest()
    source = {
        token: "one",
        other_token: "two",
        mask: "existing",
        "headers": {"Authorization": "Bearer " + "qa", "X-Api-Key": "short"},
        "context": [private_block, f"Payment {synthetic_card}.", f"{token} and {other_token}"],
        "order_id": order_id,
        "trace_id": trace_id,
        "digest": digest,
        "prose": "The bearer of the voucher can view the example.",
    }
    result = redact_payload(source)
    result_text = json.dumps(result.redacted)
    assert token not in result_text and other_token not in result_text
    assert private_block not in result_text and synthetic_card not in result_text
    assert result.redacted[mask] == "existing"
    assert {"one", "two"} <= {
        value for value in result.redacted.values() if isinstance(value, str)
    }
    assert all(result.redacted[field] == source[field] for field in (
        "order_id", "trace_id", "digest", "prose",
    ))
    assert result.redacted["headers"] == {
        "Authorization": "[REDACTED:sensitive_field]",
        "X-Api-Key": "[REDACTED:sensitive_field]",
    }
    assert redact_payload(result.redacted).redacted == result.redacted


def test_redaction_preserves_large_benign_document() -> None:
    text = "Sample local documentation with short lines and no credentials.\n" * 3000
    assert redact_text(text) == (text, [])


def test_near_match_cannot_pass_trajectory_gate() -> None:
    tools = ["lookup"] * 150
    grade = grade_trajectory(
        task_id="near-match",
        predicted_tools=tools[:-1],
        gold_tools=tools,
        predicted_db_state={},
        gold_db_state={},
    )
    assert grade.sequence_f1 > 0.99
    assert not grade.passed


async def test_benchmark_cases_require_actual_results_and_all_gate_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.syspath_prepend(str(get_project_root() / "benchmarks"))
    from run_sealed_benchmarks import (
        _benchmark_gate,
        _eval_bfcl_multiturn_task,
        _eval_bfcl_parallel_task,
        _run_tier3_redaction_and_dos,
    )

    upstream = load_upstream_pocketflow()
    assert (await _eval_bfcl_parallel_task(upstream, [], True))[2] is False
    baseline, _, durable, _ = await _eval_bfcl_parallel_task(
        upstream, ["lookup", "lookup"], True,
    )
    assert not baseline and durable
    baseline, _, durable, _ = await _eval_bfcl_multiturn_task(
        upstream, [{"synthetic": 1}], "missing_parameter",
    )
    assert not baseline and durable

    tau = {
        "tasks_evaluated": 1, "trials_per_task": 3,
        "upstream_baseline": {"pass_rate": 0.0},
        "durable_runtime": {"pass_rate": 1.0, "pass_power_3": 1.0, "duplicate_side_effects": 0},
    }
    bfcl = {"tasks_evaluated": 1, "durable_runtime": {"pass_rate": 1.0}}
    chaos = {"tasks_evaluated": 1, "durable_runtime": {"fault_resilience_rate": 1.0}}
    synthetic = await _run_tier3_redaction_and_dos()
    assert _benchmark_gate(tau, bfcl, chaos, synthetic)["gate_passed"]
    synthetic["dos_guards"]["circular_ref_blocked"] = False
    synthetic["redaction"]["decoys_flagged"] = 1
    bad_bfcl = {**bfcl, "durable_runtime": {"pass_rate": 0.0}}
    bad_chaos = {**chaos, "durable_runtime": {"fault_resilience_rate": 0.0}}
    failing = _benchmark_gate(tau, bad_bfcl, bad_chaos, synthetic)
    assert failing["gate_passed"] is False
    assert failing["checks"] == {
        "tau_replay": True, "bfcl_replay": False, "agentdojo_chaos": False,
        "redaction": False, "resource_guards": False,
    }


def test_readme_tables_match_exported_benchmark_metrics() -> None:
    root = get_project_root()
    report = json.loads((root / "docs" / "test_metrics.json").read_text(encoding="utf-8"))
    for filename in ("README.md", "README.zh.md"):
        readme = (root / filename).read_text(encoding="utf-8")
        rows = {row.split(" · ")[0]: row for row in readme.splitlines()
                if row.startswith("| **Tier ")}
        tau_row = rows["| **Tier 1A"]
        tau = report["tier1_tau_bench"]
        assert f"{tau['tasks_evaluated']} × {tau['trials_per_task']}" in tau_row
        for tier, row in ((tau, tau_row), (report["tier1_bfcl_v4"], rows["| **Tier 1B"])):
            for side in ("upstream_baseline", "durable_runtime"):
                values = tier[side]
                assert f"P95 `{values['p95_ms']} ms`" in row
                assert f"P99 `{values['p99_ms']} ms`" in row
                assert f"`{values['pass_rate'] * 100:.2f}%`".replace(
                    "100.00%", "100.0%"
                ) in row
        redaction = report["tier3_redaction_and_dos"]["redaction"]
        assert f"`{redaction['secrets_redacted']}/{redaction['secrets_tested']}`" in readme
        assert f"`{redaction['decoys_flagged']}/{redaction['decoys_tested']}`" in readme
        assert f"`{report['total_eval_sec']} s`" in readme
