from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


def wilson_ci(successes: int, total: int, z: float = 1.96) -> tuple[float, float]:
    """Compute Wilson 95% binomial confidence interval [low, high]."""
    if total <= 0:
        return (0.0, 0.0)
    p = successes / total
    z2 = z * z
    denom = 1.0 + z2 / total
    center = (p + z2 / (2.0 * total)) / denom
    half = (z * math.sqrt((p * (1.0 - p) + z2 / (4.0 * total)) / total)) / denom
    return (round(max(0.0, center - half), 4), round(min(1.0, center + half), 4))


def compute_pass_at_k(trial_outcomes: list[bool]) -> float:
    """Probability at least 1 of k trials succeeds (1.0 if any passed)."""
    if not trial_outcomes:
        return 0.0
    return 1.0 if any(trial_outcomes) else 0.0


def compute_pass_power_k(trial_outcomes: list[bool]) -> float:
    """Strict pass^k reliability: probability ALL k trials succeed consistently."""
    if not trial_outcomes:
        return 0.0
    return 1.0 if all(trial_outcomes) else 0.0


def lcs_f1(predicted: list[str], gold: list[str]) -> float:
    """Compute sequence F1 based on Longest Common Subsequence length."""
    if not predicted and not gold:
        return 1.0
    if not predicted or not gold:
        return 0.0
    m, n = len(predicted), len(gold)
    if n > m:
        predicted, gold = gold, predicted
        m, n = n, m
    previous = [0] * (n + 1)
    for tool in predicted:
        current = [0] * (n + 1)
        for j in range(1, n + 1):
            if tool == gold[j - 1]:
                current[j] = previous[j - 1] + 1
            else:
                current[j] = max(previous[j], current[j - 1])
        previous = current
    lcs = previous[n]
    if lcs == 0:
        return 0.0
    precision = lcs / m
    recall = lcs / n
    return round(2.0 * precision * recall / (precision + recall), 4)


@dataclass(frozen=True, slots=True)
class TrajectoryGrade:
    task_id: str
    exact_match: bool
    sequence_f1: float
    db_equivalent: bool
    duplicate_side_effects: int
    recovered_from_fault: bool
    passed: bool


def grade_trajectory(
    *,
    task_id: str,
    predicted_tools: list[str],
    gold_tools: list[str],
    predicted_db_state: dict[str, Any],
    gold_db_state: dict[str, Any],
    duplicate_side_effects: int = 0,
    recovered_from_fault: bool = True,
) -> TrajectoryGrade:
    """Deterministically grade a multi-step tool trajectory & final DB equivalence."""
    exact = predicted_tools == gold_tools
    f1 = 1.0 if exact else lcs_f1(predicted_tools, gold_tools)
    db_eq = predicted_db_state == gold_db_state
    passed = exact and db_eq and duplicate_side_effects == 0 and recovered_from_fault
    return TrajectoryGrade(
        task_id=task_id,
        exact_match=exact,
        sequence_f1=f1,
        db_equivalent=db_eq,
        duplicate_side_effects=duplicate_side_effects,
        recovered_from_fault=recovered_from_fault,
        passed=passed,
    )


def evaluate_regression_gate(
    *,
    baseline_pass_rate: float,
    candidate_pass_rate: float,
    candidate_dup_rate: float,
    candidate_leak_count: int,
) -> dict[str, Any]:
    """Enforce CI regression gate: candidate must never regress and must have 0 leaks/dups."""
    passed = (
        candidate_pass_rate >= baseline_pass_rate
        and candidate_dup_rate == 0.0
        and candidate_leak_count == 0
    )
    return {
        "gate_passed": passed,
        "baseline_pass_rate": round(baseline_pass_rate, 4),
        "candidate_pass_rate": round(candidate_pass_rate, 4),
        "delta": round(candidate_pass_rate - baseline_pass_rate, 4),
        "duplicate_side_effect_rate": round(candidate_dup_rate, 4),
        "leaked_secrets": candidate_leak_count,
    }
