from __future__ import annotations

import hashlib
import json
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Literal


class UnknownTransitionError(RuntimeError):
    """Raised under strict transition policy when an edge action has no target."""


class OscillationDetectedError(RuntimeError):
    """Raised when a cyclic graph repeats the same (node, state) signature."""


class SnapshotLimitError(ValueError):
    """Raised when checkpoint state violates circular-ref or byte-size DoS caps."""


TransitionMode = Literal["strict", "fallback", "dead_letter"]


def safe_json_clone(value: Any, *, max_depth: int = 32, max_bytes: int | None = None) -> Any:
    """Clone JSON-like values with cycle, depth, and optional serialized size limits."""
    seen: set[int] = set()
    remaining = max_bytes

    def _consume(size: int) -> None:
        nonlocal remaining
        if remaining is not None:
            remaining -= size
            if remaining < 0:
                raise SnapshotLimitError(f"Snapshot byte limit exceeded ({max_bytes})")

    def _scalar(obj: Any) -> Any:
        if isinstance(obj, float) and not math.isfinite(obj):
            raise SnapshotLimitError("Non-finite number in workflow state")
        if isinstance(obj, str) and remaining is not None and len(obj) > remaining:
            _consume(len(obj))
        if remaining is not None:
            try:
                encoded = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            except ValueError as exc:
                raise SnapshotLimitError("Invalid JSON scalar in workflow state") from exc
            _consume(len(encoded))
        return obj

    def _clone(obj: Any, depth: int) -> Any:
        if depth > max_depth:
            raise SnapshotLimitError(f"State nesting depth exceeded {max_depth}")
        if obj is None or isinstance(obj, (bool, int, float, str)):
            return _scalar(obj)
        obj_id = id(obj)
        if isinstance(obj, (dict, list, tuple, set)):
            _consume(2 + max(0, len(obj) - 1))
        if isinstance(obj, dict):
            if obj_id in seen:
                raise SnapshotLimitError("Circular dict reference detected in workflow state")
            seen.add(obj_id)
            try:
                _consume(len(obj))  # Colons between object keys and values.
                return {_scalar(str(k)): _clone(v, depth + 1) for k, v in obj.items()}
            finally:
                seen.remove(obj_id)
        if isinstance(obj, (list, tuple)):
            if obj_id in seen:
                raise SnapshotLimitError("Circular sequence reference detected in workflow state")
            seen.add(obj_id)
            try:
                cloned = [_clone(item, depth + 1) for item in obj]
                return cloned if isinstance(obj, list) else tuple(cloned)
            finally:
                seen.remove(obj_id)
        if isinstance(obj, set):
            return sorted((_clone(item, depth + 1) for item in obj), key=str)
        return _scalar(repr(obj))

    return _clone(value, 0)


def canonical_json_dumps(value: Any, *, max_depth: int = 32) -> str:
    """Deterministic canonical JSON serialization with cycle protection."""
    cloned = safe_json_clone(value, max_depth=max_depth)
    return json.dumps(
        cloned, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


def canonical_hash(value: Any) -> str:
    """Compute SHA-256 hash over canonical JSON representation."""
    payload = canonical_json_dumps(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(slots=True)
class SnapshotGuard:
    """Enforces circular-reference safety and byte-size ceiling on state snapshots."""

    max_bytes: int = 262_144

    def serialize(self, state: dict[str, Any]) -> str:
        bounded = safe_json_clone(state, max_bytes=self.max_bytes)
        return canonical_json_dumps(bounded)


@dataclass(slots=True)
class OscillationDetector:
    """Sliding-window detector for immediate loops (A->A) and ping-pong cycles (A<->B)."""

    window_size: int = 16
    repeat_threshold: int = 2
    _history: deque[tuple[str, str]] = field(init=False)
    _counts: dict[tuple[str, str], int] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if not 1 <= self.repeat_threshold <= self.window_size:
            raise ValueError("Repeat threshold must be between 1 and the window size")
        self._history = deque(maxlen=self.window_size)

    def observe(self, node_id: str, state: dict[str, Any]) -> str:
        state_sig = canonical_hash(state)
        key = (node_id, state_sig)
        if len(self._history) == self._history.maxlen:
            evicted = self._history.popleft()
            old_count = self._counts.get(evicted, 0)
            if old_count <= 1:
                self._counts.pop(evicted, None)
            else:
                self._counts[evicted] = old_count - 1
        self._history.append(key)
        new_count = self._counts.get(key, 0) + 1
        self._counts[key] = new_count
        if new_count >= self.repeat_threshold:
            raise OscillationDetectedError(
                f"Oscillation at '{node_id}' (sig={state_sig[:16]}, repeats={new_count})"
            )
        return state_sig


@dataclass(frozen=True, slots=True)
class TransitionPolicy:
    """Configures how missing action transitions are handled."""

    mode: TransitionMode = "strict"
    fallback_action: str = "fallback"
