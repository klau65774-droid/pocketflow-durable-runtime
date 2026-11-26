from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pocketflow_runtime.guards import SnapshotGuard, canonical_json_dumps, safe_json_clone
from pocketflow_runtime.redaction import RedactionResult, redact_payload, redact_text


class CheckpointIntegrityError(RuntimeError):
    """A stored run state or checkpoint failed its integrity check."""


@dataclass(frozen=True, slots=True)
class RunClaim:
    run_id: str
    status: str
    updated_at: float


@dataclass(frozen=True, slots=True)
class CheckpointRecord:
    run_id: str
    step_seq: int
    node_id: str
    action: str | None
    status: str
    state_json: str
    state_diff_json: str
    checksum: str
    created_at: float


@dataclass(frozen=True, slots=True)
class HitlRecord:
    interrupt_id: str
    run_id: str
    step_seq: int
    node_id: str
    reason: str
    status: str
    reviewer_note: str | None
    created_at: float
    resolved_at: float | None


class RuntimeStore:
    """Thread-safe SQLite WAL storage for checkpoints, idempotency ledger, HITL, and OTel spans."""

    def __init__(self, db_path: Path | str, *, max_snapshot_bytes: int = 262_144) -> None:
        self.db_path_str = str(db_path)
        if self.db_path_str != ":memory:":
            Path(self.db_path_str).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path_str, check_same_thread=False, timeout=10.0)
        self._conn.row_factory = sqlite3.Row
        self.snapshot_guard = SnapshotGuard(max_bytes=max_snapshot_bytes)
        try:
            self._init_schema()
        except BaseException:
            self._conn.close()
            raise

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            if self.db_path_str != ":memory:":
                mode = self._conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if mode != "wal":
                    raise sqlite3.OperationalError("SQLite WAL mode is unavailable")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY,
                    scenario_id TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    verdict TEXT NOT NULL,
                    parent_run_id TEXT,
                    forked_from_step INTEGER,
                    fault_mode TEXT NOT NULL,
                    crash_step INTEGER,
                    require_hitl INTEGER NOT NULL DEFAULT 0,
                    max_steps INTEGER NOT NULL DEFAULT 64,
                    steps_executed INTEGER NOT NULL DEFAULT 0,
                    duplicate_side_effects INTEGER NOT NULL DEFAULT 0,
                    leaked_secrets INTEGER NOT NULL DEFAULT 0,
                    redacted_secrets_count INTEGER NOT NULL DEFAULT 0,
                    failure_reason TEXT,
                    state_json TEXT NOT NULL,
                    state_checksum TEXT,
                    metrics_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
                    step_seq INTEGER NOT NULL,
                    node_id TEXT NOT NULL,
                    action TEXT,
                    status TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    state_diff_json TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    checksum_version INTEGER NOT NULL DEFAULT 2,
                    created_at REAL NOT NULL,
                    UNIQUE(run_id, step_seq)
                );

                CREATE INDEX IF NOT EXISTS workflow_parent_idx ON workflow_runs(parent_run_id);

                CREATE TABLE IF NOT EXISTS idempotency_ledger (
                    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
                    idempotency_key TEXT NOT NULL,
                    tool_name TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    execution_count INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (run_id, idempotency_key)
                );

                CREATE TABLE IF NOT EXISTS hitl_interrupts (
                    interrupt_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
                    step_seq INTEGER NOT NULL,
                    node_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reviewer_note TEXT,
                    created_at REAL NOT NULL,
                    resolved_at REAL
                );

                CREATE TABLE IF NOT EXISTS otel_spans (
                    span_id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
                    parent_span_id TEXT,
                    name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status_code TEXT NOT NULL,
                    start_time_ns INTEGER NOT NULL,
                    end_time_ns INTEGER NOT NULL,
                    attributes_json TEXT NOT NULL
                );
                """
            )
        with self.transaction():
            checkpoint_columns = self._conn.execute("PRAGMA table_info(checkpoints)").fetchall()
            if not any(column["name"] == "checksum_version" for column in checkpoint_columns):
                self._conn.execute(
                    "ALTER TABLE checkpoints ADD COLUMN checksum_version INTEGER NOT NULL DEFAULT 1"
                )
            run_columns = self._conn.execute("PRAGMA table_info(workflow_runs)").fetchall()
            if not any(column["name"] == "state_checksum" for column in run_columns):
                self._conn.execute("ALTER TABLE workflow_runs ADD COLUMN state_checksum TEXT")

    @contextmanager
    def transaction(self, *, write: bool = True) -> Iterator[None]:
        """Hold a SQLite transaction across related reads or writes."""
        with self._lock:
            if self._conn.in_transaction:
                yield
            else:
                with self._conn:
                    self._conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                    yield

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def create_run(
        self,
        *,
        run_id: str,
        scenario_id: str,
        mode: str,
        status: str,
        verdict: str,
        fault_mode: str,
        crash_step: int | None,
        require_hitl: bool,
        max_steps: int,
        initial_state: dict[str, Any],
        parent_run_id: str | None = None,
        forked_from_step: int | None = None,
    ) -> None:
        now = time.time()
        redacted = self._bounded_state(initial_state)
        state_json = self.snapshot_guard.serialize(redacted.redacted)
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO workflow_runs (
                    run_id, scenario_id, mode, status, verdict,
                    parent_run_id, forked_from_step, fault_mode, crash_step,
                    require_hitl, max_steps, redacted_secrets_count,
                    state_json, state_checksum, metrics_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    scenario_id,
                    mode,
                    status,
                    verdict,
                    parent_run_id,
                    forked_from_step,
                    fault_mode,
                    crash_step,
                    int(require_hitl),
                    max_steps,
                    redacted.findings_count,
                    state_json,
                    self._state_digest(run_id, state_json),
                    "{}",
                    now,
                    now,
                ),
            )

    def update_run(
        self,
        run_id: str,
        *,
        status: str,
        verdict: str,
        steps_executed: int,
        state: dict[str, Any],
        metrics: dict[str, Any],
        failure_reason: str | None = None,
        duplicate_side_effects: int = 0,
        leaked_secrets: int = 0,
        redacted_secrets_delta: int = 0,
    ) -> None:
        now = time.time()
        redacted = self._bounded_state(state)
        state_json = self.snapshot_guard.serialize(redacted.redacted)
        metrics_json = canonical_json_dumps(redact_payload(metrics).redacted)
        safe_reason = redact_text(failure_reason)[0] if failure_reason is not None else None
        with self.transaction():
            self._conn.execute(
                """
                UPDATE workflow_runs
                SET status = ?, verdict = ?, steps_executed = ?,
                    duplicate_side_effects = ?, leaked_secrets = ?,
                    redacted_secrets_count = redacted_secrets_count + ?,
                    failure_reason = ?, state_json = ?, state_checksum = ?,
                    metrics_json = ?, updated_at = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    verdict,
                    steps_executed,
                    duplicate_side_effects,
                    leaked_secrets,
                    redacted.findings_count + redacted_secrets_delta,
                    safe_reason,
                    state_json,
                    self._state_digest(run_id, state_json),
                    metrics_json,
                    now,
                    run_id,
                ),
            )

    def claim_run(self, claim: RunClaim, state: dict[str, Any]) -> bool:
        safe_state = self.snapshot_guard.serialize(self._bounded_state(state).redacted)
        with self.transaction():
            updated = self._conn.execute(
                """
                UPDATE workflow_runs
                SET status = 'RUNNING', verdict = 'INTERRUPTED', state_json = ?, state_checksum = ?,
                    failure_reason = NULL, updated_at = ?
                WHERE run_id = ? AND status = ? AND updated_at = ?
                """,
                (safe_state, self._state_digest(claim.run_id, safe_state), time.time(),
                 claim.run_id, claim.status, claim.updated_at),
            )
            return updated.rowcount == 1

    def save_checkpoint(
        self,
        *,
        run_id: str,
        step_seq: int,
        node_id: str,
        action: str | None,
        status: str,
        state: dict[str, Any],
        state_diff: dict[str, Any],
        advance_run: bool = False,
    ) -> CheckpointRecord:
        now = time.time()
        redacted_state = self._bounded_state(state)
        bounded_diff = safe_json_clone(
            state_diff, max_depth=36, max_bytes=3 * self.snapshot_guard.max_bytes
        )
        redacted_diff = redact_payload(bounded_diff)
        state_json = self.snapshot_guard.serialize(redacted_state.redacted)
        diff_json = canonical_json_dumps(redacted_diff.redacted, max_depth=36)
        node_id = redact_text(node_id)[0]
        action = redact_text(action)[0] if action is not None else None
        checksum = self._checkpoint_digest(run_id, step_seq, node_id, action, status,
                                           state_json, diff_json)

        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO checkpoints (
                    run_id, step_seq, node_id, action, status,
                    state_json, state_diff_json, checksum, checksum_version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 2, ?)
                """,
                (run_id, step_seq, node_id, action, status, state_json, diff_json, checksum, now),
            )
            if redacted_state.findings_count > 0:
                self._conn.execute(
                    "UPDATE workflow_runs SET redacted_secrets_count = "
                    "redacted_secrets_count + ? WHERE run_id = ?",
                    (redacted_state.findings_count, run_id),
                )
            if advance_run:
                self._conn.execute(
                    """
                    UPDATE workflow_runs SET state_json = ?, state_checksum = ?,
                                             steps_executed = ?, updated_at = ?
                    WHERE run_id = ?
                    """,
                    (state_json, self._state_digest(run_id, state_json), step_seq, now, run_id),
                )
        return CheckpointRecord(
            run_id=run_id,
            step_seq=step_seq,
            node_id=node_id,
            action=action,
            status=status,
            state_json=state_json,
            state_diff_json=diff_json,
            checksum=checksum,
            created_at=now,
        )

    def _bounded_state(self, state: dict[str, Any]) -> RedactionResult:
        bounded = safe_json_clone(state, max_bytes=self.snapshot_guard.max_bytes)
        return redact_payload(bounded)

    @staticmethod
    def _state_digest(run_id: str, state_json: str) -> str:
        payload = canonical_json_dumps([run_id, state_json])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _checkpoint_digest(
        run_id: str, step_seq: int, node_id: str, action: str | None, status: str,
        state_json: str, diff_json: str,
    ) -> str:
        payload = canonical_json_dumps([run_id, step_seq, node_id, action, status,
                                        state_json, diff_json])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _verify_checkpoint(self, row: sqlite3.Row) -> None:
        version = row["checksum_version"]
        if version == 1:
            legacy = f"{row['run_id']}:{row['step_seq']}:{row['node_id']}:{row['state_json']}"
            digest = hashlib.sha256(legacy.encode("utf-8")).hexdigest()
        elif version == 2:
            digest = self._checkpoint_digest(
                row["run_id"], row["step_seq"], row["node_id"], row["action"], row["status"],
                row["state_json"], row["state_diff_json"],
            )
        else:
            raise CheckpointIntegrityError("Unsupported checkpoint checksum version")
        if row["checksum"] != digest:
            raise CheckpointIntegrityError("Stored checkpoint failed its integrity check")

    @staticmethod
    def _ledger_key(idempotency_key: str) -> str:
        return "sha256:" + hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()

    def _lookup_ledger(self, run_id: str, idempotency_key: str) -> sqlite3.Row | None:
        row: sqlite3.Row | None = self._conn.execute(
            """
            WITH RECURSIVE lineage(id) AS (
                VALUES (?)
                UNION
                SELECT r.parent_run_id FROM workflow_runs r JOIN lineage l ON r.run_id = l.id
                WHERE r.parent_run_id IS NOT NULL
                UNION
                SELECT r.run_id FROM workflow_runs r JOIN lineage l ON r.parent_run_id = l.id
            )
            SELECT e.result_json FROM idempotency_ledger e JOIN lineage l ON e.run_id = l.id
            WHERE e.idempotency_key IN (?, ?)
            ORDER BY e.created_at, e.run_id LIMIT 1
            """,
            (run_id, self._ledger_key(idempotency_key), idempotency_key),
        ).fetchone()
        return row

    def get_idempotent_result(self, run_id: str, idempotency_key: str) -> Any | None:
        with self._lock:
            row = self._lookup_ledger(run_id, idempotency_key)
            if row is None:
                return None
            return redact_payload(json.loads(str(row["result_json"]))).redacted

    def record_idempotent_result(
        self,
        run_id: str,
        idempotency_key: str,
        tool_name: str,
        result: Any,
    ) -> tuple[Any, bool]:
        """Commit a simulated effect and return its stored result and whether it was new."""
        redacted = redact_payload(result)
        result_json = canonical_json_dumps(redacted.redacted)
        with self.transaction():
            cached = self._lookup_ledger(run_id, idempotency_key)
            if cached is not None:
                return redact_payload(json.loads(cached["result_json"])).redacted, False
            inserted = self._conn.execute(
                """
                INSERT INTO idempotency_ledger (
                    run_id, idempotency_key, tool_name, result_json, execution_count, created_at
                ) VALUES (?, ?, ?, ?, 1, ?)
                ON CONFLICT(run_id, idempotency_key) DO NOTHING
                """,
                (run_id, self._ledger_key(idempotency_key), redact_text(tool_name)[0],
                 result_json, time.time()),
            )
            if inserted.rowcount == 1:
                return redacted.redacted, True
            cached = self._lookup_ledger(run_id, idempotency_key)
            if cached is None:
                raise sqlite3.IntegrityError("Ledger conflict without a readable result")
            return redact_payload(json.loads(cached["result_json"])).redacted, False

    def count_effects(self, run_id: str, tool_name: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(execution_count), 0) FROM idempotency_ledger "
                "WHERE run_id = ? AND tool_name = ?",
                (run_id, tool_name),
            ).fetchone()
            return int(row[0])

    def create_hitl_interrupt(
        self,
        *,
        interrupt_id: str,
        run_id: str,
        step_seq: int,
        node_id: str,
        reason: str,
    ) -> HitlRecord:
        now = time.time()
        reason = redact_text(reason)[0]
        with self.transaction():
            self._conn.execute(
                """
                INSERT INTO hitl_interrupts (
                    interrupt_id, run_id, step_seq, node_id, reason,
                    status, reviewer_note, created_at, resolved_at
                ) VALUES (?, ?, ?, ?, ?, 'PENDING', NULL, ?, NULL)
                """,
                (interrupt_id, run_id, step_seq, node_id, reason, now),
            )
        return HitlRecord(
            interrupt_id=interrupt_id,
            run_id=run_id,
            step_seq=step_seq,
            node_id=node_id,
            reason=reason,
            status="PENDING",
            reviewer_note=None,
            created_at=now,
            resolved_at=None,
        )

    def resolve_hitl_interrupt(self, run_id: str, decision: str, reviewer_note: str | None) -> bool:
        if decision not in {"approve", "reject"}:
            raise ValueError("Invalid HITL decision")
        now = time.time()
        status = "APPROVED" if decision == "approve" else "REJECTED"
        safe_note = redact_text(reviewer_note)[0] if reviewer_note is not None else None
        with self.transaction():
            cur = self._conn.execute(
                """
                UPDATE hitl_interrupts
                SET status = ?, reviewer_note = ?, resolved_at = ?
                WHERE interrupt_id = (
                    SELECT interrupt_id FROM hitl_interrupts
                    WHERE run_id = ? AND status = 'PENDING'
                    ORDER BY created_at DESC, interrupt_id DESC LIMIT 1
                )
                """,
                (status, safe_note, now, run_id),
            )
            return cur.rowcount > 0

    def save_spans(self, run_id: str, spans: list[dict[str, Any]]) -> None:
        if not spans:
            return
        with self.transaction():
            for sp in spans:
                attrs = redact_payload(sp.get("attributes", {}))
                self._conn.execute(
                    """
                    INSERT INTO otel_spans (
                        span_id, trace_id, run_id, parent_span_id,
                        name, kind, status_code, start_time_ns, end_time_ns, attributes_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(sp["span_id"]),
                        str(sp["trace_id"]),
                        run_id,
                        sp.get("parent_span_id"),
                        redact_text(str(sp["name"]))[0],
                        str(sp["kind"]),
                        str(sp["status_code"]),
                        int(sp["start_time_ns"]),
                        int(sp["end_time_ns"]),
                        canonical_json_dumps(attrs.redacted),
                    ),
                )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.transaction(write=False):
            row = self._conn.execute(
                "SELECT * FROM workflow_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            if row["state_checksum"] is not None and row["state_checksum"] != self._state_digest(
                run_id, row["state_json"]
            ):
                raise CheckpointIntegrityError("Stored run state failed its integrity check")
            checkpoint_rows = self._conn.execute(
                "SELECT * FROM checkpoints WHERE run_id = ? ORDER BY step_seq ASC", (run_id,),
            ).fetchall()
            for checkpoint in checkpoint_rows:
                self._verify_checkpoint(checkpoint)
            checkpoints = [
                {
                    "step_seq": int(c["step_seq"]),
                    "node_id": str(c["node_id"]),
                    "action": redact_text(c["action"])[0] if c["action"] is not None else None,
                    "status": str(c["status"]),
                    "state": redact_payload(json.loads(c["state_json"])).redacted,
                    "state_diff": redact_payload(json.loads(c["state_diff_json"])).redacted,
                    "checksum": str(c["checksum"]),
                    "created_at": float(c["created_at"]),
                }
                for c in checkpoint_rows
            ]
            ledger = [
                {
                    "idempotency_key": redact_text(str(entry["idempotency_key"]))[0],
                    "tool_name": str(entry["tool_name"]),
                    "result": redact_payload(json.loads(entry["result_json"])).redacted,
                    "execution_count": int(entry["execution_count"]),
                    "created_at": float(entry["created_at"]),
                }
                for entry in self._conn.execute(
                    "SELECT * FROM idempotency_ledger WHERE run_id = ? ORDER BY created_at ASC",
                    (run_id,),
                ).fetchall()
            ]
            hitl_row = self._conn.execute(
                "SELECT * FROM hitl_interrupts WHERE run_id = ? ORDER BY created_at DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            hitl = (
                {
                    "interrupt_id": str(hitl_row["interrupt_id"]),
                    "step_seq": int(hitl_row["step_seq"]),
                    "node_id": str(hitl_row["node_id"]),
                    "reason": redact_text(str(hitl_row["reason"]))[0],
                    "status": str(hitl_row["status"]),
                    "reviewer_note": (
                        redact_text(hitl_row["reviewer_note"])[0]
                        if hitl_row["reviewer_note"] is not None else None
                    ),
                    "created_at": float(hitl_row["created_at"]),
                    "resolved_at": hitl_row["resolved_at"],
                }
                if hitl_row is not None
                else None
            )
            return {
                "run_id": str(row["run_id"]),
                "scenario_id": str(row["scenario_id"]),
                "mode": str(row["mode"]),
                "status": str(row["status"]),
                "verdict": str(row["verdict"]),
                "parent_run_id": row["parent_run_id"],
                "forked_from_step": row["forked_from_step"],
                "fault_mode": str(row["fault_mode"]),
                "crash_step": row["crash_step"],
                "require_hitl": bool(row["require_hitl"]),
                "max_steps": int(row["max_steps"]),
                "steps_executed": int(row["steps_executed"]),
                "duplicate_side_effects": int(row["duplicate_side_effects"]),
                "leaked_secrets": int(row["leaked_secrets"]),
                "redacted_secrets_count": int(row["redacted_secrets_count"]),
                "failure_reason": (
                    redact_text(row["failure_reason"])[0]
                    if row["failure_reason"] is not None else None
                ),
                "state": redact_payload(json.loads(row["state_json"])).redacted,
                "metrics": redact_payload(json.loads(row["metrics_json"])).redacted,
                "checkpoints": checkpoints,
                "ledger_entries": ledger,
                "hitl_interrupt": hitl,
                "created_at": float(row["created_at"]),
                "updated_at": float(row["updated_at"]),
            }

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, min(100, limit))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT run_id, scenario_id, mode, status, verdict, fault_mode,
                       steps_executed, duplicate_side_effects, leaked_secrets,
                       redacted_secrets_count, parent_run_id, forked_from_step,
                       created_at, updated_at
                FROM workflow_runs
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
            return [
                {
                    "run_id": str(r["run_id"]),
                    "scenario_id": str(r["scenario_id"]),
                    "mode": str(r["mode"]),
                    "status": str(r["status"]),
                    "verdict": str(r["verdict"]),
                    "fault_mode": str(r["fault_mode"]),
                    "steps_executed": int(r["steps_executed"]),
                    "duplicate_side_effects": int(r["duplicate_side_effects"]),
                    "leaked_secrets": int(r["leaked_secrets"]),
                    "redacted_secrets_count": int(r["redacted_secrets_count"]),
                    "parent_run_id": r["parent_run_id"],
                    "forked_from_step": r["forked_from_step"],
                    "created_at": float(r["created_at"]),
                    "updated_at": float(r["updated_at"]),
                }
                for r in rows
            ]

    def get_checkpoint(self, run_id: str, step_seq: int) -> CheckpointRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM checkpoints WHERE run_id = ? AND step_seq = ?",
                (run_id, step_seq),
            ).fetchone()
            if row is None:
                return None
            self._verify_checkpoint(row)
            return CheckpointRecord(
                run_id=str(row["run_id"]),
                step_seq=int(row["step_seq"]),
                node_id=str(row["node_id"]),
                action=row["action"],
                status=str(row["status"]),
                state_json=str(row["state_json"]),
                state_diff_json=str(row["state_diff_json"]),
                checksum=str(row["checksum"]),
                created_at=float(row["created_at"]),
            )

    def get_spans(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM otel_spans WHERE run_id = ? ORDER BY start_time_ns ASC",
                (run_id,),
            ).fetchall()
            return [
                {
                    "span_id": str(r["span_id"]),
                    "trace_id": str(r["trace_id"]),
                    "run_id": str(r["run_id"]),
                    "parent_span_id": r["parent_span_id"],
                    "name": str(r["name"]),
                    "kind": str(r["kind"]),
                    "status_code": str(r["status_code"]),
                    "start_time_ns": int(r["start_time_ns"]),
                    "end_time_ns": int(r["end_time_ns"]),
                    "duration_ms": round(
                        (int(r["end_time_ns"]) - int(r["start_time_ns"])) / 1_000_000.0, 3
                    ),
                    "attributes": redact_payload(json.loads(r["attributes_json"])).redacted,
                }
                for r in rows
            ]
