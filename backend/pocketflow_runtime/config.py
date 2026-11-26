from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def get_project_root() -> Path:
    """Return the repository root directory relative to this package."""
    return Path(__file__).resolve().parents[2]


def sanitize_relative_path(raw_path: str, base_dir: Path) -> Path:
    """Resolve a user/env path strictly inside base_dir, rejecting traversal."""
    root = base_dir.resolve()
    resolved = (root / raw_path).resolve()
    if resolved == root or not resolved.is_relative_to(root):
        raise ValueError("Database path must resolve to a file inside the project directory")
    return resolved


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """Environment-driven runtime settings without hardcoded secrets or local paths."""

    db_path: Path
    cors_origins: tuple[str, ...]
    max_steps: int
    default_timeout_sec: float
    max_snapshot_bytes: int
    log_level: str
    sealed_benchmark_dir: Path | None

    @classmethod
    def from_env(cls, root_dir: Path | None = None) -> RuntimeSettings:
        root = (root_dir or get_project_root()).resolve()
        raw_db = os.getenv("POCKETFLOW_DB_PATH", "data/runtime.sqlite3").strip()
        db_path = (
            Path(":memory:")
            if raw_db == ":memory:"
            else sanitize_relative_path(raw_db, root)
        )

        raw_origins = os.getenv(
            "POCKETFLOW_CORS_ORIGINS",
            "http://127.0.0.1:5173,http://localhost:5173",
        )
        origins = tuple(o.strip() for o in raw_origins.split(",") if o.strip())
        if "*" in origins:
            raise ValueError("CORS requires explicit allowed origins")

        max_steps = int(os.getenv("POCKETFLOW_MAX_STEPS", "64"))
        timeout_sec = float(os.getenv("POCKETFLOW_DEFAULT_TIMEOUT_SEC", "15.0"))
        if not 1 <= max_steps <= 256 or not 0.05 <= timeout_sec <= 60.0:
            raise ValueError("Default step or timeout configuration is out of bounds")
        max_snapshot_bytes = max(1024, int(os.getenv("POCKETFLOW_MAX_SNAPSHOT_BYTES", "262144")))
        log_level = os.getenv("POCKETFLOW_LOG_LEVEL", "INFO").upper()

        raw_bench = os.getenv("SEALED_BENCHMARK_DIR", "").strip()
        bench_dir = Path(raw_bench).resolve() if raw_bench else None

        return cls(
            db_path=db_path,
            cors_origins=origins,
            max_steps=max_steps,
            default_timeout_sec=timeout_sec,
            max_snapshot_bytes=max_snapshot_bytes,
            log_level=log_level,
            sealed_benchmark_dir=bench_dir,
        )
