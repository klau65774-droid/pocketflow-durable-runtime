from __future__ import annotations

import json
from pathlib import Path

from pocketflow_runtime.api import create_app
from pocketflow_runtime.config import RuntimeSettings, get_project_root


def export_openapi() -> Path:
    root = get_project_root()
    settings = RuntimeSettings(
        db_path=Path(":memory:"),
        cors_origins=("http://127.0.0.1:5173",),
        max_steps=64,
        default_timeout_sec=15.0,
        max_snapshot_bytes=262_144,
        log_level="INFO",
        sealed_benchmark_dir=None,
    )
    app = create_app(settings)
    schema = app.openapi()
    out_path = root / "backend" / "docs" / "openapi.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(schema, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return out_path


if __name__ == "__main__":
    exported = export_openapi()
    print(f"Exported OpenAPI schema to {exported.name}")
