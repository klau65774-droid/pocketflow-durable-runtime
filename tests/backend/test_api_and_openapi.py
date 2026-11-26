from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from pocketflow_runtime.api import MAX_BODY_BYTES, create_app
from pocketflow_runtime.config import RuntimeSettings, get_project_root, sanitize_relative_path


@asynccontextmanager
async def _make_client(
    tmp_path: Path, *, raise_server_exceptions: bool = True,
) -> AsyncIterator[tuple[FastAPI, httpx.AsyncClient]]:
    settings = RuntimeSettings(
        db_path=tmp_path / "api_test.sqlite3",
        cors_origins=("http://127.0.0.1:5173",),
        max_steps=32,
        default_timeout_sec=5.0,
        max_snapshot_bytes=262_144,
        log_level="INFO",
        sealed_benchmark_dir=None,
    )
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=raise_server_exceptions)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://testserver") as client,
    ):
        yield app, client


async def test_api_end_to_end_and_openapi_contract_sync(tmp_path: Path) -> None:
    async with _make_client(tmp_path) as (_, client):
        assert (await client.get("/health")).json() == {"status": "ok"}
        meta = (await client.get("/api/v1/meta")).json()
        assert meta["cpu_only"] is True
        assert meta["upstream_commit"] == "f74d023f93607b8c3268133339a5e532a949898c"

        scenarios = (await client.get("/api/v1/scenarios")).json()["scenarios"]
        assert len(scenarios) == 6

        cmp_resp = await client.post(
            "/api/v1/compare",
            json={
                "scenario_id": "parallel_batch_retry_race",
                "fault_mode": "concurrent_retry_race",
            },
        )
        assert cmp_resp.status_code == 200
        cmp_body = cmp_resp.json()
        assert cmp_body["baseline"]["verdict"] == "VULNERABLE"
        assert cmp_body["durable"]["verdict"] == "PASSED"

        run_id = cmp_body["durable"]["run_id"]
        trace = (await client.get(f"/api/v1/runs/{run_id}/trace")).json()
        assert len(trace["spans"]) >= 1
        assert "resourceSpans" in trace["otel_export"]

        sse = await client.get(f"/api/v1/runs/{run_id}/stream")
        assert sse.status_code == 200
        assert "event: done" in sse.text

        red = (await client.post(
            "/api/v1/eval/redaction",
            json={
                "payload": {
                    "order_id": "91002348712",
                    "key": "ghp_" + "1234567890abcdefghijklmnopqr",
                }
            },
        )).json()
        assert red["findings_count"] == 1
        assert red["redacted_payload"]["order_id"] == "91002348712"
        assert red["suite_summary"]["recall"] == 1.0
        assert red["suite_summary"]["benign_fpr"] == 0.0

        openapi_path = get_project_root() / "backend" / "docs" / "openapi.json"
        assert openapi_path.exists()
        saved = json.loads(openapi_path.read_text(encoding="utf-8"))
        live = (await client.get("/openapi.json")).json()
        assert saved == live

        contract = (get_project_root() / "docs" / "API_CONTRACT.md").read_text(encoding="utf-8")
        documented = {
            (method.lower(), path): {s.strip() for s in statuses.split(",")}
            for method, path, statuses in re.findall(
                r"^\| `(GET|POST)` \| `([^\n`]+)` \| ([0-9, ]+) \|",
                contract, re.MULTILINE,
            )
        }
        actual = {
            (method, path): set(operation["responses"])
            for path, operations in live["paths"].items()
            for method, operation in operations.items()
        }
        assert documented == actual
        assert (await client.get("/api/v1/benchmarks")).json() == json.loads(
            (get_project_root() / "docs" / "test_metrics.json").read_text(encoding="utf-8")
        )


async def test_validation_is_bounded_and_does_not_echo_input(tmp_path: Path) -> None:
    marker = "-".join(("sk", "proj", "z" * 24))
    async with _make_client(tmp_path) as (_, client):
        invalid = await client.post("/api/v1/runs", json={
            "scenario_id": "retail_refund_durable", "max_concurrency": marker,
        })
        assert invalid.status_code == 422
        assert invalid.json()["error"]["code"] == "VALIDATION_ERROR"
        assert invalid.json()["error"]["details"] == {}
        assert marker not in invalid.text

        payload: dict[str, Any] = {"leaf": 1}
        for _ in range(34):
            payload = {"child": payload}
        deep = await client.post("/api/v1/eval/redaction", json={"payload": payload})
        assert deep.status_code == 422
        assert (await client.post("/api/v1/runs", json={
            "scenario_id": "retail_refund_durable", "max_steps": "12",
        })).status_code == 422
        assert (await client.post("/api/v1/runs", json={
            "scenario_id": "retail_refund_durable", "typo": True,
        })).status_code == 422
        assert (await client.get("/api/v1/runs/" + "x" * 65)).status_code == 422
        assert (await client.get("/api/v1/runs", params={"limit": 101})).status_code == 422
        assert (await client.post(
            "/api/v1/eval/redaction", json={"payload": {str(i): i for i in range(257)}},
        )).status_code == 422

        async def chunks() -> AsyncIterator[bytes]:
            yield b"a" * (MAX_BODY_BYTES // 2)
            yield b"a" * (MAX_BODY_BYTES // 2 + 1)

        oversized = await client.post(
            "/api/v1/eval/redaction",
            content=chunks(),
            headers={"Content-Type": "application/json"},
        )
        assert oversized.status_code == 413
        assert oversized.json()["error"]["code"] == "BODY_TOO_LARGE"
        assert (await client.post("/api/v1/eval/redaction", content="{}")).status_code == 415
        assert (await client.post(
            "/api/v1/eval/redaction", content=b"\xf5", headers={"Content-Type": "application/json"},
        )).status_code == 400


async def test_error_envelopes_and_cors_at_app_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _make_client(tmp_path, raise_server_exceptions=False) as (app, client):
        missing = await client.get("/api/v1/runs/unknown/stream")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "RUN_NOT_FOUND"
        invalid_fault = await client.post("/api/v1/compare", json={
            "scenario_id": "retail_refund_durable", "fault_mode": "unknown",
        })
        assert invalid_fault.status_code == 422
        assert invalid_fault.json()["error"]["code"] == "INVALID_RUN_INPUT"
        method = await client.put("/health")
        assert method.status_code == 405 and "GET" in method.headers["allow"]
        assert method.json()["error"]["details"] == {}

        trusted = await client.options("/api/v1/runs", headers={
            "Origin": "http://127.0.0.1:5173", "Access-Control-Request-Method": "POST",
        })
        assert trusted.status_code == 200
        assert trusted.headers["access-control-allow-origin"] == "http://127.0.0.1:5173"
        assert "access-control-allow-credentials" not in trusted.headers
        untrusted = await client.options("/api/v1/runs", headers={
            "Origin": "https://example.invalid", "Access-Control-Request-Method": "POST",
        })
        assert untrusted.status_code == 400
        assert "access-control-allow-origin" not in untrusted.headers

        def failed_read(run_id: str) -> None:
            raise RuntimeError("synthetic internal diagnostic")

        monkeypatch.setattr(app.state.store, "get_run", failed_read)
        failure = await client.get("/api/v1/runs/unknown")
        assert failure.status_code == 500
        assert failure.json() == {
            "error": {"code": "INTERNAL_ERROR", "message": "Internal server error", "details": {}}
        }


async def test_retained_trace_segments_export_correctly_and_sse_terminates(tmp_path: Path) -> None:
    async with _make_client(tmp_path) as (_, client):
        crashed = (await client.post("/api/v1/runs", json={
            "scenario_id": "retail_refund_durable", "fault_mode": "crash_at_step", "crash_step": 3,
        })).json()
        run_path = f"/api/v1/runs/{crashed['run_id']}"
        assert (await client.post(f"{run_path}/resume", json={})).json()["status"] == "COMPLETED"
        trace = (await client.get(f"{run_path}/trace")).json()
        assert len({span["trace_id"] for span in trace["spans"]}) == 2
        export = trace["otel_export"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
        assert {span["spanId"]: span["traceId"] for span in export} == {
            span["span_id"]: span["trace_id"] for span in trace["spans"]
        }
        assert all(isinstance(span["kind"], int) and span["status"]["code"] in {1, 2}
                   for span in export)
        stream = await client.get(f"{run_path}/stream")
        assert stream.headers["content-type"].startswith("text/event-stream")
        frames = stream.text.strip().split("\n\n")
        assert frames[0].startswith("event: checkpoint\n")
        assert sum(frame.startswith("event: checkpoint\n") for frame in frames) == 4
        assert frames[-1].startswith("event: done\n")
        assert json.loads(frames[-1].split("data: ", 1)[1])["verdict"] == "PASSED"


def test_settings_enforce_default_bounds_and_project_containment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    assert sanitize_relative_path("data/demo.sqlite3", root) == root / "data" / "demo.sqlite3"
    with pytest.raises(ValueError, match="inside"):
        sanitize_relative_path("../sibling.sqlite3", root)
    with pytest.raises(ValueError, match="inside"):
        sanitize_relative_path(str(tmp_path / "other.sqlite3"), root)
    monkeypatch.setenv("POCKETFLOW_CORS_ORIGINS", "*")
    with pytest.raises(ValueError, match="explicit"):
        RuntimeSettings.from_env(root)
    monkeypatch.delenv("POCKETFLOW_CORS_ORIGINS")
    monkeypatch.setenv("POCKETFLOW_MAX_STEPS", "257")
    with pytest.raises(ValueError, match="out of bounds"):
        RuntimeSettings.from_env(root)
