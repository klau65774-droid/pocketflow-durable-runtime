from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi import Path as PathParam
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from starlette.datastructures import Headers
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from pocketflow_runtime.api_models import (
    BenchmarkPending,
    BenchmarkReport,
    CompareResponse,
    ErrorResponse,
    ForkResponse,
    HealthResponse,
    MetaResponse,
    RedactionResponse,
    RunDetail,
    RunsResponse,
    ScenariosResponse,
    TraceResponse,
)
from pocketflow_runtime.config import RuntimeSettings, get_project_root
from pocketflow_runtime.engine import DurableRuntimeEngine, InvalidRunInput
from pocketflow_runtime.guards import SnapshotLimitError, TransitionMode, safe_json_clone
from pocketflow_runtime.redaction import redact_payload, run_builtin_redaction_suite
from pocketflow_runtime.scenarios import list_scenarios
from pocketflow_runtime.storage import RuntimeStore
from pocketflow_runtime.tracer import SpanTracer

MAX_BODY_BYTES = 262_144
_IDENTIFIER = r"^[A-Za-z0-9_-]+$"
RunId = Annotated[str, PathParam(min_length=1, max_length=64, pattern=_IDENTIFIER)]
JsonObject = Annotated[dict[str, Any], Field(max_length=256)]


class JsonRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    @field_validator("input_overrides", "state_patch", "payload", check_fields=False)
    @classmethod
    def validate_json_object(cls, value: JsonObject) -> JsonObject:
        try:
            safe_json_clone(value, max_bytes=MAX_BODY_BYTES)
        except SnapshotLimitError as exc:
            raise ValueError("JSON object exceeds input limits or contains invalid values") from exc
        return value


class CreateRunRequest(JsonRequest):
    scenario_id: str = Field(..., min_length=1, max_length=64, pattern=_IDENTIFIER)
    mode: Literal["durable_runtime"] = "durable_runtime"
    fault_mode: str | None = Field(default=None, min_length=1, max_length=64, pattern=_IDENTIFIER)
    crash_step: int | None = Field(default=None, ge=1, le=4)
    require_hitl: bool = False
    max_steps: int | None = Field(default=None, ge=1, le=256)
    max_concurrency: int = Field(default=4, ge=1, le=64)
    timeout_sec: float | None = Field(default=None, ge=0.05, le=60.0)
    transition_policy: TransitionMode = "strict"
    input_overrides: JsonObject = Field(default_factory=dict)


class ResumeRunRequest(JsonRequest):
    clear_fault: bool = True


class HitlDecisionRequest(JsonRequest):
    decision: Literal["approve", "reject"]
    reviewer_note: str | None = Field(default=None, max_length=500)
    state_patch: JsonObject = Field(default_factory=dict)

    @field_validator("reviewer_note")
    @classmethod
    def validate_note_encoding(cls, value: str | None) -> str | None:
        if value is not None:
            try:
                value.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValueError("Reviewer note must be valid UTF-8") from exc
        return value


class ForkRunRequest(JsonRequest):
    from_step_seq: int = Field(..., ge=1, le=256)
    state_patch: JsonObject = Field(default_factory=dict)
    clear_fault: bool = True


class CompareRequest(JsonRequest):
    scenario_id: str = Field(..., min_length=1, max_length=64, pattern=_IDENTIFIER)
    fault_mode: str | None = Field(default=None, min_length=1, max_length=64, pattern=_IDENTIFIER)
    crash_step: int | None = Field(default=None, ge=1, le=4)
    seed: int = Field(default=42, ge=0, le=100_000)


class RedactionEvalRequest(JsonRequest):
    payload: JsonObject = Field(default_factory=dict)
    include_builtin_suite: bool = True


def _error_response(
    status_code: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "details": details or {}}},
        headers=headers,
    )


_POST_ERRORS: dict[int | str, dict[str, Any]] = {
    400: {"model": ErrorResponse, "description": "Malformed request body"},
    413: {"model": ErrorResponse, "description": "JSON request exceeds its byte limit"},
    415: {"model": ErrorResponse, "description": "JSON content type required"},
    422: {"model": ErrorResponse, "description": "Request validation failed"},
}
_NOT_FOUND: dict[int | str, dict[str, Any]] = {
    404: {"model": ErrorResponse, "description": "Resource not found"},
}
_PATH_ERRORS: dict[int | str, dict[str, Any]] = {
    **_NOT_FOUND,
    422: {"model": ErrorResponse, "description": "Invalid run identifier"},
}
_RUN_WRITE_ERRORS: dict[int | str, dict[str, Any]] = {
    **_NOT_FOUND, **_POST_ERRORS,
    409: {"model": ErrorResponse, "description": "Run status prevents this operation"},
}


class BoundedJSONBodyMiddleware:
    """Bound incoming bodies before the framework parses JSON."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] != "http" or scope["method"] != "POST"
            or not scope["path"].startswith("/api/v1/")
        ):
            await self.app(scope, receive, send)
            return
        media = Headers(scope=scope).get("content-type", "").split(";", 1)[0].strip().lower()
        if media != "application/json" and not (
            media.startswith("application/") and media.endswith("+json")
        ):
            response = _error_response(415, "JSON_REQUIRED", "A JSON request body is required")
            await response(scope, receive, send)
            return

        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            if len(body) + len(chunk) > MAX_BODY_BYTES:
                response = _error_response(413, "BODY_TOO_LARGE", "Request body exceeds its limit")
                await response(scope, receive, send)
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break

        complete: Message = {"type": "http.request", "body": bytes(body), "more_body": False}
        replayed = False

        async def replay() -> Message:
            nonlocal replayed
            if replayed:
                return await receive()
            replayed = True
            return complete

        await self.app(scope, replay, send)


def create_app(settings: RuntimeSettings | None = None) -> FastAPI:
    cfg = settings or RuntimeSettings.from_env()
    logging.basicConfig(level=getattr(logging, cfg.log_level, logging.INFO))
    store: RuntimeStore
    engine: DurableRuntimeEngine

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        nonlocal store, engine
        store = RuntimeStore(cfg.db_path, max_snapshot_bytes=cfg.max_snapshot_bytes)
        try:
            engine = DurableRuntimeEngine(
                store,
                max_snapshot_bytes=cfg.max_snapshot_bytes,
                default_timeout_sec=cfg.default_timeout_sec,
            )
            _app.state.store = store
            _app.state.engine = engine
            yield
        finally:
            store.close()

    app = FastAPI(
        title="PocketFlow Durable Runtime API",
        version="0.1.0",
        description=(
            "Durable, concurrency-safe, OTel-observable runtime "
            "upgrading The-Pocket/PocketFlow"
        ),
        lifespan=lifespan,
        responses={500: {"model": ErrorResponse, "description": "Internal server error"}},
    )
    app.state.settings = cfg

    app.add_middleware(BoundedJSONBodyMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cfg.cors_origins),
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type", "Accept"],
    )

    @app.exception_handler(StarletteHTTPException)
    async def _http_err(_req: Request, exc: StarletteHTTPException) -> JSONResponse:
        detail: dict[str, Any] = (
            exc.detail
            if isinstance(exc.detail, dict)
            else {"code": "HTTP_ERROR", "message": "Request could not be processed"}
        )
        raw_details = detail.get("details")
        parsed_details = raw_details if isinstance(raw_details, dict) else None
        return _error_response(
            exc.status_code,
            str(detail.get("code", "HTTP_ERROR")),
            str(detail.get("message", "Request failed")),
            parsed_details,
            exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_err(_req: Request, _exc: RequestValidationError) -> JSONResponse:
        return _error_response(422, "VALIDATION_ERROR", "Request validation failed")

    @app.exception_handler(InvalidRunInput)
    async def _run_input_err(_req: Request, exc: InvalidRunInput) -> JSONResponse:
        return _error_response(422, "INVALID_RUN_INPUT", str(exc))

    @app.exception_handler(Exception)
    async def _internal_err(_req: Request, exc: Exception) -> JSONResponse:
        logging.getLogger(__name__).error("Internal request failure (%s)", type(exc).__name__)
        return _error_response(500, "INTERNAL_ERROR", "Internal server error")

    @app.get("/health", response_model=HealthResponse)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/meta", response_model=MetaResponse)
    async def meta() -> dict[str, Any]:
        return {
            "service": "pocketflow-durable-runtime",
            "version": "0.1.0",
            "upstream_repo": "https://github.com/The-Pocket/PocketFlow",
            "upstream_commit": "f74d023f93607b8c3268133339a5e532a949898c",
            "upstream_license": "MIT",
            "storage_mode": "sqlite-memory" if cfg.db_path == Path(":memory:") else "sqlite-wal",
            "cpu_only": True,
        }

    @app.get("/api/v1/scenarios", response_model=ScenariosResponse)
    async def scenarios() -> dict[str, Any]:
        return {"scenarios": list_scenarios()}

    @app.post("/api/v1/runs", response_model=RunDetail, responses={**_NOT_FOUND, **_POST_ERRORS})
    async def create_run(req: CreateRunRequest) -> dict[str, Any]:
        try:
            return await engine.start_run(
                scenario_id=req.scenario_id,
                mode=req.mode,
                fault_mode=req.fault_mode,
                crash_step=req.crash_step,
                require_hitl=req.require_hitl,
                max_steps=req.max_steps if req.max_steps is not None else cfg.max_steps,
                max_concurrency=req.max_concurrency,
                timeout_sec=req.timeout_sec,
                transition_policy=req.transition_policy,
                input_overrides=req.input_overrides,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=404,
                detail={"code": "SCENARIO_NOT_FOUND", "message": "Unknown scenario"},
            ) from exc

    @app.get("/api/v1/runs", response_model=RunsResponse, responses={
        422: {"model": ErrorResponse, "description": "Invalid list limit"}
    })
    async def list_runs(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, Any]:
        return {"runs": store.list_runs(limit=limit)}

    @app.get("/api/v1/runs/{run_id}", response_model=RunDetail, responses=_PATH_ERRORS)
    async def get_run(run_id: RunId) -> dict[str, Any]:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(
                status_code=404,
                detail={"code": "RUN_NOT_FOUND", "message": "Unknown run"},
            )
        return run

    @app.post("/api/v1/runs/{run_id}/resume", response_model=RunDetail, responses=_RUN_WRITE_ERRORS)
    async def resume_run(run_id: RunId, req: ResumeRunRequest) -> dict[str, Any]:
        try:
            return await engine.resume_run(run_id, clear_fault=req.clear_fault)
        except KeyError as exc:
            raise HTTPException(
                status_code=404,
                detail={"code": "RUN_NOT_FOUND", "message": "Unknown run"},
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=409,
                detail={"code": "RUN_NOT_RESUMABLE", "message": str(exc)},
            ) from exc

    @app.post("/api/v1/runs/{run_id}/hitl", response_model=RunDetail, responses=_RUN_WRITE_ERRORS)
    async def resolve_hitl(run_id: RunId, req: HitlDecisionRequest) -> dict[str, Any]:
        try:
            return await engine.resolve_hitl(
                run_id,
                decision=req.decision,
                reviewer_note=req.reviewer_note,
                state_patch=req.state_patch,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=404,
                detail={"code": "RUN_NOT_FOUND", "message": "Unknown run"},
            ) from exc
        except InvalidRunInput:
            raise
        except ValueError as exc:
            raise HTTPException(
                status_code=409,
                detail={"code": "NO_PENDING_HITL", "message": str(exc)},
            ) from exc

    @app.post(
        "/api/v1/runs/{run_id}/fork", response_model=ForkResponse,
        responses={**_NOT_FOUND, **_POST_ERRORS},
    )
    async def fork_run(run_id: RunId, req: ForkRunRequest) -> dict[str, Any]:
        try:
            return await engine.fork_run(
                run_id,
                from_step_seq=req.from_step_seq,
                state_patch=req.state_patch,
                clear_fault=req.clear_fault,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=404,
                detail={"code": "RUN_NOT_FOUND", "message": "Unknown run"},
            ) from exc
        except LookupError as exc:
            raise HTTPException(
                status_code=404,
                detail={"code": "CHECKPOINT_NOT_FOUND", "message": "Unknown checkpoint"},
            ) from exc

    @app.get("/api/v1/runs/{run_id}/trace", response_model=TraceResponse, responses=_PATH_ERRORS)
    async def get_trace(run_id: RunId) -> dict[str, Any]:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(
                status_code=404,
                detail={"code": "RUN_NOT_FOUND", "message": "Unknown run"},
            )
        spans = store.get_spans(run_id)
        trace_id = spans[0]["trace_id"] if spans else "0" * 32
        return {
            "run_id": run_id,
            "trace_id": trace_id,
            "spans": spans,
            "otel_export": SpanTracer.build_otel_resource_spans(trace_id, spans),
        }

    @app.get(
        "/api/v1/runs/{run_id}/stream", response_class=StreamingResponse,
        responses={
            **_PATH_ERRORS,
            200: {"content": {"text/event-stream": {"schema": {"type": "string"}}}},
        },
    )
    async def stream_run(run_id: RunId) -> StreamingResponse:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(
                status_code=404,
                detail={"code": "RUN_NOT_FOUND", "message": "Unknown run"},
            )
        spans = store.get_spans(run_id)

        async def _events() -> AsyncIterator[str]:
            for ckpt in run["checkpoints"]:
                yield f"event: checkpoint\ndata: {json.dumps(ckpt, ensure_ascii=False)}\n\n"
            for sp in spans:
                yield f"event: span\ndata: {json.dumps(sp, ensure_ascii=False)}\n\n"
            done_payload = json.dumps(
                {"run_id": run_id, "status": run["status"], "verdict": run["verdict"]}
            )
            yield f"event: done\ndata: {done_payload}\n\n"

        return StreamingResponse(_events(), media_type="text/event-stream")

    @app.post("/api/v1/compare", response_model=CompareResponse, responses={
        **_NOT_FOUND, **_POST_ERRORS,
    })
    async def compare(req: CompareRequest) -> dict[str, Any]:
        try:
            return await engine.compare_baseline_vs_durable(
                scenario_id=req.scenario_id,
                fault_mode=req.fault_mode,
                crash_step=req.crash_step,
                seed=req.seed,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=404,
                detail={"code": "SCENARIO_NOT_FOUND", "message": "Unknown scenario"},
            ) from exc

    @app.post("/api/v1/eval/redaction", response_model=RedactionResponse, responses=_POST_ERRORS)
    async def eval_redaction(req: RedactionEvalRequest) -> dict[str, Any]:
        res = redact_payload(req.payload)
        suite_summary = run_builtin_redaction_suite() if req.include_builtin_suite else None
        return {
            "redacted_payload": res.redacted,
            "findings_count": res.findings_count,
            "matched_rules": list(res.matched_rules),
            "suite_summary": suite_summary,
        }

    @app.get("/api/v1/benchmarks", response_model=BenchmarkReport | BenchmarkPending)
    async def get_benchmarks() -> dict[str, Any]:
        metrics_path: Path = get_project_root() / "docs" / "test_metrics.json"
        if not metrics_path.exists():
            return {"status": "pending", "message": "Sealed benchmarks have not been executed yet."}
        data: dict[str, Any] = json.loads(metrics_path.read_text(encoding="utf-8"))
        return data

    return app


app = create_app()
