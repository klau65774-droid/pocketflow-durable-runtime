# API Contract — PocketFlow Durable Runtime

The console uses `http://127.0.0.1:5173`. Its Vite proxy forwards `/api` and `/health` to `http://127.0.0.1:8000`. The checked-in [OpenAPI document](../backend/docs/openapi.json) defines the JSON schemas used by the FastAPI routes and frontend contract tests.

## Methods, status codes, and schemas

| Method | Path | HTTP status codes | Success schema | Request schema |
| --- | --- | --- | --- | --- |
| `GET` | `/health` | 200, 500 | `HealthResponse` | — |
| `GET` | `/api/v1/meta` | 200, 500 | `MetaResponse` | — |
| `GET` | `/api/v1/scenarios` | 200, 500 | `ScenariosResponse` | — |
| `POST` | `/api/v1/runs` | 200, 400, 404, 413, 415, 422, 500 | `RunDetail` | `CreateRunRequest` |
| `GET` | `/api/v1/runs` | 200, 422, 500 | `RunsResponse` | Query `limit` |
| `GET` | `/api/v1/runs/{run_id}` | 200, 404, 422, 500 | `RunDetail` | Path `run_id` |
| `POST` | `/api/v1/runs/{run_id}/resume` | 200, 400, 404, 409, 413, 415, 422, 500 | `RunDetail` | `ResumeRunRequest` |
| `POST` | `/api/v1/runs/{run_id}/hitl` | 200, 400, 404, 409, 413, 415, 422, 500 | `RunDetail` | `HitlDecisionRequest` |
| `POST` | `/api/v1/runs/{run_id}/fork` | 200, 400, 404, 413, 415, 422, 500 | `ForkResponse` | `ForkRunRequest` |
| `GET` | `/api/v1/runs/{run_id}/trace` | 200, 404, 422, 500 | `TraceResponse` | Path `run_id` |
| `GET` | `/api/v1/runs/{run_id}/stream` | 200, 404, 422, 500 | `text/event-stream` | Path `run_id` |
| `POST` | `/api/v1/compare` | 200, 400, 404, 413, 415, 422, 500 | `CompareResponse` | `CompareRequest` |
| `POST` | `/api/v1/eval/redaction` | 200, 400, 413, 415, 422, 500 | `RedactionResponse` | `RedactionEvalRequest` |
| `GET` | `/api/v1/benchmarks` | 200, 500 | `BenchmarkReport` or `BenchmarkPending` | — |

JSON application errors use the same envelope.

```json
{"error":{"code":"VALIDATION_ERROR","message":"Request validation failed","details":{}}}
```

| Status | Error code and condition |
| --- | --- |
| 400 | `HTTP_ERROR` for a request body that cannot be read as JSON |
| 404 | `SCENARIO_NOT_FOUND`, `RUN_NOT_FOUND`, or `CHECKPOINT_NOT_FOUND` |
| 409 | `RUN_NOT_RESUMABLE` or `NO_PENDING_HITL` |
| 413 | `BODY_TOO_LARGE` |
| 415 | `JSON_REQUIRED` |
| 422 | `VALIDATION_ERROR` for schema violations or `INVALID_RUN_INPUT` for scenario and state-patch rules |
| 500 | `INTERNAL_ERROR`, including an unreadable or inconsistent persisted result |

Unregistered paths and disallowed methods use the same application envelope with `HTTP_ERROR`. CORS preflight uses the middleware's HTTP response. The default origins are the two local Vite origins with credentials disabled.

## Request limits and defaults

POST requests accept `application/json` and `application/*+json` up to 262,144 bytes. Request models reject unknown fields and use strict JSON types. Object inputs have at most 256 top-level properties with at most 32 levels of value nesting. JSON scalars must be finite and UTF-8 encodable. Scenario, fault and run identifiers are 1–64 ASCII letters, digits, underscores or hyphens.

| Model | Fields |
| --- | --- |
| `CreateRunRequest` | Required `scenario_id`. Optional `mode` defaults to `durable_runtime`. `fault_mode` defaults to the selected scenario's fault. `crash_step` accepts 1–4 and defaults to 3 for the retail crash. `require_hitl` defaults to false. `max_steps` accepts 1–256. `max_concurrency` accepts 1–64 and defaults to 4. `timeout_sec` accepts 0.05–60 seconds. Omitted step and timeout values use server settings. `transition_policy` is `strict`, `fallback`, or `dead_letter` and defaults to `strict`. `input_overrides` defaults to an empty object. |
| `ResumeRunRequest` | `clear_fault` defaults to true. |
| `HitlDecisionRequest` | Required `decision` is `approve` or `reject`. `reviewer_note` is nullable and limited to 500 characters. `state_patch` defaults to an empty object. |
| `ForkRunRequest` | Required `from_step_seq` accepts 1–256. `state_patch` defaults to an empty object and `clear_fault` defaults to true. |
| `CompareRequest` | Required `scenario_id` and optional `fault_mode`, `crash_step` and `seed`. The crash step uses the same bounds and default as create. `seed` accepts 0–100,000 and defaults to 42. It controls deterministic retry jitter for the durable batch. |
| `RedactionEvalRequest` | `payload` defaults to an empty object. `include_builtin_suite` defaults to true. |

The list query `limit` defaults to 20 and accepts 1–100. HITL applies to the retail refund scenario. State changes may supply business fields and context. Runtime-managed flags, counters and results are reserved. `order_id` accepts up to 128 printable characters subject to privacy checks. `amount_cents` accepts positive integers up to 100,000,000.

## JSON response fields

| Model | Fields |
| --- | --- |
| `HealthResponse` | `status` is `ok`. |
| `MetaResponse` | `service`, `version`, `upstream_repo`, `upstream_commit`, `upstream_license`, `storage_mode` and `cpu_only`. Storage modes are `sqlite-wal` and `sqlite-memory`. |
| `ScenariosResponse` | `scenarios` contains `id`, `title`, `description`, `fault_modes`, `default_fault` and `graph`. Graph nodes contain `id`, `label` and `kind`. Edges contain `source`, `target` and `action`. |
| `RunsResponse` | `runs` contains `RunSummary` records. |
| `RunSummary` | `run_id`, `scenario_id`, `mode`, `status`, `verdict`, `parent_run_id`, `forked_from_step`, `fault_mode`, `steps_executed`, `duplicate_side_effects`, `leaked_secrets`, `redacted_secrets_count`, `created_at` and `updated_at`. |
| `RunDetail` | All summary fields plus `crash_step`, `require_hitl`, `max_steps`, `failure_reason`, `state`, `metrics`, `checkpoints`, `ledger_entries` and `hitl_interrupt`. |
| `CheckpointRecord` | `step_seq`, `node_id`, `action`, `status`, `state`, `state_diff`, `checksum` and `created_at`. |
| `LedgerEntry` | `idempotency_key`, `tool_name`, `result`, `execution_count` and `created_at`. Keys for new writes are SHA-256 fingerprints. A replay can reuse an effect committed by another run in the same fork lineage. |
| `HitlInterrupt` | `interrupt_id`, `step_seq`, `node_id`, `reason`, `status`, `reviewer_note`, `created_at` and `resolved_at`. Status is `PENDING`, `APPROVED`, or `REJECTED`. |
| `ForkResponse` | `parent_run_id`, `forked_run` and `state_diff` with `added`, `removed` and `changed` objects. |
| `TraceResponse` | `run_id`, `trace_id`, `spans` and `otel_export`. |
| `SpanRecord` | `span_id`, `trace_id`, `run_id`, `parent_span_id`, `name`, `kind`, `status_code`, `start_time_ns`, `end_time_ns`, `duration_ms` and `attributes`. |
| `CompareResponse` | `scenario_id`, `fault_mode`, `seed`, `baseline`, `durable` and `delta_summary`. |
| `CompareSide` | `mode`, `status`, `verdict`, `failure_reason`, `output`, `duplicate_side_effects`, `elapsed_ms`, `steps_executed` and `leaked_secrets`. The durable side also includes `run_id`. |
| `CompareDelta` | `baseline_failed_or_vulnerable`, `durable_passed`, `duplicate_side_effects_prevented` and `silent_failures_prevented`. |
| `RedactionResponse` | `redacted_payload`, `findings_count`, `matched_rules` and nullable `suite_summary`. The summary includes tested and redacted secret counts, tested and flagged decoy counts, `recall`, `benign_fpr`, `secret_details` and `decoy_details`. |
| `BenchmarkReport` | The typed metrics from [test_metrics.json](test_metrics.json), with replay counts, timing, redaction, resource checks and the CI gate result. |
| `BenchmarkPending` | `status` is `pending` and `message` explains the missing report. |

Run statuses include `RUNNING`, `COMPLETED`, `PAUSED_HITL`, `CRASHED`, `FAILED`, `TRIPPED_OSCILLATION` and `DEAD_LETTERED`. Verdicts are `PASSED`, `PROTECTED`, `FAILED`, `VULNERABLE` and `INTERRUPTED`. Guard trips are protected outcomes. A pending approval or interrupted execution is shown as interrupted.

### Recovery, approval, and comparison

- Retail recovery starts after the last verified checkpoint. Its step-3 fault is placed after the simulated refund ledger commits and before the refund checkpoint. The replay reuses the committed result.
- Pending runs require an explicit HITL decision. Approval can include a validated patch. A rejected decision prevents a resume. Forks before the committed refund require their own approval whenever the source run required HITL. Refund inputs from an already committed refund checkpoint are fixed.
- The comparison may recover its simulated crash. A pending HITL decision remains pending. A missing output, duplicate mutation, leaked secret, swallowed error, or dropped workflow cannot receive a passing verdict. Requested fault selections are checked against the scenario catalogue.
- The trace endpoint lists spans from all execution segments. Its top-level `trace_id` identifies the first segment and is all zeros for an empty trace. The OTLP JSON retains each span's own trace ID and uses numeric enum values. [OTLP JSON mapping](https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding)
- The stream emits a finite snapshot. It sends checkpoint records in sequence order, then span records in timestamp order, then a `done` event with `run_id`, `status` and `verdict`.

## Environment settings

| Variable | Default and purpose |
| --- | --- |
| `POCKETFLOW_DB_PATH` | `data/runtime.sqlite3` is resolved within the project. `:memory:` selects a process-local database. |
| `POCKETFLOW_CORS_ORIGINS` | `http://127.0.0.1:5173,http://localhost:5173`. Configure explicit origins. |
| `POCKETFLOW_MAX_STEPS` | `64`, with a range of 1–256. |
| `POCKETFLOW_DEFAULT_TIMEOUT_SEC` | `15.0` seconds per execution segment with a range of 0.05–60. Batch items also receive a timeout. |
| `POCKETFLOW_MAX_SNAPSHOT_BYTES` | `262144` for state snapshot serialization. |
| `POCKETFLOW_LOG_LEVEL` | `INFO`. |
| `SEALED_BENCHMARK_DIR` | External input directory for the offline benchmark runner. |
