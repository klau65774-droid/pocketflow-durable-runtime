# Usage Guide — `pocketflow-durable-runtime`

## 1. Local Development Setup

### Prerequisites
- Python 3.12+ and `uv`
- Node.js 20+ and `npm`

### Environment Variables
Copy `.env.example` to `.env` if custom overrides are needed (all defaults run 100% locally on CPU):
- `POCKETFLOW_DB_PATH=data/runtime.sqlite3`
- `POCKETFLOW_CORS_ORIGINS=http://127.0.0.1:5173,http://localhost:5173`
- `POCKETFLOW_MAX_STEPS=64`
- `POCKETFLOW_DEFAULT_TIMEOUT_SEC=15.0`
- `POCKETFLOW_MAX_SNAPSHOT_BYTES=262144`
- `POCKETFLOW_LOG_LEVEL=INFO`
- `SEALED_BENCHMARK_DIR=<path-to-external-sealed-benchmarks>`

## 2. Running Verification Gates

```powershell
# Python lint, strict type-checking, and backend pytest suite
.venv\Scripts\ruff.exe check --config backend\pyproject.toml backend tests benchmarks
.venv\Scripts\mypy.exe --config-file backend\pyproject.toml --strict backend\pocketflow_runtime tests\backend benchmarks
.venv\Scripts\pytest.exe tests\backend -v

# Frontend OpenAPI contract tests, TypeScript check, and production build
npm --prefix frontend run test -- --run
.\frontend\node_modules\.bin\tsc.cmd -p frontend\tsconfig.json --noEmit
npm --prefix frontend run build
```

The backend gate executes all ten pinned suites and all 56 methods against each implementation.
On Windows interpreters whose native monotonic clock has a coarser resolution, the compatibility
test harness uses the real high-resolution performance counter for both implementations.
Set `POCKETFLOW_NATIVE_TEST_CLOCK=1` to exercise the interpreter's default loop clock.
The audit report records both timing profiles. [Audit report](AUDIT_AND_TEST_REPORT.md)

## 3. Reproducing 3-Tier Sealed External Benchmarks

1. Place `tau_bench/`, `bfcl_v4/`, and `agentdojo/` inside an external directory outside this repository (see `datasets/README.md`).
2. Run:
```powershell
$env:SEALED_BENCHMARK_DIR = "<path-to-external-sealed-benchmarks>"
.venv\Scripts\python.exe benchmarks\run_sealed_benchmarks.py --dataset-dir $env:SEALED_BENCHMARK_DIR --output docs\test_metrics.json
```

## 4. Console Walkthrough
1. **Chaos Compare**: Run `upstream_baseline` vs `durable_runtime` side-by-side on any of the 6 fault scenarios (`concurrent_retry_race`, `crash_at_step`, `ping_pong_loop`, `shallow_alias_and_sync_stall`, `unknown_action`, `inject_secrets_and_decoys`).
2. **Durable & Time-Travel**. Launch a simulated interruption or an approval pause. Review the verified SQLite checkpoints, approve or reject the pending step, and replay the local simulated refund through the idempotency ledger. A pre-refund fork of an approval workflow has its own gate.
3. **OTel Trace & Diff**: Inspect the OpenTelemetry span waterfall and per-step `shared` state diff (`added`/`removed`/`changed`).
4. **3-Tier Benchmarks**. Review the offline replay counts, timings and actual check results in `docs/test_metrics.json`. The retail replays use local mock effects. BFCL and AgentDojo exercise runtime routing, concurrency and guards.
5. **Redaction & DoS Lab**: Test payload secret/PII redaction and verify the built-in 15-secret / 18-decoy gate (`100%` recall, `0%` benign FPR).
