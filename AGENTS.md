# AGENTS.md — Codex Independent Audit & Fix Mode

This repository (`pocketflow-durable-runtime`) is an upgraded, durable, observable, and benchmark-evaluated runtime built on top of `The-Pocket/PocketFlow` (commit `f74d023f93607b8c3268133339a5e532a949898c`, MIT).

## Codex Independent Audit & Fix Authorization
- You are authorized to **audit and directly fix** issues across `backend/pocketflow_runtime/`, `backend/scripts/`, `frontend/`, `tests/`, `benchmarks/`, and `docs/`.
- **Strict Immutability Rule**: NEVER modify `backend/upstream_baseline/` (`pocketflow/__init__.py`, `pocketflow/__init__.pyi`, `tests/test_*.py`, `LICENSE`, `setup.py`, `UPSTREAM_META.json`). That directory MUST remain 100% verbatim upstream code for baseline A/B comparison.
- **Audit Output**: Document all findings, severity classifications, code fixes made, verification results, and final verdict (`PASS` / `PASS WITH WARNINGS` / `FAIL`) in `docs/AUDIT_AND_TEST_REPORT.md`.

## Non-Negotiable Engineering Standards
1. **100% Upstream Compatibility**: All 10 verbatim upstream `unittest` suites (56 test methods) MUST pass unmodified against both `backend/upstream_baseline/pocketflow` and `pocketflow_runtime.compat`.
2. **Contract Consistency**: `docs/API_CONTRACT.md`, `backend/docs/openapi.json`, FastAPI routes in `backend/pocketflow_runtime/api.py`, and `frontend/src/__tests__/contract.test.ts` MUST match 100%. If you modify any API route or schema, re-export `backend/docs/openapi.json`.
3. **Strict Static & Test Gates**:
   - `.venv\Scripts\ruff.exe check backend tests benchmarks` -> 0 errors
   - `.venv\Scripts\mypy.exe --strict backend\pocketflow_runtime tests\backend benchmarks` -> 0 errors
   - `.venv\Scripts\pytest.exe tests\backend -v` -> 100% pass (100% local CPU, 0 paid LLM APIs)
   - `npm --prefix frontend run test -- --run` -> 100% pass
   - `npm --prefix frontend exec tsc -- --noEmit` -> 0 errors
   - `npm --prefix frontend run build` -> clean build
4. **No False Greens & Composite Keys**:
   - Baseline failures, silent `None` returns, duplicate mutations, leaked secrets, or uncaught loops MUST render as `FAILED` / `VULNERABLE` / `INTERRUPTED`, never `PASSED`.
   - Use composite React list keys (`prefix-${index}-${id}`), never bare array indices.
5. **Privacy & Hygiene**: Zero hardcoded API keys, tokens, personal emails, phone numbers, local proxy ports (`7897`), real personal names, or absolute local drive/home paths anywhere in tracked files, docs, or tests. Construct synthetic redaction test tokens dynamically at runtime.
6. **External Blind Benchmarks**: Never hardcode benchmark fixtures into `pocketflow_runtime/`. If you change runtime logic, re-run `benchmarks/run_sealed_benchmarks.py` against external datasets (`$env:SEALED_BENCHMARK_DIR`) and update `docs/test_metrics.json` & README tables consistently.
