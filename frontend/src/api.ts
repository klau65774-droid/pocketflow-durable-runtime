import type {
  BenchmarkResponse,
  CompareResponse,
  CompareSideResult,
  ForkResponse,
  HealthResponse,
  MetaResponse,
  RedactionResponse,
  RunDetail,
  RunSummary,
  ScenarioSpec,
  TraceResponse,
  Verdict,
} from './types';

export const API_ENDPOINTS = [
  { method: 'GET', path: '/health', response: 'HealthResponse', statuses: [200, 500] },
  { method: 'GET', path: '/api/v1/meta', response: 'MetaResponse', statuses: [200, 500] },
  { method: 'GET', path: '/api/v1/scenarios', response: 'ScenariosResponse', statuses: [200, 500] },
  {
    method: 'POST', path: '/api/v1/runs', request: 'CreateRunRequest', response: 'RunDetail',
    statuses: [200, 400, 404, 413, 415, 422, 500],
  },
  { method: 'GET', path: '/api/v1/runs', response: 'RunsResponse', statuses: [200, 422, 500] },
  {
    method: 'GET', path: '/api/v1/runs/{run_id}', response: 'RunDetail', statuses: [200, 404, 422, 500],
  },
  {
    method: 'POST', path: '/api/v1/runs/{run_id}/resume',
    request: 'ResumeRunRequest', response: 'RunDetail', statuses: [200, 400, 404, 409, 413, 415, 422, 500],
  },
  {
    method: 'POST', path: '/api/v1/runs/{run_id}/hitl',
    request: 'HitlDecisionRequest', response: 'RunDetail', statuses: [200, 400, 404, 409, 413, 415, 422, 500],
  },
  {
    method: 'POST', path: '/api/v1/runs/{run_id}/fork',
    request: 'ForkRunRequest', response: 'ForkResponse', statuses: [200, 400, 404, 413, 415, 422, 500],
  },
  {
    method: 'GET', path: '/api/v1/runs/{run_id}/trace',
    response: 'TraceResponse', statuses: [200, 404, 422, 500],
  },
  {
    method: 'GET', path: '/api/v1/runs/{run_id}/stream',
    response: 'text/event-stream', statuses: [200, 404, 422, 500],
  },
  {
    method: 'POST', path: '/api/v1/compare', request: 'CompareRequest', response: 'CompareResponse',
    statuses: [200, 400, 404, 413, 415, 422, 500],
  },
  {
    method: 'POST', path: '/api/v1/eval/redaction', request: 'RedactionEvalRequest',
    response: 'RedactionResponse', statuses: [200, 400, 413, 415, 422, 500],
  },
  {
    method: 'GET', path: '/api/v1/benchmarks', response: ['BenchmarkReport', 'BenchmarkPending'],
    statuses: [200, 500],
  },
] as const;

async function requestJson<T>(url: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(url, {
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    ...init,
  });
  const body = (await resp.json().catch(() => null)) as Record<string, unknown> | null;
  if (!resp.ok) {
    const errObj = body?.error as { message?: string } | undefined;
    throw new Error(errObj?.message || `HTTP ${resp.status}`);
  }
  if (!body) {
    throw new Error('Expected a JSON response');
  }
  return body as unknown as T;
}

/**
 * Enforces the No-False-Greens rule on UI rendering: empty/missing outputs or
 * detected failure reasons must never render as PASSED.
 */
export function resolveEffectiveVerdict(
  side: Pick<CompareSideResult, 'status' | 'verdict' | 'duplicate_side_effects' | 'leaked_secrets'> &
    Partial<CompareSideResult>,
): Verdict {
  if (side.verdict === 'VULNERABLE') return 'VULNERABLE';
  if (side.duplicate_side_effects > 0 || side.leaked_secrets > 0) return 'FAILED';
  if (side.verdict === 'FAILED' || side.verdict === 'INTERRUPTED') return side.verdict;
  if (['RUNNING', 'CRASHED', 'PAUSED_HITL'].includes(side.status)) return 'INTERRUPTED';
  if (
    side.verdict === 'PROTECTED' &&
    ['TRIPPED_OSCILLATION', 'DEAD_LETTERED'].includes(side.status)
  ) return 'PROTECTED';
  if (
    side.verdict !== 'PASSED' || side.status !== 'COMPLETED' || side.failure_reason ||
    (('output' in side || 'elapsed_ms' in side || side.mode === 'upstream_baseline') &&
      (!side.output || Object.keys(side.output).length === 0))
  ) return 'FAILED';
  return 'PASSED';
}

export async function fetchHealth(): Promise<HealthResponse> {
  return requestJson('/health');
}

export async function fetchMeta(): Promise<MetaResponse> {
  return requestJson('/api/v1/meta');
}

export async function fetchScenarios(): Promise<{ scenarios: ScenarioSpec[] }> {
  return requestJson('/api/v1/scenarios');
}

export async function fetchRuns(limit = 20): Promise<{ runs: RunSummary[] }> {
  return requestJson(`/api/v1/runs?limit=${encodeURIComponent(String(limit))}`);
}

export async function fetchRunDetail(runId: string): Promise<RunDetail> {
  return requestJson(`/api/v1/runs/${encodeURIComponent(runId)}`);
}

export async function createRun(payload: {
  scenario_id: string;
  fault_mode?: string | null;
  mode?: 'durable_runtime';
  crash_step?: number | null;
  require_hitl?: boolean;
  max_steps?: number;
  max_concurrency?: number;
  timeout_sec?: number | null;
  transition_policy?: 'strict' | 'fallback' | 'dead_letter';
  input_overrides?: Record<string, unknown>;
}): Promise<RunDetail> {
  return requestJson('/api/v1/runs', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function resumeRun(runId: string, clearFault = true): Promise<RunDetail> {
  return requestJson(`/api/v1/runs/${encodeURIComponent(runId)}/resume`, {
    method: 'POST',
    body: JSON.stringify({ clear_fault: clearFault }),
  });
}

export async function decideHitl(
  runId: string,
  decision: 'approve' | 'reject',
  reviewerNote: string,
): Promise<RunDetail> {
  return requestJson(`/api/v1/runs/${encodeURIComponent(runId)}/hitl`, {
    method: 'POST',
    body: JSON.stringify({ decision, reviewer_note: reviewerNote, state_patch: {} }),
  });
}

export async function forkRun(
  runId: string,
  fromStepSeq: number,
  statePatch: Record<string, unknown>,
): Promise<ForkResponse> {
  return requestJson(`/api/v1/runs/${encodeURIComponent(runId)}/fork`, {
    method: 'POST',
    body: JSON.stringify({
      from_step_seq: fromStepSeq,
      state_patch: statePatch,
      clear_fault: true,
    }),
  });
}

export async function fetchTrace(runId: string): Promise<TraceResponse> {
  return requestJson(`/api/v1/runs/${encodeURIComponent(runId)}/trace`);
}

export async function runCompare(payload: {
  scenario_id: string;
  fault_mode?: string | null;
  crash_step?: number | null;
  seed?: number;
}): Promise<CompareResponse> {
  return requestJson('/api/v1/compare', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function evaluateRedaction(
  payload: Record<string, unknown>,
  includeBuiltinSuite = true,
): Promise<RedactionResponse> {
  return requestJson('/api/v1/eval/redaction', {
    method: 'POST',
    body: JSON.stringify({ payload, include_builtin_suite: includeBuiltinSuite }),
  });
}

export async function fetchBenchmarks(): Promise<BenchmarkResponse> {
  return requestJson('/api/v1/benchmarks');
}
