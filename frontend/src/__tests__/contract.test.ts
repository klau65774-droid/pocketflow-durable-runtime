import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { createElement } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { App } from '../App';
import { API_ENDPOINTS, resolveEffectiveVerdict } from '../api';
import { benchmarkGatePassed, benchmarkRows } from '../benchmarks';
import type { BenchmarkReport, CompareSideResult } from '../types';

interface Schema {
  $ref?: string;
  type?: string;
  anyOf?: Schema[];
  properties?: Record<string, Schema>;
  required?: string[];
}
type Content = Record<string, { schema: Schema }>;
interface Operation {
  responses: Record<string, { content: Content }>;
  requestBody?: { content: Content };
}
interface OpenApiDoc {
  paths: Record<string, Record<string, Operation>>;
  components: { schemas: Record<string, Schema> };
}

const schemaPath = resolve(process.cwd(), '../backend/docs/openapi.json');
const openapi = JSON.parse(readFileSync(schemaPath, 'utf-8')) as OpenApiDoc;
const schemaRef = (name: string) => `#/components/schemas/${name}`;

describe('Backend OpenAPI contract & No-False-Greens guarantees', () => {
  it('matches every API method, status, media type and schema in both directions', () => {
    const operations = Object.entries(openapi.paths).flatMap(([path, methods]) =>
      Object.keys(methods).map(method => `${method.toUpperCase()} ${path}`),
    );
    expect(operations.sort()).toEqual(
      API_ENDPOINTS.map(({ method, path }) => `${method} ${path}`).sort(),
    );
    for (const ep of API_ENDPOINTS) {
      const operation = openapi.paths[ep.path][ep.method.toLowerCase()];
      expect(Object.keys(operation.responses).sort()).toEqual(ep.statuses.map(String).sort());
      if ('request' in ep) {
        expect(operation.requestBody?.content['application/json'].schema.$ref).toBe(
          schemaRef(ep.request),
        );
      } else {
        expect(operation.requestBody).toBeUndefined();
      }
      for (const status of ep.statuses) {
        const content = operation.responses[String(status)].content;
        if (status !== 200) {
          expect(content['application/json'].schema.$ref).toBe(schemaRef('ErrorResponse'));
        } else if (ep.response === 'text/event-stream') {
          expect(Object.keys(content)).toEqual(['text/event-stream']);
          expect(content['text/event-stream'].schema.type).toBe('string');
        } else if (typeof ep.response === 'string') {
          expect(content['application/json'].schema.$ref).toBe(schemaRef(ep.response));
        } else {
          expect(content['application/json'].schema.anyOf?.map(schema => schema.$ref)).toEqual(
            ep.response.map(schemaRef),
          );
        }
      }
    }
  });

  it('publishes the response fields used by the console', () => {
    const models = {
      RunDetail: [
        'run_id', 'scenario_id', 'mode', 'status', 'verdict', 'parent_run_id', 'forked_from_step',
        'fault_mode', 'steps_executed', 'duplicate_side_effects', 'leaked_secrets',
        'redacted_secrets_count', 'created_at', 'updated_at', 'crash_step', 'require_hitl',
        'max_steps', 'failure_reason', 'state', 'metrics', 'checkpoints', 'ledger_entries',
        'hitl_interrupt',
      ],
      CompareResponse: ['scenario_id', 'fault_mode', 'seed', 'baseline', 'durable', 'delta_summary'],
      ErrorResponse: ['error'],
      ErrorDetail: ['code', 'message', 'details'],
      RedactionResponse: ['redacted_payload', 'findings_count', 'matched_rules', 'suite_summary'],
      TraceResponse: ['run_id', 'trace_id', 'spans', 'otel_export'],
      ForkResponse: ['parent_run_id', 'forked_run', 'state_diff'],
      BenchmarkGate: [
        'gate_passed', 'baseline_pass_rate', 'candidate_pass_rate', 'delta',
        'duplicate_side_effect_rate', 'leaked_secrets', 'checks',
      ],
    };
    for (const [model, fields] of Object.entries(models)) {
      const definition = openapi.components.schemas[model];
      expect(Object.keys(definition.properties ?? {}).sort()).toEqual([...fields].sort());
      expect(definition.required?.sort()).toEqual([...fields].sort());
    }
  });

  it('never renders empty/failed baseline outputs as PASSED (No False Greens)', () => {
    expect(
      resolveEffectiveVerdict({
        mode: 'upstream_baseline',
        status: 'FAILED',
        verdict: 'VULNERABLE',
        failure_reason: 'Silent None returned',
        output: {},
        duplicate_side_effects: 0,
        elapsed_ms: 5,
        steps_executed: 1,
        leaked_secrets: 0,
      }),
    ).toBe('VULNERABLE');

    expect(
      resolveEffectiveVerdict({
        mode: 'upstream_baseline',
        status: 'COMPLETED',
        verdict: 'PASSED',
        failure_reason: null,
        output: { refund_executions: 2 },
        duplicate_side_effects: 1,
        elapsed_ms: 5,
        steps_executed: 2,
        leaked_secrets: 0,
      }),
    ).toBe('FAILED');
  });

  it('rejects empty, leaking and incomplete supposed successes', () => {
    const success: CompareSideResult = {
      mode: 'upstream_baseline',
      status: 'COMPLETED',
      verdict: 'PASSED',
      failure_reason: null,
      output: { verified: true },
      duplicate_side_effects: 0,
      elapsed_ms: 2,
      steps_executed: 1,
      leaked_secrets: 0,
    };
    expect(resolveEffectiveVerdict(success)).toBe('PASSED');
    expect(resolveEffectiveVerdict({ ...success, output: {} })).toBe('FAILED');
    expect(resolveEffectiveVerdict({ ...success, leaked_secrets: 1 })).toBe('FAILED');
    expect(resolveEffectiveVerdict({ ...success, status: 'RUNNING' })).toBe('INTERRUPTED');
    expect(resolveEffectiveVerdict({ ...success, status: 'CRASHED' })).toBe('INTERRUPTED');
    expect(resolveEffectiveVerdict({
      ...success, status: 'TRIPPED_OSCILLATION', verdict: 'PROTECTED', failure_reason: 'Guard tripped',
    })).toBe('PROTECTED');
    expect(resolveEffectiveVerdict({
      ...success, status: 'PAUSED_HITL', verdict: 'INTERRUPTED', failure_reason: null,
    })).toBe('INTERRUPTED');
  });

  it('uses observed benchmark counts and flags when reporting failures', () => {
    const path = resolve(process.cwd(), '../docs/test_metrics.json');
    const report = JSON.parse(readFileSync(path, 'utf-8')) as BenchmarkReport;
    expect(benchmarkGatePassed(report)).toBe(report.ci_regression_gate.gate_passed);
    report.tier1_tau_bench.durable_runtime.duplicate_side_effects = 5;
    report.tier1_bfcl_v4.tasks_evaluated = 40;
    report.tier3_redaction_and_dos.redaction.recall = 0.8;
    report.tier3_redaction_and_dos.redaction.decoys_flagged = 1;
    report.tier3_redaction_and_dos.dos_guards.snapshot_byte_cap_blocked = false;
    const rows = benchmarkRows(report);
    expect(benchmarkGatePassed(report)).toBe(false);
    expect(rows[0].details).toContain('→ 5');
    expect(rows[1].cases).toBe(40);
    expect(rows[3].durable).toContain('80.00% recall');
    expect(rows[3].details).toContain('Byte cap failed');
  });

  it('clears scan results when the user edits the payload', async () => {
    const responses: Record<string, unknown> = {
      '/health': { status: 'ok' },
      '/api/v1/meta': { upstream_commit: 'sample', storage_mode: 'sqlite-memory' },
      '/api/v1/scenarios': { scenarios: [] },
      '/api/v1/runs?limit=25': { runs: [] },
      '/api/v1/benchmarks': { status: 'pending', message: 'Awaiting replay' },
      '/api/v1/eval/redaction': {
        redacted_payload: { sample: 'previous scan payload' },
        findings_count: 0,
        matched_rules: [],
        suite_summary: null,
      },
    };
    const fetchLocal = vi.fn(async (input: string) => new Response(
      JSON.stringify(responses[input]),
      { status: input in responses ? 200 : 404 },
    ));
    vi.stubGlobal('fetch', fetchLocal);
    try {
      render(createElement(App));
      fireEvent.click(screen.getByRole('tab', { name: 'Redaction & DoS Lab' }));
      expect(await screen.findByText(/previous scan payload/)).toBeInTheDocument();
      fireEvent.change(screen.getByRole('textbox', { name: 'Payload JSON to scan' }), {
        target: { value: '{"sample":"changed"}' },
      });
      expect(screen.queryByText(/previous scan payload/)).not.toBeInTheDocument();
    } finally {
      cleanup();
      vi.unstubAllGlobals();
    }
  });
});
