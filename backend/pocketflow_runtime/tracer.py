from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from pocketflow_runtime.redaction import redact_payload

_OTLP_KINDS = {"UNSPECIFIED": 0, "INTERNAL": 1, "SERVER": 2, "CLIENT": 3, "PRODUCER": 4,
               "CONSUMER": 5}
_OTLP_STATUS_CODES = {"UNSET": 0, "OK": 1, "ERROR": 2}


def compute_state_diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Compute a compact structured diff between two workflow shared-state dicts."""
    added: dict[str, Any] = {}
    removed: dict[str, Any] = {}
    changed: dict[str, dict[str, Any]] = {}

    for k, v in after.items():
        if k not in before:
            added[k] = v
        elif before[k] != v:
            changed[k] = {"before": before[k], "after": v}

    for k, v in before.items():
        if k not in after:
            removed[k] = v

    return {"added": added, "removed": removed, "changed": changed}


@dataclass(slots=True)
class SpanTracer:
    """Zero-collector OpenTelemetry-compatible Span DAG recorder."""

    run_id: str
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    spans: list[dict[str, Any]] = field(default_factory=list)
    _base_wall_ns: int = field(default_factory=time.time_ns, init=False)
    _base_mono_ns: int = field(default_factory=time.monotonic_ns, init=False)

    def now_ns(self) -> int:
        return self._base_wall_ns + (time.monotonic_ns() - self._base_mono_ns)

    def start_span(
        self,
        *,
        name: str,
        kind: str = "INTERNAL",
        parent_span_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        span_id = uuid.uuid4().hex[:16]
        redacted_attrs = redact_payload(attributes or {}).redacted
        span: dict[str, Any] = {
            "span_id": span_id,
            "trace_id": self.trace_id,
            "run_id": self.run_id,
            "parent_span_id": parent_span_id,
            "name": name,
            "kind": kind,
            "status_code": "UNSET",
            "start_time_ns": self.now_ns(),
            "end_time_ns": self.now_ns(),
            "attributes": redacted_attrs,
        }
        self.spans.append(span)
        return span

    def finish_span(
        self,
        span: dict[str, Any],
        *,
        status_code: str = "OK",
        extra_attributes: dict[str, Any] | None = None,
    ) -> None:
        span["end_time_ns"] = max(int(span["start_time_ns"]), self.now_ns())
        span["status_code"] = status_code
        if extra_attributes:
            attrs = dict(span.get("attributes", {}))
            attrs.update(redact_payload(extra_attributes).redacted)
            span["attributes"] = attrs

    @staticmethod
    def build_otel_resource_spans(trace_id: str, spans: list[dict[str, Any]]) -> dict[str, Any]:
        """Format recorded spans as standard OTLP resourceSpans JSON."""
        otlp_spans = []
        for sp in spans:
            attrs = [
                {"key": str(k), "value": {"stringValue": str(v)}}
                for k, v in dict(sp.get("attributes", {})).items()
            ]
            otlp_spans.append(
                {
                    "traceId": sp.get("trace_id", trace_id),
                    "spanId": sp["span_id"],
                    "parentSpanId": sp.get("parent_span_id") or "",
                    "name": sp["name"],
                    "kind": _OTLP_KINDS.get(sp.get("kind", "INTERNAL"), 0),
                    "startTimeUnixNano": str(sp["start_time_ns"]),
                    "endTimeUnixNano": str(sp["end_time_ns"]),
                    "status": {"code": _OTLP_STATUS_CODES.get(sp.get("status_code", "UNSET"), 0)},
                    "attributes": attrs,
                }
            )
        return {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {"stringValue": "pocketflow-durable-runtime"},
                            },
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "pocketflow_runtime.tracer", "version": "0.1.0"},
                            "spans": otlp_spans,
                        }
                    ],
                }
            ]
        }
