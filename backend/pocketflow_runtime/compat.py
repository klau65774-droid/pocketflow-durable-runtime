from __future__ import annotations

import asyncio
import contextvars
import copy
import inspect
import time
import warnings
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from typing import Any, Literal

from pocketflow_runtime.guards import (
    OscillationDetector,
    TransitionPolicy,
    UnknownTransitionError,
    safe_json_clone,
)
from pocketflow_runtime.resilience import (
    BudgetTracker,
    CircuitBreaker,
    CircuitBreakerOpenError,
    ConcurrencyBulkhead,
    RetryPolicy,
)

_RETRY_ATTEMPTS: contextvars.ContextVar[dict[int, int] | None] = contextvars.ContextVar(
    "pocketflow_retry_attempts", default=None
)

FailMode = Literal["fail_fast", "isolate_errors"]


@dataclass(frozen=True, slots=True)
class IsolatedItemError:
    """Represents a single failed batch item when fail_mode='isolate_errors'."""

    index: int
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class StepEvent:
    """Telemetry payload emitted after each graph step."""

    step_seq: int
    node_id: str
    action: str | None
    elapsed_ms: float
    before_state: dict[str, Any]
    after_state: dict[str, Any]


StepObserver = Callable[[StepEvent], None]


async def _gather_owned(coroutines: Iterable[Coroutine[Any, Any, Any]]) -> list[Any]:
    tasks: list[asyncio.Task[Any]] = []
    try:
        tasks.extend(asyncio.create_task(coro) for coro in coroutines)
        return list(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            task.cancel()
        while pending := {task for task in tasks if not task.done()}:
            try:
                await asyncio.wait(pending)
            except asyncio.CancelledError:
                continue
        for task in tasks:
            if not task.cancelled():
                task.exception()
        raise


async def _map_parallel(
    items: Iterable[Any],
    invoke: Callable[[int, Any], Coroutine[Any, Any, Any]],
    bulkhead: ConcurrencyBulkhead | None,
) -> list[Any]:
    indexed_items = enumerate(items)
    if bulkhead is None:
        return await _gather_owned(invoke(index, item) for index, item in indexed_items)

    results: list[Any] = []

    async def _work() -> None:
        for index, item in indexed_items:
            results.append(None)
            async with bulkhead:
                results[index] = await invoke(index, item)

    await _gather_owned(_work() for _ in range(bulkhead.limit))
    return results


async def _run_in_thread(node: BaseNode, shared: dict[str, Any]) -> Any:
    worker = asyncio.create_task(asyncio.to_thread(node._run, shared))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        # A Python worker thread cannot be stopped. Keep ownership until it exits.
        while not worker.done():
            try:
                await asyncio.wait({worker})
            except asyncio.CancelledError:
                continue
        if not worker.cancelled():
            worker.exception()
        raise


class BaseNode:
    """Base graph node implementing the 3-phase prep -> exec -> post lifecycle."""

    def __init__(self, node_id: str | None = None) -> None:
        self.params: dict[str, Any] = {}
        self.successors: dict[str, BaseNode] = {}
        self.node_id: str = node_id or self.__class__.__name__

    def set_params(self, params: dict[str, Any]) -> None:
        self.params = params

    def next(self, node: BaseNode, action: str = "default") -> BaseNode:
        if action in self.successors:
            warnings.warn(f"Overwriting successor for action '{action}'", stacklevel=2)
        self.successors[action] = node
        return node

    def prep(self, shared: dict[str, Any]) -> Any:
        return None

    def exec(self, prep_res: Any) -> Any:
        return None

    def post(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> Any:
        return None

    def _exec(self, prep_res: Any) -> Any:
        return self.exec(prep_res)

    def _run(self, shared: dict[str, Any]) -> Any:
        p = self.prep(shared)
        e = self._exec(p)
        return self.post(shared, p, e)

    def run(self, shared: dict[str, Any]) -> Any:
        if self.successors:
            warnings.warn("Node won't run successors. Use Flow.", stacklevel=2)
        return self._run(shared)

    def __rshift__(self, other: BaseNode) -> BaseNode:
        return self.next(other)

    def __sub__(self, action: str) -> _ConditionalTransition:
        if isinstance(action, str):
            return _ConditionalTransition(self, action)
        raise TypeError("Action must be a string")


class _ConditionalTransition:
    def __init__(self, src: BaseNode, action: str) -> None:
        self.src = src
        self.action = action

    def __rshift__(self, tgt: BaseNode) -> BaseNode:
        return self.src.next(tgt, self.action)


class Node(BaseNode):
    """Retry-capable synchronous node with optional circuit breaker & jittered backoff."""

    def __init__(
        self,
        max_retries: int = 1,
        wait: float = 0,
        *,
        node_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        super().__init__(node_id=node_id)
        self.max_retries = max_retries
        self.wait = wait
        self.retry_policy = retry_policy
        self.circuit_breaker = circuit_breaker
        self._last_sync_retry = 0
        if self._attempts < 1:
            raise ValueError("Retry attempts must be >= 1")

    @property
    def _attempts(self) -> int:
        return self.retry_policy.max_retries if self.retry_policy is not None else self.max_retries

    def _retry_delay(self, attempt: int) -> float:
        return self.retry_policy.compute_delay(attempt) if self.retry_policy else self.wait

    def _acquire_breaker(self) -> int | None:
        if self.circuit_breaker is None:
            return None
        permit = self.circuit_breaker.acquire()
        if permit is None:
            raise CircuitBreakerOpenError(f"Circuit breaker OPEN for node '{self.node_id}'")
        return permit

    @property
    def cur_retry(self) -> int:
        return (_RETRY_ATTEMPTS.get() or {}).get(id(self), self._last_sync_retry)

    @cur_retry.setter
    def cur_retry(self, value: int) -> None:
        active = _RETRY_ATTEMPTS.get() or {}
        if id(self) in active:
            _RETRY_ATTEMPTS.set({**active, id(self): value})
        else:
            self._last_sync_retry = value

    def exec_fallback(self, prep_res: Any, exc: Exception) -> Any:
        raise exc

    def _exec(self, prep_res: Any) -> Any:
        attempts = self._attempts
        breaker = self.circuit_breaker
        for attempt in range(attempts):
            permit = self._acquire_breaker()
            token = _RETRY_ATTEMPTS.set({**(_RETRY_ATTEMPTS.get() or {}), id(self): attempt})
            self._last_sync_retry = attempt
            try:
                res = self.exec(prep_res)
                if breaker is not None:
                    breaker.record_success(permit)
                return res
            except Exception as exc:
                if breaker is not None:
                    breaker.record_failure(permit)
                if attempt == attempts - 1:
                    return self.exec_fallback(prep_res, exc)
            except BaseException:
                if breaker is not None and permit is not None:
                    breaker.release(permit)
                raise
            finally:
                _RETRY_ATTEMPTS.reset(token)
            if (delay := self._retry_delay(attempt)) > 0:
                time.sleep(delay)
        raise ValueError("Retry attempts must be >= 1")


class BatchNode(Node):
    """Synchronous batch node mapping Node._exec over items."""

    def _exec(self, items: Iterable[Any] | None) -> list[Any]:
        return [super(BatchNode, self)._exec(item) for item in (items or [])]


class Flow(BaseNode):
    """Synchronous graph orchestrator with optional step budget, oscillation & transition guards."""

    def __init__(
        self,
        start: BaseNode | None = None,
        *,
        node_id: str | None = None,
        transition_policy: TransitionPolicy | None = None,
        oscillation_detector: OscillationDetector | None = None,
        budget_tracker: BudgetTracker | None = None,
        step_observer: StepObserver | None = None,
    ) -> None:
        super().__init__(node_id=node_id)
        self.start_node = start
        self.transition_policy = transition_policy
        self.oscillation_detector = oscillation_detector
        self.budget_tracker = budget_tracker
        self.step_observer = step_observer

    def start(self, start: BaseNode) -> BaseNode:
        self.start_node = start
        return start

    def _execution_guards(self) -> tuple[BudgetTracker | None, OscillationDetector | None]:
        budget = BudgetTracker(self.budget_tracker.config) if self.budget_tracker else None
        detector = self.oscillation_detector
        return (
            budget,
            OscillationDetector(detector.window_size, detector.repeat_threshold)
            if detector is not None else None,
        )

    def get_next_node(self, curr: BaseNode, action: Any) -> BaseNode | None:
        key = action or "default"
        nxt = curr.successors.get(key)
        if not nxt and curr.successors:
            if self.transition_policy is not None:
                mode = self.transition_policy.mode
                if mode == "strict":
                    valid = list(curr.successors)
                    raise UnknownTransitionError(
                        f"Unknown action '{key}' from '{curr.node_id}'; valid={valid}"
                    )
                if mode == "fallback":
                    fb = curr.successors.get(self.transition_policy.fallback_action)
                    if fb is not None:
                        return fb
                raise UnknownTransitionError(
                    f"Dead-letter transition '{key}' from '{curr.node_id}'"
                )
            warnings.warn(
                f"Flow ends: '{action}' not found in {list(curr.successors)}",
                stacklevel=2,
            )
        return nxt

    def _orch(self, shared: dict[str, Any], params: dict[str, Any] | None = None) -> Any:
        curr = copy.copy(self.start_node)
        p = safe_json_clone(params if params is not None else {**self.params})
        last_action: Any = None
        step_seq = 0
        budget, detector = self._execution_guards()
        while curr:
            step_seq += 1
            if budget is not None:
                budget.check_and_consume(tokens=100)
            if detector is not None:
                detector.observe(curr.node_id, shared)
            curr.set_params(p)
            before = safe_json_clone(shared) if self.step_observer is not None else {}
            t0 = time.perf_counter()
            last_action = curr._run(shared)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if self.step_observer is not None:
                self.step_observer(
                    StepEvent(
                        step_seq=step_seq,
                        node_id=curr.node_id,
                        action=str(last_action) if last_action is not None else None,
                        elapsed_ms=elapsed_ms,
                        before_state=before,
                        after_state=safe_json_clone(shared),
                    )
                )
            nxt = self.get_next_node(curr, last_action)
            curr = copy.copy(nxt) if nxt is not None else None
        return last_action

    def _run(self, shared: dict[str, Any]) -> Any:
        p = self.prep(shared)
        o = self._orch(shared)
        return self.post(shared, p, o)

    def post(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> Any:
        return exec_res


class BatchFlow(Flow):
    def _run(self, shared: dict[str, Any]) -> Any:
        pr = self.prep(shared) or []
        for bp in pr:
            self._orch(shared, {**self.params, **bp})
        return self.post(shared, pr, None)


class AsyncNode(Node):
    """Concurrency-safe async node using ContextVar-isolated retry tracking & optional timeout."""

    def __init__(
        self,
        max_retries: int = 1,
        wait: float = 0,
        *,
        node_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        timeout_sec: float | None = None,
    ) -> None:
        super().__init__(
            max_retries=max_retries,
            wait=wait,
            node_id=node_id,
            retry_policy=retry_policy,
            circuit_breaker=circuit_breaker,
        )
        self.timeout_sec = timeout_sec

    async def prep_async(self, shared: dict[str, Any]) -> Any:
        return None

    async def exec_async(self, prep_res: Any) -> Any:
        return None

    async def exec_fallback_async(self, prep_res: Any, exc: Exception) -> Any:
        raise exc

    async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> Any:
        return None

    async def _exec(self, prep_res: Any) -> Any:
        attempts = self._attempts
        breaker = self.circuit_breaker
        for attempt in range(attempts):
            permit = self._acquire_breaker()
            token = _RETRY_ATTEMPTS.set({**(_RETRY_ATTEMPTS.get() or {}), id(self): attempt})
            try:
                coro = self.exec_async(prep_res)
                if self.timeout_sec is not None:
                    res = await asyncio.wait_for(coro, timeout=self.timeout_sec)
                else:
                    res = await coro
                if breaker is not None:
                    breaker.record_success(permit)
                return res
            except Exception as exc:
                if breaker is not None:
                    breaker.record_failure(permit)
                if attempt == attempts - 1:
                    return await self.exec_fallback_async(prep_res, exc)
            except BaseException:
                if breaker is not None and permit is not None:
                    breaker.release(permit)
                raise
            finally:
                _RETRY_ATTEMPTS.reset(token)
            if (delay := self._retry_delay(attempt)) > 0:
                await asyncio.sleep(delay)
        raise ValueError("Retry attempts must be >= 1")

    async def run_async(self, shared: dict[str, Any]) -> Any:
        if self.successors:
            warnings.warn("Node won't run successors. Use AsyncFlow.", stacklevel=2)
        return await self._run_async(shared)

    async def _run_async(self, shared: dict[str, Any]) -> Any:
        p = await self.prep_async(shared)
        e = await self._exec(p)
        return await self.post_async(shared, p, e)

    def _run(self, shared: dict[str, Any]) -> Any:
        raise RuntimeError("Use run_async.")


class AsyncBatchNode(AsyncNode, BatchNode):
    """Sequential async batch node safely handling items=None."""

    async def _exec(self, items: Iterable[Any] | None) -> list[Any]:  # type: ignore[override]
        return [await super(AsyncBatchNode, self)._exec(item) for item in (items or [])]


class AsyncParallelBatchNode(AsyncNode, BatchNode):
    """Concurrency-safe parallel async batch node with Semaphore bulkhead & error isolation."""

    def __init__(
        self,
        max_retries: int = 1,
        wait: float = 0,
        *,
        node_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        timeout_sec: float | None = None,
        max_concurrency: int | None = None,
        fail_mode: FailMode = "fail_fast",
    ) -> None:
        super().__init__(
            max_retries=max_retries,
            wait=wait,
            node_id=node_id,
            retry_policy=retry_policy,
            circuit_breaker=circuit_breaker,
            timeout_sec=timeout_sec,
        )
        self.max_concurrency = max_concurrency
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.fail_mode: FailMode = fail_mode
        self._bulkhead_holder: list[ConcurrencyBulkhead | None] = [None]
        self._active_bulkhead: contextvars.ContextVar[list[ConcurrencyBulkhead | None] | None] = (
            contextvars.ContextVar("batch_bulkhead", default=None)
        )

    @property
    def last_bulkhead(self) -> ConcurrencyBulkhead | None:
        return (self._active_bulkhead.get() or self._bulkhead_holder)[0]

    @last_bulkhead.setter
    def last_bulkhead(self, value: ConcurrencyBulkhead | None) -> None:
        (self._active_bulkhead.get() or self._bulkhead_holder)[0] = value

    async def _run_async(self, shared: dict[str, Any]) -> Any:
        active: list[ConcurrencyBulkhead | None] = [None]
        token = self._active_bulkhead.set(active)
        try:
            return await super()._run_async(shared)
        finally:
            self._active_bulkhead.reset(token)
            self._bulkhead_holder[0] = active[0]

    async def _exec(self, items: Iterable[Any] | None) -> list[Any]:  # type: ignore[override]
        bulkhead = ConcurrencyBulkhead(self.max_concurrency) if self.max_concurrency else None
        self.last_bulkhead = bulkhead

        async def _run_one(idx: int, item: Any) -> Any:
            try:
                return await super(AsyncParallelBatchNode, self)._exec(item)
            except Exception as exc:
                if self.fail_mode != "isolate_errors":
                    raise
                return IsolatedItemError(index=idx, error_type=type(exc).__name__, message=str(exc))

        return await _map_parallel(items or (), _run_one, bulkhead)


class AsyncFlow(Flow, AsyncNode):
    """Async graph orchestrator supporting sync-node thread offloading and full guardrails."""

    def __init__(
        self,
        start: BaseNode | None = None,
        *,
        node_id: str | None = None,
        transition_policy: TransitionPolicy | None = None,
        oscillation_detector: OscillationDetector | None = None,
        budget_tracker: BudgetTracker | None = None,
        step_observer: StepObserver | None = None,
        offload_sync_nodes: bool = False,
        snapshot_max_bytes: int | None = None,
    ) -> None:
        Flow.__init__(
            self,
            start=start,
            node_id=node_id,
            transition_policy=transition_policy,
            oscillation_detector=oscillation_detector,
            budget_tracker=budget_tracker,
            step_observer=step_observer,
        )
        self.offload_sync_nodes = offload_sync_nodes
        self.isolate_branch_params = True
        self.snapshot_max_bytes = snapshot_max_bytes

    def _contains_thread_offload(self) -> bool:
        pending: list[BaseNode] = [self]
        seen: set[int] = set()
        while pending:
            node = pending.pop()
            if id(node) in seen:
                continue
            seen.add(id(node))
            if isinstance(node, AsyncFlow):
                if node.offload_sync_nodes:
                    return True
                if node.start_node is not None:
                    pending.append(node.start_node)
            pending.extend(node.successors.values())
        return False

    async def _orch_async(
        self,
        shared: dict[str, Any],
        params: dict[str, Any] | None = None,
    ) -> Any:
        curr = copy.copy(self.start_node)
        merged = dict(params if params is not None else self.params)
        p = safe_json_clone(merged) if self.isolate_branch_params else merged
        last_action: Any = None
        step_seq = 0
        budget, detector = self._execution_guards()
        while curr:
            step_seq += 1
            if budget is not None:
                budget.check_and_consume(tokens=100)
            if detector is not None:
                detector.observe(curr.node_id, shared)
            curr.set_params(p)
            before = (
                safe_json_clone(shared, max_bytes=self.snapshot_max_bytes)
                if self.step_observer is not None else {}
            )
            t0 = time.perf_counter()
            if isinstance(curr, AsyncNode):
                last_action = await curr._run_async(shared)
            elif self.offload_sync_nodes:
                last_action = await _run_in_thread(curr, shared)
            else:
                last_action = curr._run(shared)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if self.step_observer is not None:
                self.step_observer(
                    StepEvent(
                        step_seq=step_seq,
                        node_id=curr.node_id,
                        action=str(last_action) if last_action is not None else None,
                        elapsed_ms=elapsed_ms,
                        before_state=before,
                        after_state=safe_json_clone(shared, max_bytes=self.snapshot_max_bytes),
                    )
                )
            nxt = self.get_next_node(curr, last_action)
            curr = copy.copy(nxt) if nxt is not None else None
        return last_action

    async def _run_async(self, shared: dict[str, Any]) -> Any:
        p = await self.prep_async(shared)
        o = await self._orch_async(shared)
        return await self.post_async(shared, p, o)

    async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> Any:
        return exec_res


class AsyncBatchFlow(AsyncFlow, BatchFlow):
    async def _run_async(self, shared: dict[str, Any]) -> Any:
        pr = await self.prep_async(shared) or []
        for bp in pr:
            await self._orch_async(shared, {**self.params, **bp})
        return await self.post_async(shared, pr, None)


class AsyncParallelBatchFlow(AsyncFlow, BatchFlow):
    def __init__(
        self,
        start: BaseNode | None = None,
        *,
        node_id: str | None = None,
        max_concurrency: int | None = None,
        isolate_branch_params: bool = True,
        offload_sync_nodes: bool = False,
    ) -> None:
        super().__init__(start=start, node_id=node_id, offload_sync_nodes=offload_sync_nodes)
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.max_concurrency = max_concurrency
        self.isolate_branch_params = isolate_branch_params

    async def _run_async(self, shared: dict[str, Any]) -> Any:
        pr = await self.prep_async(shared) or []
        # Shared state must not be mutated by parallel branches during thread offload.
        limit = 1 if self._contains_thread_offload() else self.max_concurrency
        bulkhead = ConcurrencyBulkhead(limit) if limit else None

        async def _run_branch(_index: int, bp: dict[str, Any]) -> Any:
            return await self._orch_async(shared, {**self.params, **bp})

        await _map_parallel(pr, _run_branch, bulkhead)
        return await self.post_async(shared, pr, None)


def inspect_callable_async(fn: Callable[..., Any]) -> bool:
    """Return True if callable is a coroutine function."""
    return inspect.iscoroutinefunction(fn)
