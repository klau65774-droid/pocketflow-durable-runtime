from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest
from pocketflow_runtime.compat import (
    AsyncFlow,
    AsyncNode,
    AsyncParallelBatchFlow,
    AsyncParallelBatchNode,
    IsolatedItemError,
    Node,
)
from pocketflow_runtime.engine import load_upstream_pocketflow
from pocketflow_runtime.guards import OscillationDetector
from pocketflow_runtime.resilience import (
    BudgetConfig,
    BudgetTracker,
    CircuitBreaker,
    CircuitBreakerOpenError,
    RetryPolicy,
)


async def test_upstream_cur_retry_race_is_fixed_in_compat() -> None:
    """Reproduce silent None race in upstream and verify compat executes exact fallback."""
    pf = load_upstream_pocketflow()

    entered_last = asyncio.Event()
    fail_cnt = 0
    ok_cnt = 0

    async def _up_prep(self: Any, shared: dict[str, Any]) -> list[str]:
        return ["always_fail", "fast_ok"]

    async def _up_exec(self: Any, item: str) -> str:
        nonlocal fail_cnt, ok_cnt
        if item == "always_fail":
            fail_cnt += 1
            if fail_cnt == 3:
                entered_last.set()
                await asyncio.sleep(0.02)
            else:
                await asyncio.sleep(0.002)
            raise RuntimeError("boom")
        ok_cnt += 1
        if ok_cnt == 1:
            await entered_last.wait()
            raise RuntimeError("overwrite cur_retry")
        await asyncio.sleep(0.002)
        return "OK"

    async def _up_fb(self: Any, item: str, exc: Exception) -> str:
        return f"FB:{item}"

    async def _up_post(self: Any, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
        shared["out"] = exec_res
        return "done"

    up_cls = type(
        "UpRace",
        (pf.AsyncParallelBatchNode,),
        {
            "prep_async": _up_prep,
            "exec_async": _up_exec,
            "exec_fallback_async": _up_fb,
            "post_async": _up_post,
        },
    )
    up_shared: dict[str, Any] = {}
    await pf.AsyncFlow(start=up_cls(max_retries=3)).run_async(up_shared)
    # Upstream self.cur_retry race causes fast_ok to hit premature fallback on attempt 0
    assert up_shared["out"][1] == "FB:fast_ok"
    assert ok_cnt == 1

    compat_ok_cnt = 0

    class SafeRace(AsyncParallelBatchNode):
        async def prep_async(self, shared: dict[str, Any]) -> list[str]:
            return ["always_fail", "fast_ok"]

        async def exec_async(self, item: str) -> str:
            nonlocal compat_ok_cnt
            if item == "always_fail":
                await asyncio.sleep(0.01)
                raise RuntimeError("boom")
            compat_ok_cnt += 1
            if compat_ok_cnt == 1:
                raise RuntimeError("transient attempt 0")
            await asyncio.sleep(0.002)
            return "OK"

        async def exec_fallback_async(self, item: str, exc: Exception) -> str:
            return f"FB:{item}:retry={self.cur_retry}"

        async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
            shared["out"] = exec_res
            return "done"

    compat_shared: dict[str, Any] = {}
    await AsyncFlow(start=SafeRace(max_retries=3)).run_async(compat_shared)
    assert compat_shared["out"] == ["FB:always_fail:retry=2", "OK"]
    assert compat_ok_cnt == 2


async def test_async_batch_none_items_and_isolate_errors() -> None:
    class PartialBatch(AsyncParallelBatchNode):
        async def prep_async(self, shared: dict[str, Any]) -> list[int] | None:
            return shared.get("items")

        async def exec_async(self, item: int) -> int:
            if item == 2:
                raise ValueError("bad item 2")
            await asyncio.sleep(0.005)
            return item * 10

        async def post_async(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
            shared["res"] = exec_res
            return "done"

    s_none: dict[str, Any] = {"items": None}
    await AsyncFlow(start=PartialBatch(max_concurrency=2)).run_async(s_none)
    assert s_none["res"] == []

    s_partial: dict[str, Any] = {"items": [1, 2, 3, 4, 5]}
    node = PartialBatch(max_concurrency=2, fail_mode="isolate_errors")
    await AsyncFlow(start=node).run_async(s_partial)
    assert s_partial["res"][0] == 10
    assert isinstance(s_partial["res"][1], IsolatedItemError)
    assert s_partial["res"][2] == 30
    assert node.last_bulkhead is not None
    assert node.last_bulkhead.peak_count <= 2


async def test_branch_isolation_and_sync_offloading() -> None:
    class SyncSleeper(Node):
        def prep(self, shared: dict[str, Any]) -> list[str]:
            time.sleep(0.03)
            self.params["cfg"]["items"].append(self.params["name"])
            return list(self.params["cfg"]["items"])

        def post(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
            shared.setdefault("branches", {})[self.params["name"]] = prep_res
            return "done"

    class IsolatedBF(AsyncParallelBatchFlow):
        async def prep_async(self, shared: dict[str, Any]) -> list[dict[str, Any]]:
            nested = {"items": ["base"]}
            return [{"name": "a", "cfg": nested}, {"name": "b", "cfg": nested}]

    ticks = 0

    async def heartbeat() -> None:
        nonlocal ticks
        for _ in range(3):
            await asyncio.sleep(0.004)
            ticks += 1

    shared: dict[str, Any] = {}
    bf = IsolatedBF(start=SyncSleeper(), isolate_branch_params=True, offload_sync_nodes=True)
    await asyncio.gather(bf.run_async(shared), heartbeat())
    assert shared["branches"] == {"a": ["base", "a"], "b": ["base", "b"]}
    assert ticks == 3


async def test_timeout_and_circuit_breaker() -> None:
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout_sec=10.0)

    class SlowOrFailNode(AsyncNode):
        async def exec_async(self, prep_res: Any) -> str:
            await asyncio.sleep(0.05)
            return "late"

    slow = SlowOrFailNode(timeout_sec=0.01, circuit_breaker=breaker)
    with pytest.raises(asyncio.TimeoutError):
        await slow.run_async({})
    with pytest.raises(asyncio.TimeoutError):
        await slow.run_async({})
    assert breaker.state == "OPEN"
    with pytest.raises(CircuitBreakerOpenError):
        await slow.run_async({})


def test_retry_policy_deterministic_jitter() -> None:
    policy = RetryPolicy(max_retries=4, base_delay_sec=0.02, max_delay_sec=0.5, seed=99)
    d1 = [policy.compute_delay(i) for i in range(3)]
    d2 = [policy.compute_delay(i) for i in range(3)]
    assert d1 == d2
    assert all(d > 0 for d in d1)
    assert policy.compute_delay(5000) <= policy.max_delay_sec


async def test_nested_retry_context_and_timeout_reset() -> None:
    observations: list[tuple[str, int, int]] = []

    class Inner(AsyncNode):
        async def prep_async(self, shared: dict[str, Any]) -> None:
            observations.append(("inner prep", self.cur_retry, outer.cur_retry))

        async def exec_async(self, prep_res: Any) -> None:
            observations.append(("inner exec", self.cur_retry, outer.cur_retry))
            await asyncio.Event().wait()

        async def exec_fallback_async(self, prep_res: Any, exc: Exception) -> str:
            assert isinstance(exc, TimeoutError)
            observations.append(("inner fallback", self.cur_retry, outer.cur_retry))
            return "recovered"

        async def post_async(self, shared: dict[str, Any], prep_res: Any, result: Any) -> str:
            observations.append(("inner post", self.cur_retry, outer.cur_retry))
            return str(result)

    class Outer(AsyncNode):
        async def exec_async(self, prep_res: Any) -> str:
            if self.cur_retry == 0:
                raise ValueError("temporary")
            return str(await Inner(timeout_sec=0.01).run_async({}))

    outer = Outer(max_retries=2)
    assert await outer._exec(None) == "recovered"
    assert observations == [
        ("inner prep", 0, 1), ("inner exec", 0, 1), ("inner fallback", 0, 1), ("inner post", 0, 1),
    ]
    assert outer.cur_retry == 0


@pytest.mark.parametrize("isolated", [False, True])
async def test_parallel_item_cancellation_drains_siblings(isolated: bool) -> None:
    sibling_started = asyncio.Event()
    cleaned = asyncio.Event()

    class Batch(AsyncParallelBatchNode):
        async def exec_async(self, item: int) -> int:
            if item == 0:
                await sibling_started.wait()
                if isolated:
                    raise asyncio.CancelledError
                raise ValueError("Item failed")
            sibling_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                cleaned.set()
            return item

    batch = Batch(max_concurrency=2, fail_mode="isolate_errors" if isolated else "fail_fast")
    expected = asyncio.CancelledError if isolated else ValueError
    with pytest.raises(expected):
        async with asyncio.timeout(1):
            await batch._exec(range(2))
    assert cleaned.is_set()
    assert batch.last_bulkhead is not None
    assert batch.last_bulkhead.active_count == 0


async def test_parallel_pool_limits_pulled_items_and_preserves_returned_objects() -> None:
    ready = asyncio.Event()
    release = asyncio.Event()
    produced: list[int] = []
    returned_value = ValueError("Regular result")

    def items() -> Any:
        for item in range(100):
            produced.append(item)
            yield item

    class Batch(AsyncParallelBatchNode):
        async def exec_async(self, item: int) -> ValueError:
            if item == 1:
                ready.set()
            await release.wait()
            return returned_value

    batch = Batch(max_concurrency=2, fail_mode="isolate_errors")
    task = asyncio.create_task(batch._exec(items()))
    await asyncio.wait_for(ready.wait(), timeout=1)
    assert produced == [0, 1]
    release.set()
    assert await task == [returned_value] * 100
    assert await batch._exec([]) == []
    assert batch.last_bulkhead is not None and batch.last_bulkhead.peak_count == 0
    with pytest.raises(ValueError, match="max_concurrency"):
        AsyncParallelBatchNode(max_concurrency=0)
    with pytest.raises(ValueError, match="max_concurrency"):
        AsyncParallelBatchFlow(max_concurrency=0)
    with pytest.raises(ValueError, match="attempts"):
        AsyncNode(max_retries=0)


async def test_half_open_probe_is_exclusive_and_stale_results_are_ignored() -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout_sec=0.0)
    stale = breaker.acquire()
    failed = breaker.acquire()
    assert stale is not None and failed is not None
    breaker.record_failure(failed)
    assert breaker.state == "HALF_OPEN"
    breaker.record_success(stale)
    assert breaker.state == "HALF_OPEN"

    acquired = await asyncio.gather(*(asyncio.to_thread(breaker.acquire) for _ in range(8)))
    permits = [permit for permit in acquired if permit is not None]
    assert len(permits) == 1
    breaker.release(permits[0])
    next_probe = breaker.acquire()
    assert next_probe is not None
    breaker.record_success(next_probe)
    assert str(breaker.state) == "CLOSED"


async def test_breaker_checks_each_retry_attempt() -> None:
    seen: list[int] = []

    class Faulty(AsyncNode):
        async def exec_async(self, prep_res: Any) -> None:
            seen.append(self.cur_retry)
            raise ValueError("Temporary")

    with pytest.raises(CircuitBreakerOpenError):
        await Faulty(max_retries=2, circuit_breaker=CircuitBreaker(
            failure_threshold=1,
        )).run_async({})
    assert seen == [0]


async def test_thread_offload_does_not_mutate_after_cancel_returns() -> None:
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()
    shared: dict[str, Any] = {}

    class Worker(Node):
        def post(self, state: dict[str, Any], prep_res: Any, result: Any) -> None:
            loop.call_soon_threadsafe(started.set)
            assert release.wait(2)
            state["done"] = True

    task = asyncio.create_task(AsyncFlow(start=Worker(), offload_sync_nodes=True).run_async(shared))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert shared == {}
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert shared == {"done": True}


async def test_parallel_nested_offload_handles_shared_updates_and_branch_params() -> None:
    class Increment(Node):
        def post(self, shared: dict[str, Any], prep_res: Any, result: Any) -> None:
            count = shared["count"]
            time.sleep(0.01)
            shared["count"] = count + 1

    class Parallel(AsyncParallelBatchFlow):
        async def prep_async(self, shared: dict[str, Any]) -> list[dict[str, Any]]:
            return [{}, {}]

    shared: dict[str, Any] = {"count": 0}
    await Parallel(start=AsyncFlow(start=Increment(), offload_sync_nodes=True)).run_async(shared)
    assert shared["count"] == 2

    class Remember(AsyncNode):
        async def post_async(self, shared: dict[str, Any], prep_res: Any, result: Any) -> None:
            self.params["nested"]["count"] += 1

    first, second = Remember(), Remember()
    first >> second
    branch = Parallel(start=first, isolate_branch_params=False, max_concurrency=1)
    params = {"nested": {"count": 0}}
    branch.set_params(params)
    await branch.run_async({})
    assert params["nested"]["count"] == 4


async def test_flow_guards_reset_per_execution() -> None:
    def options() -> dict[str, Any]:
        return {
            "budget_tracker": BudgetTracker(BudgetConfig(max_steps=1)),
            "oscillation_detector": OscillationDetector(repeat_threshold=2),
        }

    flow = AsyncFlow(start=AsyncNode(), **options())
    for _ in range(2):
        assert await flow.run_async({}) is None
