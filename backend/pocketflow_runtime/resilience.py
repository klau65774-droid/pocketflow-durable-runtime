from __future__ import annotations

import asyncio
import math
import random
import threading
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Literal


class CircuitBreakerOpenError(RuntimeError):
    """Raised when a node/tool circuit breaker is open and rejects calls."""


class BudgetExceededError(RuntimeError):
    """Raised when a workflow exceeds step, wall-clock, or token budget."""


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Exponential backoff with deterministic seeded jitter."""

    max_retries: int = 3
    base_delay_sec: float = 0.01
    max_delay_sec: float = 1.0
    jitter_ratio: float = 0.25
    seed: int = 42

    def compute_delay(self, attempt: int) -> float:
        if attempt < 0 or self.base_delay_sec <= 0:
            return 0.0
        try:
            raw = min(self.max_delay_sec, self.base_delay_sec * (2.0**attempt))
        except OverflowError:
            raw = self.max_delay_sec
        rng = random.Random(self.seed + attempt)
        jitter = raw * self.jitter_ratio * rng.uniform(-1.0, 1.0)
        return float(max(0.0, min(self.max_delay_sec, raw + jitter)))


CircuitState = Literal["CLOSED", "OPEN", "HALF_OPEN"]


@dataclass(slots=True)
class CircuitBreaker:
    """Stateful circuit breaker protecting flaky downstream tools/nodes."""

    failure_threshold: int = 3
    recovery_timeout_sec: float = 5.0
    _state: CircuitState = field(default="CLOSED", init=False)
    _failure_count: int = field(default=0, init=False)
    _opened_at: float = field(default=0.0, init=False)
    _generation: int = field(default=0, init=False)
    _probe_active: bool = field(default=False, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _legacy_attempt: ContextVar[int | None] = field(
        default_factory=lambda: ContextVar("breaker_attempt", default=None), init=False, repr=False
    )

    def __post_init__(self) -> None:
        if self.failure_threshold < 1 or (
            not math.isfinite(self.recovery_timeout_sec) or self.recovery_timeout_sec < 0
        ):
            raise ValueError("Invalid circuit breaker recovery configuration")

    def _advance(self) -> CircuitState:
        elapsed = time.monotonic() - self._opened_at
        if self._state == "OPEN" and elapsed >= self.recovery_timeout_sec:
            self._state = "HALF_OPEN"
        return self._state

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._advance()

    @property
    def failure_count(self) -> int:
        with self._lock:
            return self._failure_count

    def acquire(self) -> int | None:
        """Admit one call and return the generation its outcome may update."""
        with self._lock:
            state = self._advance()
            if state == "OPEN" or self._probe_active:
                return None
            self._probe_active = state == "HALF_OPEN"
            return self._generation

    def allow_request(self) -> bool:
        attempt = self.acquire()
        self._legacy_attempt.set(attempt)
        return attempt is not None

    def _result_is_current(self, attempt: int | None) -> bool:
        if attempt is None:
            attempt = self._legacy_attempt.get()
            self._legacy_attempt.set(None)
        return self._state != "OPEN" and (attempt is None or attempt == self._generation)

    def record_success(self, attempt: int | None = None) -> None:
        with self._lock:
            if self._result_is_current(attempt):
                if self._state == "HALF_OPEN":
                    self._generation += 1
                self._failure_count = 0
                self._probe_active = False
                self._state = "CLOSED"

    def record_failure(self, attempt: int | None = None) -> None:
        with self._lock:
            if self._result_is_current(attempt):
                self._failure_count += 1
                if self._state == "HALF_OPEN" or self._failure_count >= self.failure_threshold:
                    self._generation += 1
                    self._probe_active = False
                    self._state = "OPEN"
                    self._opened_at = time.monotonic()

    def release(self, attempt: int) -> None:
        """Relinquish a cancelled probe so another caller can check recovery."""
        with self._lock:
            if self._probe_active and attempt == self._generation:
                self._generation += 1
                self._probe_active = False


@dataclass(slots=True)
class ConcurrencyBulkhead:
    """Asyncio semaphore bulkhead tracking active & peak concurrency."""

    limit: int
    _semaphore: asyncio.Semaphore = field(init=False)
    active_count: int = field(default=0, init=False)
    peak_count: int = field(default=0, init=False)
    total_acquired: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("Bulkhead limit must be >= 1")
        self._semaphore = asyncio.Semaphore(self.limit)

    async def __aenter__(self) -> ConcurrencyBulkhead:
        await self._semaphore.acquire()
        self.active_count += 1
        self.total_acquired += 1
        if self.active_count > self.peak_count:
            self.peak_count = self.active_count
        return self

    async def __aexit__(self, *args: object) -> None:
        self.active_count = max(0, self.active_count - 1)
        self._semaphore.release()


@dataclass(frozen=True, slots=True)
class BudgetConfig:
    """Hard execution limits preventing runaway workflows."""

    max_steps: int = 64
    max_wall_clock_sec: float = 30.0
    max_virtual_tokens: int = 100_000


@dataclass(slots=True)
class BudgetTracker:
    """Tracks step count, wall-clock duration, and virtual token usage."""

    config: BudgetConfig
    steps_used: int = field(default=0, init=False)
    tokens_used: int = field(default=0, init=False)
    started_at: float = field(default_factory=time.monotonic, init=False)

    def check_and_consume(self, tokens: int = 0) -> None:
        self.steps_used += 1
        self.tokens_used += max(0, tokens)
        elapsed = time.monotonic() - self.started_at
        if self.steps_used > self.config.max_steps:
            raise BudgetExceededError(
                f"Step budget exceeded: {self.steps_used} > {self.config.max_steps}"
            )
        if elapsed > self.config.max_wall_clock_sec:
            limit_s = self.config.max_wall_clock_sec
            raise BudgetExceededError(
                f"Wall-clock budget exceeded: {elapsed:.2f}s > {limit_s:.2f}s"
            )
        if self.tokens_used > self.config.max_virtual_tokens:
            raise BudgetExceededError(
                f"Token budget exceeded: {self.tokens_used} > {self.config.max_virtual_tokens}"
            )
