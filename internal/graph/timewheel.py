"""Fixed-capacity bucketed time-wheel ring buffer for O(1) trace expiration (ADR-004)."""

from __future__ import annotations

import math
from collections.abc import Hashable
from typing import Any


class BucketedTimeWheel:
    """Deterministic bucketed time-wheel replacing per-trace ``asyncio.TimerHandle`` timers."""

    __slots__ = (
        "_tick_ms",
        "_expiry_ticks",
        "_num_buckets",
        "_buckets",
        "_trace_bucket",
        "_current_tick",
        "_last_tick_ms",
    )

    def __init__(
        self,
        tick_ms: int = 50,
        expiry_ms: int = 1500,
        start_ms: int = 0,
    ) -> None:
        if tick_ms < 1:
            raise ValueError("tick_ms must be >= 1")
        if expiry_ms < tick_ms:
            raise ValueError("expiry_ms must be >= tick_ms")

        self._tick_ms = tick_ms
        self._expiry_ticks = max(1, math.ceil(expiry_ms / tick_ms))
        self._num_buckets = self._expiry_ticks + 1
        self._buckets: list[set[Any]] = [set() for _ in range(self._num_buckets)]
        self._trace_bucket: dict[Any, int] = {}
        self._current_tick = 0
        self._last_tick_ms = start_ms

    @property
    def tick_ms(self) -> int:
        return self._tick_ms

    @property
    def expiry_ticks(self) -> int:
        return self._expiry_ticks

    @property
    def num_buckets(self) -> int:
        return self._num_buckets

    def __len__(self) -> int:
        return len(self._trace_bucket)

    def __contains__(self, trace_key: Hashable) -> bool:
        return trace_key in self._trace_bucket

    def schedule(self, trace_key: Hashable, reschedule: bool = False) -> None:
        """Schedule ``trace_key`` to expire after ``expiry_ticks`` ticks."""
        existing = self._trace_bucket.get(trace_key)
        if existing is not None:
            if not reschedule:
                return
            self._buckets[existing].discard(trace_key)

        target_bucket = (self._current_tick + self._expiry_ticks) % self._num_buckets
        self._buckets[target_bucket].add(trace_key)
        self._trace_bucket[trace_key] = target_bucket

    def cancel(self, trace_key: Hashable) -> bool:
        """Cancel a scheduled ``trace_key`` in O(1) time."""
        bucket_idx = self._trace_bucket.pop(trace_key, None)
        if bucket_idx is None:
            return False
        self._buckets[bucket_idx].discard(trace_key)
        return True

    def tick(self) -> list[Any]:
        """Advance the wheel by one tick and return all expired trace keys."""
        self._current_tick = (self._current_tick + 1) % self._num_buckets
        self._last_tick_ms += self._tick_ms
        bucket = self._buckets[self._current_tick]
        if not bucket:
            return []
        expired = list(bucket)
        bucket.clear()
        for trace_key in expired:
            self._trace_bucket.pop(trace_key, None)
        return expired

    def advance_to(self, now_ms: int) -> list[Any]:
        """Advance the wheel deterministically up to ``now_ms`` and return expired traces."""
        if now_ms <= self._last_tick_ms:
            return []
        elapsed_ticks = (now_ms - self._last_tick_ms) // self._tick_ms
        if elapsed_ticks <= 0:
            return []

        steps = min(elapsed_ticks, self._num_buckets)
        expired: list[Any] = []
        for _ in range(steps):
            expired.extend(self.tick())
        if elapsed_ticks > steps:
            self._last_tick_ms += (elapsed_ticks - steps) * self._tick_ms
        return expired

    def drain_all(self) -> list[Any]:
        """Remove and return all remaining scheduled trace keys."""
        if not self._trace_bucket:
            return []
        remaining = list(self._trace_bucket.keys())
        self._trace_bucket.clear()
        for bucket in self._buckets:
            bucket.clear()
        return remaining
