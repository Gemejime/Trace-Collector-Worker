"""jemalloc mallctl stats scraper with clean fallback when libjemalloc is absent."""

from __future__ import annotations

import ctypes
import sys
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True, frozen=True)
class JemallocStats:
    """Snapshot of jemalloc native allocator counters."""

    allocated_bytes: int
    resident_bytes: int
    mapped_bytes: int
    available: bool


class JemallocStatsScraper:
    """Scrapes ``stats.allocated``, ``stats.resident``, and ``stats.mapped`` via ``mallctl``."""

    def __init__(self, mallctl_fn: Any | None = None) -> None:
        self._mallctl = mallctl_fn if mallctl_fn is not None else self._discover_mallctl()

    @staticmethod
    def _discover_mallctl() -> Any | None:
        if sys.platform == "win32":
            return None
        try:
            libc = ctypes.CDLL(None)
            fn = getattr(libc, "mallctl", None)
            if fn is None:
                return None
            fn.argtypes = [
                ctypes.c_char_p,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_size_t),
                ctypes.c_void_p,
                ctypes.c_size_t,
            ]
            fn.restype = ctypes.c_int
            return fn
        except (OSError, TypeError, AttributeError):
            return None

    @property
    def available(self) -> bool:
        return self._mallctl is not None

    def _read_size_t(self, name: bytes) -> int | None:
        if self._mallctl is None:
            return None
        val = ctypes.c_size_t(0)
        sz = ctypes.c_size_t(ctypes.sizeof(val))
        rc = int(self._mallctl(name, ctypes.byref(val), ctypes.byref(sz), None, 0))
        if rc != 0:
            return None
        return int(val.value)

    def _advance_epoch(self) -> bool:
        if self._mallctl is None:
            return False
        epoch = ctypes.c_uint64(1)
        sz = ctypes.c_size_t(ctypes.sizeof(epoch))
        rc = int(
            self._mallctl(
                b"epoch",
                ctypes.byref(epoch),
                ctypes.byref(sz),
                ctypes.byref(epoch),
                ctypes.sizeof(epoch),
            )
        )
        return rc == 0

    def scrape(self) -> JemallocStats:
        """Advance jemalloc epoch and read allocated/resident/mapped counters."""
        if self._mallctl is None:
            return JemallocStats(
                allocated_bytes=0,
                resident_bytes=0,
                mapped_bytes=0,
                available=False,
            )
        try:
            if not self._advance_epoch():
                return JemallocStats(0, 0, 0, available=False)
            allocated = self._read_size_t(b"stats.allocated")
            resident = self._read_size_t(b"stats.resident")
            mapped = self._read_size_t(b"stats.mapped")
            if allocated is None or resident is None or mapped is None:
                return JemallocStats(0, 0, 0, available=False)
            return JemallocStats(
                allocated_bytes=allocated,
                resident_bytes=resident,
                mapped_bytes=mapped,
                available=True,
            )
        except Exception:
            return JemallocStats(0, 0, 0, available=False)
