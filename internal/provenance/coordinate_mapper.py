"""Pure Python bounding box coordinate normalization to unit floats [0.0, 1.0]."""

from __future__ import annotations

import math
from collections.abc import Sequence

UnitBBox = tuple[float, float, float, float]
_ZERO_BBOX: UnitBBox = (0.0, 0.0, 0.0, 0.0)


def _clamp_unit(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    if value <= 0.0:
        return 0.0
    if value >= 1.0:
        return 1.0
    return value


def _to_finite_float(raw: object) -> float:
    if isinstance(raw, bool):
        return 0.0
    if isinstance(raw, (int, float)):
        try:
            val = float(raw)
        except OverflowError:
            return 0.0
        return val if math.isfinite(val) else 0.0
    try:
        val = float(str(raw))
    except (TypeError, ValueError):
        return 0.0
    return val if math.isfinite(val) else 0.0


def normalize_bbox(
    bbox: Sequence[float] | Sequence[object],
    page_width: float | None = None,
    page_height: float | None = None,
) -> UnitBBox:
    """Normalize ``[x1, y1, x2, y2]`` coordinates into clamped unit floats ``[0.0, 1.0]``.

    Supports both already-normalized coordinates and pixel coordinates when positive
    ``page_width`` and/or ``page_height`` are supplied. Non-finite inputs (NaN/inf),
    out-of-range values, and inverted bounds (``x1 > x2`` or ``y1 > y2``) are handled
    deterministically without raising.
    """
    if len(bbox) != 4:
        return _ZERO_BBOX

    x1 = _to_finite_float(bbox[0])
    y1 = _to_finite_float(bbox[1])
    x2 = _to_finite_float(bbox[2])
    y2 = _to_finite_float(bbox[3])

    if page_width is not None and math.isfinite(page_width) and page_width > 0.0:
        x1 /= page_width
        x2 /= page_width
    if page_height is not None and math.isfinite(page_height) and page_height > 0.0:
        y1 /= page_height
        y2 /= page_height

    x1 = _clamp_unit(x1)
    y1 = _clamp_unit(y1)
    x2 = _clamp_unit(x2)
    y2 = _clamp_unit(y2)

    if x1 > x2:
        x1, x2 = x2, x1
    if y1 > y2:
        y1, y2 = y2, y1

    return (x1, y1, x2, y2)
