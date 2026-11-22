"""Unit tests for bounding box coordinate normalization."""

from __future__ import annotations

import math

from internal.provenance.coordinate_mapper import normalize_bbox


def test_normalize_bbox_clamps_unit_coordinates() -> None:
    assert normalize_bbox([0.1, 0.2, 0.8, 0.9]) == (0.1, 0.2, 0.8, 0.9)
    assert normalize_bbox([-0.5, -1.0, 1.5, 2.0]) == (0.0, 0.0, 1.0, 1.0)


def test_normalize_bbox_pixel_to_unit() -> None:
    x1, y1, x2, y2 = normalize_bbox([100, 200, 500, 800], page_width=1000.0, page_height=1000.0)
    assert (x1, y1, x2, y2) == (0.1, 0.2, 0.5, 0.8)


def test_normalize_bbox_handles_inverted_and_non_finite_values() -> None:
    assert normalize_bbox([0.9, 0.8, 0.2, 0.1]) == (0.2, 0.1, 0.9, 0.8)
    assert normalize_bbox([math.nan, math.inf, -math.inf, 0.5]) == (0.0, 0.0, 0.0, 0.5)
    assert normalize_bbox([0.1, 0.2]) == (0.0, 0.0, 0.0, 0.0)
