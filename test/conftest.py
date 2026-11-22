"""Pytest configuration and shared CLI options for chaos and performance soak suites."""

from __future__ import annotations

import os

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register ``--soak-seconds`` CLI option for performance soak tests."""
    parser.addoption(
        "--soak-seconds",
        action="store",
        type=float,
        default=None,
        help="Duration in seconds for performance/soak benchmarks (default: 2.0s smoke).",
    )


@pytest.fixture
def soak_seconds(request: pytest.FixtureRequest) -> float:
    """Resolve soak duration from ``--soak-seconds``, ``SOAK_DURATION_SECONDS``, or 2.0s."""
    cli_val = request.config.getoption("--soak-seconds")
    if cli_val is not None:
        return max(0.5, float(cli_val))
    env_val = os.environ.get("SOAK_DURATION_SECONDS", "").strip()
    if env_val:
        return max(0.5, float(env_val))
    return 2.0
