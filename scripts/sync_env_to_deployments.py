"""Sync and drift-check environment variables from .env.prod to Helm values.yaml."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml
from dotenv import dotenv_values

SENSITIVE_KEY_TOKENS: tuple[str, ...] = ("PASSWORD", "SECRET", "TOKEN", "KEY")
DEFAULT_ENV_FILE = Path(".env.prod")
DEFAULT_VALUES_FILE = Path("deployments/helm/values.yaml")


def _configure_utf8_stdio() -> None:
    """Ensure UTF-8 stdout/stderr on Windows consoles."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def is_sensitive_key(key: str) -> bool:
    """Return True if the environment variable name indicates a secret or credential."""
    upper = key.upper()
    return any(token in upper for token in SENSITIVE_KEY_TOKENS)


def load_env_prod(env_path: Path) -> dict[str, str]:
    """Load non-sensitive environment variables from .env.prod."""
    if not env_path.is_file():
        raise FileNotFoundError(f"Source env file not found: {env_path}")

    raw_values = dotenv_values(env_path, encoding="utf-8")
    filtered: dict[str, str] = {}
    for key, val in raw_values.items():
        if val is None or is_sensitive_key(key):
            continue
        filtered[key] = str(val)
    return filtered


def build_expected_env(
    env_values: dict[str, str],
    existing_values: dict[str, Any],
) -> dict[str, str]:
    """Construct the expected .Values.env mapping while preserving secretName."""
    existing_env = existing_values.get("env")
    secret_name = ""
    if isinstance(existing_env, dict) and existing_env.get("secretName") is not None:
        secret_name = str(existing_env["secretName"])

    expected: dict[str, str] = {"secretName": secret_name}
    for key, val in env_values.items():
        if key == "secretName":
            continue
        expected[key] = val
    return expected


def compute_env_drift(
    existing_env: Any,
    expected_env: dict[str, str],
) -> tuple[list[str], list[str], list[tuple[str, str, str]]]:
    """Return (missing_keys, extra_keys, changed_items) between existing and expected env."""
    if not isinstance(existing_env, dict):
        return (list(expected_env.keys()), [], [])

    existing_norm = {str(k): "" if v is None else str(v) for k, v in existing_env.items()}
    missing = [k for k in expected_env if k not in existing_norm]
    extra = [k for k in existing_norm if k not in expected_env]
    changed = [
        (k, existing_norm[k], expected_env[k])
        for k in expected_env
        if k in existing_norm and existing_norm[k] != expected_env[k]
    ]
    return (missing, extra, changed)


def _render_env_block(expected_env: dict[str, str]) -> str:
    """Render the top-level `env:` YAML block with double-quoted string values."""

    class _QuotedStr(str):
        pass

    class _QuotedDumper(yaml.SafeDumper):
        pass

    def _represent_quoted_str(dumper: yaml.SafeDumper, data: _QuotedStr) -> yaml.ScalarNode:
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style='"')

    _QuotedDumper.add_representer(_QuotedStr, _represent_quoted_str)
    payload = {"env": {k: _QuotedStr(v) for k, v in expected_env.items()}}
    rendered = yaml.dump(
        payload,
        Dumper=_QuotedDumper,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=4096,
    )
    return rendered.rstrip() + "\n"


def update_values_file(values_path: Path, expected_env: dict[str, str]) -> None:
    """Replace or append the top-level `env:` block in values.yaml without altering other blocks."""
    original_text = values_path.read_text(encoding="utf-8")
    env_block = _render_env_block(expected_env)

    # Match top-level `env:` section up to the next top-level key or EOF.
    pattern = re.compile(r"(?ms)^env:[ \t]*\n(?:[ \t]+[^\n]*\n|[ \t]*\n)*")
    if pattern.search(original_text):
        updated_text = pattern.sub(env_block, original_text, count=1)
    else:
        separator = "" if original_text.endswith("\n") else "\n"
        updated_text = f"{original_text}{separator}\n{env_block}"

    values_path.write_text(updated_text, encoding="utf-8")


def run_sync(env_path: Path, values_path: Path, *, check_only: bool) -> int:
    """Execute --sync or --check-only and return process exit code."""
    if not values_path.is_file():
        print(f"ERROR: Helm values file not found: {values_path}", file=sys.stderr)
        return 1

    try:
        env_values = load_env_prod(env_path)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    raw_values = yaml.safe_load(values_path.read_text(encoding="utf-8"))
    if not isinstance(raw_values, dict):
        print(f"ERROR: {values_path} must contain a top-level YAML mapping.", file=sys.stderr)
        return 1

    expected_env = build_expected_env(env_values, raw_values)
    missing, extra, changed = compute_env_drift(raw_values.get("env"), expected_env)
    has_drift = bool(missing or extra or changed)

    if check_only:
        if not has_drift:
            print(f"OK: {values_path} is in sync with {env_path} ({len(expected_env)} keys).")
            return 0
        print(f"DRIFT DETECTED between {env_path} and {values_path}:", file=sys.stderr)
        for key in missing:
            print(f"  + missing in values.yaml: {key}", file=sys.stderr)
        for key in extra:
            print(f"  - extra in values.yaml:   {key}", file=sys.stderr)
        for key, old_val, new_val in changed:
            print(f"  ~ changed {key}: {old_val!r} -> {new_val!r}", file=sys.stderr)
        print(
            f"Run `python scripts/sync_env_to_deployments.py --sync` to update {values_path}.",
            file=sys.stderr,
        )
        return 1

    if not has_drift:
        print(f"OK: {values_path} is already up to date ({len(expected_env)} keys).")
        return 0

    update_values_file(values_path, expected_env)
    print(f"SYNCED: Updated {values_path} from {env_path} ({len(expected_env)} keys).")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Synchronize .env.prod into deployments/helm/values.yaml (.Values.env)."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--sync",
        action="store_true",
        help="Update deployments/helm/values.yaml from .env.prod.",
    )
    mode.add_argument(
        "--check-only",
        action="store_true",
        help="Verify zero drift between .env.prod and deployments/helm/values.yaml.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="Path to source .env.prod file (default: .env.prod).",
    )
    parser.add_argument(
        "--values-file",
        type=Path,
        default=DEFAULT_VALUES_FILE,
        help="Path to Helm values.yaml (default: deployments/helm/values.yaml).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    _configure_utf8_stdio()
    args = parse_args(argv)
    return run_sync(args.env_file, args.values_file, check_only=bool(args.check_only))


if __name__ == "__main__":
    raise SystemExit(main())
