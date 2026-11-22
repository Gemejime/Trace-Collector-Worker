"""Command entrypoints package.

Re-exports Python's standard library ``cmd`` module attributes (such as ``Cmd``)
so importing ``pdb`` or other stdlib modules that depend on ``cmd.Cmd`` continues
to work when the repository root is on ``sys.path``.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

_stdlib_cmd_path = Path(os.__file__).resolve().parent / "cmd.py"
_spec = importlib.util.spec_from_file_location("_stdlib_cmd", _stdlib_cmd_path)
if _spec is not None and _spec.loader is not None:
    _stdlib_cmd = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_stdlib_cmd)
    for _attr in dir(_stdlib_cmd):
        if not _attr.startswith("__"):
            globals()[_attr] = getattr(_stdlib_cmd, _attr)
