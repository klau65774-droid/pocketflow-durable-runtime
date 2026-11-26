from __future__ import annotations

import asyncio
import importlib.util
import io
import os
import sys
import time
import unittest
from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pocketflow_runtime.compat as compat_mod
import pytest
from pocketflow_runtime.config import get_project_root
from pocketflow_runtime.engine import load_upstream_pocketflow


def _run_upstream_test_file(test_file: Path, pocketflow_module: Any) -> unittest.TestResult:
    """Execute an unmodified upstream unittest file against a supplied pocketflow module."""
    prev_mod = sys.modules.get("pocketflow")
    prev_sys_path = sys.path.copy()
    sys.modules["pocketflow"] = pocketflow_module
    mod_name = f"_compat_suite_{test_file.stem}_{id(pocketflow_module)}"
    try:
        spec = importlib.util.spec_from_file_location(mod_name, str(test_file))
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        exec(compile(test_file.read_bytes(), str(test_file), "exec"), module.__dict__)
        suite = unittest.defaultTestLoader.loadTestsFromModule(module)
        stream = io.StringIO()
        runner = unittest.TextTestRunner(stream=stream, verbosity=0)
        return runner.run(suite)
    finally:
        sys.path[:] = prev_sys_path
        sys.modules.pop(mod_name, None)
        if prev_mod is not None:
            sys.modules["pocketflow"] = prev_mod
        else:
            sys.modules.pop("pocketflow", None)


@pytest.mark.parametrize("implementation", ["upstream_baseline", "compat"])
def test_all_upstream_suites(implementation: str) -> None:
    """Run every pinned upstream suite as written against both implementations."""
    tests_dir = get_project_root() / "backend" / "upstream_baseline" / "tests"
    test_files = sorted(tests_dir.glob("test_*.py"))
    assert len(test_files) == 10
    module = load_upstream_pocketflow() if implementation == "upstream_baseline" else compat_mod
    # Older Windows CPython uses a coarse loop clock. Equal timer deadlines make
    # two upstream aggregators observe branch completions in arbitrary order.
    use_precise_clock = (
        sys.platform == "win32"
        and time.get_clock_info("monotonic").resolution > time.get_clock_info(
            "perf_counter"
        ).resolution
        and os.getenv("POCKETFLOW_NATIVE_TEST_CLOCK") != "1"
    )

    def precise_loop_time(_loop: asyncio.BaseEventLoop) -> float:
        return time.perf_counter()

    clock = patch.object(asyncio.BaseEventLoop, "time", precise_loop_time)
    with clock if use_precise_clock else nullcontext():
        results = [_run_upstream_test_file(path, module) for path in test_files]
    assert sum(result.testsRun for result in results) == 56
    skipped = [reason for result in results for _, reason in result.skipped]
    failures = [
        f"{test.id()}\n{detail}"
        for result in results
        for test, detail in result.errors + result.failures
    ]
    assert not skipped
    assert not failures, "\n".join(failures)
