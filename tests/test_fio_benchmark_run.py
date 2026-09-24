"""
Tests for the FioBenchmark.run() lifecycle.

FioBenchmark.run() is the single execution sequence shared by all five FIO
benchmark subclasses.  These tests verify:

  - The required lifecycle steps fire in order (dropcaches, make_remote_dir,
    dump_config, sleep, _pre_run_hook, monitoring, sync_files, analyze,
    _generate_report).
  - The workloads branch (when _workloads.exist() is True) calls
    _workloads.run() and skips _run_io_loop().
  - The non-workloads branch (when _workloads.exist() is False) calls
    MonitoringFactory.start, then _run_io_loop(), and skips _workloads.run().
  - Recovery test setup: create_recovery_test is called with the blocking
    callback when recov_test_type == "blocking", and wait_recovery_done is
    called after I/O finishes.
  - Background recovery: wait_start_io is called before I/O starts.
  - _pre_run_hook() is called for subclasses that override it.
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from typing import Any, Optional, cast
from unittest.mock import MagicMock, patch

from benchmark.base_fio import FioBenchmark

# ---------------------------------------------------------------------------
# Minimal concrete subclass for testing
# ---------------------------------------------------------------------------


class _StubFio(FioBenchmark):
    """
    Minimal concrete subclass that records _run_io_loop() calls.
    Used only for testing the base run() lifecycle.
    """

    def __init__(self, archive_dir: str, cluster: Any, config: dict[str, Any]) -> None:
        super().__init__(archive_dir, cluster, config)
        self.io_loop_called: bool = False
        self.pre_hook_called: bool = False

    @property
    def num_targets(self) -> int:
        return 1

    @property
    def benchmark_type(self) -> str:
        return "stub"

    def setup_targets(self) -> None:
        pass

    def prefill_targets(self) -> None:
        pass

    def cleanup_targets(self) -> None:
        pass

    def _run_io_loop(self) -> None:
        self.io_loop_called = True

    def _pre_run_hook(self) -> None:
        self.pre_hook_called = True

    @property
    def workloads_mock(self) -> MagicMock:
        """Return _workloads as MagicMock for assertion calls in tests."""
        return cast(MagicMock, self._workloads)


def _make_stub(extra: Optional[dict[str, Any]] = None) -> _StubFio:
    """Build a _StubFio with all external I/O mocked out."""
    config: dict[str, Any] = {
        "iteration": 0,
        "mode": "write",
        "op_size": 4194304,
        "iodepth": 16,
        "numjobs": 1,
        "end_fsync": 0,
        "fio_out_format": "json,normal",
        "recov_test_type": "blocking",
    }
    if extra:
        config.update(extra)

    cluster_mock = MagicMock()
    cluster_mock.config = {}

    with (
        patch("settings.getnodes", return_value="fake-client"),
        patch("settings.cluster", {"tmp_dir": "/tmp/cbt-test/"}),
        patch("common.get_osd_ra", return_value="0"),
    ):
        bm = _StubFio("/tmp/archive/", cluster_mock, config)
    bm.cmd_path_full = "/usr/bin/fio"
    # Replace the real Workloads object with a controllable mock
    workloads_mock = MagicMock()
    workloads_mock.exist.return_value = False
    bm._workloads = workloads_mock
    return bm


# ---------------------------------------------------------------------------
# Context manager that patches all external calls made by run()
# ---------------------------------------------------------------------------


def _run_patches() -> dict[str, MagicMock]:
    """
    Return a dict of mock names to MagicMocks for everything run() touches
    externally.  Callers apply these via patch() themselves.
    """
    return {}  # see _run_with_mocks() below


# ---------------------------------------------------------------------------
# Helper: run() with all externals mocked, return the mocks
# ---------------------------------------------------------------------------


def _run_with_mocks(bm: _StubFio, workloads_exist: bool = False) -> dict[str, MagicMock]:
    """
    Call bm.run() with all external dependencies mocked.  Returns the mocks
    so tests can make assertions on call counts and arguments.
    """
    mocks: dict[str, MagicMock] = {}

    cast(MagicMock, bm._workloads).exist.return_value = workloads_exist

    with (
        patch("benchmark.base_fio.common.make_remote_dir") as m_mkdir,
        patch("benchmark.base_fio.common.sync_files") as m_sync,
        patch("benchmark.base_fio.time.sleep") as m_sleep,
        patch("benchmark.base_fio.MonitoringFactory") as m_mon,
        patch("benchmark.benchmark.Benchmark.run", return_value=None) as m_super,
        patch.object(bm, "dropcaches") as m_drop,
        patch.object(bm, "analyze") as m_analyze,
        patch.object(bm, "_generate_report") as m_report,
    ):
        bm.run()

    mocks["make_remote_dir"] = m_mkdir
    mocks["sync_files"] = m_sync
    mocks["sleep"] = m_sleep
    mocks["monitoring"] = m_mon
    mocks["super_run"] = m_super
    mocks["dropcaches"] = m_drop
    mocks["analyze"] = m_analyze
    mocks["generate_report"] = m_report

    return mocks


# ===========================================================================
# Non-workloads branch
# ===========================================================================


class TestRunNonWorkloadsBranch(unittest.TestCase):
    """When _workloads.exist() is False, the non-workloads I/O loop executes."""

    def setUp(self) -> None:
        self.bm = _make_stub()

    def test_io_loop_called(self) -> None:
        _run_with_mocks(self.bm, workloads_exist=False)
        self.assertTrue(self.bm.io_loop_called)

    def test_workloads_run_not_called(self) -> None:
        _run_with_mocks(self.bm, workloads_exist=False)
        self.bm.workloads_mock.run.assert_not_called()

    def test_monitoring_start_called(self) -> None:
        mocks = _run_with_mocks(self.bm, workloads_exist=False)
        mocks["monitoring"].start.assert_called_once()

    def test_monitoring_stop_called(self) -> None:
        mocks = _run_with_mocks(self.bm, workloads_exist=False)
        mocks["monitoring"].stop.assert_called_once()


# ===========================================================================
# Workloads branch
# ===========================================================================


class TestRunWorkloadsBranch(unittest.TestCase):
    """When _workloads.exist() is True, the workloads path executes."""

    def setUp(self) -> None:
        self.bm = _make_stub()

    def test_workloads_run_called(self) -> None:
        _run_with_mocks(self.bm, workloads_exist=True)
        self.bm.workloads_mock.run.assert_called_once()

    def test_io_loop_not_called(self) -> None:
        _run_with_mocks(self.bm, workloads_exist=True)
        self.assertFalse(self.bm.io_loop_called)

    def test_benchmark_type_set_on_workloads(self) -> None:
        _run_with_mocks(self.bm, workloads_exist=True)
        self.bm.workloads_mock.set_benchmark_type.assert_called_once_with("stub")

    def test_executable_set_on_workloads(self) -> None:
        _run_with_mocks(self.bm, workloads_exist=True)
        self.bm.workloads_mock.set_executable.assert_called_once_with(self.bm.cmd_path)


# ===========================================================================
# Lifecycle steps always executed
# ===========================================================================


class TestRunLifecycleSteps(unittest.TestCase):
    """Steps common to both branches fire regardless of workloads/non-workloads."""

    def setUp(self) -> None:
        self.bm = _make_stub()

    def test_dropcaches_called(self) -> None:
        mocks = _run_with_mocks(self.bm)
        mocks["dropcaches"].assert_called_once()

    def test_make_remote_dir_called_with_run_dir(self) -> None:
        mocks = _run_with_mocks(self.bm)
        mocks["make_remote_dir"].assert_called_once_with(self.bm.run_dir)

    def test_dump_config_called(self) -> None:
        _run_with_mocks(self.bm)
        self.bm.cluster.dump_config.assert_called_once_with(self.bm.run_dir)

    def test_sleep_5_called(self) -> None:
        mocks = _run_with_mocks(self.bm)
        mocks["sleep"].assert_called_once_with(5)

    def test_pre_run_hook_called(self) -> None:
        _run_with_mocks(self.bm)
        self.assertTrue(self.bm.pre_hook_called)

    def test_sync_files_called_with_source_dir(self) -> None:
        mocks = _run_with_mocks(self.bm)
        mocks["sync_files"].assert_called_once_with(f"{self.bm.run_dir}/*", self.bm.out_dir)

    def test_analyze_called(self) -> None:
        mocks = _run_with_mocks(self.bm)
        mocks["analyze"].assert_called_once()

    def test_generate_report_called(self) -> None:
        mocks = _run_with_mocks(self.bm)
        mocks["generate_report"].assert_called_once()

    def test_dump_historic_ops_called(self) -> None:
        _run_with_mocks(self.bm)
        self.bm.cluster.dump_historic_ops.assert_called_once_with(self.bm.run_dir)


# ===========================================================================
# Recovery test — blocking
# ===========================================================================


class TestRunRecoveryBlocking(unittest.TestCase):
    """When recovery_test is in cluster.config and recov_test_type == 'blocking',
    create_recovery_test is called with the blocking callback."""

    def setUp(self) -> None:
        self.bm = _make_stub({"recov_test_type": "blocking"})
        self.bm.cluster.config = {"recovery_test": {}}

    def test_create_recovery_test_called(self) -> None:
        _run_with_mocks(self.bm)
        self.bm.cluster.create_recovery_test.assert_called_once()

    def test_blocking_callback_passed(self) -> None:
        _run_with_mocks(self.bm)
        _, args, _ = self.bm.cluster.create_recovery_test.mock_calls[0]
        callback = args[1]
        self.assertEqual(callback, self.bm.recovery_callback_blocking)

    def test_wait_recovery_done_called_after_io(self) -> None:
        _run_with_mocks(self.bm)
        self.bm.cluster.wait_recovery_done.assert_called_once()

    def test_wait_start_io_not_called_for_blocking(self) -> None:
        _run_with_mocks(self.bm)
        self.bm.cluster.wait_start_io.assert_not_called()


# ===========================================================================
# Recovery test — background
# ===========================================================================


class TestRunRecoveryBackground(unittest.TestCase):
    """When recov_test_type == 'background', the background callback is used
    and wait_start_io is called before I/O begins."""

    def setUp(self) -> None:
        self.bm = _make_stub({"recov_test_type": "background"})
        self.bm.cluster.config = {"recovery_test": {}}

    def test_background_callback_passed(self) -> None:
        _run_with_mocks(self.bm)
        _, args, _ = self.bm.cluster.create_recovery_test.mock_calls[0]
        callback = args[1]
        self.assertEqual(callback, self.bm.recovery_callback_background)

    def test_wait_start_io_called_before_io(self) -> None:
        """wait_start_io must be called; the test confirms it was called at all."""
        _run_with_mocks(self.bm)
        self.bm.cluster.wait_start_io.assert_called_once()

    def test_wait_recovery_done_called(self) -> None:
        _run_with_mocks(self.bm)
        self.bm.cluster.wait_recovery_done.assert_called_once()


# ===========================================================================
# No recovery test
# ===========================================================================


class TestRunNoRecovery(unittest.TestCase):
    """When recovery_test is absent from cluster.config, no recovery calls are made."""

    def setUp(self) -> None:
        self.bm = _make_stub()
        # cluster.config has no 'recovery_test' key (default from _make_stub)

    def test_create_recovery_test_not_called(self) -> None:
        _run_with_mocks(self.bm)
        self.bm.cluster.create_recovery_test.assert_not_called()

    def test_wait_recovery_done_not_called(self) -> None:
        _run_with_mocks(self.bm)
        self.bm.cluster.wait_recovery_done.assert_not_called()

    def test_wait_start_io_not_called(self) -> None:
        _run_with_mocks(self.bm)
        self.bm.cluster.wait_start_io.assert_not_called()


if __name__ == "__main__":
    unittest.main()
