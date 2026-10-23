# pylint: disable=protected-access,abstract-class-instantiated
"""
Unit tests for FioBenchmark (benchmark/base_fio.py).

These tests verify:
  - All common FIO parameters are parsed correctly from config.
  - total_iodepth / iodepth precedence is correct.
  - time / ramp are stored as Optional[int], not str(None).
  - calculate_iodepth_per_target() dispatches and distributes correctly.
  - distribute_total_iodepth() edge cases (even, remainder, underflow).
  - parse_results() extracts JSON blocks into json_output.* files.
  - analyze() delegates to parse_results().
  - recovery_callback_blocking() and recovery_callback_background().
  - Instantiating FioBenchmark without implementing abstract methods raises TypeError.
"""

# pyright: strict, reportPrivateUsage=false

import json
import logging
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from benchmark.base_fio import FioBenchmark
from iodepth import distribute_total_iodepth

# ---------------------------------------------------------------------------
# Concrete stub — implements all four abstract methods so we can instantiate
# ---------------------------------------------------------------------------


class _ConcreteFioBenchmark(FioBenchmark):
    """Minimal concrete subclass used purely for testing FioBenchmark."""

    @property
    def num_targets(self) -> int:
        return 1

    @property
    def benchmark_type(self) -> str:
        return "test"

    def setup_targets(self) -> None:
        pass

    def prefill_targets(self) -> None:
        pass

    def cleanup_targets(self) -> None:
        pass

    def _run_io_loop(self) -> None:
        pass


def _make(extra: Optional[dict[str, Any]] = None) -> _ConcreteFioBenchmark:
    """
    Build a _ConcreteFioBenchmark with minimal config, bypassing all
    cluster / settings I/O.
    """
    base_config: dict[str, Any] = {
        "iteration": 0,
        "mode": "write",
        "op_size": 4194304,
        "iodepth": 16,
        "numjobs": 1,
        "end_fsync": 0,
        "rwmixread": 50,
        "fio_out_format": "json,normal",
        "recov_test_type": "blocking",
    }
    if extra:
        base_config.update(extra)

    cluster_mock = MagicMock()
    cluster_mock.config = {}

    with (
        patch("settings.getnodes", return_value="fake-client"),
        patch("settings.cluster", {"tmp_dir": "/tmp/cbt-test/"}),
        patch("common.get_osd_ra", return_value="0"),
    ):
        bm = _ConcreteFioBenchmark("/tmp/archive/", cluster_mock, base_config)
    return bm


# ---------------------------------------------------------------------------
# Abstract enforcement
# ---------------------------------------------------------------------------


class TestFioBenchmarkAbstract(unittest.TestCase):
    """FioBenchmark cannot be instantiated without all abstract methods."""

    def test_cannot_instantiate_directly(self) -> None:
        """Instantiating FioBenchmark directly raises TypeError."""
        with self.assertRaises(TypeError):
            FioBenchmark("/tmp/archive/", MagicMock(), {"iteration": 0})  # type: ignore[abstract]

    def test_missing_num_targets_raises(self) -> None:
        """A subclass missing num_targets / _run_io_loop raises TypeError."""
        class _Missing(FioBenchmark):
            def setup_targets(self) -> None:
                pass

            def prefill_targets(self) -> None:
                pass

            def cleanup_targets(self) -> None:
                pass

        with self.assertRaises(TypeError):
            _Missing("/tmp/archive/", MagicMock(), {"iteration": 0})  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# Parameter parsing
# ---------------------------------------------------------------------------


class TestFioBenchmarkParamParsing(unittest.TestCase):
    """All common FIO parameters are parsed and typed correctly."""

    def setUp(self) -> None:
        self.bm = _make()

    def test_cmd_path_default(self) -> None:
        """cmd_path defaults to /usr/bin/fio."""
        self.assertEqual(self.bm.cmd_path, "/usr/bin/fio")

    def test_cmd_path_custom(self) -> None:
        """cmd_path is read from config."""
        bm = _make({"cmd_path": "/opt/fio/bin/fio"})
        self.assertEqual(bm.cmd_path, "/opt/fio/bin/fio")

    def test_mode_default(self) -> None:
        """mode defaults to 'write'."""
        self.assertEqual(self.bm.mode, "write")

    def test_numjobs_is_int(self) -> None:
        """numjobs is coerced to int even when supplied as a string."""
        bm = _make({"numjobs": "4"})
        self.assertIsInstance(bm.numjobs, int)
        self.assertEqual(bm.numjobs, 4)

    def test_op_size_is_int(self) -> None:
        """op_size is stored as int."""
        self.assertIsInstance(self.bm.op_size, int)
        self.assertEqual(self.bm.op_size, 4194304)

    def test_end_fsync_is_int(self) -> None:
        """end_fsync is stored as int."""
        self.assertIsInstance(self.bm.end_fsync, int)

    def test_rwmixread_and_write_sum_to_100(self) -> None:
        """rwmixread and rwmixwrite are complementary and sum to 100."""
        bm = _make({"rwmixread": 70})
        self.assertEqual(bm.rwmixread, 70)
        self.assertEqual(bm.rwmixwrite, 30)
        self.assertEqual(bm.rwmixread + bm.rwmixwrite, 100)

    def test_norandommap_false_by_default(self) -> None:
        """norandommap is False by default."""
        self.assertFalse(self.bm.norandommap)

    def test_norandommap_true(self) -> None:
        """norandommap is set to True from config."""
        bm = _make({"norandommap": True})
        self.assertTrue(bm.norandommap)

    def test_random_distribution_none_by_default(self) -> None:
        """random_distribution is None when not configured."""
        self.assertIsNone(self.bm.random_distribution)

    def test_random_distribution_set(self) -> None:
        """random_distribution is read from config."""
        bm = _make({"random_distribution": "zipf:1.2"})
        self.assertEqual(bm.random_distribution, "zipf:1.2")

    def test_rate_iops_none_by_default(self) -> None:
        """rate_iops is None when not configured."""
        self.assertIsNone(self.bm.rate_iops)

    def test_log_avg_msec_none_by_default(self) -> None:
        """log_avg_msec is None when not configured."""
        self.assertIsNone(self.bm.log_avg_msec)

    def test_fio_out_format_default(self) -> None:
        """fio_out_format defaults to 'json,normal'."""
        self.assertEqual(self.bm.fio_out_format, "json,normal")

    def test_recov_test_type_default(self) -> None:
        """recov_test_type defaults to 'blocking'."""
        self.assertEqual(self.bm.recov_test_type, "blocking")

    def test_startdelay_none_by_default(self) -> None:
        """startdelay is None when not configured."""
        self.assertIsNone(self.bm.startdelay)

    def test_startdelay_set_from_config(self) -> None:
        """startdelay is read from config."""
        bm = _make({"startdelay": 5})
        self.assertEqual(bm.startdelay, 5)

    def test_time_based_false_by_default(self) -> None:
        """time_based is False by default."""
        self.assertFalse(self.bm.time_based)

    def test_time_based_true(self) -> None:
        """time_based is set to True from config."""
        bm = _make({"time_based": True})
        self.assertTrue(bm.time_based)


# ---------------------------------------------------------------------------
# time / ramp Optional[int] fix
# ---------------------------------------------------------------------------


class TestFioBenchmarkTimeRamp(unittest.TestCase):
    """time and ramp are stored as Optional[int], not str(None)."""

    def test_time_none_when_not_configured(self) -> None:
        """time is None when absent from config."""
        bm = _make()
        self.assertIsNone(bm.time)

    def test_ramp_none_when_not_configured(self) -> None:
        """ramp is None when absent from config."""
        bm = _make()
        self.assertIsNone(bm.ramp)

    def test_time_is_int_when_set(self) -> None:
        """time is stored as int, not str."""
        bm = _make({"time": 60})
        self.assertIsInstance(bm.time, int)
        self.assertEqual(bm.time, 60)

    def test_ramp_is_int_when_set(self) -> None:
        """ramp is stored as int, not str."""
        bm = _make({"ramp": 10})
        self.assertIsInstance(bm.ramp, int)
        self.assertEqual(bm.ramp, 10)

    def test_time_string_value_converted_to_int(self) -> None:
        """time is coerced to int even when supplied as a string."""
        bm = _make({"time": "120"})
        self.assertIsInstance(bm.time, int)
        self.assertEqual(bm.time, 120)


# ---------------------------------------------------------------------------
# total_iodepth / iodepth precedence
# ---------------------------------------------------------------------------


class TestFioBenchmarkIodepthPrecedence(unittest.TestCase):
    """total_iodepth takes precedence over iodepth when both are present."""

    def test_iodepth_used_when_no_total(self) -> None:
        """iodepth is used directly when total_iodepth is not set."""
        bm = _make({"iodepth": 32})
        self.assertEqual(bm.iodepth, 32)

    def test_total_iodepth_overrides_iodepth(self) -> None:
        """total_iodepth takes precedence over iodepth when both are present."""
        bm = _make({"iodepth": 16, "total_iodepth": 64})
        self.assertEqual(bm.iodepth, 64)

    def test_total_iodepth_only(self) -> None:
        """total_iodepth alone sets iodepth correctly."""
        bm = _make({"total_iodepth": 48})
        self.assertEqual(bm.iodepth, 48)


# ---------------------------------------------------------------------------
# calculate_iodepth_per_target
# ---------------------------------------------------------------------------


class TestCalculateIodepthPerTarget(unittest.TestCase):
    """calculate_iodepth_per_target dispatches based on iodepth_key."""

    def setUp(self) -> None:
        self.bm = _make({"iodepth": 8})

    def test_flat_assignment_with_iodepth_key(self) -> None:
        """iodepth key assigns the same depth to every target."""
        result = self.bm.calculate_iodepth_per_target(3, "iodepth")
        self.assertEqual(result, {0: 8, 1: 8, 2: 8})

    def test_distribution_with_total_iodepth_key(self) -> None:
        """total_iodepth key distributes depth across targets with remainder front-loaded."""
        bm = _make({"total_iodepth": 10})
        result = bm.calculate_iodepth_per_target(3, "total_iodepth")
        self.assertEqual(sum(result.values()), 10)
        self.assertEqual(result[0], 4)
        self.assertEqual(result[1], 3)
        self.assertEqual(result[2], 3)


# ---------------------------------------------------------------------------
# distribute_total_iodepth
# ---------------------------------------------------------------------------


class TestDistributeTotalIodepth(unittest.TestCase):
    """distribute_total_iodepth mirrors the contract from test_iodepth_calculation.py."""

    def test_evenly_divisible(self) -> None:
        """Total evenly divisible by num_targets gives equal depth to each."""
        result = distribute_total_iodepth(4, 16)
        self.assertEqual(result, {0: 4, 1: 4, 2: 4, 3: 4})

    def test_remainder_front_loaded(self) -> None:
        """Remainder is spread to the first R targets."""
        result = distribute_total_iodepth(3, 10)
        self.assertEqual(result[0], 4)
        self.assertEqual(result[1], 3)
        self.assertEqual(result[2], 3)

    def test_sum_equals_total(self) -> None:
        """Sum of all per-target depths always equals the requested total."""
        for total in range(1, 25):
            for nvols in range(1, total + 1):
                result = distribute_total_iodepth(nvols, total)
                self.assertEqual(sum(result.values()), total, f"total={total} nvols={nvols}")

    def test_no_zero_iodepth(self) -> None:
        """No target receives a zero iodepth when total >= num_targets."""
        for total in range(1, 20):
            for nvols in range(1, total + 1):
                result = distribute_total_iodepth(nvols, total)
                for vid, depth in result.items():
                    self.assertGreater(depth, 0, f"total={total} nvols={nvols} vol={vid}")

    def test_underflow_clamps_target_count(self) -> None:
        """When total < num_targets, only 'total' targets are returned."""
        result = distribute_total_iodepth(5, 2)
        self.assertEqual(len(result), 2)
        for depth in result.values():
            self.assertEqual(depth, 1)

    def test_underflow_emits_warning(self) -> None:
        """Underflow condition emits a warning log."""
        with self.assertLogs("cbt", level=logging.WARNING):
            distribute_total_iodepth(5, 2)


# ---------------------------------------------------------------------------
# parse_results
# ---------------------------------------------------------------------------


class TestParseResults(unittest.TestCase):
    """parse_results() extracts JSON blocks from output.* files."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.bm = _make()
        self.bm.archive_dir = self.tmp

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, name: str, content: str) -> Path:
        p = Path(self.tmp) / name
        p.write_text(content, encoding="utf-8")
        return p

    def test_clean_json_extracted(self) -> None:
        """A file containing only JSON produces a valid json_output file."""
        obj = {"fio version": "fio-3.30", "jobs": []}
        self._write("output.0", json.dumps(obj, indent=2))
        self.bm.parse_results()
        out = Path(self.tmp) / "json_output.0"
        self.assertTrue(out.exists())
        self.assertIn("fio version", json.loads(out.read_text()))

    def test_mixed_stdout_only_json_kept(self) -> None:
        """Non-JSON lines surrounding the JSON block are stripped from output."""
        content = 'fio: some log\n{\n  "fio version": "fio-3.30"\n}\nmore log\n'
        self._write("output.0", content)
        self.bm.parse_results()
        out_text = (Path(self.tmp) / "json_output.0").read_text()
        self.assertNotIn("fio: some log", out_text)
        self.assertIn("fio version", out_text)

    def test_non_numeric_suffix_ignored(self) -> None:
        """Files with non-numeric suffixes (e.g. output.log) are not processed."""
        self._write("output.log", '{"fio version": "x"}\n')
        self.bm.parse_results()
        self.assertFalse((Path(self.tmp) / "json_output.log").exists())

    def test_multiple_files_all_processed(self) -> None:
        """All output.N files in the archive directory are processed."""
        for i in range(3):
            self._write(f"output.{i}", json.dumps({"vol": i}, indent=2))
        self.bm.parse_results()
        for i in range(3):
            out = Path(self.tmp) / f"json_output.{i}"
            self.assertTrue(out.exists())
            self.assertEqual(json.loads(out.read_text())["vol"], i)

    def test_recursive_subdir_processed(self) -> None:
        """output.* files in subdirectories are processed recursively."""
        subdir = Path(self.tmp) / "workload" / "rbdfio" / "iodepth-000016"
        subdir.mkdir(parents=True)
        (subdir / "output.0").write_text(json.dumps({"jobs": []}, indent=2), encoding="utf-8")
        self.bm.parse_results()
        self.assertTrue((subdir / "json_output.0").exists())


# ---------------------------------------------------------------------------
# analyze
# ---------------------------------------------------------------------------


class TestAnalyze(unittest.TestCase):
    """analyze() delegates to parse_results()."""

    def test_analyze_calls_parse_results(self) -> None:
        """analyze() delegates JSON extraction to parse_results()."""
        bm = _make()
        with patch.object(bm, "parse_results") as mock_parse:
            bm.analyze("/some/dir")
        mock_parse.assert_called_once_with()


# ---------------------------------------------------------------------------
# Recovery callbacks
# ---------------------------------------------------------------------------


class TestRecoveryCallbacks(unittest.TestCase):
    """recovery_callback_blocking sends SIGINT; background logs completion."""

    def setUp(self) -> None:
        self.bm = _make()

    def test_blocking_calls_killall_sigint(self) -> None:
        """recovery_callback_blocking sends SIGINT (-2) to fio processes."""
        with (
            patch("settings.getnodes", return_value="fake-client"),
            patch("common.pdsh") as mock_pdsh,
        ):
            mock_pdsh.return_value = MagicMock()
            self.bm.recovery_callback_blocking()
        mock_pdsh.assert_called_once_with("fake-client", "sudo killall -2 fio")

    def test_background_logs_completion(self) -> None:
        """recovery_callback_background logs a completion message."""
        with self.assertLogs("cbt", level=logging.INFO) as cm:
            self.bm.recovery_callback_background()
        self.assertTrue(any("Recovery" in msg for msg in cm.output))

    def test_killall_fio_uses_custom_signal(self) -> None:
        """_killall_fio passes the supplied signal number to killall."""
        with (
            patch("settings.getnodes", return_value="fake-client"),
            patch("common.pdsh") as mock_pdsh,
        ):
            mock_pdsh.return_value = MagicMock()
            self.bm._killall_fio("-9")
        mock_pdsh.assert_called_once_with("fake-client", "sudo killall -9 fio")

    def test_killall_fio_uses_cmd_path_name(self) -> None:
        """_killall_fio derives the process name from cmd_path, not a hardcoded string."""
        bm = _make({"cmd_path": "/opt/fio-custom/bin/fio-3.35"})
        with (
            patch("settings.getnodes", return_value="fake-client"),
            patch("common.pdsh") as mock_pdsh,
        ):
            mock_pdsh.return_value = MagicMock()
            bm._killall_fio("-2")
        mock_pdsh.assert_called_once_with("fake-client", "sudo killall -2 fio-3.35")


if __name__ == "__main__":
    unittest.main()
