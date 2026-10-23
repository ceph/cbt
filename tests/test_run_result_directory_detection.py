"""
Unit tests for RunResult directory detection and timeseries writing.

Tests verify that timeseries and hockey-stick data are written at the
correct directory levels in the hierarchy.
"""

# pylint: disable=protected-access,unused-argument,not-callable,too-many-locals

import json
import shutil
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from post_processing.run_results.benchmark_result import BenchmarkResult
from post_processing.run_results.rbdfio import RBDFIO
from post_processing.run_results.run_result import RunResult


class TestRunResultDirectoryDetection(unittest.TestCase):
    """Test directory detection for timeseries output"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        # Create a mock RunResult subclass
        self.mock_run_result = MagicMock(spec=RunResult)
        self.mock_run_result._determine_aggregation_directory_from_file = types.MethodType(
            RunResult._determine_aggregation_directory_from_file, self.mock_run_result
        )

    def test_determine_aggregation_directory_from_file_with_total_iodepth(self) -> None:
        """Test detection of total_iodepth directory from file path"""
        file_path = Path(
            "/home/user/results/00000000/id-806228fa/seq8kwrite.host.com/"
            "rbdfio/numjobs-001/total_iodepth-256/iodepth-000032/json_output.0"
        )

        result = self.mock_run_result._determine_aggregation_directory_from_file(file_path)

        expected = Path(
            "/home/user/results/00000000/id-806228fa/seq8kwrite.host.com/rbdfio/numjobs-001/total_iodepth-256"
        )
        self.assertEqual(result, expected)

    def test_determine_aggregation_directory_from_file_with_iodepth_only(self) -> None:
        """Test detection of iodepth directory when no total_iodepth exists"""
        file_path = Path("/home/user/results/00000000/id-123/test/rbdfio/numjobs-001/iodepth-032/json_output.0")

        result = self.mock_run_result._determine_aggregation_directory_from_file(file_path)

        expected = Path("/home/user/results/00000000/id-123/test/rbdfio/numjobs-001/iodepth-032")
        self.assertEqual(result, expected)

    def test_determine_aggregation_directory_from_file_no_depth_dirs(self) -> None:
        """Test fallback to parent directory when no depth directories exist"""
        file_path = Path("/home/user/results/00000000/id-123/test/rbdfio/numjobs-001/json_output.0")

        result = self.mock_run_result._determine_aggregation_directory_from_file(file_path)

        expected = Path("/home/user/results/00000000/id-123/test/rbdfio/numjobs-001")
        self.assertEqual(result, expected)

    def test_determine_aggregation_directory_from_file_prefers_total_iodepth(self) -> None:
        """Test that total_iodepth is preferred over iodepth"""
        # Path with both total_iodepth and iodepth
        file_path = Path("/home/user/results/rbdfio/numjobs-001/total_iodepth-256/iodepth-000032/json_output.0")

        result = self.mock_run_result._determine_aggregation_directory_from_file(file_path)

        # Should stop at total_iodepth, not continue to iodepth
        expected = Path("/home/user/results/rbdfio/numjobs-001/total_iodepth-256")
        self.assertEqual(result, expected)

    def test_determine_aggregation_directory_from_file_ignores_look_alike_directories(self) -> None:
        """Directories that merely start with or contain 'iodepth' are not aggregation directories"""
        file_path = Path("/home/user/results/iodepthfoo/rbdfio/numjobs-001/json_output.0")

        result = self.mock_run_result._determine_aggregation_directory_from_file(file_path)

        self.assertEqual(result, Path("/home/user/results/iodepthfoo/rbdfio/numjobs-001"))


class TestRunResultTimeseriesWriting(unittest.TestCase):
    """Test timeseries data writing at correct directory levels"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.test_dir = Path("/tmp/test_run_result")

    @patch("json.dump")
    @patch("post_processing.run_results.run_result.Path.open")
    @patch("post_processing.run_results.run_result.Path.mkdir")
    def test_write_timeseries_by_directory_groups_correctly(
        self, mock_mkdir: MagicMock, mock_open: MagicMock, mock_json_dump: MagicMock
    ) -> None:
        """Test that timeseries data is grouped and written by directory"""
        # Create a mock RunResult
        mock_result = MagicMock(spec=RunResult)
        mock_result._timeseries_by_directory = {
            Path("/test/total_iodepth-256"): {
                "randread_4k_256": {
                    "benchmark": "fio",
                    "operation": "randread",
                    "blocksize": "4k",
                    "numjobs": "1",
                    "metadata": {},
                    "timeseries": [],
                }
            },
            Path("/test/total_iodepth-128"): {
                "randwrite_4k_128": {
                    "benchmark": "fio",
                    "operation": "randwrite",
                    "blocksize": "4k",
                    "numjobs": "1",
                    "metadata": {},
                    "timeseries": [],
                }
            },
        }

        # Call the actual method
        RunResult._write_and_clear_timeseries_by_directory(mock_result)

        # Verify mkdir was called for each directory
        self.assertEqual(mock_mkdir.call_count, 2)

        # Verify json.dump was called twice (once per file)
        self.assertEqual(mock_json_dump.call_count, 2)

        # Verify data was cleared
        self.assertEqual(len(mock_result._timeseries_by_directory), 0)

    @patch("json.dump")
    @patch("builtins.open")
    def test_write_timeseries_creates_visualisation_subdirectory(
        self, mock_open: MagicMock, mock_json_dump: MagicMock
    ) -> None:
        """Test that visualisation subdirectory is created at aggregation level"""
        # Use a real temporary directory for this test
        test_dir = Path(tempfile.mkdtemp())
        try:
            agg_dir = test_dir / "total_iodepth-256"
            agg_dir.mkdir(parents=True)

            mock_result = MagicMock(spec=RunResult)
            mock_result._timeseries_by_directory = {
                agg_dir: {
                    "test_key": {
                        "benchmark": "fio",
                        "operation": "randread",
                        "blocksize": "4k",
                        "numjobs": "1",
                        "metadata": {},
                        "timeseries": [],
                    }
                }
            }

            RunResult._write_and_clear_timeseries_by_directory(mock_result)

            # Verify visualisation subdirectory was created
            expected_dir = agg_dir / "visualisation"
            self.assertTrue(expected_dir.exists(), f"Expected {expected_dir} to exist")
            self.assertTrue(expected_dir.is_dir(), f"Expected {expected_dir} to be a directory")
        finally:
            # Clean up
            shutil.rmtree(test_dir, ignore_errors=True)


class TestRunResultIntegration(unittest.TestCase):
    """Integration tests for directory detection and writing"""

    @patch("post_processing.run_results.rbdfio.RBDFIO._create_benchmark_result")
    def test_timeseries_grouping_by_file_location(self, mock_create_benchmark_result: MagicMock) -> None:
        """Test that files from different directories are grouped separately"""
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            dir_128 = tmp_dir / "numjobs-001" / "total_iodepth-128" / "iodepth-000032"
            dir_256 = tmp_dir / "numjobs-001" / "total_iodepth-256" / "iodepth-000064"
            dir_128.mkdir(parents=True)
            dir_256.mkdir(parents=True)

            file_128 = dir_128 / "json_output.0"
            file_256 = dir_256 / "json_output.0"
            minimal_fio_json = json.dumps({"jobs": [{"sys_cpu": 0.0, "usr_cpu": 0.0}]})
            file_128.write_text(minimal_fio_json, encoding="utf-8")
            file_256.write_text(minimal_fio_json, encoding="utf-8")

            mock_res_128 = MagicMock(spec=BenchmarkResult)
            mock_res_128.resource_file_path = file_128
            mock_res_128.operation = "randread"
            mock_res_128.blocksize = "4k"
            mock_res_128.iodepth = "32"
            mock_res_128.number_of_jobs = "1"
            mock_res_128.io_details = {"iops": "500.0", "bandwidth": "2000.0", "latency": "2.0"}
            mock_res_128.global_options = {"bs": "4k", "rw": "randread"}
            mock_res_128.get_timeseries_data.return_value = {
                "benchmark": "fio",
                "operation": "randread",
                "blocksize": "4k",
                "numjobs": "1",
                "iodepth": "32",
                "metadata": {},
                "timeseries": [{"time_ms": 1000, "iops": 500.0}],
            }

            mock_res_256 = MagicMock(spec=BenchmarkResult)
            mock_res_256.resource_file_path = file_256
            mock_res_256.operation = "randread"
            mock_res_256.blocksize = "4k"
            mock_res_256.iodepth = "64"
            mock_res_256.number_of_jobs = "1"
            mock_res_256.io_details = {"iops": "1000.0", "bandwidth": "4000.0", "latency": "1.0"}
            mock_res_256.global_options = {"bs": "4k", "rw": "randread"}
            mock_res_256.get_timeseries_data.return_value = {
                "benchmark": "fio",
                "operation": "randread",
                "blocksize": "4k",
                "numjobs": "1",
                "iodepth": "64",
                "metadata": {},
                "timeseries": [{"time_ms": 1000, "iops": 1000.0}],
            }

            def side_effect(path: Path) -> BenchmarkResult:
                if path == file_128:
                    return mock_res_128
                return mock_res_256

            mock_create_benchmark_result.side_effect = side_effect

            run_result = RBDFIO(tmp_dir, "json_output", include_timeseries=True)
            run_result.process()

            vis_128_file = (
                tmp_dir / "numjobs-001" / "total_iodepth-128" / "visualisation" / "4k_1_randread_32_timeseries.json"
            )
            vis_256_file = (
                tmp_dir / "numjobs-001" / "total_iodepth-256" / "visualisation" / "4k_1_randread_64_timeseries.json"
            )

            self.assertTrue(vis_128_file.exists())
            self.assertTrue(vis_256_file.exists())

            with vis_128_file.open(encoding="utf-8") as f:
                data_128 = json.load(f)
                self.assertEqual(data_128["iodepth"], "32")
                self.assertEqual(data_128["timeseries"][0]["iops"], 500.0)

            with vis_256_file.open(encoding="utf-8") as f:
                data_256 = json.load(f)
                self.assertEqual(data_256["iodepth"], "64")
                self.assertEqual(data_256["timeseries"][0]["iops"], 1000.0)

            # Verify memory was cleared
            self.assertEqual(len(run_result._timeseries_by_directory), 0)


if __name__ == "__main__":
    unittest.main()

# Made with Bob
