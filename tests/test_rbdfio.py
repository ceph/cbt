"""
Unit tests for the RBDFIO class
"""

# pyright: strict, reportPrivateUsage=false

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from post_processing.run_results.rbdfio import RBDFIO


class TestRBDFIO(unittest.TestCase):
    """Test cases for RBDFIO class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_path = Path(self.temp_dir)

        # Create test FIO output data for multiple volumes
        self.test_data: dict[str, Any] = {
            "global options": {
                "bs": "4096",
                "rw": "randread",
                "iodepth": "32",
                "numjobs": "1",
                "runtime": "60",
            },
            "jobs": [
                {
                    "read": {
                        "io_bytes": 1000000000,
                        "bw_bytes": 16666666,
                        "iops": 4000.0,
                        "total_ios": 244140,
                        "clat_ns": {"mean": 8000000.0, "stddev": 500000.0},
                    },
                    "write": {
                        "io_bytes": 0,
                        "bw_bytes": 0,
                        "iops": 0.0,
                        "total_ios": 0,
                        "clat_ns": {"mean": 0.0, "stddev": 0.0},
                    },
                    "sys_cpu": 5.5,
                    "usr_cpu": 10.2,
                }
            ],
        }

        # Create multiple test files simulating multiple volumes
        for i in range(3):
            test_file = self.test_path / f"json_output.{i}"
            with open(test_file, "w") as f:
                json.dump(self.test_data, f)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test RBDFIO initialization"""
        rbdfio = RBDFIO(self.test_path, "json_output")

        self.assertEqual(rbdfio._path, self.test_path)
        self.assertFalse(rbdfio._has_been_processed)
        self.assertEqual(len(rbdfio._files), 3)

    def test_type_property(self) -> None:
        """Test type property returns correct value"""
        rbdfio = RBDFIO(self.test_path, "json_output")

        self.assertEqual(rbdfio.type, "rbdfio")

    def test_find_files_for_testrun(self) -> None:
        """Test finding files matching the pattern"""
        rbdfio = RBDFIO(self.test_path, "json_output")

        files = rbdfio._find_files_for_testrun("json_output")

        self.assertEqual(len(files), 3)
        for i, file_path in enumerate(sorted(files)):
            self.assertTrue(file_path.name.startswith("json_output."))
            self.assertTrue(file_path.name.endswith(str(i)))

    def test_find_files_no_matches(self) -> None:
        """Test finding files when no matches exist"""
        rbdfio = RBDFIO(self.test_path, "json_output")

        files = rbdfio._find_files_for_testrun("nonexistent")

        self.assertEqual(len(files), 0)

    def test_sum_io_details(self) -> None:
        """Test summing IO details from multiple volumes"""
        rbdfio = RBDFIO(self.test_path, "json_output")

        existing_values = {
            "io_bytes": "1000000000",
            "iops": "4000.0",
            "bandwidth_bytes": "16666666",
            "total_ios": "244140",
            "latency": "8.0",
            "std_deviation": "0.5",
        }

        new_values = {
            "io_bytes": "1000000000",
            "iops": "4000.0",
            "bandwidth_bytes": "16666666",
            "total_ios": "244140",
            "latency": "8.0",
            "std_deviation": "0.5",
        }

        result = rbdfio._sum_io_details(existing_values, new_values)

        # Check summed values
        self.assertAlmostEqual(float(result["io_bytes"]), 2000000000.0, places=0)
        self.assertAlmostEqual(float(result["iops"]), 8000.0, places=1)
        self.assertAlmostEqual(float(result["bandwidth_bytes"]), 33333332.0, places=0)
        self.assertEqual(int(result["total_ios"]), 488280)

        # Latency should be weighted average (same values = same result)
        self.assertAlmostEqual(float(result["latency"]), 8.0, places=1)

    def test_sum_io_details_different_latencies(self) -> None:
        """Test summing IO details with different latencies"""
        rbdfio = RBDFIO(self.test_path, "json_output")

        existing_values = {
            "io_bytes": "1000000000",
            "iops": "4000.0",
            "bandwidth_bytes": "16666666",
            "total_ios": "100000",
            "latency": "5.0",
            "std_deviation": "0.5",
        }

        new_values = {
            "io_bytes": "1000000000",
            "iops": "4000.0",
            "bandwidth_bytes": "16666666",
            "total_ios": "200000",
            "latency": "10.0",
            "std_deviation": "1.0",
        }

        result = rbdfio._sum_io_details(existing_values, new_values)

        # Weighted average: (5*100000 + 10*200000) / 300000 = 8.333...
        self.assertAlmostEqual(float(result["latency"]), 8.333, places=2)

    def test_create_benchmark_result(self) -> None:
        """Test creating benchmark result returns FIO instance"""
        rbdfio = RBDFIO(self.test_path, "json_output")
        test_file = self.test_path / "json_output.0"

        result = rbdfio._create_benchmark_result(test_file)

        self.assertEqual(result.source, "fio")
        self.assertEqual(result.blocksize, "4096")
        self.assertEqual(result.operation, "randread")
        self.assertEqual(result.iodepth, "32")
        self.assertEqual(result.number_of_jobs, "1")

    def test_create_resource_result(self) -> None:
        """Test creating resource result returns FIOResource instance"""
        rbdfio = RBDFIO(self.test_path, "json_output")
        test_file = self.test_path / "json_output.0"

        result = rbdfio._create_resource_result(test_file)

        # FIOResource should have source property
        self.assertEqual(result.source, "fio")

    def test_process_results(self) -> None:
        """Test processing results aggregates data correctly"""
        rbdfio = RBDFIO(self.test_path, "json_output")

        # Process the results
        rbdfio.process()

        self.assertTrue(rbdfio._has_been_processed)
        self.assertIsNotNone(rbdfio._processed_data)

        # Check that data was aggregated
        # Should have operation -> numjobs -> blocksize -> iodepth structure
        self.assertIn("randread", rbdfio._processed_data)

    @patch("post_processing.run_results.benchmarks.fio.FIO.get_timeseries_data")
    def test_process_results_with_timeseries(self, mock_get_timeseries_data: Any) -> None:
        """Test processing results with time-series data enabled"""
        mock_get_timeseries_data.side_effect = [
            {
                "benchmark": "fio",
                "operation": "randread",
                "blocksize": "4096",
                "numjobs": "1",
                "iodepth": "32",
                "metadata": {
                    "start_time_epoch": 1.0,
                    "end_time_epoch": 2.0,
                    "duration_seconds": 1.0,
                    "num_volumes": 1,
                    "sampling_interval_ms": 1000,
                    "log_avg_msec": 1000,
                },
                "timeseries": [
                    {
                        "timestamp_sec": 1.0,
                        "iops": 100.0,
                        "bandwidth_bytes": 4096.0,
                        "mean_latency_ms": 2.0,
                        "max_latency_ms": 3.0,
                        "p50_latency_ms": 1.5,
                        "p95_latency_ms": 2.5,
                        "p99_latency_ms": 2.8,
                        "num_samples": 1,
                    }
                ],
                "maximum_iops": "100",
                "maximum_bandwidth": "4096",
                "latency_at_max_iops": "2.0",
                "latency_at_max_bandwidth": "2.0",
                "timestamp_at_max_iops": "1.0",
                "timestamp_at_max_bandwidth": "1.0",
                "maximum_latency": "3.0",
                "timestamp_at_max_latency": "1.0",
                "maximum_cpu_usage": "0.0",
                "maximum_memory_usage": "0.0",
            },
            {
                "benchmark": "fio",
                "operation": "randread",
                "blocksize": "4096",
                "numjobs": "1",
                "iodepth": "32",
                "metadata": {
                    "start_time_epoch": 1.0,
                    "end_time_epoch": 2.0,
                    "duration_seconds": 1.0,
                    "num_volumes": 1,
                    "sampling_interval_ms": 1000,
                    "log_avg_msec": 1000,
                },
                "timeseries": [
                    {
                        "timestamp_sec": 1.0,
                        "iops": 200.0,
                        "bandwidth_bytes": 8192.0,
                        "mean_latency_ms": 4.0,
                        "max_latency_ms": 5.0,
                        "p50_latency_ms": 3.5,
                        "p95_latency_ms": 4.5,
                        "p99_latency_ms": 4.8,
                        "num_samples": 1,
                    }
                ],
                "maximum_iops": "200",
                "maximum_bandwidth": "8192",
                "latency_at_max_iops": "4.0",
                "latency_at_max_bandwidth": "4.0",
                "timestamp_at_max_iops": "1.0",
                "timestamp_at_max_bandwidth": "1.0",
                "maximum_latency": "5.0",
                "timestamp_at_max_latency": "1.0",
                "maximum_cpu_usage": "0.0",
                "maximum_memory_usage": "0.0",
            },
            {
                "benchmark": "fio",
                "operation": "randread",
                "blocksize": "4096",
                "numjobs": "1",
                "iodepth": "32",
                "metadata": {
                    "start_time_epoch": 1.0,
                    "end_time_epoch": 2.0,
                    "duration_seconds": 1.0,
                    "num_volumes": 1,
                    "sampling_interval_ms": 1000,
                    "log_avg_msec": 1000,
                },
                "timeseries": [
                    {
                        "timestamp_sec": 1.0,
                        "iops": 300.0,
                        "bandwidth_bytes": 12288.0,
                        "mean_latency_ms": 6.0,
                        "max_latency_ms": 7.0,
                        "p50_latency_ms": 5.5,
                        "p95_latency_ms": 6.5,
                        "p99_latency_ms": 6.8,
                        "num_samples": 1,
                    }
                ],
                "maximum_iops": "300",
                "maximum_bandwidth": "12288",
                "latency_at_max_iops": "6.0",
                "latency_at_max_bandwidth": "6.0",
                "timestamp_at_max_iops": "1.0",
                "timestamp_at_max_bandwidth": "1.0",
                "maximum_latency": "7.0",
                "timestamp_at_max_latency": "1.0",
                "maximum_cpu_usage": "0.0",
                "maximum_memory_usage": "0.0",
            },
        ]

        rbdfio = RBDFIO(self.test_path, "json_output", include_timeseries=True)

        # Process the results
        rbdfio.process()

        self.assertTrue(rbdfio._has_been_processed)

        # Verify that timeseries data was written to disk
        # Check the visualisation directory was created
        vis_dir = self.test_path / "visualisation"
        self.assertTrue(vis_dir.exists(), "Visualisation directory should be created")

        # Find the timeseries JSON file
        ts_files = list(vis_dir.glob("*_timeseries.json"))
        self.assertEqual(len(ts_files), 1, "Should have exactly one timeseries file")

        # Load and verify the aggregated data
        with open(ts_files[0]) as f:
            aggregated_data = json.load(f)

        # CRITICAL TEST: Verify aggregation across all 3 volumes
        # This test would have FAILED before the fix because only the last volume's
        # data would be present (300 IOPS instead of 600)
        self.assertEqual(aggregated_data["metadata"]["num_volumes"], 3, "Should aggregate data from all 3 volumes")

        # Verify IOPS are summed: 100 + 200 + 300 = 600
        first_point = aggregated_data["timeseries"][0]
        self.assertAlmostEqual(
            first_point["iops"], 600.0, places=1, msg="IOPS should be summed across all volumes (100+200+300=600)"
        )

        # Verify bandwidth is summed: 4096 + 8192 + 12288 = 24576
        self.assertAlmostEqual(
            first_point["bandwidth_bytes"], 24576.0, places=1, msg="Bandwidth should be summed across all volumes"
        )

        # Verify latency is weighted-averaged by IOPS (correct statistical approach)
        # Weighted avg = (100*2.0 + 200*4.0 + 300*6.0) / (100+200+300)
        #              = (200 + 800 + 1800) / 600 = 2800/600 = 4.667ms
        self.assertAlmostEqual(
            first_point["mean_latency_ms"], 4.667, places=2, msg="Mean latency should be weighted average by IOPS"
        )

        # Verify max_latency takes the maximum value (worst case across all volumes)
        # max(3.0, 5.0, 7.0) = 7.0
        self.assertAlmostEqual(
            first_point["max_latency_ms"], 7.0, places=2, msg="Max latency should be the maximum across all volumes"
        )

        # Verify other latency percentiles are weighted-averaged by IOPS
        # p50: (100*1.5 + 200*3.5 + 300*5.5) / 600 = 4.167
        self.assertAlmostEqual(
            first_point["p50_latency_ms"], 4.167, places=2, msg="P50 latency should be weighted average"
        )
        # p95: (100*2.5 + 200*4.5 + 300*6.5) / 600 = 5.167
        self.assertAlmostEqual(
            first_point["p95_latency_ms"], 5.167, places=2, msg="P95 latency should be weighted average"
        )
        # p99: (100*2.8 + 200*4.8 + 300*6.8) / 600 = 5.467
        self.assertAlmostEqual(
            first_point["p99_latency_ms"], 5.467, places=2, msg="P99 latency should be weighted average"
        )

        # After processing with memory-efficient mode, data should be written and cleared
        self.assertEqual(
            len(rbdfio._timeseries_data), 0, "Timeseries data should be empty after memory-efficient write"
        )

    def test_get_results_without_processing(self) -> None:
        """Test getting results automatically processes if not done"""
        rbdfio = RBDFIO(self.test_path, "json_output")

        # get() should automatically call process() if not already processed
        results = rbdfio.get()

        self.assertTrue(rbdfio._has_been_processed)
        self.assertIsNotNone(results)
        self.assertIsInstance(results, dict)

    def test_get_results_after_processing(self) -> None:
        """Test getting results after processing returns data"""
        rbdfio = RBDFIO(self.test_path, "json_output")

        rbdfio.process()
        results = rbdfio.get()

        self.assertIsNotNone(results)
        self.assertIsInstance(results, dict)


# Made with Bob
