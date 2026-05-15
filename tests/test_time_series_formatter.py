"""
Unit tests for the time-series parser module.
"""

# pyright: strict, reportPrivateUsage=false

import unittest

import pandas as pd

from post_processing.parsers.fio_time_series_parser import FIOTimeSeriesParser


class TestFIOTimeSeriesParser(unittest.TestCase):
    """Test cases for FIOTimeSeriesParser class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.formatter = FIOTimeSeriesParser(
            archive_directory="/tmp/test", benchmark="fio", operation="randread", blocksize="4k", numjobs="1"
        )

    def test_format_with_all_metrics(self) -> None:
        """Test formatting with all metrics present"""
        # Create sample DataFrames
        iops_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 3.0], "iops": [1000.0, 1100.0, 1050.0]})

        bandwidth_df = pd.DataFrame(
            {
                "timestamp_sec": [1.0, 2.0, 3.0],
                "bandwidth_bytes": [4096000.0, 4505600.0, 4300800.0],
            }
        )

        mean_latency_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 3.0], "latency_ms": [2.5, 2.3, 2.4]})

        max_latency_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 3.0], "latency_ms": [10.0, 9.5, 9.8]})

        p50_latency_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 3.0], "latency_ms": [2.0, 1.9, 2.1]})

        p95_latency_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 3.0], "latency_ms": [5.0, 4.8, 4.9]})

        p99_latency_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 3.0], "latency_ms": [8.0, 7.5, 7.8]})

        dataframes = {
            "iops": iops_df,
            "bandwidth": bandwidth_df,
            "mean_latency": mean_latency_df,
            "max_latency": max_latency_df,
            "p50_latency": p50_latency_df,
            "p95_latency": p95_latency_df,
            "p99_latency": p99_latency_df,
        }
        result = self.formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=2,
            log_avg_msec=1000,
        )

        self.assertIsNotNone(result)
        assert result is not None  # Type narrowing

        # Check metadata
        self.assertEqual(result["benchmark"], "fio")
        self.assertEqual(result["operation"], "randread")
        self.assertEqual(result["blocksize"], "4k")
        self.assertEqual(result["numjobs"], "1")
        self.assertEqual(result["metadata"]["num_volumes"], 2)
        self.assertEqual(result["metadata"]["log_avg_msec"], 1000)
        self.assertAlmostEqual(result["metadata"]["duration_seconds"], 2.0, places=1)

        # Check timeseries data
        self.assertEqual(len(result["timeseries"]), 3)

        # Check first data point
        point = result["timeseries"][0]
        self.assertAlmostEqual(point["timestamp_sec"], 1.0, places=1)
        self.assertAlmostEqual(point["iops"], 1000.0, places=1)
        self.assertAlmostEqual(point["bandwidth_bytes"], 4096000.0, places=1)
        self.assertAlmostEqual(point["mean_latency_ms"], 2.5, places=2)
        self.assertAlmostEqual(point["max_latency_ms"], 10.0, places=2)
        self.assertAlmostEqual(point["p50_latency_ms"], 2.0, places=2)
        self.assertAlmostEqual(point["p95_latency_ms"], 5.0, places=2)
        self.assertAlmostEqual(point["p99_latency_ms"], 8.0, places=2)

    def test_format_with_only_iops(self) -> None:
        """Test formatting with only IOPS data"""
        iops_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0], "iops": [1000.0, 1100.0]})

        dataframes = {
            "iops": iops_df,
            "bandwidth": None,
            "mean_latency": None,
            "max_latency": None,
            "p50_latency": None,
            "p95_latency": None,
            "p99_latency": None,
        }
        result = self.formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=1,
            log_avg_msec=1000,
        )

        self.assertIsNotNone(result)
        assert result is not None

        self.assertEqual(len(result["timeseries"]), 2)

        # Check that missing metrics are filled with 0
        point = result["timeseries"][0]
        self.assertAlmostEqual(point["iops"], 1000.0, places=1)
        self.assertAlmostEqual(point["bandwidth_bytes"], 0.0, places=1)
        self.assertAlmostEqual(point["mean_latency_ms"], 0.0, places=2)

    def test_format_with_only_latency(self) -> None:
        """
        Test formatting with only latency data.

        When there are no throughput metrics at all, the data is returned as-is
        with IOPS/bandwidth filled as 0. This is an edge case that shouldn't
        occur in normal FIO runs (which always produce IOPS/bandwidth logs).
        """
        mean_latency_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0], "latency_ms": [2.5, 2.3]})

        dataframes = {
            "iops": None,
            "bandwidth": None,
            "mean_latency": mean_latency_df,
            "max_latency": None,
            "p50_latency": None,
            "p95_latency": None,
            "p99_latency": None,
        }
        result = self.formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=1,
            log_avg_msec=1000,
        )

        self.assertIsNotNone(result)
        assert result is not None

        self.assertEqual(len(result["timeseries"]), 2)

        # Check that IOPS/BW are filled with 0 (edge case behavior)
        point = result["timeseries"][0]
        self.assertAlmostEqual(point["iops"], 0.0, places=1)
        self.assertAlmostEqual(point["bandwidth_bytes"], 0.0, places=1)
        self.assertAlmostEqual(point["mean_latency_ms"], 2.5, places=2)

    def test_format_with_misaligned_timestamps(self) -> None:
        """
        Test formatting with different timestamps in different metrics.

        With the zero-throughput filtering, timestamps that have zero values
        for ALL throughput metrics (IOPS and bandwidth) are removed.
        """
        iops_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 3.0], "iops": [1000.0, 1100.0, 1050.0]})

        # Bandwidth has different timestamps
        bandwidth_df = pd.DataFrame(
            {"timestamp_sec": [1.5, 2.5, 3.5], "bandwidth_bytes": [4096000.0, 4505600.0, 4300800.0]}
        )

        dataframes = {
            "iops": iops_df,
            "bandwidth": bandwidth_df,
            "mean_latency": None,
            "max_latency": None,
            "p50_latency": None,
            "p95_latency": None,
            "p99_latency": None,
        }
        result = self.formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=1,
            log_avg_msec=1000,
        )

        self.assertIsNotNone(result)
        assert result is not None

        # Should have 6 timestamps (all have at least one throughput metric)
        self.assertEqual(len(result["timeseries"]), 6)

        # Verify no timestamps have BOTH IOPS and bandwidth as zero
        for point in result["timeseries"]:
            has_throughput = point["iops"] > 0 or point["bandwidth_bytes"] > 0
            self.assertTrue(has_throughput, f"Timestamp {point['timestamp_sec']} has zero throughput")

        # Check specific points
        # First point should have IOPS but no bandwidth
        point = result["timeseries"][0]
        self.assertAlmostEqual(point["timestamp_sec"], 1.0, places=1)
        self.assertAlmostEqual(point["iops"], 1000.0, places=1)
        self.assertAlmostEqual(point["bandwidth_bytes"], 0.0, places=1)

    def test_format_filters_zero_throughput_timestamps(self) -> None:
        """
        Test that timestamps with zero throughput are filtered out.

        This test simulates the real-world scenario where IOPS/bandwidth logs
        and latency logs have different timestamps due to FIO's logging behavior.
        The parser should only keep timestamps that have actual throughput data.
        """
        # IOPS data at specific timestamps (simulating _iops.log)
        iops_df = pd.DataFrame({"timestamp_sec": [0.999, 1.999, 2.999, 3.999], "iops": [670.0, 675.0, 680.0, 549.0]})

        # Latency data at different timestamps (simulating _clat.log)
        mean_latency_df = pd.DataFrame(
            {"timestamp_sec": [1.009, 2.001, 3.000, 4.000], "latency_ms": [23.97, 23.22, 23.54, 28.04]}
        )

        dataframes = {
            "iops": iops_df,
            "bandwidth": None,
            "mean_latency": mean_latency_df,
            "max_latency": None,
            "p50_latency": None,
            "p95_latency": None,
            "p99_latency": None,
        }

        result = self.formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=1,
            log_avg_msec=1000,
        )

        self.assertIsNotNone(result)
        assert result is not None

        # Should only have 4 timestamps (from IOPS data)
        # Timestamps from latency-only data should be filtered out
        self.assertEqual(len(result["timeseries"]), 4)

        # Verify no zero IOPS values exist
        for point in result["timeseries"]:
            self.assertGreater(point["iops"], 0.0, f"Found zero IOPS at timestamp {point['timestamp_sec']}")

        # Verify we have the IOPS timestamps
        timestamps = [point["timestamp_sec"] for point in result["timeseries"]]
        self.assertIn(0.999, timestamps)
        self.assertIn(1.999, timestamps)
        self.assertIn(2.999, timestamps)
        self.assertIn(3.999, timestamps)

        # Verify latency-only timestamps are NOT present
        self.assertNotIn(1.009, timestamps)
        self.assertNotIn(2.001, timestamps)

    def test_format_with_empty_dataframes(self) -> None:
        """Test formatting with empty DataFrames"""
        empty_df = pd.DataFrame({"timestamp_sec": [], "iops": []})

        dataframes = {
            "iops": empty_df,
            "bandwidth": None,
            "mean_latency": None,
            "max_latency": None,
            "p50_latency": None,
            "p95_latency": None,
            "p99_latency": None,
        }
        result = self.formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=1,
            log_avg_msec=1000,
        )

        # Should return None for empty data
        self.assertIsNone(result)

    def test_format_with_all_none(self) -> None:
        """Test formatting with all None DataFrames"""
        dataframes = {
            "iops": None,
            "bandwidth": None,
            "mean_latency": None,
            "max_latency": None,
            "p50_latency": None,
            "p95_latency": None,
            "p99_latency": None,
        }
        result = self.formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=1,
            log_avg_msec=1000,
        )

        # Should return None when no data
        self.assertIsNone(result)

    def test_metadata_calculation(self) -> None:
        """Test metadata calculation with specific timestamps"""
        iops_df = pd.DataFrame({"timestamp_sec": [10.0, 20.0, 30.0], "iops": [1000.0, 1100.0, 1050.0]})

        dataframes = {
            "iops": iops_df,
            "bandwidth": None,
            "mean_latency": None,
            "max_latency": None,
            "p50_latency": None,
            "p95_latency": None,
            "p99_latency": None,
        }
        result = self.formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=3,
            log_avg_msec=2000,
        )

        self.assertIsNotNone(result)
        assert result is not None

        metadata = result["metadata"]
        self.assertAlmostEqual(metadata["start_time_epoch"], 10.0, places=1)
        self.assertAlmostEqual(metadata["end_time_epoch"], 30.0, places=1)
        self.assertAlmostEqual(metadata["duration_seconds"], 20.0, places=1)
        self.assertEqual(metadata["num_volumes"], 3)
        self.assertEqual(metadata["sampling_interval_ms"], 2000)

    def test_custom_benchmark_info(self) -> None:
        """Test parser with custom benchmark information"""
        formatter = FIOTimeSeriesParser(
            archive_directory="/tmp/test", benchmark="custom_bench", operation="seqwrite", blocksize="128k", numjobs="4"
        )

        iops_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0], "iops": [500.0, 550.0]})

        dataframes = {
            "iops": iops_df,
            "bandwidth": None,
            "mean_latency": None,
            "max_latency": None,
            "p50_latency": None,
            "p95_latency": None,
            "p99_latency": None,
        }
        result = formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=1,
            log_avg_msec=1000,
        )

        self.assertIsNotNone(result)
        assert result is not None

        self.assertEqual(result["benchmark"], "custom_bench")
        self.assertEqual(result["operation"], "seqwrite")
        self.assertEqual(result["blocksize"], "128k")
        self.assertEqual(result["numjobs"], "4")

    def test_timeseries_sorted_by_timestamp(self) -> None:
        """Test that timeseries data is sorted by timestamp"""
        # Create unsorted data
        iops_df = pd.DataFrame({"timestamp_sec": [3.0, 1.0, 2.0], "iops": [1050.0, 1000.0, 1100.0]})

        dataframes = {
            "iops": iops_df,
            "bandwidth": None,
            "mean_latency": None,
            "max_latency": None,
            "p50_latency": None,
            "p95_latency": None,
            "p99_latency": None,
        }
        result = self.formatter._format_time_series(
            dataframes=dataframes,
            num_volumes=1,
            log_avg_msec=1000,
        )

        self.assertIsNotNone(result)
        assert result is not None

        # Check that timestamps are sorted
        timestamps = [point["timestamp_sec"] for point in result["timeseries"]]
        self.assertEqual(timestamps, sorted(timestamps))

        # Check that values match sorted order
        self.assertAlmostEqual(result["timeseries"][0]["iops"], 1000.0, places=1)
        self.assertAlmostEqual(result["timeseries"][1]["iops"], 1100.0, places=1)
        self.assertAlmostEqual(result["timeseries"][2]["iops"], 1050.0, places=1)


# Made with Bob
