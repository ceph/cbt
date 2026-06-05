"""
Unit tests for the TimeSeriesReportGenerator class
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from post_processing.reports.time_series_report_generator import (
    TimeSeriesReportGenerator,
)


class TestTimeSeriesReportGenerator(unittest.TestCase):
    """Test cases for TimeSeriesReportGenerator class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.archive_dir = Path(self.temp_dir) / "test_archive"

        # Use timeseries nested structure: operation/total_iodepth-X/visualisation/
        # This matches the structure expected by find_timeseries_visualisation_directories()
        self.vis_dir = self.archive_dir / "read" / "total_iodepth-128" / "visualisation"
        self.vis_dir.mkdir(parents=True)

        # Create test time-series data files with actual JSON content
        # Format: {blocksize}_{numjobs}_{operation}_{iodepth}_timeseries.json
        self._create_test_timeseries_file("4096_1_read_1_timeseries.json", 4096, "1")
        self._create_test_timeseries_file("65536_1_read_1_timeseries.json", 65536, "1")
        # Add test file with different iodepth to test per-iodepth plotting
        self._create_test_timeseries_file("4096_1_read_128_timeseries.json", 4096, "128")

    def _create_test_timeseries_file(self, filename: str, blocksize: int, iodepth: str) -> None:
        """Create a test time-series JSON file with realistic data"""
        test_data = {
            "benchmark": "fio",
            "operation": "read",
            "blocksize": f"{blocksize}",
            "numjobs": "1",
            "iodepth": iodepth,
            "metadata": {
                "start_time_epoch": 1000.0,
                "end_time_epoch": 1100.0,
                "duration_seconds": 100.0,
                "num_volumes": 1,
                "sampling_interval_ms": 1000,
                "log_avg_msec": 1000,
            },
            "timeseries": [
                {
                    "timestamp_sec": 1000.0,
                    "iops": 1000.0,
                    "bandwidth_bytes": 4096000.0,
                    "mean_latency_ms": 1.5,
                    "max_latency_ms": 5.0,
                    "p50_latency_ms": 1.2,
                    "p95_latency_ms": 2.5,
                    "p99_latency_ms": 3.5,
                    "num_samples": 100,
                },
                {
                    "timestamp_sec": 1001.0,
                    "iops": 1200.0,
                    "bandwidth_bytes": 4915200.0,
                    "mean_latency_ms": 1.3,
                    "max_latency_ms": 4.5,
                    "p50_latency_ms": 1.1,
                    "p95_latency_ms": 2.3,
                    "p99_latency_ms": 3.2,
                    "num_samples": 100,
                },
            ],
            # Pre-calculated maximum values with timestamps
            "maximum_iops": "1200",
            "maximum_bandwidth": "4915200",
            "latency_at_max_iops": "1.300000",
            "latency_at_max_bandwidth": "1.300000",
            "timestamp_at_max_iops": "1001.0",
            "timestamp_at_max_bandwidth": "1001.0",
            "maximum_latency": "1.500000",
            "timestamp_at_max_latency": "1000.0",
            "maximum_cpu_usage": "0.00",
            "maximum_memory_usage": "0.00",
        }
        with open(self.vis_dir / filename, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_generate_report_title(self) -> None:
        """Test generating report title"""
        output_dir = f"{self.temp_dir}/output"

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        title = generator._generate_report_title()

        self.assertIn("Time-Series Performance Report", title)
        self.assertIn("test-archive", title)

    def test_generate_report_name(self) -> None:
        """Test generating report name with timestamp"""
        output_dir = f"{self.temp_dir}/output"

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        report_name = generator._generate_report_name()

        self.assertTrue(report_name.startswith("timeseries_performance_report_"))
        self.assertTrue(report_name.endswith(".md"))
        # Should contain timestamp in format YYMMDD_HHMMSS
        self.assertIn("_", report_name)

    def test_get_plot_file_stem(self) -> None:
        """Test extracting plot file stem from time-series plot filename"""
        output_dir = f"{self.temp_dir}/output"

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        # Test IOPS plot
        iops_file = Path("4096_1_read_iops_timeseries.svg")
        stem = generator._get_plot_file_stem(iops_file)
        self.assertEqual(stem, "4096_1_read")

        # Test bandwidth plot
        bw_file = Path("65536_1_read_bandwidth_timeseries.svg")
        stem = generator._get_plot_file_stem(bw_file)
        self.assertEqual(stem, "65536_1_read")

        # Test latency plot
        lat_file = Path("4096_1_read_latency_timeseries.svg")
        stem = generator._get_plot_file_stem(lat_file)
        self.assertEqual(stem, "4096_1_read")

    @patch("post_processing.reports.time_series_report_generator.TimeSeriesPlotter")
    def test_copy_images_creates_plots_if_missing(self, mock_plotter_class: MagicMock) -> None:
        """Test that _copy_images creates plots if they don't exist"""
        output_dir = f"{self.temp_dir}/output"

        mock_plotter = MagicMock()
        mock_plotter_class.return_value = mock_plotter

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        generator._copy_images()

        # Should create plotter and call draw_and_save
        mock_plotter_class.assert_called_once()
        mock_plotter.draw_and_save.assert_called_once()

    def test_find_common_timeseries_file_names(self) -> None:
        """Test finding common time-series file names"""
        output_dir = f"{self.temp_dir}/output"

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        file_names = generator._find_common_timeseries_file_names()

        # Should find both time-series files
        self.assertEqual(len(file_names), 3)
        self.assertIn("4096_1_read_1_timeseries.json", file_names)
        self.assertIn("4096_1_read_128_timeseries.json", file_names)
        self.assertIn("65536_1_read_1_timeseries.json", file_names)

    def test_find_and_sort_data_files(self) -> None:
        """Test finding and sorting time-series data files"""
        output_dir = f"{self.temp_dir}/output"

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        data_files = generator._find_and_sort_data_files()

        # Should find all three files (including different iodepth), sorted by blocksize then numjobs
        # Note: Files are sorted alphabetically, so "128" comes before "1" in string sorting
        self.assertEqual(len(data_files), 3)
        keys = list(data_files.keys())
        # Verify all expected files are present
        self.assertIn("4096_1_read_1_timeseries", keys)
        self.assertIn("4096_1_read_128_timeseries", keys)
        self.assertIn("65536_1_read_1_timeseries", keys)
        # Verify 4K files come before 64K files
        idx_4k_1 = keys.index("4096_1_read_1_timeseries")
        idx_4k_128 = keys.index("4096_1_read_128_timeseries")
        idx_64k = keys.index("65536_1_read_1_timeseries")
        self.assertLess(idx_4k_1, idx_64k)
        self.assertLess(idx_4k_128, idx_64k)

    def test_find_and_sort_file_paths(self) -> None:
        """Test finding and sorting file paths"""
        # Create multiple time-series files
        self._create_test_timeseries_file("8192_1_write_1_timeseries.json", 8192, "1")
        self._create_test_timeseries_file("16384_1_read_1_timeseries.json", 16384, "1")

        output_dir = f"{self.temp_dir}/output"

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        paths = generator._find_and_sort_file_paths(
            paths=[self.vis_dir],
            search_pattern="*_timeseries.json",
            index=0,
        )

        # Should find all 5 files (3 from setUp + 2 created here), sorted by blocksize
        self.assertEqual(len(paths), 5)
        # First file should be 4096
        self.assertTrue(paths[0].name.startswith("4096"))

    def test_add_configuration_yaml_files(self) -> None:
        """Test adding configuration YAML files to report"""
        # Create a mock YAML file
        results_dir = self.archive_dir / "results"
        results_dir.mkdir(parents=True)
        yaml_file = results_dir / "cbt_config.yaml"
        yaml_file.write_text("test: config\nkey: value\n")

        output_dir = f"{self.temp_dir}/output"

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        # Mock the report object
        generator._report = MagicMock()

        generator._add_configuration_yaml_files()

        # Should add header and paragraph
        generator._report.new_header.assert_called_once()
        generator._report.new_paragraph.assert_called_once()

    def test_get_all_number_of_jobs_values_with_timeseries_plots(self) -> None:
        """
        Test that _get_all_number_of_jobs_values correctly handles time-series plot filenames.

        This test ensures the fix for the KeyError when parsing time-series plot files
        (e.g., "4k_1_randread_iops_timeseries.svg") works correctly.

        The method should use _get_plot_file_stem() to strip the metric and timeseries
        suffixes before parsing, preventing KeyError: 'timeseries' in TITLE_CONVERSION.
        """
        output_dir = f"{self.temp_dir}/output"

        # Create time-series plot files with different metrics and numjobs
        plots_dir = self.vis_dir
        plots_dir.mkdir(parents=True, exist_ok=True)

        # Create plot files with various metrics and numjobs values
        plot_files = [
            "4096_1_read_iops_timeseries.svg",
            "4096_1_read_bandwidth_timeseries.svg",
            "4096_1_read_latency_timeseries.svg",
            "4096_2_read_iops_timeseries.svg",
            "65536_4_write_bandwidth_timeseries.svg",
            "65536_4_write_latency_timeseries.svg",
        ]

        for plot_file in plot_files:
            (plots_dir / plot_file).touch()

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        # Manually set plot files to our test files
        generator._plot_files = [plots_dir / f for f in plot_files]

        # This should not raise KeyError: 'timeseries'
        try:
            numjobs_values = generator._get_all_number_of_jobs_values()
        except KeyError as e:
            self.fail(f"_get_all_number_of_jobs_values raised KeyError: {e}")

        # Should extract unique numjobs values: 1, 2, 4
        self.assertEqual(len(numjobs_values), 3)
        self.assertIn("1", numjobs_values)
        self.assertIn("2", numjobs_values)
        self.assertIn("4", numjobs_values)

        # Should be sorted
        self.assertEqual(numjobs_values, ["1", "2", "4"])

    def test_find_and_sort_plot_files_by_numjobs_blocksize_iodepth(self) -> None:
        """Test that plots are sorted by numjobs, then blocksize, then iodepth"""
        output_dir = f"{self.temp_dir}/output"

        # Create plot files with different numjobs, blocksizes, and iodepths
        # Format: {blocksize}_{numjobs}_{operation}_{iodepth}_{metric}_timeseries.svg
        plots_dir = Path(output_dir) / "plots"
        plots_dir.mkdir(parents=True, exist_ok=True)

        plot_files = [
            # numjobs=1, various blocksizes and iodepths
            "4096_1_read_128_iops_timeseries.svg",  # bs=4096, nj=1, iod=128
            "4096_1_read_1_iops_timeseries.svg",  # bs=4096, nj=1, iod=1
            "65536_1_read_1_bandwidth_timeseries.svg",  # bs=65536, nj=1, iod=1
            "4096_1_read_8_iops_timeseries.svg",  # bs=4096, nj=1, iod=8
            # numjobs=2, various blocksizes and iodepths
            "8192_2_write_16_iops_timeseries.svg",  # bs=8192, nj=2, iod=16
            "4096_2_read_1_iops_timeseries.svg",  # bs=4096, nj=2, iod=1
            "8192_2_write_8_iops_timeseries.svg",  # bs=8192, nj=2, iod=8
        ]

        for plot_file in plot_files:
            (plots_dir / plot_file).touch()

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        # Manually set plots directory to our test directory
        generator._plots_directory = plots_dir

        # Get sorted plot files
        sorted_plots = generator._find_and_sort_plot_files()

        # Extract filenames for easier assertion
        sorted_names = [p.name for p in sorted_plots]

        # Expected order: sorted by (numjobs, blocksize, iodepth)
        expected_order = [
            # numjobs=1 group
            "4096_1_read_1_iops_timeseries.svg",  # nj=1, bs=4096, iod=1
            "4096_1_read_8_iops_timeseries.svg",  # nj=1, bs=4096, iod=8
            "4096_1_read_128_iops_timeseries.svg",  # nj=1, bs=4096, iod=128
            "65536_1_read_1_bandwidth_timeseries.svg",  # nj=1, bs=65536, iod=1
            # numjobs=2 group
            "4096_2_read_1_iops_timeseries.svg",  # nj=2, bs=4096, iod=1
            "8192_2_write_8_iops_timeseries.svg",  # nj=2, bs=8192, iod=8
            "8192_2_write_16_iops_timeseries.svg",  # nj=2, bs=8192, iod=16
        ]

        self.assertEqual(sorted_names, expected_order)

    def test_add_plots_single_column_layout(self) -> None:
        """Test that _add_plots creates single-column tables (columns=1)"""
        output_dir = f"{self.temp_dir}/output"

        # Create plot files
        plots_dir = Path(output_dir) / "plots"
        plots_dir.mkdir(parents=True, exist_ok=True)

        plot_files = [
            "4096_1_read_1_iops_timeseries.svg",
            "4096_1_read_1_latency_timeseries.svg",
        ]

        for plot_file in plot_files:
            (plots_dir / plot_file).touch()

        generator = TimeSeriesReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        # Set up the generator with our test plots
        generator._plots_directory = plots_dir
        generator._plot_files = [plots_dir / f for f in plot_files]

        # Mock the report object to capture new_table calls
        generator._report = MagicMock()

        # Call _add_plots
        generator._add_plots()

        # Verify that new_table was called with columns=1 (not columns=2)
        # Get all calls to new_table
        table_calls = list(generator._report.new_table.call_args_list)

        # Should have at least one table call
        self.assertGreater(len(table_calls), 0)

        # All table calls should use columns=1
        for call in table_calls:
            # call is a tuple of (args, kwargs)
            kwargs = call[1]
            self.assertEqual(kwargs.get("columns"), 1, "Table should use single column layout")


if __name__ == "__main__":
    unittest.main()

# Made with Bob
