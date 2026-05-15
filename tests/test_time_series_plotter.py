"""
Unit tests for the time-series plotter module.
"""

# pyright: strict, reportPrivateUsage=false
# pylint: disable=protected-access, unused-argument, too-many-public-methods

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import matplotlib.pyplot as plt

from post_processing.plotter.time_series_plotter import TimeSeriesPlotter
from post_processing.post_processing_types import (
    TimeSeriesDataPoint,
    TimeSeriesFormatType,
    TimeSeriesMetadata,
)


class TestTimeSeriesPlotter(unittest.TestCase):
    """Test cases for TimeSeriesPlotter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.archive_dir = Path(self.temp_dir) / "archive"
        self.archive_dir.mkdir(parents=True)

        # Create sample time-series data
        self.sample_metadata: TimeSeriesMetadata = {
            "start_time_epoch": 0.0,
            "end_time_epoch": 10.0,
            "duration_seconds": 10.0,
            "num_volumes": 2,
            "sampling_interval_ms": 1000,
            "log_avg_msec": 1000,
        }

        self.sample_timeseries: list[TimeSeriesDataPoint] = [
            {
                "timestamp_sec": 1.0,
                "iops": 1000.0,
                "bandwidth_bytes": 4096000.0,
                "mean_latency_ms": 2.5,
                "max_latency_ms": 10.0,
                "p50_latency_ms": 2.0,
                "p95_latency_ms": 5.0,
                "p99_latency_ms": 8.0,
                "num_samples": 100,
            },
            {
                "timestamp_sec": 2.0,
                "iops": 1100.0,
                "bandwidth_bytes": 4505600.0,
                "mean_latency_ms": 2.3,
                "max_latency_ms": 9.5,
                "p50_latency_ms": 1.9,
                "p95_latency_ms": 4.8,
                "p99_latency_ms": 7.5,
                "num_samples": 110,
            },
            {
                "timestamp_sec": 3.0,
                "iops": 1050.0,
                "bandwidth_bytes": 4300800.0,
                "mean_latency_ms": 2.4,
                "max_latency_ms": 9.8,
                "p50_latency_ms": 2.1,
                "p95_latency_ms": 4.9,
                "p99_latency_ms": 7.8,
                "num_samples": 105,
            },
        ]

        self.sample_data: TimeSeriesFormatType = {
            "benchmark": "fio",
            "operation": "randread",
            "blocksize": "4096",
            "numjobs": "1",
            "iodepth": "1",
            "metadata": self.sample_metadata,
            "timeseries": self.sample_timeseries,
            "maximum_iops": "1100.0",
            "maximum_bandwidth": "4.30",
            "latency_at_max_iops": "2.3",
            "latency_at_max_bandwidth": "2.4",
            "timestamp_at_max_iops": "1001.0",
            "timestamp_at_max_bandwidth": "1002.0",
            "maximum_latency": "2.4",
            "timestamp_at_max_latency": "1002.0",
            "maximum_cpu_usage": "50.0",
            "maximum_memory_usage": "1024.0",
        }

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test TimeSeriesPlotter initialization"""
        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)

        self.assertEqual(plotter._archive_directory, self.archive_dir)
        self.assertEqual(plotter._plotter, plt)
        self.assertEqual(plotter._figure_size, (12, 3))
        self.assertEqual(plotter._dpi, 100)
        self.assertEqual(plotter._output_dir, self.archive_dir / "visualisation")

    def test_initialization_with_custom_params(self) -> None:
        """Test TimeSeriesPlotter initialization with custom parameters"""
        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt, figure_size=(10, 5), dpi=150)

        self.assertEqual(plotter._figure_size, (10, 5))
        self.assertEqual(plotter._dpi, 150)

    def test_generate_output_path(self) -> None:
        """Test output path generation"""
        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)

        output_path = plotter._generate_output_path(self.sample_data, "iops")

        expected_path = self.archive_dir / "visualisation" / "4096_1_randread_1_iops_timeseries.svg"
        self.assertEqual(output_path, expected_path)

    def test_generate_output_path_different_metrics(self) -> None:
        """Test output path generation for different metrics"""
        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)

        iops_path = plotter._generate_output_path(self.sample_data, "iops")
        bandwidth_path = plotter._generate_output_path(self.sample_data, "bandwidth")
        latency_path = plotter._generate_output_path(self.sample_data, "latency")

        self.assertIn("1_iops_timeseries.svg", str(iops_path))
        self.assertIn("1_bandwidth_timeseries.svg", str(bandwidth_path))
        self.assertIn("1_latency_timeseries.svg", str(latency_path))

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.close")
    def test_plot_iops(self, mock_close: MagicMock, mock_subplots: MagicMock) -> None:
        """Test IOPS plotting"""
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
        plotter._plot_iops(self.sample_data)

        # Should create subplots
        mock_subplots.assert_called_once()

        # Should plot data
        mock_axes.plot.assert_called_once()

        # Should configure axes
        mock_axes.set_title.assert_called_once()
        mock_axes.set_xlabel.assert_called_once()
        mock_axes.set_ylabel.assert_called_once()
        mock_axes.legend.assert_called_once()
        mock_axes.grid.assert_called_once()

        # Should save figure
        mock_figure.savefig.assert_called_once()

        # Should close figure
        mock_close.assert_called_once()

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.close")
    def test_plot_bandwidth(self, mock_close: MagicMock, mock_subplots: MagicMock) -> None:
        """Test bandwidth plotting"""
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
        plotter._plot_bandwidth(self.sample_data)

        # Should create subplots
        mock_subplots.assert_called_once()

        # Should plot data
        mock_axes.plot.assert_called_once()

        # Should save figure
        mock_figure.savefig.assert_called_once()

        # Should close figure
        mock_close.assert_called_once()

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.close")
    def test_plot_latency(self, mock_close: MagicMock, mock_subplots: MagicMock) -> None:
        """Test latency plotting with percentile bands"""
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
        plotter._plot_latency(self.sample_data)

        # Should create subplots
        mock_subplots.assert_called_once()

        # Should plot multiple lines (mean, p50, p95, p99, max)
        # and fill_between for bands
        self.assertGreater(mock_axes.plot.call_count, 0)
        self.assertGreater(mock_axes.fill_between.call_count, 0)

        # Should save figure
        mock_figure.savefig.assert_called_once()

        # Should close figure
        mock_close.assert_called_once()

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.close")
    def test_plot_time_series_creates_all_plots(self, mock_close: MagicMock, mock_subplots: MagicMock) -> None:
        """Test that plot_time_series creates all three plot types"""
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
        plotter.plot_time_series(self.sample_data)

        # Should create 3 plots (IOPS, bandwidth, latency)
        self.assertEqual(mock_subplots.call_count, 3)
        self.assertEqual(mock_figure.savefig.call_count, 3)
        self.assertEqual(mock_close.call_count, 3)

    def test_plot_time_series_with_empty_data(self) -> None:
        """Test plotting with empty timeseries data"""
        empty_data: TimeSeriesFormatType = {
            "benchmark": "fio",
            "operation": "randread",
            "blocksize": "4k",
            "numjobs": "1",
            "iodepth": "1",
            "metadata": self.sample_metadata,
            "timeseries": [],
            "maximum_iops": "0.0",
            "maximum_bandwidth": "0.0",
            "latency_at_max_iops": "0.0",
            "latency_at_max_bandwidth": "0.0",
            "timestamp_at_max_iops": "0.0",
            "timestamp_at_max_bandwidth": "0.0",
            "maximum_latency": "0.0",
            "timestamp_at_max_latency": "0.0",
            "maximum_cpu_usage": "0.0",
            "maximum_memory_usage": "0.0",
        }

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)

        # Should not raise an error
        plotter.plot_time_series(empty_data)

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.close")
    def test_plot_iops_with_zero_values(self, mock_close: MagicMock, mock_subplots: MagicMock) -> None:
        """Test IOPS plotting with all zero values"""
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        zero_data: TimeSeriesFormatType = {
            "benchmark": "fio",
            "operation": "randread",
            "blocksize": "4k",
            "numjobs": "1",
            "iodepth": "1",
            "metadata": self.sample_metadata,
            "timeseries": [
                {
                    "timestamp_sec": 1.0,
                    "iops": 0.0,
                    "bandwidth_bytes": 0.0,
                    "mean_latency_ms": 0.0,
                    "max_latency_ms": 0.0,
                    "p50_latency_ms": 0.0,
                    "p95_latency_ms": 0.0,
                    "p99_latency_ms": 0.0,
                    "num_samples": 0,
                }
            ],
            "maximum_iops": "0.0",
            "maximum_bandwidth": "0.0",
            "latency_at_max_iops": "0.0",
            "latency_at_max_bandwidth": "0.0",
            "timestamp_at_max_iops": "0.0",
            "timestamp_at_max_bandwidth": "0.0",
            "maximum_latency": "0.0",
            "timestamp_at_max_latency": "0.0",
            "maximum_cpu_usage": "0.0",
            "maximum_memory_usage": "0.0",
        }

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
        plotter._plot_iops(zero_data)

        # Should not create plot for zero data
        mock_subplots.assert_not_called()

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.close")
    def test_plot_bandwidth_with_zero_values(self, mock_close: MagicMock, mock_subplots: MagicMock) -> None:
        """Test bandwidth plotting with all zero values"""
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        zero_data: TimeSeriesFormatType = {
            "benchmark": "fio",
            "operation": "randread",
            "blocksize": "4k",
            "numjobs": "1",
            "iodepth": "1",
            "metadata": self.sample_metadata,
            "timeseries": [
                {
                    "timestamp_sec": 1.0,
                    "iops": 0.0,
                    "bandwidth_bytes": 0.0,
                    "mean_latency_ms": 0.0,
                    "max_latency_ms": 0.0,
                    "p50_latency_ms": 0.0,
                    "p95_latency_ms": 0.0,
                    "p99_latency_ms": 0.0,
                    "num_samples": 0,
                }
            ],
            "maximum_iops": "0.0",
            "maximum_bandwidth": "0.0",
            "latency_at_max_iops": "0.0",
            "latency_at_max_bandwidth": "0.0",
            "timestamp_at_max_iops": "0.0",
            "timestamp_at_max_bandwidth": "0.0",
            "maximum_latency": "0.0",
            "timestamp_at_max_latency": "0.0",
            "maximum_cpu_usage": "0.0",
            "maximum_memory_usage": "0.0",
        }

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
        plotter._plot_bandwidth(zero_data)

        # Should not create plot for zero data
        mock_subplots.assert_not_called()

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.close")
    def test_plot_latency_with_zero_values(self, mock_close: MagicMock, mock_subplots: MagicMock) -> None:
        """Test latency plotting with all zero values"""
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        zero_data: TimeSeriesFormatType = {
            "benchmark": "fio",
            "operation": "randread",
            "blocksize": "4k",
            "numjobs": "1",
            "iodepth": "1",
            "metadata": self.sample_metadata,
            "timeseries": [
                {
                    "timestamp_sec": 1.0,
                    "iops": 0.0,
                    "bandwidth_bytes": 0.0,
                    "mean_latency_ms": 0.0,
                    "max_latency_ms": 0.0,
                    "p50_latency_ms": 0.0,
                    "p95_latency_ms": 0.0,
                    "p99_latency_ms": 0.0,
                    "num_samples": 0,
                }
            ],
            "maximum_iops": "0.0",
            "maximum_bandwidth": "0.0",
            "latency_at_max_iops": "0.0",
            "latency_at_max_bandwidth": "0.0",
            "timestamp_at_max_iops": "0.0",
            "timestamp_at_max_bandwidth": "0.0",
            "maximum_latency": "0.0",
            "timestamp_at_max_latency": "0.0",
            "maximum_cpu_usage": "0.0",
            "maximum_memory_usage": "0.0",
        }

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
        plotter._plot_latency(zero_data)

        # Should not create plot for zero data
        mock_subplots.assert_not_called()

    def test_plot_from_file(self) -> None:
        """Test plotting from JSON file"""
        # Create a JSON file with sample data
        vis_dir = self.archive_dir / "visualisation"
        vis_dir.mkdir(parents=True, exist_ok=True)

        json_file = vis_dir / "4k_randread_timeseries.json"
        with json_file.open("w", encoding="utf8") as f:
            json.dump(self.sample_data, f)

        with patch("matplotlib.pyplot.subplots") as mock_subplots, patch("matplotlib.pyplot.close"):
            mock_figure = MagicMock()
            mock_axes = MagicMock()
            mock_subplots.return_value = (mock_figure, mock_axes)

            plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
            plotter.plot_from_file(str(json_file))

            # Should create 3 plots
            self.assertEqual(mock_subplots.call_count, 3)

    def test_plot_from_file_not_found(self) -> None:
        """Test plotting from non-existent file"""
        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)

        # Should not raise an error
        plotter.plot_from_file("/nonexistent/file.json")

    def test_plot_from_file_invalid_json(self) -> None:
        """Test plotting from file with invalid JSON"""
        vis_dir = self.archive_dir / "visualisation"
        vis_dir.mkdir(parents=True, exist_ok=True)

        json_file = vis_dir / "invalid.json"
        with json_file.open("w", encoding="utf8") as f:
            f.write("invalid json content")

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)

        # Should not raise an error
        plotter.plot_from_file(str(json_file))

    def test_plot_all_in_directory(self) -> None:
        """Test plotting all JSON files in a directory"""
        vis_dir = self.archive_dir / "visualisation"
        vis_dir.mkdir(parents=True, exist_ok=True)

        # Create multiple JSON files
        for i in range(3):
            json_file = vis_dir / f"test_{i}_timeseries.json"
            with json_file.open("w", encoding="utf8") as f:
                json.dump(self.sample_data, f)

        with patch("matplotlib.pyplot.subplots") as mock_subplots, patch("matplotlib.pyplot.close"):
            mock_figure = MagicMock()
            mock_axes = MagicMock()
            mock_subplots.return_value = (mock_figure, mock_axes)

            plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
            plotter.plot_all_in_directory()

            # Should create 9 plots (3 files x 3 plot types each)
            self.assertEqual(mock_subplots.call_count, 9)

    def test_plot_all_in_directory_no_files(self) -> None:
        """Test plotting all files when directory has no JSON files"""
        vis_dir = self.archive_dir / "visualisation"
        vis_dir.mkdir(parents=True, exist_ok=True)

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)

        # Should not raise an error
        plotter.plot_all_in_directory()

    def test_plot_all_in_directory_not_exists(self) -> None:
        """Test plotting all files when directory doesn't exist"""
        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)

        # Should not raise an error
        plotter.plot_all_in_directory()

    def test_configure_axes(self) -> None:
        """Test axes configuration"""
        mock_axes = MagicMock()

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
        plotter._configure_axes(
            ax=mock_axes,
            title="Test Title",
            xlabel="X Label",
        )

        mock_axes.set_title.assert_called_once_with("Test Title", fontsize=14, fontweight="bold")
        mock_axes.set_xlabel.assert_called_once_with("X Label", fontsize=12)
        mock_axes.set_ylim.assert_called_once_with(bottom=0)

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.close")
    def test_bandwidth_conversion_to_mb(self, mock_close: MagicMock, mock_subplots: MagicMock) -> None:
        """Test that bandwidth is correctly converted from bytes to MB/s"""
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter = TimeSeriesPlotter(archive_directory=str(self.archive_dir), plotter=plt)
        plotter._plot_bandwidth(self.sample_data)

        # Get the plot call arguments
        plot_call_args = mock_axes.plot.call_args

        # The second argument should be the bandwidth values in MB/s
        bandwidth_values = plot_call_args[0][1]

        # Check that values are converted (4096000 bytes = ~3.906 MB)
        expected_first_value = 4096000.0 / (1024 * 1024)
        self.assertAlmostEqual(bandwidth_values[0], expected_first_value, places=2)

    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.close")
    def test_output_directory_created(self, mock_close: MagicMock, mock_subplots: MagicMock) -> None:
        """Test that output directory is created if it doesn't exist"""
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        # Use a new directory that doesn't exist
        new_archive = self.archive_dir / "new_test"
        plotter = TimeSeriesPlotter(archive_directory=str(new_archive), plotter=plt)

        plotter.plot_time_series(self.sample_data)

        # Output directory should be created
        self.assertTrue((new_archive / "visualisation").exists())


# Made with Bob
