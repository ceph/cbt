"""
Unit tests for the post_processing/plotter common_format_plotter module
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from unittest.mock import MagicMock, patch

from matplotlib.axes import Axes

from post_processing.plotter.common_format_plotter import (
    BLOCKSIZE_THRESHOLD_KB,
    BYTES_TO_MB_DIVISOR,
    ERROR_BAR_CAP_SIZE,
    KB_CONVERSION_FACTOR,
    NANOSECONDS_TO_MS_DIVISOR,
    CommonFormatPlotter,
)
from post_processing.plotter.cpu_plotter import CPUPlotter
from post_processing.plotter.io_plotter import IOPlotter
from post_processing.plotter.plot_data_results import PlotDataResult


class ConcreteCommonFormatPlotter(CommonFormatPlotter):
    """Concrete implementation for testing abstract base class"""

    def draw_and_save(self) -> None:
        """Dummy implementation"""
        pass

    def _generate_output_file_name(self, files: list) -> str:  # type: ignore[type-arg]
        """Dummy implementation"""
        return "test.svg"


class TestCommonFormatPlotterConstants(unittest.TestCase):
    """Test cases for module-level constants"""

    def test_blocksize_threshold_kb(self) -> None:
        """Test BLOCKSIZE_THRESHOLD_KB constant"""
        self.assertEqual(BLOCKSIZE_THRESHOLD_KB, 64)

    def test_bytes_to_mb_divisor(self) -> None:
        """Test BYTES_TO_MB_DIVISOR constant"""
        self.assertEqual(BYTES_TO_MB_DIVISOR, 1024 * 1024)

    def test_nanoseconds_to_ms_divisor(self) -> None:
        """Test NANOSECONDS_TO_MS_DIVISOR constant"""
        self.assertEqual(NANOSECONDS_TO_MS_DIVISOR, 1_000_000)

    def test_error_bar_cap_size(self) -> None:
        """Test ERROR_BAR_CAP_SIZE constant"""
        self.assertEqual(ERROR_BAR_CAP_SIZE, 3)

    def test_kb_conversion_factor(self) -> None:
        """Test KB_CONVERSION_FACTOR constant"""
        self.assertEqual(KB_CONVERSION_FACTOR, 1024)


class TestCommonFormatPlotterHelperMethods(unittest.TestCase):
    """Test cases for CommonFormatPlotter helper methods"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        mock_plotter = MagicMock()
        self.plotter = ConcreteCommonFormatPlotter(mock_plotter)

    def test_calculate_blocksize_kb_small(self) -> None:
        """Test _calculate_blocksize_kb with small blocksize"""
        result = self.plotter._calculate_blocksize_kb("4096")
        self.assertEqual(result, 4)

    def test_calculate_blocksize_kb_medium(self) -> None:
        """Test _calculate_blocksize_kb with medium blocksize"""
        result = self.plotter._calculate_blocksize_kb("65536")
        self.assertEqual(result, 64)

    def test_calculate_blocksize_kb_large(self) -> None:
        """Test _calculate_blocksize_kb with large blocksize"""
        result = self.plotter._calculate_blocksize_kb("1048576")
        self.assertEqual(result, 1024)

    def test_should_use_bandwidth_below_threshold(self) -> None:
        """Test _should_use_bandwidth returns False for blocksize below threshold"""
        result = self.plotter._should_use_bandwidth(32)
        self.assertFalse(result)

    def test_should_use_bandwidth_at_threshold(self) -> None:
        """Test _should_use_bandwidth returns True for blocksize at threshold"""
        result = self.plotter._should_use_bandwidth(64)
        self.assertTrue(result)

    def test_should_use_bandwidth_above_threshold(self) -> None:
        """Test _should_use_bandwidth returns True for blocksize above threshold"""
        result = self.plotter._should_use_bandwidth(128)
        self.assertTrue(result)

    def test_convert_bandwidth_to_mb(self) -> None:
        """Test _convert_bandwidth_to_mb conversion"""
        # 100 MB in bytes = 100 * 1024 * 1024 = 104857600
        result = self.plotter._convert_bandwidth_to_mb("104857600")
        self.assertAlmostEqual(result, 100.0, places=2)

    def test_convert_bandwidth_to_mb_fractional(self) -> None:
        """Test _convert_bandwidth_to_mb with fractional result"""
        # 1.5 MB in bytes = 1.5 * 1024 * 1024 = 1572864
        result = self.plotter._convert_bandwidth_to_mb("1572864")
        self.assertAlmostEqual(result, 1.5, places=2)

    def test_convert_std_dev_to_ms(self) -> None:
        """Test _convert_std_dev_to_ms conversion"""
        # 5 ms in nanoseconds = 5 * 1000000 = 5000000
        result = self.plotter._convert_std_dev_to_ms("5000000")
        self.assertAlmostEqual(result, 5.0, places=2)

    def test_convert_std_dev_to_ms_fractional(self) -> None:
        """Test _convert_std_dev_to_ms with fractional result"""
        # 2.5 ms in nanoseconds = 2.5 * 1000000 = 2500000
        result = self.plotter._convert_std_dev_to_ms("2500000")
        self.assertAlmostEqual(result, 2.5, places=2)

    def test_extract_x_axis_data_small_blocksize(self) -> None:
        """Test _extract_x_axis_data with small blocksize (uses IOPS)"""
        data = {
            "blocksize": "4096",  # 4KB
            "bandwidth_bytes": "1000000",
            "iops": "250.5",
        }
        x_value, x_label = self.plotter._extract_x_axis_data(data)
        self.assertAlmostEqual(x_value, 250.5, places=2)
        self.assertEqual(x_label, "IOps")

    def test_extract_x_axis_data_large_blocksize(self) -> None:
        """Test _extract_x_axis_data with large blocksize (uses bandwidth)"""
        data = {
            "blocksize": "131072",  # 128KB
            "bandwidth_bytes": "104857600",  # 100 MB
            "iops": "800",
        }
        x_value, x_label = self.plotter._extract_x_axis_data(data)
        self.assertAlmostEqual(x_value, 100.0, places=2)
        self.assertEqual(x_label, "Bandwidth (MB/s)")

    def test_extract_x_axis_data_threshold_blocksize(self) -> None:
        """Test _extract_x_axis_data at threshold blocksize (uses bandwidth)"""
        data = {
            "blocksize": "65536",  # 64KB (at threshold)
            "bandwidth_bytes": "52428800",  # 50 MB
            "iops": "800",
        }
        x_value, x_label = self.plotter._extract_x_axis_data(data)
        self.assertAlmostEqual(x_value, 50.0, places=2)
        self.assertEqual(x_label, "Bandwidth (MB/s)")

    @patch("post_processing.plotter.common_format_plotter.log")
    def test_validate_cpu_data_availability_with_cpu_data(self, mock_log: MagicMock) -> None:
        """Test _validate_cpu_data_availability when CPU data exists"""
        data = {"cpu": "45.5"}
        result = self.plotter._validate_cpu_data_availability(data, True)
        self.assertTrue(result)
        mock_log.warning.assert_not_called()

    @patch("post_processing.plotter.common_format_plotter.log")
    def test_validate_cpu_data_availability_without_cpu_data(self, mock_log: MagicMock) -> None:
        """Test _validate_cpu_data_availability when CPU data is missing"""
        data = {"latency": "5000000"}
        result = self.plotter._validate_cpu_data_availability(data, True)
        self.assertFalse(result)
        mock_log.warning.assert_called_once()
        self.assertIn("CPU data not found", mock_log.warning.call_args[0][0])

    @patch("post_processing.plotter.common_format_plotter.log")
    def test_validate_cpu_data_availability_not_requested(self, mock_log: MagicMock) -> None:
        """Test _validate_cpu_data_availability when resource plotting not requested"""
        data = {"latency": "5000000"}
        result = self.plotter._validate_cpu_data_availability(data, False)
        self.assertFalse(result)
        mock_log.warning.assert_not_called()

    @patch("post_processing.plotter.common_format_plotter.CPUPlotter")
    @patch("post_processing.plotter.common_format_plotter.IOPlotter")
    def test_initialize_plotters_with_label(
        self, mock_io_plotter_class: MagicMock, mock_cpu_plotter_class: MagicMock
    ) -> None:
        """Test _initialize_plotters with custom label"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)
        mock_io_plotter_class.return_value = mock_io_plotter
        mock_cpu_plotter_class.return_value = mock_cpu_plotter

        io_plotter, cpu_plotter = self.plotter._initialize_plotters(mock_axes, "Custom Label")

        self.assertEqual(io_plotter, mock_io_plotter)
        self.assertEqual(cpu_plotter, mock_cpu_plotter)
        self.assertEqual(io_plotter.y_label, "Latency (ms)")
        self.assertEqual(io_plotter.plot_label, "Custom Label")

    @patch("post_processing.plotter.common_format_plotter.CPUPlotter")
    @patch("post_processing.plotter.common_format_plotter.IOPlotter")
    def test_initialize_plotters_without_label(
        self, mock_io_plotter_class: MagicMock, mock_cpu_plotter_class: MagicMock
    ) -> None:
        """Test _initialize_plotters with default label"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)
        mock_io_plotter_class.return_value = mock_io_plotter
        mock_cpu_plotter_class.return_value = mock_cpu_plotter

        io_plotter, _ = self.plotter._initialize_plotters(mock_axes, None)

        self.assertEqual(io_plotter.plot_label, "IO Details")

    def test_extract_plot_data_with_error_bars(self) -> None:
        """Test _extract_plot_data with error bars enabled"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        sorted_plot_data = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "250",
                "latency": "5000000",
                "std_deviation": "500000",
            },
            "2": {
                "blocksize": "4096",
                "bandwidth_bytes": "2000000",
                "iops": "500",
                "latency": "4000000",
                "std_deviation": "400000",
            },
        }

        result = self.plotter._extract_plot_data(
            sorted_plot_data, mock_axes, mock_io_plotter, mock_cpu_plotter, True, False
        )

        self.assertIsInstance(result, PlotDataResult)
        self.assertEqual(len(result.x_data), 2)
        self.assertEqual(len(result.error_bars), 2)
        self.assertEqual(result.cap_size, ERROR_BAR_CAP_SIZE)
        self.assertFalse(result.plot_resource_usage)
        # Verify error bars are converted from ns to ms
        self.assertAlmostEqual(result.error_bars[0], 0.5, places=2)
        self.assertAlmostEqual(result.error_bars[1], 0.4, places=2)

    def test_extract_plot_data_with_cpu_usage(self) -> None:
        """Test _extract_plot_data with CPU usage enabled"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        sorted_plot_data = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "250",
                "latency": "5000000",
                "std_deviation": "500000",
                "cpu": "45.5",
            },
        }

        result = self.plotter._extract_plot_data(
            sorted_plot_data, mock_axes, mock_io_plotter, mock_cpu_plotter, True, True
        )

        self.assertIsInstance(result, PlotDataResult)
        self.assertTrue(result.plot_resource_usage)
        self.assertEqual(result.cap_size, 0)  # No error bars when CPU is plotted
        mock_cpu_plotter.add_y_data.assert_called_once_with("45.5")

    def test_extract_plot_data_large_blocksize_uses_bandwidth(self) -> None:
        """Test _extract_plot_data uses bandwidth for large blocksizes"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        sorted_plot_data = {
            "1": {
                "blocksize": "131072",  # 128KB
                "bandwidth_bytes": "104857600",  # 100 MB
                "iops": "800",
                "latency": "5000000",
                "std_deviation": "500000",
            },
        }

        result = self.plotter._extract_plot_data(
            sorted_plot_data, mock_axes, mock_io_plotter, mock_cpu_plotter, False, False
        )

        self.assertIsInstance(result, PlotDataResult)
        # Should use bandwidth (100 MB)
        self.assertAlmostEqual(result.x_data[0], 100.0, places=2)
        mock_axes.set_xlabel.assert_called_once_with("Bandwidth (MB/s)")

    def test_render_plots_with_error_bars(self) -> None:
        """Test _render_plots with error bars"""
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        plot_result = PlotDataResult(
            x_data=[100.0, 200.0],
            error_bars=[0.5, 0.6],
            cap_size=ERROR_BAR_CAP_SIZE,
            plot_resource_usage=False,
            x_label="IOps",
        )

        self.plotter._render_plots(mock_io_plotter, mock_cpu_plotter, plot_result)

        mock_io_plotter.plot_with_error_bars.assert_called_once_with(
            x_data=[100.0, 200.0], error_data=[0.5, 0.6], cap_size=ERROR_BAR_CAP_SIZE
        )
        mock_cpu_plotter.plot.assert_not_called()

    def test_render_plots_with_cpu_usage(self) -> None:
        """Test _render_plots with CPU usage"""
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        plot_result = PlotDataResult(
            x_data=[100.0, 200.0], error_bars=[0.0, 0.0], cap_size=0, plot_resource_usage=True, x_label="IOps"
        )

        self.plotter._render_plots(mock_io_plotter, mock_cpu_plotter, plot_result)

        mock_io_plotter.plot_with_error_bars.assert_called_once()
        mock_cpu_plotter.plot.assert_called_once_with(x_data=[100.0, 200.0])

    def test_calculate_error_bar_with_error_bars_enabled(self) -> None:
        """Test _calculate_error_bar when error bars are enabled"""
        data = {"std_deviation": "1000000"}  # 1ms in nanoseconds
        result = self.plotter._calculate_error_bar(data, True, False)
        self.assertAlmostEqual(result, 1.0, places=2)

    def test_calculate_error_bar_with_error_bars_disabled(self) -> None:
        """Test _calculate_error_bar when error bars are disabled"""
        data = {"std_deviation": "1000000"}
        result = self.plotter._calculate_error_bar(data, False, False)
        self.assertEqual(result, 0.0)

    def test_calculate_error_bar_with_resource_plotting(self) -> None:
        """Test _calculate_error_bar when resource plotting is enabled"""
        data = {"std_deviation": "1000000"}
        result = self.plotter._calculate_error_bar(data, True, True)
        self.assertEqual(result, 0.0)

    def test_calculate_error_bar_missing_std_deviation(self) -> None:
        """Test _calculate_error_bar with missing std_deviation field"""
        data: dict[str, str] = {}
        result = self.plotter._calculate_error_bar(data, True, False)
        self.assertEqual(result, 0.0)

    def test_extract_plot_data_empty_dataset(self) -> None:
        """Test _extract_plot_data raises ValueError for empty dataset"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        with self.assertRaises(ValueError) as context:
            self.plotter._extract_plot_data({}, mock_axes, mock_io_plotter, mock_cpu_plotter, False, False)
        self.assertIn("empty dataset", str(context.exception))

    def test_extract_plot_data_all_missing_latency_field(self) -> None:
        """Test _extract_plot_data raises ValueError when all data points are invalid"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        sorted_plot_data = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "250",
                # Missing "latency" field
                "std_deviation": "500000",
            },
        }

        with self.assertRaises(ValueError) as context:
            self.plotter._extract_plot_data(
                sorted_plot_data, mock_axes, mock_io_plotter, mock_cpu_plotter, False, False
            )
        self.assertIn("No valid data points", str(context.exception))

    def test_extract_plot_data_partial_failure(self) -> None:
        """Test _extract_plot_data continues processing after individual failures"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        sorted_plot_data = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "250",
                # Missing "latency" field - should fail
                "std_deviation": "500000",
            },
            "2": {
                "blocksize": "4096",
                "bandwidth_bytes": "2000000",
                "iops": "500",
                "latency": "4000000",
                "std_deviation": "400000",
            },
        }

        result = self.plotter._extract_plot_data(
            sorted_plot_data, mock_axes, mock_io_plotter, mock_cpu_plotter, False, False
        )

        # Should have extracted data from the second entry only
        self.assertEqual(len(result.x_data), 1)
        self.assertAlmostEqual(result.x_data[0], 500.0, places=2)

    def test_extract_plot_data_x_label_fallback(self) -> None:
        """Test _extract_plot_data provides fallback x_label"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        sorted_plot_data = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "250",
                "latency": "5000000",
                "std_deviation": "500000",
            },
        }

        result = self.plotter._extract_plot_data(
            sorted_plot_data, mock_axes, mock_io_plotter, mock_cpu_plotter, False, False
        )

        # Should have a valid x_label (not "Unknown" since data is valid)
        self.assertIn(result.x_label, ["IOps", "Bandwidth (MB/s)"])

    def test_extract_plot_data_invalid_latency_value(self) -> None:
        """Test _extract_plot_data handles invalid latency values"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        sorted_plot_data = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "250",
                "latency": "invalid",  # Invalid value
                "std_deviation": "500000",
            },
            "2": {
                "blocksize": "4096",
                "bandwidth_bytes": "2000000",
                "iops": "500",
                "latency": "4000000",
                "std_deviation": "400000",
            },
        }

        # Should skip invalid entry and process valid one
        result = self.plotter._extract_plot_data(
            sorted_plot_data, mock_axes, mock_io_plotter, mock_cpu_plotter, False, False
        )

        # Should have extracted data from the second entry only
        self.assertEqual(len(result.x_data), 1)
        self.assertAlmostEqual(result.x_data[0], 500.0, places=2)

    @patch("post_processing.plotter.common_format_plotter.log")
    def test_extract_plot_data_invalid_cpu_value(self, mock_log: MagicMock) -> None:
        """Test _extract_plot_data handles invalid CPU values"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        sorted_plot_data = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "250",
                "latency": "5000000",
                "cpu": "invalid",  # Invalid CPU value
            },
            "2": {
                "blocksize": "4096",
                "bandwidth_bytes": "2000000",
                "iops": "500",
                "latency": "4000000",
                "cpu": "50.5",
            },
        }

        result = self.plotter._extract_plot_data(
            sorted_plot_data, mock_axes, mock_io_plotter, mock_cpu_plotter, False, True
        )

        # Should log warning about invalid CPU value
        mock_log.warning.assert_called()
        # Check for either the specific invalid CPU warning or the general CPU data not found warning
        warning_message = str(mock_log.warning.call_args)
        self.assertTrue(
            "Invalid CPU value" in warning_message or "CPU data not found" in warning_message
        )

        # Should still process valid data
        self.assertEqual(len(result.x_data), 2)

    @patch("post_processing.plotter.common_format_plotter.log")
    def test_extract_plot_data_missing_cpu_disables_resource_plotting(self, mock_log: MagicMock) -> None:
        """Test that missing CPU data disables resource plotting with warning"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)

        sorted_plot_data = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "250",
                "latency": "5000000",
                # Missing CPU data
            },
        }

        result = self.plotter._extract_plot_data(
            sorted_plot_data, mock_axes, mock_io_plotter, mock_cpu_plotter, False, True
        )

        # Should log warning about missing CPU data
        mock_log.warning.assert_called()
        warning_message = mock_log.warning.call_args[0][0]
        self.assertIn("Unable to plot CPU usage", warning_message)
        self.assertIn("CPU data not found", warning_message)

        # Resource plotting should be disabled
        self.assertFalse(result.plot_resource_usage)


class TestCommonFormatPlotterTitleGeneration(unittest.TestCase):
    """Test cases for title generation methods"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        mock_plotter = MagicMock()
        self.plotter = ConcreteCommonFormatPlotter(mock_plotter)

    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    def test_construct_title_from_list_single_blocksize(self, mock_get_details: MagicMock) -> None:
        """Test title generation when all files have same blocksize"""
        from pathlib import Path

        mock_get_details.side_effect = [
            ("4096", "100", "read", "1"),
            ("4096", "50", "write", "1"),
        ]

        files = [Path("4096_100_read_1.json"), Path("4096_50_write_1.json")]
        title = self.plotter._construct_title_from_list_of_file_names(files)

        self.assertEqual(title, "4096 blocksize comparison")

    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    def test_construct_title_from_list_single_operation_and_percentage(self, mock_get_details: MagicMock) -> None:
        """Test title generation when operation and percentage are same"""
        from pathlib import Path

        mock_get_details.side_effect = [
            ("4096", "100", "read", "1"),
            ("8192", "100", "read", "1"),
        ]

        files = [Path("4096_100_read_1.json"), Path("8192_100_read_1.json")]
        title = self.plotter._construct_title_from_list_of_file_names(files)

        self.assertEqual(title, "100 read comparison")

    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    def test_construct_title_from_list_single_operation(self, mock_get_details: MagicMock) -> None:
        """Test title generation when only operation is same"""
        from pathlib import Path

        mock_get_details.side_effect = [
            ("4096", "100", "read", "1"),
            ("8192", "50", "read", "1"),
        ]

        files = [Path("4096_100_read_1.json"), Path("8192_50_read_1.json")]
        title = self.plotter._construct_title_from_list_of_file_names(files)

        self.assertEqual(title, "read comparison")

    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    def test_construct_title_from_list_no_common_elements(self, mock_get_details: MagicMock) -> None:
        """Test title generation when no common elements"""
        from pathlib import Path

        mock_get_details.side_effect = [
            ("4096", "100", "read", "1"),
            ("8192", "50", "write", "1"),
        ]

        files = [Path("4096_100_read_1.json"), Path("8192_50_write_1.json")]
        title = self.plotter._construct_title_from_list_of_file_names(files)

        self.assertIn("4096 100 read", title)
        self.assertIn("Vs", title)
        self.assertIn("8192 50 write", title)

    def test_add_title_single_file(self) -> None:
        """Test _add_title with single file"""
        from pathlib import Path

        with patch.object(self.plotter, "_construct_title_from_file_name", return_value="Test Title") as mock_construct:
            self.plotter._add_title([Path("test.json")])
            mock_construct.assert_called_once_with("test.json")
            self.plotter._plotter.title.assert_called_once_with("Test Title")

    def test_add_title_multiple_files(self) -> None:
        """Test _add_title with multiple files"""
        from pathlib import Path

        files = [Path("test1.json"), Path("test2.json")]
        with patch.object(
            self.plotter, "_construct_title_from_list_of_file_names", return_value="Comparison Title"
        ) as mock_construct:
            self.plotter._add_title(files)
            mock_construct.assert_called_once_with(files)
            self.plotter._plotter.title.assert_called_once_with("Comparison Title")


class TestCommonFormatPlotterAxisSettings(unittest.TestCase):
    """Test cases for axis setting methods"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        mock_plotter = MagicMock()
        self.plotter = ConcreteCommonFormatPlotter(mock_plotter)

    def test_set_axis_with_maximum_values(self) -> None:
        """Test _set_axis with explicit maximum values"""
        self.plotter._set_axis(maximum_values=(100, 200))

        self.plotter._plotter.xlim.assert_called_once_with(0, 100)
        self.plotter._plotter.ylim.assert_called_once_with(0, 200)

    def test_set_axis_without_maximum_values(self) -> None:
        """Test _set_axis with auto-scaling (None)"""
        self.plotter._set_axis(maximum_values=None)

        self.plotter._plotter.xlim.assert_called_once_with(0, None)
        self.plotter._plotter.ylim.assert_called_once_with(0, None)


class TestCommonFormatPlotterIntegration(unittest.TestCase):
    """Integration tests for the refactored method"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        mock_plotter = MagicMock()
        self.plotter = ConcreteCommonFormatPlotter(mock_plotter)

    @patch("post_processing.plotter.common_format_plotter.CPUPlotter")
    @patch("post_processing.plotter.common_format_plotter.IOPlotter")
    def test_add_single_file_data_complete_flow(
        self, mock_io_plotter_class: MagicMock, mock_cpu_plotter_class: MagicMock
    ) -> None:
        """Test complete flow of _add_single_file_data_with_optional_errorbars"""
        mock_axes = MagicMock(spec=Axes)
        mock_io_plotter = MagicMock(spec=IOPlotter)
        mock_cpu_plotter = MagicMock(spec=CPUPlotter)
        mock_io_plotter_class.return_value = mock_io_plotter
        mock_cpu_plotter_class.return_value = mock_cpu_plotter

        file_data = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "250",
                "latency": "5000000",
                "std_deviation": "500000",
            },
            "2": {
                "blocksize": "4096",
                "bandwidth_bytes": "2000000",
                "iops": "500",
                "latency": "4000000",
                "std_deviation": "400000",
            },
        }

        self.plotter._add_single_file_data_with_optional_errorbars(
            file_data=file_data,  # type: ignore[arg-type]
            main_axes=mock_axes,
            plot_error_bars=True,
            plot_resource_usage=False,
            label="Test",
        )

        # Verify plotters were initialized
        mock_io_plotter_class.assert_called_once()
        mock_cpu_plotter_class.assert_called_once()

        # Verify data was added
        self.assertEqual(mock_io_plotter.add_y_data.call_count, 2)

        # Verify plot was rendered
        mock_io_plotter.plot_with_error_bars.assert_called_once()


# Made with Bob
