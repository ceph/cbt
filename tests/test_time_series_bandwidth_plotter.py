"""
Unit tests for the post_processing/plotter time_series_bandwidth_plotter module class
"""

# pyright: strict, reportPrivateUsage=false
# pylint: disable=protected-access
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from unittest.mock import MagicMock

from matplotlib.axes import Axes

from post_processing.plotter.time_series_bandwidth_plotter import (
    BANDWIDTH_PLOT_DEFAULT_COLOUR,
    BANDWIDTH_PLOT_LABEL,
    BANDWIDTH_Y_LABEL,
    BYTES_TO_MB_DIVISOR,
    TimeSeriesBandwidthPlotter,
)


class TestTimeSeriesBandwidthPlotter(unittest.TestCase):
    """Test cases for TimeSeriesBandwidthPlotter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.mock_axes = MagicMock(spec=Axes)
        self.plotter = TimeSeriesBandwidthPlotter(self.mock_axes)

    def test_initialization(self) -> None:
        """Test TimeSeriesBandwidthPlotter initialization"""
        self.assertEqual(self.plotter._main_axes, self.mock_axes)
        self.assertEqual(self.plotter._y_data, [])

    def test_add_y_data_converts_bytes_to_mb(self) -> None:
        """Test that add_y_data converts bytes to MB/s"""
        # 1048576 bytes = 1 MB
        self.plotter.add_y_data("1048576")
        self.assertAlmostEqual(self.plotter._y_data[0], 1.0)

        # 2097152 bytes = 2 MB
        self.plotter.add_y_data("2097152")
        self.assertAlmostEqual(self.plotter._y_data[1], 2.0)

        # 5242880 bytes = 5 MB
        self.plotter.add_y_data("5242880")
        self.assertAlmostEqual(self.plotter._y_data[2], 5.0)

    def test_plot_with_default_colour(self) -> None:
        """Test plotting bandwidth data with default colour"""
        self.plotter.add_y_data("1048576")  # 1 MB
        self.plotter.add_y_data("2097152")  # 2 MB

        x_data = [1.0, 2.0]
        self.plotter.plot(x_data)

        # Should set label and y_label
        self.assertEqual(self.plotter._label, BANDWIDTH_PLOT_LABEL)
        self.assertEqual(self.plotter._y_label, BANDWIDTH_Y_LABEL)

        # Should call plot on main axes with default colour
        self.mock_axes.set_ylabel.assert_called_once_with(BANDWIDTH_Y_LABEL)
        self.mock_axes.plot.assert_called_once()

        # Verify plot was called with correct parameters
        call_args = self.mock_axes.plot.call_args
        self.assertEqual(list(call_args[0][0]), x_data)
        self.assertEqual(list(call_args[0][1]), [1.0, 2.0])
        self.assertEqual(call_args[1]["color"], BANDWIDTH_PLOT_DEFAULT_COLOUR)
        self.assertEqual(call_args[1]["linewidth"], 1.5)
        self.assertEqual(call_args[1]["alpha"], 0.8)
        self.assertEqual(call_args[1]["label"], BANDWIDTH_PLOT_LABEL)

    def test_plot_with_custom_colour(self) -> None:
        """Test plotting bandwidth data with custom colour"""
        self.plotter.add_y_data("1048576")

        x_data = [1.0]
        custom_colour = "#00FF00"
        self.plotter.plot(x_data, colour=custom_colour)

        # Verify plot was called with custom colour
        call_args = self.mock_axes.plot.call_args
        self.assertEqual(call_args[1]["color"], custom_colour)

    def test_bandwidth_constants(self) -> None:
        """Test bandwidth plotter constants"""
        self.assertEqual(BANDWIDTH_PLOT_DEFAULT_COLOUR, "xkcd:purple")
        self.assertEqual(BANDWIDTH_Y_LABEL, "Bandwidth (MB/s)")
        self.assertEqual(BANDWIDTH_PLOT_LABEL, "Bandwidth")
        self.assertEqual(BYTES_TO_MB_DIVISOR, 1024 * 1024)


# Made with Bob
