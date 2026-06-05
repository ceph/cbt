"""
Unit tests for the post_processing/plotter time_series_iops_plotter module class
"""

# pyright: strict, reportPrivateUsage=false
# pylint: disable=protected-access
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from unittest.mock import MagicMock

from matplotlib.axes import Axes

from post_processing.plotter.time_series_iops_plotter import (
    IOPS_PLOT_DEFAULT_COLOUR,
    IOPS_PLOT_LABEL,
    IOPS_Y_LABEL,
    TimeSeriesIOPSPlotter,
)


class TestTimeSeriesIOPSPlotter(unittest.TestCase):
    """Test cases for TimeSeriesIOPSPlotter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.mock_axes = MagicMock(spec=Axes)
        self.plotter = TimeSeriesIOPSPlotter(self.mock_axes)

    def test_initialization(self) -> None:
        """Test TimeSeriesIOPSPlotter initialization"""
        self.assertEqual(self.plotter._main_axes, self.mock_axes)
        self.assertEqual(self.plotter._y_data, [])

    def test_add_y_data(self) -> None:
        """Test adding IOPS data"""
        self.plotter.add_y_data("1000.5")
        self.plotter.add_y_data("2500.75")

        self.assertEqual(len(self.plotter._y_data), 2)
        self.assertAlmostEqual(self.plotter._y_data[0], 1000.5)
        self.assertAlmostEqual(self.plotter._y_data[1], 2500.75)

    def test_plot_with_default_colour(self) -> None:
        """Test plotting IOPS data with default colour"""
        self.plotter.add_y_data("1000")
        self.plotter.add_y_data("2000")

        x_data = [1.0, 2.0]
        self.plotter.plot(x_data)

        # Should set label and y_label
        self.assertEqual(self.plotter._label, IOPS_PLOT_LABEL)
        self.assertEqual(self.plotter._y_label, IOPS_Y_LABEL)

        # Should call plot on main axes with default colour
        self.mock_axes.set_ylabel.assert_called_once_with(IOPS_Y_LABEL)
        self.mock_axes.plot.assert_called_once()

        # Verify plot was called with correct parameters
        call_args = self.mock_axes.plot.call_args
        self.assertEqual(list(call_args[0][0]), x_data)
        self.assertEqual(list(call_args[0][1]), [1000.0, 2000.0])
        self.assertEqual(call_args[1]["color"], IOPS_PLOT_DEFAULT_COLOUR)
        self.assertEqual(call_args[1]["linewidth"], 1.5)
        self.assertEqual(call_args[1]["alpha"], 0.8)
        self.assertEqual(call_args[1]["label"], IOPS_PLOT_LABEL)

    def test_plot_with_custom_colour(self) -> None:
        """Test plotting IOPS data with custom colour"""
        self.plotter.add_y_data("1000")

        x_data = [1.0]
        custom_colour = "#FF0000"
        self.plotter.plot(x_data, colour=custom_colour)

        # Verify plot was called with custom colour
        call_args = self.mock_axes.plot.call_args
        self.assertEqual(call_args[1]["color"], custom_colour)

    def test_iops_constants(self) -> None:
        """Test IOPS plotter constants"""
        self.assertEqual(IOPS_PLOT_DEFAULT_COLOUR, "xkcd:blue")
        self.assertEqual(IOPS_Y_LABEL, "IOPS (ops/s)")
        self.assertEqual(IOPS_PLOT_LABEL, "IOPS")


# Made with Bob
