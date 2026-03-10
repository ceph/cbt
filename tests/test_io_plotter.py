"""
Unit tests for the post_processing/plotter IO_plotter module class
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from unittest.mock import MagicMock

from matplotlib.axes import Axes

from post_processing.plotter.io_plotter import (
    IO_PLOT_DEFAULT_COLOUR,
    IO_PLOT_LABEL,
    IO_Y_LABEL,
    IOPlotter,
)


class TestIOPlotter(unittest.TestCase):
    """Test cases for IOPlotter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.mock_axes = MagicMock(spec=Axes)
        self.plotter = IOPlotter(self.mock_axes)

    def test_initialization(self) -> None:
        """Test IOPlotter initialization"""
        self.assertEqual(self.plotter._main_axes, self.mock_axes)
        self.assertEqual(self.plotter._y_data, [])

    def test_add_y_data_converts_nanoseconds_to_milliseconds(self) -> None:
        """Test that add_y_data converts nanoseconds to milliseconds"""
        # 5,000,000 nanoseconds = 5 milliseconds
        self.plotter.add_y_data("5000000")
        self.assertAlmostEqual(self.plotter._y_data[0], 5.0)

        # 10,000,000 nanoseconds = 10 milliseconds
        self.plotter.add_y_data("10000000")
        self.assertAlmostEqual(self.plotter._y_data[1], 10.0)

    def test_plot_raises_not_implemented(self) -> None:
        """Test that plot method raises NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.plotter.plot([1.0, 2.0])

    def test_plot_with_error_bars(self) -> None:
        """Test plotting IO data with error bars"""
        self.plotter.plot_label = "Test IO"
        self.plotter.add_y_data("5000000")  # 5ms
        self.plotter.add_y_data("10000000")  # 10ms

        x_data = [100.0, 200.0]
        error_data = [0.5, 1.0]
        cap_size = 3

        self.plotter.plot_with_error_bars(x_data, error_data, cap_size)

        # Should call errorbar on main axes
        self.mock_axes.errorbar.assert_called_once()
        call_args = self.mock_axes.errorbar.call_args

        # Verify x_data and y_data
        self.assertEqual(list(call_args[0][0]), x_data)
        self.assertEqual(list(call_args[0][1]), [5.0, 10.0])

        # Verify error bars
        self.assertEqual(list(call_args[1]["yerr"]), error_data)
        self.assertEqual(call_args[1]["capsize"], cap_size)
        self.assertEqual(call_args[1]["ecolor"], "red")

    def test_plot_with_error_bars_no_caps(self) -> None:
        """Test plotting with cap_size=0 (no error bar caps)"""
        self.plotter.add_y_data("5000000")

        x_data = [100.0]
        error_data = [0.5]

        self.plotter.plot_with_error_bars(x_data, error_data, cap_size=0)

        call_args = self.mock_axes.errorbar.call_args
        self.assertEqual(call_args[1]["capsize"], 0)

    def test_io_constants(self) -> None:
        """Test IO plotter constants"""
        self.assertEqual(IO_PLOT_DEFAULT_COLOUR, "#5ca904")
        self.assertEqual(IO_Y_LABEL, "Latency (ms)")
        self.assertEqual(IO_PLOT_LABEL, "IO Details")


# Made with Bob
