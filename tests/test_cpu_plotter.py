"""
Unit tests for the post_processing/plotter cpu plotter module class
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from unittest.mock import MagicMock

from matplotlib.axes import Axes

from post_processing.plotter.cpu_plotter import (
    CPU_PLOT_DEFAULT_COLOUR,
    CPU_PLOT_LABEL,
    CPU_Y_LABEL,
    CPUPlotter,
)


class TestCPUPlotter(unittest.TestCase):
    """Test cases for CPUPlotter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.mock_axes = MagicMock(spec=Axes)
        self.mock_twin_axes = MagicMock(spec=Axes)
        self.mock_axes.twinx.return_value = self.mock_twin_axes
        self.plotter = CPUPlotter(self.mock_axes)

    def test_initialization(self) -> None:
        """Test CPUPlotter initialization"""
        self.assertEqual(self.plotter._main_axes, self.mock_axes)
        self.assertEqual(self.plotter._y_data, [])

    def test_add_y_data(self) -> None:
        """Test adding CPU data"""
        self.plotter.add_y_data("45.5")
        self.plotter.add_y_data("67.8")

        self.assertEqual(len(self.plotter._y_data), 2)
        self.assertAlmostEqual(self.plotter._y_data[0], 45.5)
        self.assertAlmostEqual(self.plotter._y_data[1], 67.8)

    def test_plot(self) -> None:
        """Test plotting CPU data"""
        self.plotter.add_y_data("50.0")
        self.plotter.add_y_data("60.0")

        x_data = [100.0, 200.0]
        self.plotter.plot(x_data)

        # Should create twin axes
        self.mock_axes.twinx.assert_called_once()

        # Should set label and y_label
        self.assertEqual(self.plotter._label, CPU_PLOT_LABEL)
        self.assertEqual(self.plotter._y_label, CPU_Y_LABEL)

        # Should call plot on twin axes
        self.mock_twin_axes.set_ylabel.assert_called_once_with(CPU_Y_LABEL)
        self.mock_twin_axes.plot.assert_called_once()

    def test_cpu_constants(self) -> None:
        """Test CPU plotter constants"""
        self.assertEqual(CPU_PLOT_DEFAULT_COLOUR, "#5ca904")
        self.assertEqual(CPU_Y_LABEL, "System CPU use (%)")
        self.assertEqual(CPU_PLOT_LABEL, "CPU use")


# Made with Bob
