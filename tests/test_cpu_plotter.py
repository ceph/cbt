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
    CPU_PLOT_LABEL,
    CPU_SOURCE_COLOURS,
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
        self.assertEqual(self.plotter._y_data_by_source, {})

    def test_add_y_data_legacy_format(self) -> None:
        """Test adding CPU data in legacy string format"""
        self.plotter.add_y_data("45.5")
        self.plotter.add_y_data("67.8")

        self.assertIn("default", self.plotter._y_data_by_source)
        self.assertEqual(len(self.plotter._y_data_by_source["default"]), 2)
        self.assertAlmostEqual(self.plotter._y_data_by_source["default"][0], 45.5)
        self.assertAlmostEqual(self.plotter._y_data_by_source["default"][1], 67.8)

    def test_add_y_data_multi_source_format(self) -> None:
        """Test adding CPU data in multi-source dict format"""
        self.plotter.add_y_data({"fio": "45.5", "collectl": "47.8"})
        self.plotter.add_y_data({"fio": "50.0", "collectl": "52.3"})

        self.assertIn("fio", self.plotter._y_data_by_source)
        self.assertIn("collectl", self.plotter._y_data_by_source)
        self.assertEqual(len(self.plotter._y_data_by_source["fio"]), 2)
        self.assertEqual(len(self.plotter._y_data_by_source["collectl"]), 2)
        self.assertAlmostEqual(self.plotter._y_data_by_source["fio"][0], 45.5)
        self.assertAlmostEqual(self.plotter._y_data_by_source["collectl"][0], 47.8)

    def test_plot_legacy_single_source(self) -> None:
        """Test plotting CPU data with legacy single source"""
        self.plotter.add_y_data("50.0")
        self.plotter.add_y_data("60.0")

        x_data = [100.0, 200.0]
        self.plotter.plot(x_data)

        # Should create twin axes
        self.mock_axes.twinx.assert_called_once()

        # Should set y_label
        self.mock_twin_axes.set_ylabel.assert_called_once_with(CPU_Y_LABEL)

        # Y-axis should always be fixed 0-100%
        self.mock_twin_axes.set_ylim.assert_called_once_with(0, 100)

        # Should call plot on twin axes
        self.mock_twin_axes.plot.assert_called_once()

    def test_plot_multi_source(self) -> None:
        """Test plotting CPU data with multiple sources"""
        self.plotter.add_y_data({"fio": "50.0", "collectl": "52.0"})
        self.plotter.add_y_data({"fio": "60.0", "collectl": "62.0"})

        x_data = [100.0, 200.0]
        self.plotter.plot(x_data)

        # Should create twin axes
        self.mock_axes.twinx.assert_called_once()

        # Should set y_label
        self.mock_twin_axes.set_ylabel.assert_called_once_with(CPU_Y_LABEL)

        # Y-axis should always be fixed 0-100%
        self.mock_twin_axes.set_ylim.assert_called_once_with(0, 100)

        # Should call plot twice (once per source)
        self.assertEqual(self.mock_twin_axes.plot.call_count, 2)
        # Should not add inline legend (legend appears below the plot)
        self.mock_twin_axes.legend.assert_not_called()

    def test_cpu_constants(self) -> None:
        """Test CPU plotter constants"""
        self.assertEqual(CPU_Y_LABEL, "System CPU use (%)")
        self.assertEqual(CPU_PLOT_LABEL, "CPU use")
        self.assertIn("fio", CPU_SOURCE_COLOURS)
        self.assertIn("collectl", CPU_SOURCE_COLOURS)
        self.assertIn("default", CPU_SOURCE_COLOURS)


# Made with Bob
