"""
Unit tests for the post_processing/plotter module classes
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from unittest.mock import MagicMock

from matplotlib.axes import Axes

from post_processing.plotter.axis_plotter import AxisPlotter


class ConcreteAxisPlotter(AxisPlotter):
    """Concrete implementation of AxisPlotter for testing"""

    def plot(self, x_data: list[float], colour: str = "") -> None:
        """Concrete implementation of abstract plot method"""
        self._plot(x_data, self._main_axes, colour)

    def add_y_data(self, data_value: str) -> None:
        """Concrete implementation of abstract add_y_data method"""
        self._y_data.append(float(data_value))


class TestAxisPlotter(unittest.TestCase):
    """Test cases for AxisPlotter base class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.mock_axes = MagicMock(spec=Axes)
        self.plotter = ConcreteAxisPlotter(self.mock_axes)

    def test_initialization(self) -> None:
        """Test AxisPlotter initialization"""
        self.assertEqual(self.plotter._main_axes, self.mock_axes)
        self.assertEqual(self.plotter._y_data, [])
        self.assertEqual(self.plotter._y_label, "")
        self.assertEqual(self.plotter._label, "")

    def test_y_label_property_getter(self) -> None:
        """Test y_label property getter"""
        self.plotter._y_label = "Test Label"
        self.assertEqual(self.plotter.y_label, "Test Label")

    def test_y_label_property_setter(self) -> None:
        """Test y_label property setter"""
        self.plotter.y_label = "New Label"
        self.assertEqual(self.plotter._y_label, "New Label")

    def test_y_label_setter_warning_on_overwrite(self) -> None:
        """Test that setting y_label twice logs a warning"""
        self.plotter.y_label = "First Label"

        with self.assertLogs("plotter", level="WARNING") as log_context:
            self.plotter.y_label = "Second Label"

        self.assertIn("Y label value already set", log_context.output[0])
        self.assertEqual(self.plotter._y_label, "Second Label")

    def test_plot_label_property_getter(self) -> None:
        """Test plot_label property getter"""
        self.plotter._label = "Test Plot Label"
        self.assertEqual(self.plotter.plot_label, "Test Plot Label")

    def test_plot_label_property_setter(self) -> None:
        """Test plot_label property setter"""
        self.plotter.plot_label = "New Plot Label"
        self.assertEqual(self.plotter._label, "New Plot Label")

    def test_plot_label_setter_warning_on_overwrite(self) -> None:
        """Test that setting plot_label twice logs a warning"""
        self.plotter.plot_label = "First Label"

        with self.assertLogs("plotter", level="WARNING") as log_context:
            self.plotter.plot_label = "Second Label"

        self.assertIn("Plot label value already set", log_context.output[0])
        self.assertEqual(self.plotter._label, "Second Label")

    def test_add_y_data(self) -> None:
        """Test adding y-axis data"""
        self.plotter.add_y_data("10.5")
        self.plotter.add_y_data("20.3")
        self.plotter.add_y_data("30.7")

        self.assertEqual(len(self.plotter._y_data), 3)
        self.assertAlmostEqual(self.plotter._y_data[0], 10.5)
        self.assertAlmostEqual(self.plotter._y_data[1], 20.3)
        self.assertAlmostEqual(self.plotter._y_data[2], 30.7)

    def test_plot_calls_internal_plot(self) -> None:
        """Test that plot method calls _plot"""
        self.plotter.y_label = "Test Y"
        self.plotter.plot_label = "Test Plot"
        self.plotter.add_y_data("10")
        self.plotter.add_y_data("20")

        x_data = [1.0, 2.0]
        self.plotter.plot(x_data, "blue")

        # Verify axes methods were called
        self.mock_axes.set_ylabel.assert_called_once_with("Test Y")
        self.mock_axes.tick_params.assert_called_once()
        self.mock_axes.plot.assert_called_once()


# Made with Bob
