"""
Unit tests for the post_processing/plotter time_series_latency_plotter module class
"""

# pyright: strict, reportPrivateUsage=false
# pylint: disable=protected-access
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from unittest.mock import MagicMock

from matplotlib.axes import Axes

from post_processing.plotter.time_series_latency_plotter import (
    LATENCY_MEAN_COLOR,
    LATENCY_P50_COLOR,
    LATENCY_P95_COLOR,
    LATENCY_P99_COLOR,
    LATENCY_PLOT_LABEL,
    LATENCY_Y_LABEL,
    TimeSeriesLatencyPlotter,
)


class TestTimeSeriesLatencyPlotter(unittest.TestCase):
    """Test cases for TimeSeriesLatencyPlotter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.mock_axes = MagicMock(spec=Axes)
        self.plotter = TimeSeriesLatencyPlotter(self.mock_axes)

    def test_initialization(self) -> None:
        """Test TimeSeriesLatencyPlotter initialization"""
        self.assertEqual(self.plotter._main_axes, self.mock_axes)
        self.assertEqual(self.plotter._y_data, [])
        self.assertEqual(self.plotter._p50_data, [])
        self.assertEqual(self.plotter._p95_data, [])
        self.assertEqual(self.plotter._p99_data, [])
        self.assertEqual(self.plotter._max_data, [])

    def test_add_y_data(self) -> None:
        """Test adding mean latency data"""
        self.plotter.add_y_data("5.5")
        self.plotter.add_y_data("10.3")

        self.assertEqual(len(self.plotter._y_data), 2)
        self.assertAlmostEqual(self.plotter._y_data[0], 5.5)
        self.assertAlmostEqual(self.plotter._y_data[1], 10.3)

    def test_add_p50_data(self) -> None:
        """Test adding P50 latency data"""
        self.plotter.add_p50_data("3.0")
        self.plotter.add_p50_data("4.5")

        self.assertEqual(len(self.plotter._p50_data), 2)
        self.assertAlmostEqual(self.plotter._p50_data[0], 3.0)
        self.assertAlmostEqual(self.plotter._p50_data[1], 4.5)

    def test_add_p95_data(self) -> None:
        """Test adding P95 latency data"""
        self.plotter.add_p95_data("8.0")
        self.plotter.add_p95_data("12.5")

        self.assertEqual(len(self.plotter._p95_data), 2)
        self.assertAlmostEqual(self.plotter._p95_data[0], 8.0)
        self.assertAlmostEqual(self.plotter._p95_data[1], 12.5)

    def test_add_p99_data(self) -> None:
        """Test adding P99 latency data"""
        self.plotter.add_p99_data("15.0")
        self.plotter.add_p99_data("20.5")

        self.assertEqual(len(self.plotter._p99_data), 2)
        self.assertAlmostEqual(self.plotter._p99_data[0], 15.0)
        self.assertAlmostEqual(self.plotter._p99_data[1], 20.5)

    def test_add_max_data(self) -> None:
        """Test adding max latency data"""
        self.plotter.add_max_data("25.0")
        self.plotter.add_max_data("30.5")

        self.assertEqual(len(self.plotter._max_data), 2)
        self.assertAlmostEqual(self.plotter._max_data[0], 25.0)
        self.assertAlmostEqual(self.plotter._max_data[1], 30.5)

    def test_plot_with_all_data(self) -> None:
        """Test plotting latency data with all percentiles"""
        # Add data for all metrics
        self.plotter.add_y_data("5.0")
        self.plotter.add_y_data("6.0")
        self.plotter.add_p50_data("3.0")
        self.plotter.add_p50_data("4.0")
        self.plotter.add_p95_data("8.0")
        self.plotter.add_p95_data("9.0")
        self.plotter.add_p99_data("12.0")
        self.plotter.add_p99_data("13.0")
        self.plotter.add_max_data("20.0")
        self.plotter.add_max_data("21.0")

        x_data = [1.0, 2.0]
        self.plotter.plot(x_data)

        # Should set label and y_label
        self.assertEqual(self.plotter._label, LATENCY_PLOT_LABEL)
        self.assertEqual(self.plotter._y_label, LATENCY_Y_LABEL)

        # Should call set_ylabel
        self.mock_axes.set_ylabel.assert_called_once_with(LATENCY_Y_LABEL)

        # Should call fill_between for percentile bands (2 times: P50-P95, P95-P99)
        self.assertEqual(self.mock_axes.fill_between.call_count, 2)

        # Should call plot for percentile lines and mean (5 times: P50, P95, P99, mean, max)
        self.assertEqual(self.mock_axes.plot.call_count, 5)

    def test_plot_with_minimal_data(self) -> None:
        """Test plotting with only mean latency data"""
        self.plotter.add_y_data("5.0")
        self.plotter.add_y_data("6.0")

        x_data = [1.0, 2.0]
        self.plotter.plot(x_data)

        # Should still set labels
        self.assertEqual(self.plotter._label, LATENCY_PLOT_LABEL)
        self.assertEqual(self.plotter._y_label, LATENCY_Y_LABEL)

        # Should not call fill_between (no percentile data)
        self.mock_axes.fill_between.assert_not_called()

        # Should call plot once for mean latency
        self.assertEqual(self.mock_axes.plot.call_count, 1)

    def test_plot_skips_max_when_zero(self) -> None:
        """Test that max latency is not plotted when all values are zero"""
        self.plotter.add_y_data("5.0")
        self.plotter.add_max_data("0.0")

        x_data = [1.0]
        self.plotter.plot(x_data)

        # Should only plot mean (not max since it's zero)
        self.assertEqual(self.mock_axes.plot.call_count, 1)

    def test_latency_constants(self) -> None:
        """Test latency plotter constants"""
        self.assertEqual(LATENCY_MEAN_COLOR, "xkcd:orange")
        self.assertEqual(LATENCY_P50_COLOR, "xkcd:green")
        self.assertEqual(LATENCY_P95_COLOR, "xkcd:red")
        self.assertEqual(LATENCY_P99_COLOR, "xkcd:dark red")
        self.assertEqual(LATENCY_Y_LABEL, "Latency (ms)")
        self.assertEqual(LATENCY_PLOT_LABEL, "Mean Latency")


# Made with Bob
