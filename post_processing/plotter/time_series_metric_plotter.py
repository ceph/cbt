"""
Base class for time-series metric plotters.

This module provides the TimeSeriesMetricPlotter base class that contains
common plotting logic for time-series metrics (bandwidth, IOPS, latency).
Subclasses only need to override specific attributes and conversion logic.
"""

from abc import ABC, abstractmethod
from logging import Logger, getLogger

from post_processing.plotter.axis_plotter import AxisPlotter

log: Logger = getLogger("plotter")

# Default plotting parameters
DEFAULT_LINE_WIDTH: float = 1.5
DEFAULT_ALPHA: float = 0.8


class TimeSeriesMetricPlotter(AxisPlotter, ABC):
    """
    Base class for plotting time-series metrics.

    Provides common plotting functionality for metrics like bandwidth, IOPS, and latency.
    Subclasses should override:
    - _get_default_color(): Return the default color for this metric
    - _get_y_label(): Return the y-axis label
    - _get_plot_label(): Return the plot label for the legend
    - _convert_value(value): Convert raw value to display units (optional)
    """

    @abstractmethod
    def _get_default_color(self) -> str:
        """
        Get the default color for this metric.

        Returns:
            Color string (e.g., "xkcd:purple", "#FF0000")
        """

    @abstractmethod
    def _get_y_label(self) -> str:
        """
        Get the y-axis label for this metric.

        Returns:
            Y-axis label string (e.g., "Bandwidth (MB/s)")
        """

    @abstractmethod
    def _get_plot_label(self) -> str:
        """
        Get the plot label for the legend.

        Returns:
            Plot label string (e.g., "Bandwidth")
        """

    def _convert_value(self, value: float) -> float:
        """
        Convert raw value to display units.

        Default implementation returns value unchanged.
        Override in subclasses for unit conversion (e.g., bytes to MB).

        Args:
            value: Raw value from data

        Returns:
            Converted value for display
        """
        return value

    def add_y_data(self, data_value: str) -> None:
        """
        Add a point of data for this plot.

        Args:
            data_value: A single value as a string.
                       Will be converted using _convert_value() internally.
        """
        converted_value = self._convert_value(float(data_value))
        self._y_data.append(converted_value)

    def plot(self, x_data: list[float], colour: str = "") -> None:
        """
        Plot data on the main axes.

        Args:
            x_data: The data for the x-axis (timestamps)
            colour: The colour for the plot line (optional, uses default if not provided)
        """
        axis = self._main_axes
        self._label = self._get_plot_label()
        self._y_label = self._get_y_label()
        plot_colour = colour if colour else self._get_default_color()

        axis.set_ylabel(self._y_label)  # pyright: ignore[reportUnknownMemberType]
        axis.plot(  # pyright: ignore[reportUnknownMemberType]
            x_data,
            self._y_data,
            color=plot_colour,
            linewidth=DEFAULT_LINE_WIDTH,
            alpha=DEFAULT_ALPHA,
            label=self._label,
        )


# Made with Bob
