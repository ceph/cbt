"""
A file containing the TimeSeriesBandwidthPlotter class for plotting bandwidth over time.
"""

from logging import Logger, getLogger

from post_processing.plotter.time_series_metric_plotter import TimeSeriesMetricPlotter

log: Logger = getLogger("plotter")

BANDWIDTH_PLOT_DEFAULT_COLOUR: str = "xkcd:purple"  # Purple from xkcd color survey
BANDWIDTH_Y_LABEL: str = "Bandwidth (MB/s)"
BANDWIDTH_PLOT_LABEL: str = "Bandwidth"
BYTES_TO_MB_DIVISOR: int = 1024 * 1024


class TimeSeriesBandwidthPlotter(TimeSeriesMetricPlotter):
    """
    A class to plot bandwidth data over time on a time-series plot.
    Converts bandwidth from bytes to MB/s.
    """

    def _get_default_color(self) -> str:
        """
        Return the default color for bandwidth plots.
        """
        return BANDWIDTH_PLOT_DEFAULT_COLOUR

    def _get_y_label(self) -> str:
        """
        Return the y-axis label for bandwidth plots.
        """
        return BANDWIDTH_Y_LABEL

    def _get_plot_label(self) -> str:
        """
        Return the plot label for bandwidth plots.
        """
        return BANDWIDTH_PLOT_LABEL

    def _convert_value(self, value: float) -> float:
        """
        Convert bandwidth from bytes to MB/s.

        Args:
            value: Bandwidth value in bytes

        Returns:
            Bandwidth value in MB/s
        """
        return value / BYTES_TO_MB_DIVISOR


# Made with Bob
