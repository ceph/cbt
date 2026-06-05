"""
A file containing the TimeSeriesIOPSPlotter class for plotting IOPS over time.
"""

from logging import Logger, getLogger

from post_processing.plotter.time_series_metric_plotter import TimeSeriesMetricPlotter

log: Logger = getLogger("plotter")

IOPS_PLOT_DEFAULT_COLOUR: str = "xkcd:blue"  # Blue from xkcd color survey
IOPS_Y_LABEL: str = "IOPS (ops/s)"
IOPS_PLOT_LABEL: str = "IOPS"


class TimeSeriesIOPSPlotter(TimeSeriesMetricPlotter):
    """
    A class to plot IOPS data over time on a time-series plot.
    """

    def _get_default_color(self) -> str:
        """Return the default color for IOPS plots."""
        return IOPS_PLOT_DEFAULT_COLOUR

    def _get_y_label(self) -> str:
        """Return the y-axis label for IOPS plots."""
        return IOPS_Y_LABEL

    def _get_plot_label(self) -> str:
        """Return the plot label for IOPS plots."""
        return IOPS_PLOT_LABEL


# Made with Bob
