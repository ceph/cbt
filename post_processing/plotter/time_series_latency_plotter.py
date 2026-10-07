"""
A file containing the TimeSeriesLatencyPlotter class for plotting latency over time.
"""

from logging import Logger, getLogger

from matplotlib.axes import Axes

from post_processing.plotter.time_series_metric_plotter import (
    DEFAULT_ALPHA,
    DEFAULT_LINE_WIDTH,
    TimeSeriesMetricPlotter,
)

log: Logger = getLogger("plotter")

LATENCY_MEAN_COLOR: str = "xkcd:orange"  # Orange from xkcd color survey
LATENCY_P50_COLOR: str = "xkcd:green"  # Green from xkcd color survey
LATENCY_P95_COLOR: str = "xkcd:red"  # Red from xkcd color survey
LATENCY_P99_COLOR: str = "xkcd:dark red"  # Dark red from xkcd color survey
LATENCY_MAX_COLOR: str = "xkcd:dark grey"  # Dark grey from xkcd color survey
LATENCY_Y_LABEL: str = "Latency (ms)"
LATENCY_PLOT_LABEL: str = "Mean Latency"
PERCENTILE_BAND_ALPHA: float = 0.2


class TimeSeriesLatencyPlotter(TimeSeriesMetricPlotter):
    """
    A class to plot latency data over time on a time-series plot.
    Supports mean, percentile (P50, P95, P99), and max latency values.
    """

    def __init__(self, main_axis: Axes) -> None:
        """
        Initialize the TimeSeriesLatencyPlotter with a matplotlib Axes object.

        Args:
            main_axis: The main matplotlib Axes object for this plot
        """
        super().__init__(main_axis)
        self._p50_data: list[float] = []
        self._p95_data: list[float] = []
        self._p99_data: list[float] = []
        self._max_data: list[float] = []

    def _get_default_color(self) -> str:
        """Return the default color for latency plots."""
        return LATENCY_MEAN_COLOR

    def _get_y_label(self) -> str:
        """Return the y-axis label for latency plots."""
        return LATENCY_Y_LABEL

    def _get_plot_label(self) -> str:
        """Return the plot label for latency plots."""
        return LATENCY_PLOT_LABEL

    def add_p50_data(self, data_value: str) -> None:
        """
        Add a point of P50 latency data.

        Args:
            data_value: A single P50 latency value in milliseconds as a string.
        """
        self._p50_data.append(float(data_value))

    def add_p95_data(self, data_value: str) -> None:
        """
        Add a point of P95 latency data.

        Args:
            data_value: A single P95 latency value in milliseconds as a string.
        """
        self._p95_data.append(float(data_value))

    def add_p99_data(self, data_value: str) -> None:
        """
        Add a point of P99 latency data.

        Args:
            data_value: A single P99 latency value in milliseconds as a string.
        """
        self._p99_data.append(float(data_value))

    def add_max_data(self, data_value: str) -> None:
        """
        Add a point of max latency data.

        Args:
            data_value: A single max latency value in milliseconds as a string.
        """
        self._max_data.append(float(data_value))

    def plot(self, x_data: list[float], colour: str = "") -> None:
        """
        Plot latency data on the main axes with percentile bands.

        Creates a plot showing mean latency with shaded regions for
        P50-P95 and P95-P99 percentile ranges, plus lines for specific percentiles.

        Args:
            x_data: The data for the x-axis (timestamps)
            colour: The colour for the plot line (optional, not used for latency plots)
        """
        latency_axis = self._main_axes
        self._label = self._get_plot_label()
        self._y_label = self._get_y_label()

        latency_axis.set_ylabel(self._y_label)  # pyright: ignore[reportUnknownMemberType]

        # Plot percentile bands (from bottom to top)
        # P50-P95 band
        if self._p50_data and self._p95_data and any(self._p50_data) and any(self._p95_data):
            latency_axis.fill_between(  # pyright: ignore[reportUnknownMemberType]
                x_data,
                self._p50_data,
                self._p95_data,
                color=LATENCY_P50_COLOR,
                alpha=PERCENTILE_BAND_ALPHA,
                label="P50-P95 Range",
            )

        # P95-P99 band
        if self._p95_data and self._p99_data and any(self._p95_data) and any(self._p99_data):
            latency_axis.fill_between(  # pyright: ignore[reportUnknownMemberType]
                x_data,
                self._p95_data,
                self._p99_data,
                color=LATENCY_P95_COLOR,
                alpha=PERCENTILE_BAND_ALPHA,
                label="P95-P99 Range",
            )

        # Plot lines for specific percentiles
        if self._p50_data and any(self._p50_data):
            latency_axis.plot(  # pyright: ignore[reportUnknownMemberType]
                x_data,
                self._p50_data,
                color=LATENCY_P50_COLOR,
                linewidth=DEFAULT_LINE_WIDTH,
                alpha=DEFAULT_ALPHA,
                label="P50 Latency",
                linestyle="--",
            )

        if self._p95_data and any(self._p95_data):
            latency_axis.plot(  # pyright: ignore[reportUnknownMemberType]
                x_data,
                self._p95_data,
                color=LATENCY_P95_COLOR,
                linewidth=DEFAULT_LINE_WIDTH,
                alpha=DEFAULT_ALPHA,
                label="P95 Latency",
                linestyle="--",
            )

        if self._p99_data and any(self._p99_data):
            latency_axis.plot(  # pyright: ignore[reportUnknownMemberType]
                x_data,
                self._p99_data,
                color=LATENCY_P99_COLOR,
                linewidth=DEFAULT_LINE_WIDTH,
                alpha=DEFAULT_ALPHA,
                label="P99 Latency",
                linestyle=":",
            )

        # Plot mean latency as main line
        if self._y_data and any(self._y_data):
            latency_axis.plot(  # pyright: ignore[reportUnknownMemberType]
                x_data,
                self._y_data,
                color=LATENCY_MEAN_COLOR,
                linewidth=DEFAULT_LINE_WIDTH + 0.5,
                alpha=DEFAULT_ALPHA,
                label=self._label,
            )

        # Optionally plot max latency
        if self._max_data and any(self._max_data) and max(self._max_data) > 0:
            latency_axis.plot(  # pyright: ignore[reportUnknownMemberType]
                x_data,
                self._max_data,
                color=LATENCY_MAX_COLOR,
                linewidth=DEFAULT_LINE_WIDTH - 0.5,
                alpha=0.5,
                label="Max Latency",
                linestyle=":",
            )


# Made with Bob
