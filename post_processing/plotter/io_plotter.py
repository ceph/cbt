"""
A file containing the classes and code required to add an IO usage
data line to a plot
"""

from logging import Logger, getLogger

from post_processing.plotter.axis_plotter import AxisPlotter

log: Logger = getLogger("plotter")

IO_PLOT_DEFAULT_COLOUR: str = "#5ca904"
IO_Y_LABEL: str = "Latency (ms)"
IO_PLOT_LABEL: str = "IO Details"


class IOPlotter(AxisPlotter):
    """
    A class to add IO latency measurements to a plot on the main axes.
    """

    def add_y_data(self, data_value: str) -> None:
        """
        Add a point of IO latency data for this plot.

        Args:
            data_value: A single latency value in nanoseconds as a string.
                       Will be converted to milliseconds internally.
        """
        self._y_data.append(float(data_value) / (1000 * 1000))

    def plot(self, x_data: list[float], colour: str = "") -> None:
        """
        This should never be called for an IO plot, so assert if it is
        """
        raise NotImplementedError

    def plot_with_error_bars(self, x_data: list[float], error_data: list[float], cap_size: int) -> None:
        """
        Plot IO data with error bars on the main axes.

        Args:
            x_data: The data for the x-axis (throughput values)
            error_data: The error bar data (standard deviations in milliseconds)
            cap_size: The size of the error bar caps in points. Use 0 for no caps.
        """
        io_axis = self._main_axes
        io_axis.set_ylabel(self.y_label)
        # io_axis.tick_params(axis="y")  # pyright: ignore[reportUnknownMemberType]
        io_axis.errorbar(  # pyright: ignore[reportUnknownMemberType]
            x_data, self._y_data, yerr=error_data, fmt="+-", capsize=cap_size, ecolor="red", label=self._label
        )
