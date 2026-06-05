"""
A file containing the classes and code required to add a resource usage
data line to a plot
"""

from logging import Logger, getLogger

from post_processing.plotter.axis_plotter import AxisPlotter

log: Logger = getLogger("plotter")

CPU_PLOT_DEFAULT_COLOUR: str = "xkcd:leaf green"  # Leaf green from xkcd color survey
CPU_Y_LABEL: str = "System CPU use (%)"
CPU_PLOT_LABEL: str = "CPU use"


class CPUPlotter(AxisPlotter):
    """
    A class to add the resource use measurements to a plot as separate axes
    """

    def add_y_data(self, data_value: str) -> None:
        """
        Add a point of CPU data for this plot

        :param cpu_value: A single value for CPU usage
        :type cpu_value: str
        """
        self._y_data.append(float(data_value))

    def plot(self, x_data: list[float], colour: str = "") -> None:
        cpu_axis = self._main_axes.twinx()
        self._label = CPU_PLOT_LABEL
        self._y_label = CPU_Y_LABEL
        self._plot(x_data=x_data, axis=cpu_axis, colour=CPU_PLOT_DEFAULT_COLOUR)
