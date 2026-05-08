"""
A base class for recording data points for a plot axis, and plotting the axis.
the x-axis data is fixed for all sub-types, only the y-data changes
"""

from abc import ABC, abstractmethod
from logging import Logger, getLogger

from matplotlib.axes import Axes

log: Logger = getLogger("plotter")


class AxisPlotter(ABC):
    """
    A class to add the resource use measurements to a plot as separate axes
    """

    def __init__(self, main_axis: Axes) -> None:
        """
        Initialize the AxisPlotter with a matplotlib Axes object.

        Args:
            main_axis: The main matplotlib Axes object for this plot
        """
        self._main_axes = main_axis
        self._y_data: list[float] = []
        self._y_label: str = ""
        self._label: str = ""

    @property
    def y_label(self) -> str:
        """
        Return the value for the y_label that is set.
        This is currently not called, but is required for the corresponding
        setter to function

        :return: The value stored for the y label
        :rtype: str
        """
        return self._y_label

    @y_label.setter
    def y_label(self, label: str) -> None:
        if self._y_label:
            log.warning("Y label value already set to %s, changing to %s", self._y_label, label)
        self._y_label = label

    @property
    def plot_label(self) -> str:
        """
        return the value of the label set for this particular plot, as used
        in the legend to the plot.

        :return: The label that is currently set
        :rtype: str
        """
        return self._label

    @plot_label.setter
    def plot_label(self, label: str) -> None:
        if self._label:
            log.warning("Plot label value already set to %s, changing to %s", self._label, label)
        self._label = label

    @abstractmethod
    def plot(self, x_data: list[float], colour: str = "") -> None:
        """
        Plot the data for the axis

        :param x_data: The data for the x-axis
        :type x_data: list[Union[int, float]]
        :param colour: The colour for the axis and plot line
        :type colour: str
        """

    @abstractmethod
    def add_y_data(self, data_value: str) -> None:
        """
        Add a point of data to the y axis

        :param data_value:The value to add to the plot
        :type data_value: str
        """

    def _plot(self, x_data: list[float], axis: Axes, colour: str) -> None:
        """
        Plot this axis
        """
        axis.set_ylabel(self._y_label)  # pyright: ignore[reportUnknownMemberType]
        axis.tick_params(axis="y", colors=colour)  # pyright: ignore[reportUnknownMemberType]
        axis.plot(  # pyright: ignore[reportUnknownMemberType]
            x_data, self._y_data, "+-", color=colour, label=self._label
        )
