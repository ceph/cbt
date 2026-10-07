"""
A file containing the classes and code required to add a resource usage
data line to a plot
"""

from logging import Logger, getLogger
from typing import Union

from matplotlib.axes import Axes

from post_processing.plotter.axis_plotter import AxisPlotter

log: Logger = getLogger("plotter")

CPU_Y_LABEL: str = "System CPU use (%)"
CPU_PLOT_LABEL: str = "CPU use"

# Color mapping for different resource sources
CPU_SOURCE_COLOURS: dict[str, str] = {
    "fio": "xkcd:leaf green",
    "collectl": "xkcd:sky blue",
    "default": "xkcd:orange",
}


class CPUPlotter(AxisPlotter):
    """
    A class to add CPU usage measurements to a plot as separate axes.

    Supports both single-source (legacy) and multi-source formats:
    - Legacy: data_value is a string "45.2"
    - Multi-source: data_value is a dict {"fio": "45.2", "collectl": "47.8"}

    When multiple sources are present, plots separate lines for each source.
    """

    def __init__(self, main_axis: "Axes") -> None:
        """Initialize CPUPlotter with support for multiple data sources."""
        super().__init__(main_axis)
        # Store data per source: {"fio": [val1, val2, ...], "collectl": [...]}
        self._y_data_by_source: dict[str, list[float]] = {}

    def add_y_data(self, data_value: Union[str, dict[str, str]]) -> None:
        """
        Add a point of CPU data for this plot.

        Supports both legacy single-value format and new multi-source format.

        Args:
            data_value: Either a single string value (legacy) or dict of {source: value}
        """
        if isinstance(data_value, dict):
            # Multi-source format: {"fio": "45.2", "collectl": "47.8"}
            for source, value in data_value.items():
                if source not in self._y_data_by_source:
                    self._y_data_by_source[source] = []
                try:
                    self._y_data_by_source[source].append(float(value))
                except (ValueError, TypeError) as e:
                    log.warning("Invalid CPU value for source %s: %s (%s)", source, value, e)
                    self._y_data_by_source[source].append(0.0)
        else:
            # Legacy single-value format: "45.2"
            if "default" not in self._y_data_by_source:
                self._y_data_by_source["default"] = []
            try:
                self._y_data_by_source["default"].append(float(data_value))
            except (ValueError, TypeError) as e:
                log.warning("Invalid CPU value: %s (%s)", data_value, e)
                self._y_data_by_source["default"].append(0.0)

    def plot(self, x_data: list[float], colour: str = "") -> None:
        """
        Plot CPU data, creating separate lines for each source.

        Args:
            x_data: X-axis data points (typically queue depths or time)
            colour: Ignored - colors are determined per source
        """
        if not self._y_data_by_source:
            log.debug("No CPU data to plot")
            return

        cpu_axis = self._main_axes.twinx()
        cpu_axis.set_ylabel(CPU_Y_LABEL)

        # Plot a line for each source
        for source in sorted(self._y_data_by_source.keys()):
            y_data = self._y_data_by_source[source]

            # Determine label and color for this source
            if source == "default":
                label = CPU_PLOT_LABEL
            else:
                label = f"{CPU_PLOT_LABEL} ({source})"

            source_colour = CPU_SOURCE_COLOURS.get(source, CPU_SOURCE_COLOURS["default"])

            # Plot this source's data
            cpu_axis.plot(x_data, y_data, label=label, color=source_colour, linestyle="-", linewidth=1.5, marker="o")

        # Add legend if multiple sources
        if len(self._y_data_by_source) > 1:
            cpu_axis.legend(loc="upper right")
