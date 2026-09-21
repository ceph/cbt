"""
A file containing the classes and code required to add a memory usage
data line to a plot
"""

from logging import Logger, getLogger
from typing import Union

from matplotlib.axes import Axes
from typing_extensions import override

from post_processing.plotter.axis_plotter import AxisPlotter

log: Logger = getLogger("plotter")

MEMORY_Y_LABEL: str = "Memory use (Mb)"
MEMORY_PLOT_LABEL: str = "Memory use"

# Color mapping for different resource sources.
# Colours are verified to be perceptually distinct under normal vision and the three
# main forms of colour-blindness (deuteranopia, protanopia, tritanopia) using ΔE≥15
# for every pair. Also distinct from IO lines (xkcd:cerulean) and CPU lines.
MEMORY_SOURCE_COLOURS: dict[str, str] = {
    "fio": "xkcd:periwinkle",  # blue-purple
    "collectl": "xkcd:magenta",  # vivid pink-purple
    "top": "xkcd:slate blue",  # cool blue-grey
    "default": "xkcd:moss green",  # muted earthy green (legacy single-source fallback)
}


class MemoryPlotter(AxisPlotter):
    """
    A class to add memory usage measurements to a plot as separate axes.

    Supports both single-source (legacy) and multi-source formats:
    - Legacy: data_value is a string "1024.5"
    - Multi-source: data_value is a dict {"fio": "1024.5", "collectl": "2048.0"}

    When multiple sources are present, plots separate lines for each source.
    """

    def __init__(self, main_axis: Axes) -> None:
        """Initialize MemoryPlotter with support for multiple data sources."""
        super().__init__(main_axis)
        # Store data per source: {"fio": [val1, val2, ...], "collectl": [...]}
        self._y_data_by_source: dict[str, list[float]] = {}

    @override
    def add_y_data(self, data_value: Union[str, dict[str, str]]) -> None:
        """
        Add a point of memory data for this plot.

        Supports both legacy single-value format and new multi-source format.

        Args:
            data_value: Either a single string value (legacy) or dict of {source: value}
        """
        if isinstance(data_value, dict):
            # Multi-source format: {"fio": "1024.5", "collectl": "2048.0"}
            for source, value in data_value.items():
                if source not in self._y_data_by_source:
                    self._y_data_by_source[source] = []
                try:
                    self._y_data_by_source[source].append(float(value))
                except (ValueError, TypeError) as e:
                    log.warning("Invalid memory value for source %s: %s (%s)", source, value, e)
                    self._y_data_by_source[source].append(0.0)
        else:
            # Legacy single-value format: "1024.5"
            if "default" not in self._y_data_by_source:
                self._y_data_by_source["default"] = []
            try:
                self._y_data_by_source["default"].append(float(data_value))
            except (ValueError, TypeError) as e:
                log.warning("Invalid memory value: %s (%s)", data_value, e)
                self._y_data_by_source["default"].append(0.0)

    @override
    def plot(self, x_data: list[Union[int, float]], colour: str = "") -> None:
        """
        Plot memory data, creating separate lines for each source.

        Args:
            x_data: X-axis data points (typically queue depths or time)
            colour: Ignored - colors are determined per source
        """
        if not self._y_data_by_source:
            log.debug("No memory data to plot")
            return

        memory_axis: Axes = self._main_axes.twinx()
        memory_axis.set_ylabel(MEMORY_Y_LABEL)

        # Plot a line for each source
        for source in sorted(self._y_data_by_source.keys()):
            y_data = self._y_data_by_source[source]

            # Determine label and color for this source
            if source == "default":
                label = MEMORY_PLOT_LABEL
            else:
                label = f"{MEMORY_PLOT_LABEL} ({source})"

            source_colour = MEMORY_SOURCE_COLOURS.get(source, MEMORY_SOURCE_COLOURS["default"])

            # Plot this source's data
            memory_axis.plot(x_data, y_data, label=label, color=source_colour, linestyle="-", linewidth=1.5, marker="s")

        # Add legend if multiple sources
        if len(self._y_data_by_source) > 1:
            memory_axis.legend(loc="upper right")
