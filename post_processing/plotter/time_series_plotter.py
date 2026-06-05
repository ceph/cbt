"""
Time-series plotter for FIO benchmark results.

This module provides the TimeSeriesPlotter class which generates time-series
plots for IOPS, bandwidth, and latency metrics from FIO benchmark runs.

Unlike CommonFormatPlotter which handles hockey-stick plots, this plotter
is designed specifically for time-series data visualization.

This plotter now uses the AxisPlotter pattern for consistency with other
plotters in the codebase, enabling easy addition of resource usage metrics.
"""

import json
from logging import Logger, getLogger
from pathlib import Path
from types import ModuleType
from typing import Optional

from matplotlib.axes import Axes
from matplotlib.figure import Figure

from post_processing.common import KB_CONVERSION_FACTOR, PLOT_FILE_EXTENSION, TITLE_CONVERSION
from post_processing.plotter.cpu_plotter import CPUPlotter
from post_processing.plotter.time_series_bandwidth_plotter import TimeSeriesBandwidthPlotter
from post_processing.plotter.time_series_iops_plotter import TimeSeriesIOPSPlotter
from post_processing.plotter.time_series_latency_plotter import TimeSeriesLatencyPlotter
from post_processing.post_processing_types import TimeSeriesDataPoint, TimeSeriesFormatType

log: Logger = getLogger("plotter")

# Plot styling constants
DEFAULT_FIGURE_SIZE = (12, 3)
DEFAULT_DPI = 100


class TimeSeriesPlotter:
    """
    Generate time-series plots from FIO benchmark data.

    This class creates line plots showing how performance metrics change
    over time during a benchmark run. It supports:
    - IOPS over time
    - Bandwidth over time
    - Latency over time (with percentile bands)
    - Optional CPU usage overlay on any metric

    Unlike CommonFormatPlotter which is designed for hockey-stick plots,
    this plotter works with TimeSeriesFormatType data and uses the
    AxisPlotter pattern for consistency and extensibility.
    """

    def __init__(
        self,
        archive_directory: str,
        plotter: ModuleType,
        figure_size: tuple[int, int] = DEFAULT_FIGURE_SIZE,
        dpi: int = DEFAULT_DPI,
    ) -> None:
        """
        Initialize the time-series plotter.

        Args:
            archive_directory: Directory containing benchmark results
            plotter: Matplotlib pyplot module for plotting
            figure_size: Figure size in inches (width, height)
            dpi: Dots per inch for plot resolution
        """
        self._archive_directory = Path(archive_directory)
        self._plotter = plotter
        self._figure_size = figure_size
        self._dpi = dpi
        self._output_dir = self._archive_directory / "visualisation"

    def draw_and_save(self) -> None:
        """
        Generate plots for all time-series JSON files in the archive directory.

        This method follows the same pattern as other plotters in the codebase,
        scanning for time-series data files and generating plots for each.
        """
        self.plot_all_in_directory()

    def plot_time_series(self, data: TimeSeriesFormatType, plot_cpu: bool = False) -> None:
        """
        Generate all time-series plots for the given data.

        Creates separate plots for IOPS, bandwidth, and latency metrics.
        Optionally overlays CPU usage on each plot.

        Args:
            data: Time-series data in TimeSeriesFormatType format
            plot_cpu: Whether to overlay CPU usage on the plots
        """
        if not data.get("timeseries"):
            log.warning("No time-series data to plot")
            return

        # Ensure output directory exists
        self._output_dir.mkdir(parents=True, exist_ok=True)

        # Generate individual plots
        self._plot_iops(data, plot_cpu)
        self._plot_bandwidth(data, plot_cpu)
        self._plot_latency(data, plot_cpu)

        log.debug(
            "Generated time-series plots for %s %s %s",
            data["benchmark"],
            data["operation"],
            data["blocksize"],
        )

    def _plot_iops(self, data: TimeSeriesFormatType, plot_cpu: bool = False) -> None:
        """
        Plot IOPS over time using TimeSeriesIOPSPlotter.

        Args:
            data: Time-series data containing IOPS measurements
            plot_cpu: Whether to overlay CPU usage on the plot
        """
        timeseries = data["timeseries"]
        timestamps = [point["timestamp_sec"] for point in timeseries]

        # Skip if no IOPS data
        iops_values = [point["iops"] for point in timeseries]
        if not any(iops_values):
            log.debug("No IOPS data to plot")
            return

        fig, ax = self._plotter.subplots(figsize=self._figure_size, dpi=self._dpi)

        # Initialize IOPS plotter
        iops_plotter = TimeSeriesIOPSPlotter(main_axis=ax)

        # Collect IOPS data
        for point in timeseries:
            iops_plotter.add_y_data(str(point["iops"]))

        # Plot IOPS
        iops_plotter.plot(x_data=timestamps)

        # Optionally add CPU overlay
        if plot_cpu and self._has_cpu_data(timeseries):
            cpu_plotter = CPUPlotter(main_axis=ax)
            for point in timeseries:
                cpu_value = point.get("cpu")
                if cpu_value is not None:
                    cpu_plotter.add_y_data(str(cpu_value))
            cpu_plotter.plot(x_data=timestamps)

        self._configure_axes(
            ax=ax,
            title=self._generate_title(data, "IOPS"),
            xlabel="Time (seconds)",
        )

        ax.legend(loc="best")  # pyright: ignore[reportUnknownMemberType]
        ax.grid(True, alpha=0.3)  # pyright: ignore[reportUnknownMemberType]

        output_path = self._generate_output_path(data, "iops")
        self._save_plot(fig, output_path)
        self._plotter.close(fig)

    def _plot_bandwidth(self, data: TimeSeriesFormatType, plot_cpu: bool = False) -> None:
        """
        Plot bandwidth over time using TimeSeriesBandwidthPlotter.

        Args:
            data: Time-series data containing bandwidth measurements
            plot_cpu: Whether to overlay CPU usage on the plot
        """
        timeseries = data["timeseries"]
        timestamps = [point["timestamp_sec"] for point in timeseries]

        # Skip if no bandwidth data
        bandwidth_values = [point["bandwidth_bytes"] for point in timeseries]
        if not any(bandwidth_values):
            log.debug("No bandwidth data to plot")
            return

        fig, ax = self._plotter.subplots(figsize=self._figure_size, dpi=self._dpi)

        # Initialize bandwidth plotter
        bandwidth_plotter = TimeSeriesBandwidthPlotter(main_axis=ax)

        # Collect bandwidth data
        for point in timeseries:
            bandwidth_plotter.add_y_data(str(point["bandwidth_bytes"]))

        # Plot bandwidth
        bandwidth_plotter.plot(x_data=timestamps)

        # Optionally add CPU overlay
        if plot_cpu and self._has_cpu_data(timeseries):
            cpu_plotter = CPUPlotter(main_axis=ax)
            for point in timeseries:
                cpu_value = point.get("cpu")
                if cpu_value is not None:
                    cpu_plotter.add_y_data(str(cpu_value))
            cpu_plotter.plot(x_data=timestamps)

        self._configure_axes(
            ax=ax,
            title=self._generate_title(data, "Bandwidth"),
            xlabel="Time (seconds)",
        )

        ax.legend(loc="best")  # pyright: ignore[reportUnknownMemberType]
        ax.grid(True, alpha=0.3)  # pyright: ignore[reportUnknownMemberType]

        output_path = self._generate_output_path(data, "bandwidth")
        self._save_plot(fig, output_path)
        self._plotter.close(fig)

    def _plot_latency(self, data: TimeSeriesFormatType, plot_cpu: bool = False) -> None:
        """
        Plot latency over time with percentile bands using TimeSeriesLatencyPlotter.

        Creates a plot showing mean latency with shaded regions for
        P50-P95 and P95-P99 percentile ranges.

        Args:
            data: Time-series data containing latency measurements
            plot_cpu: Whether to overlay CPU usage on the plot
        """
        timeseries = data["timeseries"]
        timestamps = [point["timestamp_sec"] for point in timeseries]

        # Skip if no latency data
        mean_latency = [point["mean_latency_ms"] for point in timeseries]
        if not any(mean_latency):
            log.debug("No latency data to plot")
            return

        fig, ax = self._plotter.subplots(figsize=self._figure_size, dpi=self._dpi)

        # Initialize latency plotter
        latency_plotter = TimeSeriesLatencyPlotter(main_axis=ax)

        # Collect latency data
        for point in timeseries:
            latency_plotter.add_y_data(str(point["mean_latency_ms"]))
            latency_plotter.add_p50_data(str(point["p50_latency_ms"]))
            latency_plotter.add_p95_data(str(point["p95_latency_ms"]))
            latency_plotter.add_p99_data(str(point["p99_latency_ms"]))
            latency_plotter.add_max_data(str(point["max_latency_ms"]))

        # Plot latency
        latency_plotter.plot(x_data=timestamps)

        # Optionally add CPU overlay
        if plot_cpu and self._has_cpu_data(timeseries):
            cpu_plotter = CPUPlotter(main_axis=ax)
            for point in timeseries:
                cpu_value = point.get("cpu")
                if cpu_value is not None:
                    cpu_plotter.add_y_data(str(cpu_value))
            cpu_plotter.plot(x_data=timestamps)

        self._configure_axes(
            ax=ax,
            title=self._generate_title(data, "Latency"),
            xlabel="Time (seconds)",
        )

        ax.legend(loc="best", fontsize="small")  # pyright: ignore[reportUnknownMemberType]
        ax.grid(True, alpha=0.3)  # pyright: ignore[reportUnknownMemberType]

        output_path = self._generate_output_path(data, "latency")
        self._save_plot(fig, output_path)
        self._plotter.close(fig)

    def _has_cpu_data(self, timeseries: list[TimeSeriesDataPoint]) -> bool:
        """
        Check if CPU data is available in the time series.

        Args:
            timeseries: List of time series data points

        Returns:
            True if CPU data is present, False otherwise
        """
        return any("cpu" in point for point in timeseries)

    def _generate_title(self, data: TimeSeriesFormatType, metric: str) -> str:
        """
        Generate a plot title for time-series data.

        Follows the same pattern as CommonFormatPlotter for consistency:
        - Converts blocksize from bytes to KB (e.g., "4096" -> "4K")
        - Converts operation to human-readable format (e.g., "randwrite" -> "Random Write")
        - Includes iodepth to distinguish plots with different iodepth values
        - Excludes numjobs since plots are sorted by numjobs in reports

        Args:
            data: Time-series data containing benchmark metadata
            metric: Metric name (e.g., "IOPS", "Bandwidth", "Latency")

        Returns:
            Formatted title string (e.g., "IOPS Over Time - 4K Random Write (iodepth=8)")
        """
        # Convert blocksize from bytes to KB
        blocksize_bytes = int(data["blocksize"])
        blocksize_kb = int(blocksize_bytes / KB_CONVERSION_FACTOR)
        blocksize_str = f"{blocksize_kb}K"

        # Convert operation to human-readable format
        operation = data["operation"]
        operation_str = TITLE_CONVERSION.get(operation, operation)

        # Get iodepth value
        iodepth = data.get("iodepth", "1")

        return f"{metric} Over Time - {blocksize_str} {operation_str} (iodepth={iodepth})"

    def _configure_axes(
        self,
        ax: Axes,
        title: str,
        xlabel: str,
    ) -> None:
        """
        Configure plot axes with labels and title.

        Args:
            ax: Matplotlib axes object
            title: Plot title
            xlabel: X-axis label
        """
        ax.set_title(title, fontsize=14, fontweight="bold")  # pyright: ignore[reportUnknownMemberType]
        ax.set_xlabel(xlabel, fontsize=12)  # pyright: ignore[reportUnknownMemberType]

        # Start y-axis at 0 for better visualization
        ax.set_ylim(bottom=0)  # pyright: ignore[reportUnknownMemberType]

    def _generate_output_path(self, data: TimeSeriesFormatType, metric: str) -> Path:
        """
        Generate output file path for a plot.

        Args:
            data: Time-series data containing benchmark metadata
            metric: Metric name (e.g., "iops", "bandwidth", "latency")

        Returns:
            Path object for the output file
        """
        iodepth = data.get("iodepth", "1")
        filename = (
            f"{data['blocksize']}_{data['numjobs']}_{data['operation']}_{iodepth}_{metric}_timeseries."
            f"{PLOT_FILE_EXTENSION}"
        )
        return self._output_dir / filename

    def _save_plot(self, fig: Figure, output_path: Path) -> None:
        """
        Save plot to disk.

        Args:
            fig: Matplotlib figure object
            output_path: Path where plot should be saved
        """
        fig.savefig(
            output_path,
            format=PLOT_FILE_EXTENSION,
            bbox_inches="tight",
            dpi=self._dpi,
        )
        log.debug("Saved plot to %s", output_path)

    def plot_from_file(self, json_file_path: str, plot_cpu: bool = False) -> None:
        """
        Load time-series data from JSON file and generate plots.

        Args:
            json_file_path: Path to JSON file containing TimeSeriesFormatType data
            plot_cpu: Whether to overlay CPU usage on the plots
        """
        file_path = Path(json_file_path)
        if not file_path.exists():
            log.error("Time-series data file not found: %s", json_file_path)
            return

        try:
            with file_path.open("r", encoding="utf8") as f:
                data: TimeSeriesFormatType = json.load(f)

            self.plot_time_series(data, plot_cpu)

        except (json.JSONDecodeError, KeyError) as e:
            log.error("Failed to load time-series data from %s: %s", json_file_path, e)

    def plot_all_in_directory(self, directory: Optional[str] = None, plot_cpu: bool = False) -> None:
        """
        Generate plots for all time-series JSON files in a directory.

        Args:
            directory: Directory to search for JSON files. If None, uses
                      the archive directory's visualisation subdirectory.
            plot_cpu: Whether to overlay CPU usage on the plots
        """
        search_dir = Path(directory) if directory else self._output_dir

        if not search_dir.exists():
            log.warning("Directory not found: %s", search_dir)
            return

        json_files = list(search_dir.glob("*_timeseries.json"))

        if not json_files:
            log.info("No time-series JSON files found in %s", search_dir)
            return

        log.info("Generating plots for %d time-series files", len(json_files))

        for json_file in json_files:
            log.debug("Processing %s", json_file.name)
            self.plot_from_file(str(json_file), plot_cpu)


# Made with Bob
