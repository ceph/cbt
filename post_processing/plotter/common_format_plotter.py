"""
A file containing the classes and code required to read a file stored in the common
intermediate format introduced in PR 319 (https://github.com/ceph/cbt/pull/319) and produce a hockey-stick curve graph
"""

from abc import ABC, abstractmethod
from logging import Logger, getLogger
from pathlib import Path

# the ModuleType does exists in the types module, so no idea why pylint is
# flagging this
from types import ModuleType  # pylint: disable=[no-name-in-module]
from typing import Optional

from matplotlib.axes import Axes

from post_processing.common import (
    DATA_FILE_EXTENSION_WITH_DOT,
    PLOT_FILE_EXTENSION,
    get_blocksize_percentage_operation_numjobs_from_file_name,
)
from post_processing.plotter.cpu_plotter import CPUPlotter
from post_processing.plotter.io_plotter import IOPlotter
from post_processing.plotter.plot_data_collector import PlotDataCollector
from post_processing.plotter.plot_data_results import DataPointResult, PlotDataResult
from post_processing.post_processing_types import CommonFormatDataType, PlotDataType

log: Logger = getLogger("plotter")

# Module-level constants for data conversion and plotting
BLOCKSIZE_THRESHOLD_KB = 64
BYTES_TO_MB_DIVISOR = 1024 * 1024  # Using 1024 for MiB
NANOSECONDS_TO_MS_DIVISOR = 1_000_000
ERROR_BAR_CAP_SIZE = 3
KB_CONVERSION_FACTOR = 1024


# pylint: disable=too-few-public-methods, too-many-locals
class CommonFormatPlotter(ABC):
    """
    The base class for plotting results curves
    """

    def __init__(self, plotter: ModuleType) -> None:
        self._plotter = plotter

    @abstractmethod
    def draw_and_save(self) -> None:
        """
        Produce the plot file(s) for each of the intermediate data files in the
        given directory and save them to disk
        """

    @abstractmethod
    def _generate_output_file_name(self, files: list[Path]) -> str:
        """
        Generate the name for the file the plot will be saved to.
        """

    def _add_title(self, source_files: list[Path]) -> None:
        """
        Given the source file full path, generate the title for the
        data plot and add it to the plot
        """

        title: str = ""

        if len(source_files) == 1:
            title = self._construct_title_from_file_name(source_files[0].parts[-1])
        else:
            title = self._construct_title_from_list_of_file_names(source_files)

        self._plotter.title(title)

    def _construct_title_from_list_of_file_names(self, file_paths: list[Path]) -> str:
        """
        Given a list of file paths construct a plot title.

        If there is a common element then the title will be
        '<common_element> comparison'
        e.g if all files had a blocksize = 16K the title would be
        '16k blocksize comparison'
        """
        titles: list[tuple[str, str, str]] = []
        blocksizes: list[str] = []
        read_percents: list[str] = []
        operations: list[str] = []

        for file in file_paths:
            (blocksize, read_percent, operation, _) = get_blocksize_percentage_operation_numjobs_from_file_name(
                file.stem
            )
            titles.append((blocksize, read_percent, operation))

            if blocksize not in blocksizes:
                blocksizes.append(blocksize)
            if read_percent not in read_percents:
                read_percents.append(read_percent)
            if operation not in operations:
                operations.append(operation)

        if len(blocksizes) == 1:
            return f"{blocksizes[0]} blocksize comparison"

        if len(operations) == 1 and len(read_percents) == 1:
            return f"{read_percents[0]} {operations[0]} comparison"

        if len(operations) == 1:
            return f"{operations[0]} comparison"

        title: str = " ".join(titles.pop(0))
        for details in titles:
            title += "\nVs "
            title += " ".join(details)

        return title

    def _construct_title_from_file_name(self, file_name: str) -> str:
        """
        given a single file name construct a plot title from the blocksize,
        read percent and operation contained in the title
        """
        (blocksize, read_percent, operation, _) = get_blocksize_percentage_operation_numjobs_from_file_name(
            file_name[: -len(DATA_FILE_EXTENSION_WITH_DOT)]
        )

        return f"{blocksize} {read_percent} {operation}"

    def _set_axis(self, maximum_values: Optional[tuple[int, int]] = None) -> None:
        """
        Set the range for the plot axes starting from 0.

        Args:
            maximum_values: Optional tuple of (maximum_x, maximum_y) values.
                           If None, matplotlib will auto-scale the axes.
                           If provided, sets explicit limits for both axes.
        """
        maximum_x: Optional[int] = None
        maximum_y: Optional[int] = None

        if maximum_values is not None:
            maximum_x = maximum_values[0]
            maximum_y = maximum_values[1]

        self._plotter.xlim(0, maximum_x)
        self._plotter.ylim(0, maximum_y)

    def _sort_plot_data(self, unsorted_data: CommonFormatDataType) -> PlotDataType:
        """
        Sort the data read from the file by queue depth
        """
        keys: list[str] = [key for key in unsorted_data.keys() if isinstance(unsorted_data[key], dict)]
        plot_data: PlotDataType = {}
        sorted_plot_data: PlotDataType = {}
        for key, data in unsorted_data.items():
            if isinstance(data, dict):
                plot_data[key] = data

        sorted_keys: list[str] = sorted(keys, key=int)
        for key in sorted_keys:
            sorted_plot_data[key] = plot_data[key]

        return sorted_plot_data

    def _calculate_blocksize_kb(self, blocksize_bytes: str) -> int:
        """Convert blocksize from bytes to kilobytes."""
        return int(int(blocksize_bytes) / KB_CONVERSION_FACTOR)

    def _should_use_bandwidth(self, blocksize_kb: int) -> bool:
        """Determine if bandwidth should be used instead of IOPS based on blocksize."""
        return blocksize_kb >= BLOCKSIZE_THRESHOLD_KB

    def _convert_bandwidth_to_mb(self, bandwidth_bytes: str) -> float:
        """Convert bandwidth from bytes to megabytes."""
        return float(bandwidth_bytes) / BYTES_TO_MB_DIVISOR

    def _convert_std_dev_to_ms(self, std_dev_ns: str) -> float:
        """Convert standard deviation from nanoseconds to milliseconds."""
        return float(std_dev_ns) / NANOSECONDS_TO_MS_DIVISOR

    def _extract_x_axis_data(self, data: dict[str, str]) -> tuple[float, str]:
        """
        Extract x-axis data point and label based on blocksize.

        Returns:
            Tuple of (x_value, x_label) where x_value is either bandwidth or IOPS
        """
        blocksize_kb = self._calculate_blocksize_kb(data["blocksize"])

        if self._should_use_bandwidth(blocksize_kb):
            x_value = self._convert_bandwidth_to_mb(data["bandwidth_bytes"])
            x_label = "Bandwidth (MB/s)"
        else:
            x_value = float(data["iops"])
            x_label = "IOps"

        return x_value, x_label

    def _validate_cpu_data_availability(self, data: dict[str, str], plot_resource_usage: bool) -> bool:
        """
        Check if CPU data is available when resource plotting is requested.

        Returns:
            True if resource usage should be plotted, False otherwise
        """
        if data.get("cpu") is None and plot_resource_usage:
            log.warning(
                "Unable to plot CPU usage: CPU data not found in intermediate files. Disabling resource usage plotting."
            )
            return False
        return plot_resource_usage

    def _calculate_error_bar(
        self, data: dict[str, str], plot_error_bars: bool, resource_plotting_enabled: bool
    ) -> float:
        """
        Calculate error bar value for a data point.

        Args:
            data: Data dictionary containing std_deviation
            plot_error_bars: Whether error bars are enabled
            resource_plotting_enabled: Whether resource plotting is active

        Returns:
            Error bar value in milliseconds, or 0.0 if not applicable
        """
        if plot_error_bars and not resource_plotting_enabled:
            if "std_deviation" not in data:
                log.warning("Missing 'std_deviation' field, using 0 for error bar")
                return 0.0
            return self._convert_std_dev_to_ms(data["std_deviation"])
        return 0.0

    def _initialize_plotters(self, main_axes: Axes, label: Optional[str]) -> tuple[IOPlotter, CPUPlotter]:
        """Initialize and configure IO and CPU plotters."""
        io_plot_label = label if label else "IO Details"

        cpu_plotter = CPUPlotter(main_axis=main_axes)
        io_plotter = IOPlotter(main_axis=main_axes)
        io_plotter.y_label = "Latency (ms)"
        io_plotter.plot_label = io_plot_label

        return io_plotter, cpu_plotter

    def _validate_input_data(self, sorted_plot_data: PlotDataType) -> None:
        """
        Validate input data is not empty.

        Args:
            sorted_plot_data: The data to validate

        Raises:
            ValueError: If data is empty
        """
        if not sorted_plot_data:
            raise ValueError("Cannot extract plot data from empty dataset")

    def _process_data_point(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        data: dict[str, str],
        queue_depth: str,
        io_plotter: IOPlotter,
        cpu_plotter: CPUPlotter,
        plot_error_bars: bool,
        resource_plotting_enabled: bool,
    ) -> DataPointResult:
        """
        Process a single data point for plotting.

        Args:
            data: Data dictionary for this point
            queue_depth: Queue depth identifier for error messages
            io_plotter: IO plotter to receive latency data
            cpu_plotter: CPU plotter to receive CPU data
            plot_error_bars: Whether error bars are enabled
            resource_plotting_enabled: Whether resource plotting is active

        Returns:
            DataPointResult with processed values

        Raises:
            ValueError: If required fields are missing or invalid
            TypeError: If data types are incorrect
        """
        # Validate required fields exist
        required_fields = ["latency", "blocksize"]
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            raise ValueError(f"Missing required fields for queue depth {queue_depth}: {', '.join(missing_fields)}")

        # Validate latency is numeric
        latency_value = data["latency"]
        try:
            float(latency_value)  # Validate it's convertible to float
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid latency value for queue depth {queue_depth}: {latency_value}") from e

        # Extract x-axis data (may raise ValueError)
        x_value, x_label = self._extract_x_axis_data(data)

        # Add latency data
        io_plotter.add_y_data(latency_value)

        # Handle CPU data with proper validation
        resource_available = resource_plotting_enabled
        if resource_plotting_enabled:
            cpu_value = data.get("cpu")
            if cpu_value is None:
                resource_available = False
            else:
                # Validate CPU value is numeric
                try:
                    float(cpu_value)
                    cpu_plotter.add_y_data(cpu_value)
                except (ValueError, TypeError):
                    log.warning(
                        "Invalid CPU value for queue depth %s: %s. Skipping CPU data.",
                        queue_depth,
                        cpu_value,
                    )
                    resource_available = False

        # Calculate error bars
        error_bar = self._calculate_error_bar(data, plot_error_bars, resource_available)

        return DataPointResult(
            x_value=x_value,
            x_label=x_label,
            error_bar=error_bar,
            resource_available=resource_available,
        )

    def _extract_plot_data(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        sorted_plot_data: PlotDataType,
        main_axes: Axes,
        io_plotter: IOPlotter,
        cpu_plotter: CPUPlotter,
        plot_error_bars: bool,
        plot_resource_usage: bool,
    ) -> PlotDataResult:
        """
        Extract and process data points for plotting.

        Args:
            sorted_plot_data: Pre-sorted performance data by queue depth
            main_axes: Matplotlib axes object for plotting
            io_plotter: IO plotter instance to receive latency data
            cpu_plotter: CPU plotter instance to receive CPU data
            plot_error_bars: Whether to include standard deviation error bars
            plot_resource_usage: Whether to include CPU usage on secondary axis

        Returns:
            PlotDataResult containing extracted x_data, error_bars, cap_size,
            updated plot_resource_usage flag, and x_label

        Raises:
            ValueError: If sorted_plot_data is empty or contains invalid entries
        """
        self._validate_input_data(sorted_plot_data)

        # Initialize data collectors
        data_collector = PlotDataCollector()
        resource_plotting_enabled = plot_resource_usage
        x_label: Optional[str] = None
        cpu_warning_logged = False  # Track if we've already warned about missing CPU data

        for queue_depth, data in sorted_plot_data.items():
            try:
                # Process single data point
                point_result = self._process_data_point(
                    data=data,
                    queue_depth=queue_depth,
                    io_plotter=io_plotter,
                    cpu_plotter=cpu_plotter,
                    plot_error_bars=plot_error_bars,
                    resource_plotting_enabled=resource_plotting_enabled,
                )

                # Set x-axis label once (from first valid data point)
                if x_label is None:
                    x_label = point_result.x_label
                    main_axes.set_xlabel(x_label)  # pyright: ignore[reportUnknownMemberType]

                # Disable resource plotting if CPU data unavailable (only check once)
                if resource_plotting_enabled and not point_result.resource_available:
                    if not cpu_warning_logged:
                        log.warning(
                            "Unable to plot CPU usage: CPU data not found in intermediate files. "
                            "Disabling resource usage plotting."
                        )
                        cpu_warning_logged = True
                    resource_plotting_enabled = False

                # Collect the processed data
                data_collector.add_point(point_result.x_value, point_result.error_bar)

            except (KeyError, ValueError, TypeError) as e:
                log.error(
                    "Failed to process data for queue depth %s: %s. Skipping this data point.",
                    queue_depth,
                    e,
                )
                continue

        # Validate we extracted at least some data
        if data_collector.is_empty():
            raise ValueError("No valid data points could be extracted from dataset")

        # Determine error bar cap size
        cap_size = ERROR_BAR_CAP_SIZE if (plot_error_bars and not resource_plotting_enabled) else 0

        return PlotDataResult(
            x_data=data_collector.x_data,
            error_bars=data_collector.error_bars,
            cap_size=cap_size,
            plot_resource_usage=resource_plotting_enabled,
            x_label=x_label or "Unknown",  # Fallback if no label was set
        )

    def _render_plots(
        self,
        io_plotter: IOPlotter,
        cpu_plotter: CPUPlotter,
        plot_result: PlotDataResult,
    ) -> None:
        """Render the IO and optional CPU plots."""
        io_plotter.plot_with_error_bars(
            x_data=plot_result.x_data, error_data=plot_result.error_bars, cap_size=plot_result.cap_size
        )

        if plot_result.plot_resource_usage:
            cpu_plotter.plot(x_data=plot_result.x_data)

    def _add_single_file_data_with_optional_errorbars(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        file_data: CommonFormatDataType,
        main_axes: Axes,
        plot_error_bars: bool = False,
        plot_resource_usage: bool = False,
        label: Optional[str] = None,
    ) -> None:
        """
        Add data from a single file to the plot with optional error bars and resource usage.

        Each point represents latency vs throughput (IOPS or bandwidth) for a given queue depth.
        Error bars are displayed in red with a blue plot line when enabled.

        Args:
            file_data: Performance data in common format
            main_axes: Matplotlib axes object for plotting
            plot_error_bars: Whether to include standard deviation error bars
            plot_resource_usage: Whether to include CPU usage on secondary axis
            label: Custom label for the IO plot line (defaults to "IO Details")
        """
        # Initialize plotters
        io_plotter, cpu_plotter = self._initialize_plotters(main_axes, label)

        # Sort and prepare data
        sorted_plot_data = self._sort_plot_data(file_data)

        # Extract plot data with validation
        plot_result = self._extract_plot_data(
            sorted_plot_data, main_axes, io_plotter, cpu_plotter, plot_error_bars, plot_resource_usage
        )

        # Render plots
        self._render_plots(io_plotter, cpu_plotter, plot_result)

    def _save_plot(self, file_path: str) -> None:
        """
        save the plot to disk as a svg file

        The bbox_inches="tight" option makes sure that the legend is included
        in the plot and not cut off
        """
        self._plotter.savefig(file_path, format=f"{PLOT_FILE_EXTENSION}", bbox_inches="tight")

    def _clear_plot(self) -> None:
        """
        Clear the plot data
        """
        self._plotter.close()
        # self._plotter.clf()
