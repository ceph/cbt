"""
Code to automatically generate a time-series report from a directory containing
time-series performance results.

The report will be generated in markdown format using the create_report()
method and the resulting file saved to the specified output directory.

Optionally the markdown report can be converted to a pdf using pandoc by
calling save_as_pdf().
The pdf file will have the same name as the markdown file, and be saved in
the same output directory.

For blocksizes < 64K: displays IOPS and latency time-series plots
For blocksizes >= 64K: displays bandwidth and latency time-series plots
"""

import subprocess
from datetime import datetime
from logging import Logger, getLogger
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plotter

from post_processing.common import (
    DATA_FILE_EXTENSION_WITH_DOT,
    PLOT_FILE_EXTENSION_WITH_DOT,
    TITLE_CONVERSION,
    find_timeseries_visualisation_directories,
    get_blocksize_percentage_operation_numjobs_from_file_name,
    get_resource_details_from_file,
    read_intermediate_file,
    strip_confidential_data_from_yaml,
)
from post_processing.plotter.time_series_plotter import TimeSeriesPlotter
from post_processing.post_processing_types import CommonFormatDataType
from post_processing.reports.report_generator import ReportGenerator

log: Logger = getLogger("reports")


class TimeSeriesReportGenerator(ReportGenerator):
    """
    The class responsible for generating a report from a single time-series FIO run.

    Shows time-series plots (IOPS, bandwidth, latency over time) instead of
    hockey-stick curves.

    For blocksizes < 64K: displays IOPS and latency
    For blocksizes >= 64K: displays bandwidth and latency
    """

    # Threshold for switching between IOPS and bandwidth display
    BLOCKSIZE_THRESHOLD_BYTES: int = 65536  # 64K in bytes

    # Additional LaTeX header file for timeseries-specific formatting
    TIMESERIES_HEADER_FILE_PATH: str = "include/timeseries_report.tex"

    def _get_additional_header_files(self) -> list[str]:
        """
        Get additional LaTeX header files for timeseries reports.

        Returns the timeseries-specific header that scales images to 50% height.

        Returns:
            List containing the path to the timeseries header file
        """
        return [self.TIMESERIES_HEADER_FILE_PATH]

    def _generate_report_title(self) -> str:
        """
        Generate the title for the report.

        Any _ must be converted to a - otherwise the pandoc conversion to PDF
        will fail
        """
        title: str = f"Time-Series Performance Report for {''.join(self._build_strings)}"
        return title

    def _generate_report_name(self) -> str:
        """
        The report name is of the format:
            timeseries_performance_report_yymmdd_hhmmss.md
        """
        current_datetime: datetime = datetime.now()

        # Convert to string
        datetime_string: str = current_datetime.strftime("%y%m%d_%H%M%S")
        output_file_name: str = f"timeseries_performance_report_{datetime_string}.{self.MARKDOWN_FILE_EXTENSION}"
        return output_file_name

    def _find_visualisation_directories(self, archive_path: Path) -> list[Path]:
        """
        Find timeseries visualisation directories.

        Overrides base class to find directories containing timeseries data
        (under iodepth-X or total_iodepth-X subdirectories).

        Args:
            archive_path: Path to the archive directory

        Returns:
            List of timeseries visualisation directory paths
        """
        return find_timeseries_visualisation_directories(archive_path)

    def _get_plot_file_stem(self, image_file: Path) -> str:
        """
        Get the file stem for a time-series report plot file.

        For time-series reports, removes the metric suffix, iodepth, and "_timeseries" suffix.
        Example: "4k_1_randread_8_iops_timeseries.svg" -> "4k_1_randread"

        Args:
            image_file: Path to the plot file

        Returns:
            The file stem without metric, iodepth, and timeseries suffixes
        """
        stem = image_file.stem
        # Remove _timeseries suffix if present
        if stem.endswith("_timeseries"):
            stem = stem[: -len("_timeseries")]
        # Remove metric suffix (_iops, _bandwidth, _latency)
        for metric in ["_iops", "_bandwidth", "_latency"]:
            if stem.endswith(metric):
                stem = stem[: -len(metric)]
                break
        # Remove iodepth (last part after removing metric)
        # Format at this point: {blocksize}_{numjobs}_{operation}_{iodepth}
        parts = stem.split("_")
        if len(parts) >= 4:
            # Remove the last part (iodepth) to get back to expected format
            stem = "_".join(parts[:-1])
        return stem

    def get_latency_throughput_from_file(self, file_path: Path) -> tuple[str, str]:
        """
        Reads the data stored in the time-series intermediate file format and returns the
        maximum throughput in either iops or MB/s, and the latency in ms recorded for that throughput.

        Time-series format has pre-calculated maximum values at the top level.
        Overrides the base class method to handle time-series specific format.
        """
        data: CommonFormatDataType = read_intermediate_file(file_path=f"{file_path}")

        # Get blocksize to determine whether to use IOPS or bandwidth
        blocksize_str = data.get("blocksize", "0")
        assert isinstance(blocksize_str, str)
        blocksize: int = int(int(blocksize_str) / 1024)

        throughput_key: str = "maximum_iops"
        latency_key: str = "latency_at_max_iops"
        throughput_type: str = "IOps"

        if blocksize >= 64:
            throughput_key = "maximum_bandwidth"
            latency_key = "latency_at_max_bandwidth"
            throughput_type = "MB/s"

        # Get the pre-calculated maximum values
        throughput = data[throughput_key]
        assert isinstance(throughput, str)
        max_throughput: float = float(throughput)

        # Convert bandwidth from bytes to MB/s if needed
        if blocksize >= 64:
            max_throughput = max_throughput / (1000 * 1000)

        latency = data[latency_key]
        assert isinstance(latency, str)
        latency_at_maximum_throughput: float = float(latency)

        return (f"{max_throughput:.0f} {throughput_type}", f"{latency_at_maximum_throughput:.1f}")

    def _add_summary_table(self) -> None:  # pylint: disable=[too-many-locals]
        """
        Add a table that contains a summary of the time-series results.

        The table format is:
        |Workload Name|Number of Jobs|Iodepth|Maximum Throughput|Maximum Latency|

        - Iodepth: total_iodepth if exists, otherwise iodepth
        - Maximum Throughput: bandwidth (MB/s) for blocksize >= 64K, IOPS for < 64K
        - Values are in format: <value>@<timestamp>
        - Rows are sorted by workload name, then by iodepth
        """
        self._report.new_header(level=1, title=f"Summary of results for {''.join(self._build_strings)}")

        log.info("Generating summary table for time-series data")

        # Single table format with iodepth and timestamp columns
        headers: str = "|Workload Name|Number of Jobs|Iodepth|Maximum Throughput|Maximum Latency|"
        alignment: str = "| :--- | :---: | :---: | ---: | ---: |"

        if self._plot_resources:
            alignment += " ---: |"
            headers += "System CPU (%)|"

        # Collect data for the table with sorting information
        # Structure: list of tuples (workload_name, iodepth_int, data_row)
        table_rows: list[tuple[str, int, str]] = []

        # Process each time-series data file
        for _, file_data in self._data_files.items():
            for file_path in file_data:
                log.debug("Looking at file %s", file_path)

                # Extract iodepth from file path
                iodepth = self._extract_iodepth_from_path(file_path)

                # Get maximum throughput and latency with timestamps
                (max_throughput_with_ts, max_latency_with_ts) = self._get_max_metrics_with_timestamps(file_path)
                (cpu_usage, _) = get_resource_details_from_file(file_path)

                # Remove _timeseries suffix from filename before parsing
                # Filename format: {blocksize}_{numjobs}_{operation}_{iodepth}_timeseries
                file_stem = file_path.stem
                if file_stem.endswith("_timeseries"):
                    file_stem = file_stem[: -len("_timeseries")]

                # Remove iodepth (last part) before parsing with the common function
                # which expects format: {blocksize}_{numjobs}_{operation}
                file_parts = file_stem.split("_")
                if len(file_parts) >= 4:
                    # Remove the last part (iodepth) to get back to the expected format
                    file_stem_without_iodepth = "_".join(file_parts[:-1])
                else:
                    file_stem_without_iodepth = file_stem

                (blocksize, percent, operation, number_of_jobs) = (
                    get_blocksize_percentage_operation_numjobs_from_file_name(file_stem_without_iodepth)
                )
                workload_name: str = f"{blocksize} {percent} {operation}"

                # Build the data row
                data: str = (
                    f"|[{workload_name}](#{file_path.stem.replace('_', '-')})|"
                    + f"{number_of_jobs}|{iodepth}|{max_throughput_with_ts}|{max_latency_with_ts}|"
                )

                if self._plot_resources:
                    data += f"{cpu_usage}|"

                # Convert iodepth to int for sorting (handle "N/A" case)
                try:
                    iodepth_int = int(iodepth)
                except ValueError:
                    iodepth_int = 0  # Put N/A entries at the beginning

                table_rows.append((workload_name, iodepth_int, data))

        # Sort by workload name, then by iodepth
        table_rows.sort(key=lambda x: (x[0], x[1]))

        # Output table if we have data
        if table_rows:
            self._report.new_line(text=headers)
            self._report.new_line(text=alignment)
            for _, _, data_row in table_rows:
                self._report.new_line(text=data_row)

    def _extract_iodepth_from_path(self, file_path: Path) -> str:
        """
        Extract iodepth value from the file path.

        Looks for 'total_iodepth-XXX' or 'iodepth-XXX' in the path.
        Prefers total_iodepth if both exist.

        Args:
            file_path: Path to the data file

        Returns:
            The iodepth value as a string, or "N/A" if not found
        """
        path_parts = file_path.parts

        # Look for total_iodepth first (higher priority)
        for part in path_parts:
            if part.startswith("total_iodepth-"):
                return part.split("-")[1]

        # If no total_iodepth, look for iodepth
        for part in path_parts:
            if part.startswith("iodepth-"):
                return part.split("-")[1]

        return "N/A"

    def _get_max_metrics_with_timestamps(self, file_path: Path) -> tuple[str, str]:  # pylint: disable=too-many-locals
        """
        Get maximum throughput and latency with their timestamps from a time-series file.

        Reads the pre-calculated maximum values and timestamps from the intermediate file.
        Formats them as: <value>@<timestamp>

        Args:
            file_path: Path to the time-series intermediate file

        Returns:
            Tuple of (max_throughput_with_timestamp, max_latency_with_timestamp)
        """
        data: CommonFormatDataType = read_intermediate_file(file_path=f"{file_path}")

        # Get blocksize to determine whether to use IOPS or bandwidth
        blocksize_str = data.get("blocksize", "0")
        assert isinstance(blocksize_str, str)
        blocksize: int = int(int(blocksize_str) / 1024)

        # Determine which metrics to use based on blocksize
        if blocksize >= 64:
            # Use bandwidth for large blocksizes
            throughput_key = "maximum_bandwidth"
            throughput_timestamp_key = "timestamp_at_max_bandwidth"
            throughput_type = "MB/s"

            throughput = data[throughput_key]
            assert isinstance(throughput, str)
            max_throughput = float(throughput) / (1000 * 1000)  # Convert bytes to MB/s

            timestamp = data[throughput_timestamp_key]
            assert isinstance(timestamp, str)
            throughput_timestamp = float(timestamp)

            max_throughput_str = f"{max_throughput:.0f} {throughput_type}@{throughput_timestamp:.1f}s"
        else:
            # Use IOPS for small blocksizes
            throughput_key = "maximum_iops"
            throughput_timestamp_key = "timestamp_at_max_iops"
            throughput_type = "IOps"

            throughput = data[throughput_key]
            assert isinstance(throughput, str)
            max_throughput = float(throughput)

            timestamp = data[throughput_timestamp_key]
            assert isinstance(timestamp, str)
            throughput_timestamp = float(timestamp)

            max_throughput_str = f"{max_throughput:.0f} {throughput_type}@{throughput_timestamp:.1f}s"

        # Get maximum latency and its timestamp
        latency_key = "maximum_latency"
        latency_timestamp_key = "timestamp_at_max_latency"

        latency = data[latency_key]
        assert isinstance(latency, str)
        max_latency = float(latency)

        latency_ts = data[latency_timestamp_key]
        assert isinstance(latency_ts, str)
        latency_timestamp = float(latency_ts)

        max_latency_str = f"{max_latency:.1f}@{latency_timestamp:.1f}s"

        return (max_throughput_str, max_latency_str)

    def _add_configuration_yaml_files(self) -> None:
        """
        Add the configuration yaml file to the report.

        Same as simple report - single YAML file.
        """
        self._report.new_header(level=1, title="Configuration yaml")
        yaml_file_paths: list[Path] = self._find_configuration_yaml_files()

        if not yaml_file_paths:
            log.warning("No configuration YAML files found")
            return

        yaml_file_path: Path = yaml_file_paths[0]

        file_contents: str = yaml_file_path.read_text()
        safe_contents = strip_confidential_data_from_yaml(file_contents)
        markdown_string: str = f"```{safe_contents}```"

        self._report.new_paragraph(markdown_string)

    def _copy_images(self) -> None:
        """
        Copy the time-series plot files to a 'plots' subdirectory in the output directory
        so the markdown can link to them using a known relative path.

        If plots don't exist or force_refresh is set, generate them using TimeSeriesPlotter.
        """
        plot_files: list[Path] = []
        for directory in self._data_directories:
            # Check for existing time-series plot files in this directory
            existing_plots = list(directory.glob(f"*_timeseries{PLOT_FILE_EXTENSION_WITH_DOT}"))

            # If there are no plot files in the directory or force_refresh is set, create them
            if len(existing_plots) == 0 or self._force_refresh:
                log.info("Generating time-series plots for %s", directory.parent)
                ts_plotter = TimeSeriesPlotter(str(directory.parent), plotter, figure_size=(12, 6), dpi=100)
                ts_plotter.draw_and_save()
                # Collect the newly generated plot files
                plot_files.extend(list(directory.glob(f"*_timeseries{PLOT_FILE_EXTENSION_WITH_DOT}")))
            else:
                # Use existing plots
                plot_files.extend(existing_plots)

        # Copy all plot files to the plots directory
        for plot_file in plot_files:
            # Use shell=False for security - pass command as list
            subprocess.call(["/usr/bin/env", "cp", str(plot_file), f"{self._plots_directory}/"])

        log.debug(
            "Copied %d time-series plot files to %s",
            len(plot_files),
            self._plots_directory,
        )

    def _find_and_sort_file_paths(self, paths: list[Path], search_pattern: str, index: Optional[int] = 0) -> list[Path]:
        """
        Given the search_pattern find all the files in a Path that match
        that pattern, and return them as a list sorted numerically by file name.

        Same implementation as SimpleReportGenerator.
        """
        unsorted_paths: list[Path] = list(paths[0].glob(search_pattern))
        assert index is not None
        return self._sort_list_of_paths(unsorted_paths, index)

    def _find_and_sort_plot_files(self) -> list[Path]:
        """
        Find all the time-series plot files in the directory.

        Overrides base class to only include files with _timeseries suffix,
        filtering out simple and comparison plot files.

        Sorts by numjobs (for section grouping), then blocksize, then iodepth.
        This ensures plots appear in the correct order within each numjobs section,
        matching the summary table sorting.

        Returns:
            List of Path objects for time-series plot files only, sorted by
            (numjobs, blocksize, iodepth)
        """
        all_plots = self._find_and_sort_file_paths(
            paths=[self._plots_directory], search_pattern=f"*{PLOT_FILE_EXTENSION_WITH_DOT}"
        )
        # Filter to only include time-series plots (those with _timeseries in the stem)
        timeseries_plots = [plot for plot in all_plots if "_timeseries" in plot.stem]

        # Sort by numjobs, blocksize, and iodepth
        # Format: {blocksize}_{numjobs}_{operation}_{iodepth}_{metric}_timeseries.svg
        def get_sort_key(plot_path: Path) -> tuple[int, int, int]:
            parts = plot_path.stem.split("_")
            blocksize = int(parts[0])
            numjobs = int(parts[1])
            # iodepth is the 4th part (index 3), default to 0 if not present
            iodepth = int(parts[3]) if len(parts) > 3 else 0
            return (numjobs, blocksize, iodepth)

        return sorted(timeseries_plots, key=get_sort_key)

    def _find_and_sort_data_files(self) -> dict[str, list[Path]]:
        """
        Find and sort all the time-series data files that will be needed for
        producing the summary table.

        Overrides base class to look for *_timeseries.json files instead of
        regular .json files.
        """
        # Find common time-series file names across all data directories
        unique_file_names: list[str] = self._find_common_timeseries_file_names()

        # Sort by blocksize and numjobs
        sorted_data_file_names: list[str] = sorted(
            unique_file_names,
            key=lambda file_name: (
                int(file_name.split("_")[0]),
                int(file_name.split("_")[1]),
            ),
        )

        sorted_data_files: dict[str, list[Path]] = {}
        for file_name in sorted_data_file_names:
            file_name_without_extension: str = file_name[: -len(DATA_FILE_EXTENSION_WITH_DOT)]
            sorted_data_files[file_name_without_extension] = self._find_files_with_filename(file_name_without_extension)

        return sorted_data_files

    def _find_common_timeseries_file_names(self) -> list[str]:
        """
        Find common time-series file names across all data directories.

        Returns:
            List of time-series JSON file names (with extension)
        """
        if not self._data_directories:
            return []

        # Collect all time-series file names from all directories
        all_files: set[str] = set()
        for directory in self._data_directories:
            files = set(path.parts[-1] for path in directory.glob(f"*_timeseries{DATA_FILE_EXTENSION_WITH_DOT}"))
            all_files.update(files)

        # For a single archive, return all unique files
        if len(self._data_directories) == 1:
            return sorted(list(all_files))

        # Check if all directories share a common ancestor (same archive)
        # For time-series reports from a single archive, we want all unique files
        # even if they're in different subdirectories (different iodepth values)
        try:
            # Check if all directories are from the same archive by looking at the archive root
            # Path structure: archive/results/00000000/id-xxx/workload/rbdfio/
            # numjobs-xxx/total_iodepth-xxx/visualisation
            # We need to check if they all share the same archive root (before /results/)
            archive_roots = set()
            for d in self._data_directories:
                # Find the archive root by looking for the 'results' directory in parents
                for i, parent in enumerate(d.parents):
                    if parent.name == "results" and i > 0:
                        # The parent before 'results' is the archive root
                        archive_roots.add(d.parents[i + 1])
                        break

            # If all directories are from the same archive, return all unique files
            if len(archive_roots) == 1:
                log.debug("All directories from same archive, returning %d unique files", len(all_files))
                return sorted(list(all_files))
        except (IndexError, AttributeError):
            pass

        # For multiple archives, check if files exist in ALL directories
        common_files: set[str] = set()
        for file_name in all_files:
            # Check if this file exists in all of the directories
            found_count = sum(1 for d in self._data_directories if (d / file_name).exists())
            # Only include if found in ALL directories
            if found_count == len(self._data_directories):
                common_files.add(file_name)

        log.debug("Multiple archives detected, returning %d common files", len(common_files))
        return sorted(list(common_files))

    def _add_plots(self) -> None:  # pylint: disable=too-many-locals
        """
        Add the plots to the report in single-column layout.

        Overrides base class to use full-width plots (1 column instead of 2).
        Plots are organized by numjobs sections, then by operation, and sorted
        by blocksize and iodepth within each section (via _find_and_sort_plot_files).

        Note: This method has many local variables because it consolidates plotting logic.
        The complexity is justified as it provides consistent plot organization across reports.
        """
        self._report.new_header(level=1, title="Response Curves")
        empty_table_header: list[str] = [""]

        for number_of_jobs in self._get_all_number_of_jobs_values():
            self._report.new_header(level=2, title=f"Number of Jobs {number_of_jobs}")
            image_tables: dict[str, list[str]] = {}

            for _, operation in TITLE_CONVERSION.items():
                image_tables[operation] = empty_table_header.copy()

            for image_file in self._plot_files:
                # Get the file stem without any prefix (e.g., "Comparison_")
                file_stem = self._get_plot_file_stem(image_file)
                (blocksize, percent, operation, numjobs) = get_blocksize_percentage_operation_numjobs_from_file_name(
                    file_stem
                )
                if numjobs == number_of_jobs:
                    title: str = f"{blocksize} {percent} {operation}"

                    image_line: str = self._report.new_inline_image(
                        text=title, path=f"{self._plots_directory.parts[-1]}/{image_file.parts[-1]}"
                    )
                    anchor: str = f'<a name="{file_stem.replace("_", "-")}"></a>'

                    image_line = f"{anchor}{image_line}"

                    image_tables[operation].append(image_line)

            # Create the correct sections and add a table for each section to the report
            for section, data in image_tables.items():
                # We don't want to display a section if it doesn't contain any plots
                if len(data) > len(empty_table_header):
                    self._report.new_header(level=3, title=section)
                    table_images = data

                    # Single column layout - one row per image
                    number_of_rows: int = len(table_images)
                    self._report.new_table(columns=1, rows=number_of_rows, text=table_images, text_align="center")


# Made with Bob
