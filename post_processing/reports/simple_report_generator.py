"""
Code to automatically generate a report from a directory containing a set of
performance results in the intermediate format in PR 319.

The report will be generated in markdowm format using the create_report()
method and the resulting file saved to the specified output directory.

Optionally the markdown report can be converted to a pdf using pandoc by
calling save_as_pdf().
The pdf file will have the same name as the markdown file, and be saved in
the same output directory.
"""

import subprocess
from datetime import datetime
from logging import Logger, getLogger
from pathlib import Path
from typing import Optional

from post_processing.common import (
    PLOT_FILE_EXTENSION_WITH_DOT,
    TITLE_CONVERSION,
    get_blocksize_percentage_operation_numjobs_from_file_name,
    get_latency_throughput_from_file,
    get_resource_details_from_file,
    strip_confidential_data_from_yaml,
)
from post_processing.plotter.simple_plotter import SimplePlotter
from post_processing.reports.report_generator import ReportGenerator

log: Logger = getLogger("reports")


class SimpleReportGenerator(ReportGenerator):
    """
    The class responsible for generating a report from a single FIO run

    Typically this is run using a Workload.
    """

    def _generate_report_title(self) -> str:
        title: str = f"Performance Report for {''.join(self._build_strings)}"
        return title

    def _generate_report_name(self) -> str:
        current_datetime: datetime = datetime.now()

        # Convert to string
        datetime_string: str = current_datetime.strftime("%y%m%d_%H%M%S")
        output_file_name: str = f"performance_report_{datetime_string}.{self.MARKDOWN_FILE_EXTENSION}"
        return output_file_name

    def _add_summary_table(self) -> None:  # pylint: disable=[too-many-locals]
        self._report.new_header(level=1, title=f"Summary of results for {''.join(self._build_strings)}")

        # We cannot use the mdutils table object here as it can only justify
        # all the colums in the same way, and we want to justify different
        # columns differently.
        # Therefore we have to build the table ourselves

        log.info("Generating summary table")

        headers: str = "|Workload Name|Number of Jobs|Maximum Throughput|Latency (ms)|"
        alignment: str = "| :--- | :---: | ---: | ---: |"

        if self._plot_resources:
            alignment += " ---: |"
            headers += "System CPU (%)|"

        self._report.new_line(text=headers)
        self._report.new_line(text=alignment)

        data_tables: dict[str, list[str]] = {}
        for _, operation in TITLE_CONVERSION.items():
            data_tables[operation] = []

        for _, file_data in self._data_files.items():
            for file_path in file_data:
                log.debug("Looking at file %s", file_path)
                (max_throughput, latency_ms) = get_latency_throughput_from_file(file_path)
                (cpu_usage, _) = get_resource_details_from_file(file_path)
                # Eventually we will return the memory usage here, but for the first pass we'll just grab CPU

                (blocksize, percent, operation, number_of_jobs) = (
                    get_blocksize_percentage_operation_numjobs_from_file_name(file_path.stem)
                )
                workload_name: str = f"{blocksize} {percent} {operation}"
                data: str = (
                    f"|[{workload_name}](#{file_path.stem.replace('_', '-')})|"
                    + f"{number_of_jobs}|{max_throughput}|{latency_ms}|{cpu_usage}|"
                )

                data_tables[operation].append(data)

        # Add all data rows to the table
        for _, operation_data in data_tables.items():
            for line in operation_data:
                self._report.new_line(text=line)

    def _get_plot_file_stem(self, image_file: Path) -> str:
        """
        Get the file stem for a simple report plot file.

        For simple reports, the stem is used as-is without any prefix removal.

        Args:
            image_file: Path to the plot file

        Returns:
            The file stem without extension
        """
        return image_file.stem

    def _add_configuration_yaml_files(self) -> None:
        self._report.new_header(level=1, title="Configuration yaml")
        yaml_file_path: Path = self._find_configuration_yaml_files()[0]

        file_contents: str = yaml_file_path.read_text()
        safe_contents = strip_confidential_data_from_yaml(file_contents)
        markdown_string: str = f"```{safe_contents}```"

        self._report.new_paragraph(markdown_string)

    def _copy_images(self) -> None:
        plot_files: list[Path] = []
        for directory in self._data_directories:
            # Check for existing plot files in this directory
            existing_plots = list(directory.glob(f"*{PLOT_FILE_EXTENSION_WITH_DOT}"))

            # If there are no plot files in the directory or force_refresh is set, create them
            if len(existing_plots) == 0 or self._force_refresh:
                log.info("Generating plots for %s", directory.parent)
                plotter = SimplePlotter(str(directory.parent), self._plot_error_bars, self._plot_resources)
                plotter.draw_and_save()
                # Collect the newly generated plot files
                plot_files.extend(list(directory.glob(f"*{PLOT_FILE_EXTENSION_WITH_DOT}")))
            else:
                # Use existing plots
                plot_files.extend(existing_plots)

        # Filter out time-series files (they have _timeseries suffix before the extension)
        # Simple reports should only include standard hockey-stick plots
        filtered_plot_files = [plot_file for plot_file in plot_files if "_timeseries" not in plot_file.stem]

        for plot_file in filtered_plot_files:
            # Use shell=False for security - pass command as list
            subprocess.call(["/usr/bin/env", "cp", str(plot_file), f"{self._plots_directory}/"])

    def _find_and_sort_file_paths(self, paths: list[Path], search_pattern: str, index: Optional[int] = 0) -> list[Path]:
        unsorted_paths: list[Path] = list(paths[0].glob(search_pattern))
        assert index is not None
        return self._sort_list_of_paths(unsorted_paths, index)
