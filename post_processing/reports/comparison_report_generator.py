"""
Code to automatically generate a comparison report from two or more directories
containing a set of performance results in the intermediate format in PR 319.

The report will be generated in markdowm format using the create_report()
method and the resulting file saved to the specified output directory.

Optionally the markdown report can be converted to a pdf using pandoc by
calling save_as_pdf()

The pdf file will have the same name as the markdown file, and be saved in the same output
directory.
"""

import subprocess
from logging import Logger, getLogger
from pathlib import Path
from typing import Optional

from post_processing.common import (
    PLOT_FILE_EXTENSION_WITH_DOT,
    TITLE_CONVERSION,
    calculate_percent_difference_to_baseline,
    get_blocksize_percentage_operation_numjobs_from_file_name,
    get_date_time_string,
    get_latency_throughput_from_file,
    strip_confidential_data_from_yaml,
)
from post_processing.plotter.directory_comparison_plotter import DirectoryComparisonPlotter
from post_processing.reports.report_generator import ReportGenerator

log: Logger = getLogger("reports")


class ComparisonReportGenerator(ReportGenerator):
    """
    The class responsible for generating a comparison report. That is a single report that
    compares one or more sets of test data, and creates plots that have data from multiple
    runs on a single plot

    This can either be a comparison of two individual files, or multiple test run directories
    containing several individual FIO runs
    """

    # No need to override __init__ anymore - base class handles everything

    def _generate_report_title(self) -> str:
        title: str = f"Comparitive Performance Report for {' vs '.join(self._build_strings)}"
        return title

    def _get_plot_file_stem(self, image_file: Path) -> str:
        """
        Get the file stem for a comparison report plot file.

        For comparison reports, removes the "Comparison_" prefix from the stem.

        Args:
            image_file: Path to the plot file

        Returns:
            The file stem without the "Comparison_" prefix
        """
        stem = image_file.stem
        if stem.startswith("Comparison_"):
            return stem[len("Comparison_") :]
        return stem

    def _add_summary_table(self) -> None:
        self._report.new_header(level=1, title=f"Comparison summary for {' vs '.join(self._build_strings)}")
        # We cannot use the mdutils table object here as it can only justify
        # all the colums in the same way, and we want to justify different
        # columns differently.
        # Therefore we have to build the table ourselves

        data_tables: dict[str, list[str]] = {}
        for _, operation in TITLE_CONVERSION.items():
            data_tables[operation] = []

        (table_header, table_justfication_string) = self._generate_table_headers()
        self._generate_table_rows(data_tables)

        for operation, data in data_tables.items():
            if data:
                self._report.new_line(text=f"|{operation}|{table_header}")
                self._report.new_line(text=table_justfication_string)
                for line in data:
                    self._report.new_line(text=line)
                self._report.new_line()

    def _add_configuration_yaml_files(self) -> None:
        self._report.new_header(level=1, title="Configuration yaml files")

        yaml_paragraph: str = (
            "Only yaml files that differ by more than 20 lines from the yaml file for the "
            + "baseline directory will be added here in addition to the baseline yaml"
        )

        self._report.new_paragraph(yaml_paragraph)
        self._report.new_line()

        yaml_files: list[Path] = self._find_configuration_yaml_files()

        base_yaml_file: Path = yaml_files.pop(0)
        self._add_yaml_file_title_and_contents(base_yaml_file)

        for yaml_file in yaml_files:
            if self._yaml_file_has_more_that_20_differences(base_yaml_file, yaml_file):
                self._add_yaml_file_title_and_contents(yaml_file)

    def _add_yaml_file_title_and_contents(self, file_path: Path) -> None:
        """
        Add a title heading and the contents of a yaml file to the report
        """
        self._report.new_header(level=2, title=f"{file_path.parts[-2]}")

        file_contents: str = file_path.read_text()
        safe_contents = strip_confidential_data_from_yaml(file_contents)
        markdown_string: str = f"```{safe_contents}```"
        self._report.new_paragraph(markdown_string)

    def _generate_report_name(self) -> str:
        datetime_string: str = get_date_time_string()
        output_file_name: str = f"comparitive_performance_report_{datetime_string}.{self.MARKDOWN_FILE_EXTENSION}"
        return output_file_name

    def _find_and_sort_file_paths(self, paths: list[Path], search_pattern: str, index: Optional[int] = 0) -> list[Path]:
        sorted_paths: list[Path] = []
        unsorted_paths: list[Path] = []

        for directory in paths:
            unsorted_paths.extend(list(directory.glob(search_pattern)))

        assert index is not None
        sorted_paths = self._sort_list_of_paths(unsorted_paths, index)

        return sorted_paths

    def _find_and_sort_plot_files(self) -> list[Path]:
        """
        Find all the comparison plot files in the directory.

        This overrides the one in the ReportGenerator as the comparison plot
        files have a different naming convention:
            Comparison_<blocksize>B_<read%>_<write%>_<operation>.svg

        Filters to only include files starting with "Comparison_" to avoid
        accidentally including simple or time-series plot files.
        """
        all_plots = self._find_and_sort_file_paths(
            paths=[self._plots_directory], search_pattern=f"*{PLOT_FILE_EXTENSION_WITH_DOT}", index=1
        )
        # Filter to only include comparison plots (those starting with "Comparison_")
        comparison_plots = [plot for plot in all_plots if plot.stem.startswith("Comparison_")]
        return comparison_plots

    def _create_comparison_plots(self) -> None:
        """
        Generate the comparison plots and save them in the correct place
        """
        plotter: DirectoryComparisonPlotter = DirectoryComparisonPlotter(
            output_directory=f"{self._plots_directory}",
            directories=[f"{directory}" for directory in self._archive_directories],
        )
        plotter.draw_and_save()

    def _copy_images(self) -> None:
        self._create_comparison_plots()

    def _generate_table_headers(self) -> tuple[str, str]:
        """
        Generate the header lines for the table
        """
        # Use archive directories (not data directories which now contain multiple subdirs per archive)
        # The first archive is always the baseline
        archive_dirs = self._archive_directories.copy()
        baseline_dir = archive_dirs.pop(0)

        table_header: str = f"numjobs|{baseline_dir.parts[-1]}|"
        table_justfication_string: str = "| :--- | ---: | ---: |"

        if len(archive_dirs) < 2:
            for directory in archive_dirs:
                table_header += f"{directory.parts[-1]}|%change throughput|%change latency|"
                table_justfication_string += " ---: | ---: | ---: |"
        else:
            for directory in archive_dirs:
                table_header += f"{directory.parts[-1]}|%change|"
                table_justfication_string += " ---: | ---: |"

        return (table_header, table_justfication_string)

    def _generate_table_rows(self, data_tables: dict[str, list[str]]) -> None:  # pylint: disable=[too-many-locals]
        """
        Generate the data for all the rows in the table
        """
        for file_name, file_paths in self._data_files.items():
            (blocksize, percentage, operation, number_of_jobs) = (
                get_blocksize_percentage_operation_numjobs_from_file_name(file_name)
            )

            data_string: str = f"|[{blocksize}"
            if percentage:
                data_string += f"_{percentage}"

            data_string += f"](#{file_name.replace('_', '-')})|"

            data_string += f"{number_of_jobs}|"

            (baseline_max_throughput, baseline_latency_ms) = get_latency_throughput_from_file(file_paths.pop(0))

            if len(self._data_directories) < 2:
                data_string += f"{baseline_max_throughput}@{baseline_latency_ms}ms|"
            else:
                data_string += f"{baseline_max_throughput.split(' ')[0]}@{baseline_latency_ms}ms|"

            for file_path in file_paths:
                (max_throughput, latency_ms) = get_latency_throughput_from_file(file_path)
                throughput_percentage_difference: str = calculate_percent_difference_to_baseline(
                    baseline=baseline_max_throughput, comparison=max_throughput
                )

                if len(self._data_directories) < 2:
                    latency_percentage_difference: str = calculate_percent_difference_to_baseline(
                        baseline=baseline_latency_ms, comparison=latency_ms
                    )
                    data_string += (
                        f"{max_throughput}@{latency_ms}ms|"
                        + f"{throughput_percentage_difference}|"
                        + f"{latency_percentage_difference}|"
                    )
                else:
                    data_string += f"{max_throughput.split(' ')[0]}@{latency_ms}|{throughput_percentage_difference}|"

            data_tables[operation].append(data_string)

    def _yaml_file_has_more_that_20_differences(self, base_file: Path, comparison_file: Path) -> bool:
        """
        If there are more that 20 differences between base and comparison then
        return True, otherwise False
        """
        # Use shell=False for security - pass command as list
        diff_command: list[str] = [
            "/usr/bin/env",
            "diff",
            "-wy",
            "--suppress-common-lines",
            str(base_file),
            str(comparison_file),
        ]
        wc_command: list[str] = ["/usr/bin/env", "wc", "-l"]

        output: bytes
        try:
            # Use two subprocess calls with pipe to avoid shell=True
            with subprocess.Popen(diff_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as diff_process:
                with subprocess.Popen(
                    wc_command, stdin=diff_process.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                ) as wc_process:
                    if diff_process.stdout:
                        diff_process.stdout.close()  # Allow diff_process to receive SIGPIPE if wc_process exits
                    output, _ = wc_process.communicate()
        except (subprocess.CalledProcessError, OSError):
            return False

        output_as_string: str = output.decode().strip()

        return int(output_as_string) > 20
