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
    get_blocksize_percentage_operation_from_file_name,
    get_latency_throughput_from_file,
    strip_confidential_data_from_yaml,
)
from post_processing.plotter.simple_plotter import SimplePlotter
from post_processing.reports.report_generator import ReportGenerator

log: Logger = getLogger("cbt")


class SimpleReportGenerator(ReportGenerator):
    def _generate_report_title(self) -> str:
        title: str = f"Performance Report for {''.join(self._build_strings)}"
        return title

    def _generate_report_name(self) -> str:
        current_datetime: datetime = datetime.now()

        # Convert to string
        datetime_string: str = current_datetime.strftime("%y%m%d_%H%M%S")
        output_file_name: str = f"performance_report_{datetime_string}.{self.MARKDOWN_FILE_EXTENSION}"
        return output_file_name

    def _add_summary_table(self) -> None:
        self._report.new_header(level=1, title=f"Summary of results for {''.join(self._build_strings)}")

        # We cannot use the mdutils table object here as it can only justify
        # all the colums in the same way, and we want to justify different
        # columns differently.
        # Therefore we have to build the table ourselves

        self._report.new_line(text="|Workload Name|Maximum Throughput|Latency (ms)|")
        self._report.new_line(text="| :--- | ---: | ---: |")

        data_tables: dict[str, list[str]] = {}
        for _, operation in TITLE_CONVERSION.items():
            data_tables[operation] = []

        for file_name in self._data_files.keys():
            for file_path in self._data_files[file_name]:
                (max_throughput, latency_ms) = get_latency_throughput_from_file(file_path)

                (_, _, operation) = get_blocksize_percentage_operation_from_file_name(file_path.stem)
                data: str = f"|[{file_path.stem}](#{file_path.stem.replace('_', '-')})|{max_throughput}|{latency_ms}|"

                data_tables[operation].append(data)

        for operation in data_tables.keys():
            for line in data_tables[operation]:
                self._report.new_line(text=line)

    def _add_plots(self) -> None:
        self._report.new_header(level=1, title="Response Curves")
        empty_table_header: list[str] = ["", ""]
        image_tables: dict[str, list[str]] = {}

        for _, operation in TITLE_CONVERSION.items():
            image_tables[operation] = empty_table_header.copy()

        for image_file in self._plot_files:
            (blocksize, percent, operation) = get_blocksize_percentage_operation_from_file_name(image_file.stem)
            title: str = f"{blocksize}K {percent} {operation}"

            image_line: str = self._report.new_inline_image(
                text=title, path=f"{self._plots_directory.parts[-1]}/{image_file.parts[-1]}"
            )
            anchor: str = f'<a name="{image_file.stem.replace("_", "-")}"></a>'

            image_line = f"{anchor}{image_line}"

            image_tables[operation].append(image_line)

        # Create the correct sections and add a table for each section to the report

        for section in image_tables.keys():
            # We don't want to display a section if it doesn't contain any plots
            if len(image_tables[section]) > len(empty_table_header):
                self._report.new_header(level=2, title=section)
                table_images = image_tables[section]

                # We need to calculate the rumber of rows, but new_table() requires the
                # exact number of items to fill the table, so we may need to add a dummy
                # entry at the end
                number_of_rows: int = len(table_images) // 2
                if len(table_images) % 2 > 0:
                    number_of_rows += 1
                    table_images.append("")
                self._report.new_table(columns=2, rows=number_of_rows, text=table_images, text_align="center")

    def _add_configuration_yaml_file(self) -> None:
        self._report.new_header(level=1, title="Configuration yaml")
        yaml_file_path: Path = self._find_configuration_yaml_files()[0]

        file_contents: str = yaml_file_path.read_text()
        safe_contents = strip_confidential_data_from_yaml(file_contents)
        markdown_string: str = f"```{safe_contents}```"

        self._report.new_paragraph(markdown_string)

    def _copy_images(self) -> None:
        plot_files: list[Path] = []
        for directory in self._data_directories:
            plot_files.extend(list(directory.glob(f"*{PLOT_FILE_EXTENSION_WITH_DOT}")))

            # If there are no plotfiles in the directory then we should create them
            if len(plot_files) == 0:
                plotter = SimplePlotter(str(directory.parent))
                plotter.draw_and_save()
                plot_files.extend(list(directory.glob(f"*{PLOT_FILE_EXTENSION_WITH_DOT}")))

        for plot_file in plot_files:
            subprocess.call(f"cp {plot_file} {self._plots_directory}/", shell=True)

    def _find_and_sort_file_paths(self, paths: list[Path], search_pattern: str, index: Optional[int] = 0) -> list[Path]:
        unsorted_paths: list[Path] = list(paths[0].glob(search_pattern))
        assert index is not None
        return self._sort_list_of_paths(unsorted_paths, index)

    def _find_configuration_yaml_files(self) -> list[Path]:
        file_paths: list[Path] = list(self._archive_directories[0].glob("**/cbt_config.yaml"))

        # This should only ever return a single path as each archive directory
        # should only ever contain a single cbt_config.yaml file
        return file_paths
