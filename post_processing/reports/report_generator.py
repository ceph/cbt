"""
Common code for generating a performance report in markdown and pdf format
"""

import subprocess
from abc import ABC, abstractmethod
from logging import Logger, getLogger
from os import chdir
from pathlib import Path
from typing import Optional

# It seems that the mdutils package doesn't contain library stubs or a py.typed
# marker, which causes an error here. This is an issue that would need to be
# fixed in the mdutils library, so we will have to ignore the error for the
# moment
from mdutils.mdutils import MdUtils  # type:  ignore [import-untyped]

from post_processing.common import (
    DATA_FILE_EXTENSION_WITH_DOT,
    PLOT_FILE_EXTENSION_WITH_DOT,
    find_common_data_file_names,
    get_date_time_string,
)

log: Logger = getLogger("reports")


class ReportGenerator(ABC):
    """
    The base class for all classes responsible for generating a markdown and
    pdf report
    """

    MARKDOWN_FILE_EXTENSION: str = "md"
    PDF_FILE_EXTENSION: str = "pdf"

    # tex header file location
    BASE_HEADER_FILE_PATH: str = "include/performance_report.tex"

    def __init__(
        self,
        archive_directories: list[str],
        output_directory: str,
        no_error_bars: bool = False,
        force_refresh: bool = False,
    ) -> None:
        self._plot_error_bars: bool = not no_error_bars
        self._force_refresh: bool = force_refresh

        self._archive_directories: list[Path] = []
        self._data_directories: list[Path] = []
        self._build_strings: list[str] = []
        self._data_files: dict[str, list[Path]] = {}

        for archive_directory in archive_directories:
            archive_path: Path = Path(archive_directory)
            self._archive_directories.append(archive_path)
            data_directory: Path = Path(f"{archive_directory}/visualisation")
            self._data_directories.append(data_directory)
            # We need to replace all _ characters in the build string as pandoc conversion
            # breaks if there are _ characters in the file anywhere
            self._build_strings.append(f"{archive_path.parts[-1]}".replace("_", "-"))

        # self._data_directories = self._data_directories
        self._data_files = self._find_and_sort_data_files()

        self._output_directory: Path = Path(output_directory)
        self._plots_directory: Path = Path(f"{self._generate_plot_directory_name()}")

        report_name = self._generate_report_name()
        self._report_path = Path(f"{output_directory}/{report_name}")

        self._plot_files: list[Path] = []

        self._report: MdUtils

    def create_report(self) -> None:
        """
        Read the data files and generate a report from them and the
        """

        # Copy all the png files to a plots subrirectory of the output directory
        # specified
        self._create_plots_results_directory()
        self._copy_images()
        self._plot_files = self._find_and_sort_plot_files()

        self._report = MdUtils(
            file_name=f"{self._report_path}",
            title=f"{self._generate_report_title()}",
            author="CBT Report Generator",
        )

        self._add_summary_table()
        self._add_plots()
        self._add_configuration_yaml_files()

        # Add a table of contents
        self._report.new_table_of_contents(depth=3)

        # Finally, save the report file in markdown format to the specified
        # outupt directory
        self._save_report()

    def save_as_pdf(self) -> int:
        """
        Convert a report in markdown format and save it as a pdf file.
        To do this we use pandoc.
        """
        # We need to change directory so we can include a relative reference to
        # the plot files
        chdir(self._output_directory)
        header_file = self._create_header_files()

        pdf_file_path: Path = Path(f"{self._output_directory}/{self._report_path.stem}.{self.PDF_FILE_EXTENSION}")

        pandoc_command: str = (
            f"/usr/bin/env pandoc -H {header_file} "
            + "-f markdown-implicit_figures  --columns=10 "
            + "-V papersize=A4 -V documentclass=report --top-level-division=chapter "
            + f"-o {pdf_file_path} {self._report_path}"
        )

        return_code: int = subprocess.call(
            pandoc_command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )
        if return_code != 0:
            log.error("Unable to convert %s to pdf format", f"{self._report_path}")
        else:
            header_file.unlink()

        return return_code

    @abstractmethod
    def _generate_report_title(self) -> str:
        """
        Generate the title for the report.

        Any _ must be converted to a - otherwise the pandoc conversion to PDF
        will fail
        """

    @abstractmethod
    def _generate_report_name(self) -> str:
        """
        The report name is of the format:
            performance_report_yymmdd_hhmmss.md
        """

    @abstractmethod
    def _add_summary_table(self) -> None:
        """
        Add a table that contains a summary of the results.

        For a simple report this ill be of the format:

        | workload_name | Maximum Throughput |    Latency (ms)|
        | <name>        |       <iops_or_bw> |    <latency_ms>|

        for a comparison report the format is:
        TODO: fill this in
        """

    @abstractmethod
    def _add_plots(self) -> None:
        """
        Add the plots to the report.
        We are using a table to get multiple images on a single line
        """

    @abstractmethod
    def _add_configuration_yaml_files(self) -> None:
        """
        Add the configuration yaml file to the report
        """

    @abstractmethod
    def _find_and_sort_file_paths(self, paths: list[Path], search_pattern: str, index: Optional[int] = 0) -> list[Path]:
        """
        Given the search_pattern find all the files in a Path that match
        that pattern, and terutn them as a list sorted numerically by file
        name
        """

    @abstractmethod
    def _find_configuration_yaml_files(self) -> list[Path]:
        """
        Find the path to the configuration yaml files in the archive
        directories
        """

    @abstractmethod
    def _copy_images(self) -> None:
        """
        Copy the plot files to a 'plots' subdirectory in the output directory
        so the markdown can link to them using a known relative path
        """

    def _all_plot_files_exist(self) -> bool:
        """
        return true if plot files exist for all the data files. If we have
        multiple data directories then we should have only 1 plot file for
        each data file that exists in every directory
        """
        return len(self._plot_files) == len(self._data_files.keys())

    def _find_and_sort_plot_files(self) -> list[Path]:
        """
        Find all the plot files in the directory. That is any file that
        has the .png file extension
        """
        return self._find_and_sort_file_paths(
            paths=[self._plots_directory], search_pattern=f"*{PLOT_FILE_EXTENSION_WITH_DOT}"
        )

    def _find_and_sort_data_files(self) -> dict[str, list[Path]]:
        """
        Find and sort all the data files that will be needed for
        producing the summary table
        """
        unique_file_names: list[str] = find_common_data_file_names(self._data_directories)
        sorted_data_file_names: list[str] = sorted(unique_file_names, key=lambda a: int(a.split("_")[0][:-1]))

        sorted_data_files: dict[str, list[Path]] = {}
        for file_name in sorted_data_file_names:
            file_name_without_extension: str = file_name[: -len(DATA_FILE_EXTENSION_WITH_DOT)]
            sorted_data_files[file_name_without_extension] = self._find_files_with_filename(file_name_without_extension)

        return sorted_data_files

    def _save_report(self) -> None:
        """
        Save the report file to disk
        """
        self._report.create_md_file()

    def _create_plots_results_directory(self) -> None:
        """
        Create the plots sub-directory in the output directory
        """
        subprocess.call(f"mkdir -p {self._plots_directory}", shell=True)

    def _sort_list_of_paths(self, paths: list[Path], index: int) -> list[Path]:
        """
        Sort a list of path files into numerical order of the file name
        """
        sorted_filenames: list[Path] = sorted(paths, key=lambda a: int(a.stem.split("_")[index][:-1]))
        return sorted_filenames

    def _find_files_with_filename(self, file_name: str) -> list[Path]:
        """
        Given a file name. find all the paths to the files with that name
        """
        file_paths: list[Path] = []

        for directory in self._data_directories:
            file_paths.extend(directory.glob(f"*{file_name}{DATA_FILE_EXTENSION_WITH_DOT}"))

        return file_paths

    def _create_header_files(self) -> Path:
        """
        Create the header file in tex format that is used to provide
        headers and footers when the report is created in pdf format.

        Replace any placeholders with the correct values.
        """
        # Note: This currently only enters the build string, but it could be
        # expanded in the future to give more detals
        current_file_path: Path = Path(__file__)

        cbt_directory: str = ""
        for part in current_file_path.parts:
            cbt_directory += f"{part}/"
            if part == "cbt":
                break

        base_header_file: Path = Path(f"{cbt_directory}/{self.BASE_HEADER_FILE_PATH}")

        tex_output_path: Path = Path(f"{self._output_directory}/perf_report.tex")

        # TODO: What do we want to actually do here????
        build_string: str = " vs ".join(self._build_strings)

        try:
            tex_contents: str = base_header_file.read_text()
            tex_contents = tex_contents.replace("BUILD", build_string)
            tex_output_path.write_text(tex_contents)
        except FileNotFoundError:
            log.error("Unable to read from %s", base_header_file)
        except PermissionError:
            log.error("Unable to write to %s", tex_output_path)

        return tex_output_path

    def _generate_plot_directory_name(self) -> str:
        """
        Generate a unique plot directory name for each run of the tool
        """
        base_directory_name: str = f"{self._output_directory}/plots"
        date_string: str = get_date_time_string()

        unique_directory_name: str = f"{base_directory_name}.{date_string}"
        return unique_directory_name
