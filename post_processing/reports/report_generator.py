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
    TITLE_CONVERSION,
    find_common_data_file_names,
    find_hockey_stick_visualisation_directories,
    get_blocksize_percentage_operation_numjobs_from_file_name,
    get_date_time_string,
)

log: Logger = getLogger("reports")


# pylint: disable=too-many-instance-attributes
class ReportGenerator(ABC):
    """
    The base class for all classes responsible for generating a markdown and
    PDF report
    """

    MARKDOWN_FILE_EXTENSION: str = "md"
    PDF_FILE_EXTENSION: str = "pdf"

    # tex header file location
    BASE_HEADER_FILE_PATH: str = "include/performance_report.tex"

    def __init__(  # pylint: disable=[too-many-arguments, too-many-positional-arguments]
        self,
        archive_directories: list[str],
        output_directory: str,
        no_error_bars: bool = False,
        force_refresh: bool = False,
        plot_resources: bool = False,
    ) -> None:
        self._plot_error_bars: bool = not no_error_bars
        self._force_refresh: bool = force_refresh
        self._plot_resources: bool = plot_resources

        self._archive_directories: list[Path] = []
        self._data_directories: list[Path] = []
        self._build_strings: list[str] = []
        self._data_files: dict[str, list[Path]] = {}

        for archive_directory in archive_directories:
            archive_path: Path = Path(archive_directory)
            self._archive_directories.append(archive_path)

            # Find visualisation directories (type depends on subclass)
            visualisation_directories = self._find_visualisation_directories(archive_path)

            # Reject legacy top-level visualisation directories unless force_refresh was requested.
            # force_refresh regenerates intermediate files before report generator construction,
            # so the legacy directory may still exist alongside the new nested structure.
            if (
                len(visualisation_directories) == 1
                and visualisation_directories[0].name == "visualisation"
                and visualisation_directories[0].parent == archive_path
                and not self._force_refresh
            ):
                error_msg = (
                    f"\nError: Legacy visualisation directory structure detected in '{archive_path}'.\n"
                    f"Reports require the new nested structure (operation/visualisation/).\n"
                    f"Please re-run with the --force_refresh flag to regenerate the intermediate format files.\n"
                )
                log.error(error_msg)
                raise ValueError(error_msg)

            # Check if no visualisation directories were found
            if not visualisation_directories:
                error_msg = (
                    f"\nError: No visualisation directories with data found in '{archive_path}'.\n"
                    f"Please ensure the data has been processed, or use --force_refresh to regenerate files.\n"
                )
                log.error(error_msg)
                raise ValueError(error_msg)

            self._data_directories.extend(visualisation_directories)

            # We need to replace all _ characters in the build string as pandoc conversion
            # breaks if there are _ characters in the file anywhere
            self._build_strings.append(f"{archive_path.parts[-1]}".replace("_", "-"))

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

        # Copy all the svg files to a plots subrirectory of the output directory
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

    def _get_additional_header_files(self) -> list[str]:
        """
        Get additional LaTeX header files to include during PDF conversion.

        Subclasses can override this method to add report-specific headers.
        These will be included in addition to the base header file.

        Default implementation returns an empty list (no additional headers).

        Returns:
            List of paths to additional header files
        """
        return []

    def save_as_pdf(self) -> int:
        """
        Convert a report in markdown format and save it as a pdf file.
        To do this we use pandoc.
        """
        # We need to change directory so we can include a relative reference to
        # the plot files
        chdir(self._output_directory)
        header_files = self._create_header_files()

        pdf_file_path: Path = Path(f"{self._output_directory}/{self._report_path.stem}.{self.PDF_FILE_EXTENSION}")

        pandoc_command: list[str] = ["/usr/bin/env", "pandoc"]

        for header_file in header_files:
            pandoc_command.extend(["-H", str(header_file)])

        pandoc_command.extend(
            [
                "-f",
                "markdown-implicit_figures",
                "--columns=10",
                "-V",
                "papersize=A4",
                "-V",
                "documentclass=report",
                "--top-level-division=chapter",
                "-o",
                str(pdf_file_path),
                str(self._report_path),
            ]
        )

        return_code: int = subprocess.call(
            pandoc_command, shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )
        if return_code != 0:
            log.error("Unable to convert %s to pdf format", f"{self._report_path}")
        else:
            # Clean up all header files
            for header_file in header_files:
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

            | workload_name | Number of Jobs | Maximum Throughput |    Latency (ms)|
            | <name>        | <numjobs>      | <iops_or_bw>       |    <latency_ms>|

        for a comparison report for 2 runs the format is:
        | <operation_type> | <baseline_workload_name>       | <workload_name>
        | %change throughput>  | %change latency   |

        | <blocksize>      | <baseline_througput>@<latency> | <througput>@<latency>
        | <%change_throughput> | <%change_latency> |

        for a comparison report for more than 2 runs the format is:
        | <operation_type> | <baseline_workload_name> | <workload_1_name>     | %change
        | <workload_2_name>     | %change   |

        | <blocksize>      | <througput>@<latency>    | <througput>@<latency> | <%change>
        | <througput>@<latency> | <%change> |
        """

    @abstractmethod
    def _get_plot_file_stem(self, image_file: Path) -> str:
        """
        Get the file stem for a plot file, removing any prefixes.

        For simple reports: returns the stem as-is
        For comparison reports: removes "Comparison_" prefix

        Args:
            image_file: Path to the plot file

        Returns:
            The file stem without any prefix
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
        that pattern, and return them as a list sorted numerically by file
        name
        """

    @abstractmethod
    def _copy_images(self) -> None:
        """
        Copy the plot files to a 'plots' subdirectory in the output directory
        so the markdown can link to them using a known relative path
        """

    def _find_visualisation_directories(self, archive_path: Path) -> list[Path]:
        """
        Find visualisation directories for this report type.

        Default implementation finds hockey-stick visualisation directories.
        Subclasses can override to find different types (e.g., timeseries).

        Args:
            archive_path: Path to the archive directory

        Returns:
            List of visualisation directory paths
        """
        return find_hockey_stick_visualisation_directories(archive_path)

    def _add_plots(self) -> None:  # pylint: disable=too-many-locals
        """
        Add the plots to the report.
        We are using a table to get multiple images on a single line.

        This is a template method that calls helper methods to be implemented by subclasses.

        Note: This method has many local variables (17) because it consolidates plotting logic
        that was previously duplicated across SimpleReportGenerator and ComparisonReportGenerator.
        The complexity is justified as it eliminates ~90 lines of duplicate code and provides
        a single source of truth for plot generation. Further refactoring would require breaking
        this into smaller methods, which would reduce readability for this template method pattern.
        """
        self._report.new_header(level=1, title="Response Curves")
        empty_table_header: list[str] = ["", ""]

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

                    # We need to calculate the number of rows, but new_table() requires the
                    # exact number of items to fill the table, so we may need to add a dummy
                    # entry at the end
                    number_of_rows: int = len(table_images) // 2
                    if len(table_images) % 2 > 0:
                        number_of_rows += 1
                        table_images.append("")
                    self._report.new_table(columns=2, rows=number_of_rows, text=table_images, text_align="center")

    def _find_configuration_yaml_files(self) -> list[Path]:
        """
        Find the path to the configuration yaml files in the archive directories.

        This method searches for cbt_config.yaml files in the results directories
        associated with each archive directory. It handles both cases where the
        archive directory is within the results tree or contains a results subdirectory.

        Returns:
            List of Path objects for cbt_config.yaml files found
        """
        file_paths: list[Path] = []

        for directory in self._archive_directories:
            # Search both above and below the current directory for the config yaml
            # This handles different calling contexts (during run vs. post-processing)

            # First, try to find results directory in parent paths
            paths: list[Path] = [path for path in directory.parents if f"{path}".endswith("/results")]

            # If not found in parents, look in subdirectories
            if len(paths) == 0:
                paths = [path for path in directory.iterdir() if f"{path}".endswith("/results")]

            # Search for yaml files in the results directories
            for path in paths:
                file_paths.extend(path.glob("**/cbt_config.yaml"))

        return file_paths

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
        has the .svg file extension
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

    def _save_report(self) -> None:
        """
        Save the report file to disk
        """
        self._report.create_md_file()

    def _create_plots_results_directory(self) -> None:
        """
        Create the plots sub-directory in the output directory
        """
        self._plots_directory.mkdir(exist_ok=True)

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

    def _create_header_files(self) -> list[Path]:
        """
        Create the header files in tex format that are used to provide
        headers and footers when the report is created in pdf format.

        Copies all header files (base + additional) to the output directory.
        Replaces BUILD placeholder with the build string in all files.

        Returns:
            List of paths to the copied header files in the output directory
        """
        current_file_path: Path = Path(__file__)

        cbt_directory: str = ""
        for part in current_file_path.parts:
            cbt_directory += f"{part}/"
            if part == "cbt":
                break

        # Combine base header and any additional headers
        all_header_paths = [self.BASE_HEADER_FILE_PATH, *self._get_additional_header_files()]

        build_string: str = " vs ".join(self._build_strings)
        output_header_files: list[Path] = []

        for header_path in all_header_paths:
            source_path = Path(f"{cbt_directory}/{header_path}")
            dest_path = Path(f"{self._output_directory}/{source_path.name}")

            try:
                tex_contents: str = source_path.read_text(encoding="utf-8")
                # Replace BUILD placeholder if present
                tex_contents = tex_contents.replace("BUILD", build_string)
                dest_path.write_text(tex_contents, encoding="utf-8")
                output_header_files.append(dest_path)
            except FileNotFoundError:
                log.error("Unable to read from %s", source_path)
            except PermissionError:
                log.error("Unable to write to %s", dest_path)

        return output_header_files

    def _generate_plot_directory_name(self) -> str:
        """
        Generate a unique plot directory name for each run of the tool
        """
        base_directory_name: str = f"{self._output_directory}/plots"
        date_string: str = get_date_time_string()

        unique_directory_name: str = f"{base_directory_name}.{date_string}"
        return unique_directory_name

    def _get_all_number_of_jobs_values(self) -> list[str]:
        """
        Get all the possible number_of_jobs values for this set of data.
        Works for both simple, comparison and timeseries plot file naming conventions.

        Simple format: <blocksize>_<percent>_<operation>_<numjobs>.svg
        Comparison format: Comparison_<blocksize>_<percent>_<operation>_<numjobs>.svg
        Time-series format: <blocksize>_<numjobs>_<operation>_<metric>_timeseries.svg
        """
        numbers_of_jobs: set[str] = set()
        for image_file in self._plot_files:
            # Use the subclass-specific method to get the proper stem
            # This handles different naming conventions (simple, comparison, time-series)
            stem = self._get_plot_file_stem(image_file)

            (_, _, _, number_of_jobs) = get_blocksize_percentage_operation_numjobs_from_file_name(stem)
            numbers_of_jobs.add(number_of_jobs)

        return sorted(numbers_of_jobs)
