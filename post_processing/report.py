"""
Code used to generate a full performance report. This includes post processing
any results into an intermediate format, plotting the curves for the report,
and generating the report file.

This is called from both the script files in /tools to manually generate a
performance report, and from within the CBT code if we have been told to
"""

import os
import traceback
from argparse import Namespace
from logging import Logger, getLogger

from post_processing.formatter.common_output_formatter import CommonOutputFormatter
from post_processing.log_configuration import setup_logging
from post_processing.post_processing_types import ReportOptions
from post_processing.reports.comparison_report_generator import ComparisonReportGenerator
from post_processing.reports.report_generator import ReportGenerator
from post_processing.reports.simple_report_generator import SimpleReportGenerator

setup_logging()
log: Logger = getLogger(name="reports")


def parse_namespace_to_options(arguments: Namespace, comparison_report: bool = False) -> ReportOptions:
    """
    Parse a namespace as used by argparse into our internal NamedTuple representation
    """
    no_error_bars: bool = False
    plot_resources: bool = False
    archives: list[str] = []
    output_directory: str = arguments.output_directory

    if comparison_report:
        archives.append(arguments.baseline)
        for directory in arguments.archives.split(","):
            archives.append(directory)
    else:
        archives.append(arguments.archive)

    if hasattr(arguments, "no_error_bars"):
        no_error_bars = arguments.no_error_bars

    if hasattr(arguments, "plot_resources"):
        plot_resources = arguments.plot_resources

    return ReportOptions(
        archives=archives,
        output_directory=output_directory,
        create_pdf=arguments.create_pdf,
        results_file_root=arguments.results_file_root,
        force_refresh=arguments.force_refresh,
        no_error_bars=no_error_bars,
        comparison=comparison_report,
        plot_resources=plot_resources,
    )


class Report:
    """
    Represents a report that will be created from a CBT fio run.

    It will perform all the necessary steps to convert the raw data into a
    report
    """

    def __init__(self, options: ReportOptions) -> None:
        self._options: ReportOptions = options

        self._result_code: int = 0

    @property
    def result_code(self) -> int:
        """
        The return code from creating the reports. This is used when creating
        reports using the helper scripts
        """
        return self._result_code

    def generate(self, throw_exception: bool = False) -> None:
        """
        Do all the steps necessary to create the report file.

        Args:
            throw_exception: If True, re-raises exceptions after logging them.
                           If False, exceptions are caught and logged but not re-raised.
                           Defaults to False.
        """
        log.info("Creating directory %s to contain the reports", self._options.output_directory)
        os.makedirs(name=f"{self._options.output_directory}", exist_ok=True)

        try:
            self._generate_intermediate_files()
            report_generator: ReportGenerator

            if self._options.comparison:
                report_generator = ComparisonReportGenerator(
                    archive_directories=self._options.archives,
                    output_directory=self._options.output_directory,
                    force_refresh=self._options.force_refresh,
                )
            else:
                report_generator = SimpleReportGenerator(
                    archive_directories=self._options.archives,
                    output_directory=self._options.output_directory,
                    no_error_bars=self._options.no_error_bars,
                    force_refresh=self._options.force_refresh,
                    plot_resources=self._options.plot_resources,
                )

            report_generator.create_report()

            if self._options.create_pdf:
                report_generator.save_as_pdf()

        except Exception as e:  # pylint: disable=[broad-exception-caught]
            # Generating the intermediate files can raise a broad range of exceptions from
            # the different sub-modules called. Therefore we want to catch them all and raise
            # an error message as this will directly impact any future steps
            self._result_code = 1
            error_text: str = (
                "Post processing has failed due to an exeption. Report may not be generated."
                + f"\n The exception was {e}"
                + f"\nWith stack trace {traceback.format_exc()}"
            )
            log.warning(error_text)
            if throw_exception:
                raise e

    def _generate_intermediate_files(self) -> None:
        """
        If the raw fio results have not yet been post-processed then we need to do
        that now before trying to produce the report
        """

        for directory in self._options.archives:
            output_directory: str = f"{directory}/visualisation/"

            if not os.path.exists(output_directory) or not os.listdir(output_directory) or self._options.force_refresh:
                # Either the directory doesn't exists, or is empty, or the user has told us to regenerate the files

                log.debug("Creating directory %s", output_directory)
                os.makedirs(name=output_directory, exist_ok=True)

                log.info("Generating intermediate files for %s in directory %s", directory, output_directory)
                formatter: CommonOutputFormatter = CommonOutputFormatter(
                    archive_directory=directory, filename_root=self._options.results_file_root
                )

                try:
                    formatter.convert_all_files()
                    formatter.write_output_file()
                except Exception as e:  # pylint: disable=[broad-exception-caught]
                    # Generating the intermediate files can raise a broad range of exceptions from
                    # the different sub-modules called. Therefore we want to catch them all and raise
                    # an error message as this will directly impact any future steps
                    log.error(
                        "Encountered an error parsing results in directory %s with name %s",
                        directory,
                        self._options.results_file_root,
                    )
                    log.exception(e)
                    raise e
