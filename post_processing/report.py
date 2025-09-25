"""
Code used to generate a full performance report. This includes post processing
any results into an intermediate format, plotting the curves for the report,
and generating the report file.

This is called from both the script files in /tools to manually generate a
performance report, and from within the CBT code if we have been told to
"""

import os
from argparse import Namespace
from logging import Logger, getLogger
from typing import NamedTuple

from post_processing.formatter.common_output_formatter import CommonOutputFormatter
from post_processing.log_configuration import setup_logging
from post_processing.reports.comparison_report_generator import ComparisonReportGenerator
from post_processing.reports.simple_report_generator import SimpleReportGenerator

setup_logging()
log: Logger = getLogger("reports")


class ReportOptions(NamedTuple):
    archives: list[str]
    output_directory: str
    results_file_root: str
    create_pdf: bool
    force_refresh: bool
    no_error_bars: bool
    comparison: bool


def parse_namespace_to_options(arguments: Namespace, comparison_report: bool = False) -> ReportOptions:
    no_error_bars: bool = False
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

    return ReportOptions(
        archives=archives,
        output_directory=output_directory,
        create_pdf=arguments.create_pdf,
        results_file_root=arguments.results_file_root,
        force_refresh=arguments.force_refresh,
        no_error_bars=no_error_bars,
        comparison=comparison_report,
    )

class Report:
    def __init__(self, options: ReportOptions) -> None:
        self._options: ReportOptions = options

        self._result_code: int = 0

    @property
    def result_code(self) -> int:
        return self._result_code

    def generate(self) -> None:
        """
        Do all the steps necessary to create the report file
        """
        log.info("Creating directory %s to contain the reports" % self._options.output_directory)
        os.makedirs(f"{self._options.output_directory}", exist_ok=True)

        try:
            self._generate_intermediate_files()

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
                )

            report_generator.create_report()

            if self._options.create_pdf:
                report_generator.save_as_pdf()

        except Exception as e:
            self._result_code = 1
            raise (e)

    def _generate_intermediate_files(self) -> None:
        """
        If the raw fio results have not yet been post-processed then we need to do
        that now before trying to produce the report
        """

        for directory in self._options.archives:
            output_directory: str = f"{directory}/visualisation/"

            if not os.path.exists(output_directory) or not os.listdir(output_directory) or self._options.force_refresh:
                # Either the directory doesn't exists, or is empty, or the user has told us to regenerate the files

                log.debug("Creating directory %s" % output_directory)
                os.makedirs(output_directory, exist_ok=True)

                log.info("Generating intermediate files for %s in directory %s" % (directory, output_directory))
                formatter: CommonOutputFormatter = CommonOutputFormatter(
                    archive_directory=directory, filename_root=self._options.results_file_root
                )

                try:
                    formatter.convert_all_files()
                    formatter.write_output_file()
                except Exception as e:
                    log.error(
                        "Encountered an error parsing results in directory %s with name %s"
                        % (directory, self._options.results_file_root)
                    )
                    log.exception(e)
                    raise e
