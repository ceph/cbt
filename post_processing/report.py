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
from pathlib import Path

from post_processing.formatter.base_formatter import BaseFormatter
from post_processing.formatter.common_output_formatter import CommonOutputFormatter
from post_processing.formatter.time_series_output_formatter import (
    TimeSeriesOutputFormatter,
)
from post_processing.log_configuration import setup_logging
from post_processing.post_processing_types import ReportOptions, ReportType
from post_processing.reports.comparison_report_generator import ComparisonReportGenerator
from post_processing.reports.report_generator import ReportGenerator
from post_processing.reports.simple_report_generator import SimpleReportGenerator
from post_processing.reports.time_series_report_generator import (
    TimeSeriesReportGenerator,
)

setup_logging()
log: Logger = getLogger(name="reports")


def parse_namespace_to_options(
    arguments: Namespace,
    comparison_report: bool = False,
    timeseries_report: bool = False,
) -> ReportOptions:
    """
    Parse a namespace as used by argparse into our internal NamedTuple representation
    """
    no_error_bars: bool = False
    plot_resources: bool = False
    archives: list[str] = []
    output_directory: str = arguments.output_directory

    # Determine report type
    if timeseries_report:
        report_type = ReportType.TIMESERIES
        archives.append(arguments.archive)
    elif comparison_report:
        report_type = ReportType.COMPARISON
        archives.append(arguments.baseline)
        for directory in arguments.archives.split(","):
            archives.append(directory)
    else:
        report_type = ReportType.SIMPLE
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
        report_type=report_type,
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

            if self._options.report_type == ReportType.COMPARISON:
                report_generator = ComparisonReportGenerator(
                    archive_directories=self._options.archives,
                    output_directory=self._options.output_directory,
                    force_refresh=self._options.force_refresh,
                )
            elif self._options.report_type == ReportType.TIMESERIES:
                report_generator = TimeSeriesReportGenerator(
                    archive_directories=self._options.archives,
                    output_directory=self._options.output_directory,
                    force_refresh=self._options.force_refresh,
                    plot_resources=self._options.plot_resources,
                )
            else:  # ReportType.SIMPLE
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

    def _archive_has_intermediate_files(self, directory: str) -> bool:
        """
        Check whether an archive already contains the intermediate files needed
        for the selected report type.

        For time-series reports, intermediate files are expected in the new
        nested operation/visualisation/ structure with *_timeseries.json names.

        For simple/comparison reports, accept both:
        - new nested operation/visualisation/*.json layout
        - legacy top-level visualisation/*.json layout

        For non-time-series reports, *_timeseries.json files do not count as
        hockey-stick intermediate data.
        """
        archive_path = Path(directory)

        if self._options.report_type == ReportType.TIMESERIES:
            return any(archive_path.glob("**/visualisation/*_timeseries.json"))

        return any(
            not file_path.name.endswith("_timeseries.json")
            for file_path in archive_path.glob("**/visualisation/*.json")
        )

    def _generate_intermediate_files(self) -> None:
        """
        If the raw fio results have not yet been post-processed then we need to do
        that now before trying to produce the report
        """

        for directory in self._options.archives:
            if not self._archive_has_intermediate_files(directory) or self._options.force_refresh:
                log.debug("Preparing to generate intermediate files for %s", directory)
                os.makedirs(name=f"{directory}/visualisation/", exist_ok=True)

                log.info("Generating intermediate files for %s", directory)

                # Use the appropriate formatter based on report type
                formatter: BaseFormatter
                if self._options.report_type == ReportType.TIMESERIES:
                    formatter = TimeSeriesOutputFormatter(
                        archive_directory=directory, filename_root=self._options.results_file_root
                    )
                else:
                    formatter = CommonOutputFormatter(
                        archive_directory=directory, filename_root=self._options.results_file_root
                    )

                try:
                    # With memory-efficient approach, data is written during process()
                    formatter.process()
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
