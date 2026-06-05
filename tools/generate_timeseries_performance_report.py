#!/usr/bin/env -S python3 -B
"""
A script to automatically generate a time-series report from a set of performance run data
in the common time-series format.

The archive should contain the 'visualisation' sub directory where all
the time-series .json and plot files reside.

tools/fio_common_output_wrapper.py will generate the .json files and the TimeSeriesPlotter
module can be used to generate the plot files

Usage:
        generate_timeseries_performance_report.py  --archive=<full_path_to_results_directory>
                                        --output_directory=<full_path_to_directory_to_store_report>
                                        --create_pdf
                                        --force_refresh
                                        --plot_resources


Input:
        --output_directory      [Required] The directory to write the time-series report
                                    to. If this does not exists it will be created.

        --archive               [Required] The directory that contains the common
                                    time-series format .json files and plot files to include
                                    in the report.

        --create_pdf            [Optional] Create a pdf file of the report markdown
                                    file.
                                    This requires pandoc to be installed,
                                    and be on the path.

        --force_refresh         [Optional] Generate the intermediate and plot files
                                    from the raw data, even if they already exist

        --plot_resources        [Optional] Also draw CPU and memory usage, as recorded
                                    by fio on the plots

Examples:

    Generate a markdown time-series report file for the results in '/tmp/squid_main' directory
    and save it in the '/tmp/main_results' directory:

    generate_timeseries_performance_report.py  --archive=/tmp/squid_main
                                    --output_directory=/tmp/main_results

    Additionally generate a pdf report file for the example above:

    generate_timeseries_performance_report.py  --archive=/tmp/squid_main
                                    --output_directory=/tmp/main_results
                                    --create_pdf
"""

from argparse import SUPPRESS, ArgumentParser
from logging import Logger, getLogger

from post_processing.log_configuration import setup_logging
from post_processing.post_processing_types import ReportOptions
from post_processing.report import Report, parse_namespace_to_options

setup_logging()
log: Logger = getLogger("reports")
log.info("=== Starting Post Processing of CBT results ===")


def main() -> int:
    """
    Main routine for the script
    """

    description: str = "Produces a time-series performance report in markdown format \n"
    description += "from the time-series json and svg files stored in the visualisation\n"
    description += "subdirectory of the directory given by --archive\n"
    description += "The resulting report(s) are saved in the specified output directory.\n"
    description += "The json files must be in the time-series format with *_timeseries.json naming"

    parser: ArgumentParser = ArgumentParser(description=description)

    parser.add_argument(
        "--output_directory",
        type=str,
        required=True,
        help="The directory to store the time-series report file(s)",
    )
    parser.add_argument(
        "--archive",
        type=str,
        required=True,
        help="The directory that contains the set of time-series json results files and generated plot files"
        + "for a particular test run",
    )
    parser.add_argument(
        "--create_pdf",
        action="store_true",
        help="Generate a pdf report file in addition to the markdown report",
    )

    parser.add_argument(
        "--force_refresh",
        action="store_true",
        required=False,
        help="Regenerate the intermediate files and plots, even if they exist",
    )

    # timeseries reports do not currently produce resource statistics
    parser.add_argument(
        "--plot_resources",
        action="store_true",
        required=False,
        help=SUPPRESS,
    )

    # the following argument is required to exist for future processing, but is not needed for timeseries reports
    parser.add_argument("--results_file_root", type=str, required=False, default="json_output", help=SUPPRESS)

    report_options: ReportOptions = parse_namespace_to_options(arguments=parser.parse_args(), timeseries_report=True)

    report: Report = Report(options=report_options)

    try:
        report.generate(throw_exception=True)
    except Exception:
        log.exception("FAILED: Encountered an error generating the time-series report")

    return report.result_code


if __name__ == "__main__":
    main()

# Made with Bob
