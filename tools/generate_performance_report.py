#!/usr/bin/env -S python3 -B
"""
A script to automatically generate a report from a set of performance run data
in the common intermediate format described in CBT PR 319.
The archive should contain the 'visualisation' sub directory where all
the .json and plot files reside.

tools/fio_common_output_wrapper.py will generate the .json files and the SimplePlotter
module in CBT PR 321 can be used to generate the plot files

Usage:
        generate_performance_report.py  --archive=<full_path_to_results_directory>
                                        --output_directory=<full_path_to_directory_to_store_report>
                                        --results_file_root="ch_json_result"
                                        --create_pdf
                                        --no_error_bars
                                        --force_refresh


Input:
        --output_directory      [Required] The directory to write the comparison plot
                                    to. If this does not exists it will be created.

        --archive               [Required] The directory that contains the common
                                    format .json files and plot files to include
                                    in the report.

        --results_file_root     [Optional]  The base name for the json output files
                                    produced from an fio run in cbt.
                                    Default: "json_output"

        --create_pdf            [Optional] Create a pdf file of the report markdown
                                    file.
                                    This requires pandoc to be installed,
                                    and be on the path.

        --no_error_bars         [Optional] Do not draw error bars on the plots
                                    included in the report

        --force_refresh         [Optional] Generate the intermediate and plot files
                                    from the raw data, even if they already exist

Examples:

    Generate a markdown report file for the results in '/tmp/squid_main' directory
    and save it in the '/tmp/main_results' directory:

    generate_performance_report.py  --archive=/tmp/squid_main
                                    --output_directory =/tmp/main_results

    Additionally generate a pdf report file for the example above:

    generate_performance_report.py  --archive=/tmp/squid_main
                                    --output_directory =/tmp/main_results
                                    --create_pdf
"""

from argparse import ArgumentParser
from logging import Logger, getLogger

from post_processing.log_configuration import setup_logging
from post_processing.report import Report, ReportOptions, parse_namespace_to_options

setup_logging()

log: Logger = getLogger("reports")


def main() -> int:
    """
    Main routine for the script
    """

    description: str = "Produces a performance report in markdown format \n"
    description += "from the json and png files stored in the visualisation\n"
    description += "subdirectory of the directory given by --archive\n"
    description += "The resulting report(s) are saved in the specified output directory.\n"
    description += "The json files must be in the correct format, as described by CBT PR 319\n"
    description += "(https://github.com/ceph/cbt/pull/319)"

    parser: ArgumentParser = ArgumentParser(description=description)

    parser.add_argument(
        "--output_directory",
        type=str,
        required=True,
        help="The directory to store the comparison plot file(s)",
    )
    parser.add_argument(
        "--archive",
        type=str,
        required=False,
        help="The directory that contains the set of json results files and generated plot files"
        + "for a particular test run",
    )
    parser.add_argument(
        "--create_pdf",
        action="store_true",
        help="Generate a pdf report file in addition to the markdown report",
    )
    parser.add_argument(
        "--results_file_root",
        type=str,
        required=False,
        default="json_output",
        help="The filename root of all the CBT output json files",
    )

    parser.add_argument(
        "--no_error_bars",
        action="store_true",
        required=False,
        help="Do not generate error bars for the plots",
    )

    parser.add_argument(
        "--force_refresh",
        action="store_true",
        required=False,
        help="Regenerate the intermediate files and plots, even if they exist",
    )

    report_options: ReportOptions = parse_namespace_to_options(arguments=parser.parse_args())

    report: Report = Report(report_options)
    try:
        report.generate()

    except Exception:
        log.exception("FAILED: Encountered an error plotting results")

    return report.result_code


if __name__ == "__main__":
    main()
