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
                                        --create_pdf


Input:
        --output_directory  [Required] The directory to write the comparison plot
                                    to. If this does not exists it will be created.

        --archive           [Required] The directory that contains the common
                                format .json files and plot files to include
                                in the report.

        --create_pdf        [Optional] Create a pdf file of the report markdown
                                file.
                                This requires pandoc to be installed,
                                and be on the path.

Examples:

    Generate a markdown report file for the results in '/tmp/squid_main' directory
    ans sabve it in the '/tmp/main_results' directory:

    generate_performance_report.py  --archive=/tmp/squid_main
                                    --output_directory =/tmp/main_results

    Additionally generate a pdf report file for the example above:

    generate_performance_report.py  --archive=/tmp/squid_main
                                    --output_directory =/tmp/main_results
                                    --create_pdf
"""

import os
import subprocess
from argparse import ArgumentParser, Namespace
from logging import INFO, Logger, basicConfig, getLogger

from post_processing.reports.simple_report_generator import SimpleReportGenerator

log: Logger = getLogger(f"{os.path.basename(__file__)}")


def main() -> int:
    """
    Main routine for the script
    """

    result: int = 0

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

    arguments: Namespace = parser.parse_args()

    # will only create the output directory if it does not already exist
    subprocess.run(f"mkdir -p -m0755 {arguments.output_directory}", shell=True)

    report_generator = SimpleReportGenerator(
        archive_directories=arguments.archive, output_directory=arguments.output_directory
    )

    try:
        report_generator.create_report()

        if arguments.create_pdf:
            report_generator.save_as_pdf()

    except Exception:
        log.exception("Encountered an error plotting results")
        result = 1

    return result


def initialise_logging() -> None:
    """
    Set up the logging for the sub-modules
    """
    basicConfig(level=INFO, format="%(name)-20s: %(levelname)-8s %(message)s")


if __name__ == "__main__":
    initialise_logging()
    main()
