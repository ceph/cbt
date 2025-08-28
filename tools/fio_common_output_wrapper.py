#!/usr/bin/env python3

"""
Usage:
        fio_common_output_wrapper.py --archive=<archive_dir>
                                     --results_file_root=<file_root>

Input:
        --archive               [Required]  The achive directory that contains the
                                    results filed from an fio run of cbt

        --results_file_root     [Optional]  The base name for the json output files
                                    produced from an fio run in cbt.
                                    Default: "json_output"

Examples:
            fio_common_output_wrapper.py --archive="/tmp/ch_cbt_run"

            fio_common_output_wrapper.py --archive="/tmp/ch_cbt_run" --results_file_root="ch_json_result"
"""

import os
from argparse import ArgumentParser, Namespace
from logging import Logger, getLogger

from post_processing.formatter.common_output_formatter import CommonOutputFormatter
from post_processing.log_configuration import setup_logging

setup_logging()

log: Logger = getLogger("formatter")


def main() -> int:
    """
    Main routine for the script
    """

    result: int = 0

    parser: ArgumentParser = ArgumentParser(description="Parse cbt json output into a common format")
    parser.add_argument("--archive", type=str, required=True, help="The archive directory used for the CBT results")
    parser.add_argument(
        "--results_file_root",
        type=str,
        required=False,
        default="json_output",
        help="The filename root of all the CBT output json files",
    )

    args: Namespace = parser.parse_args()

    output_directory: str = f"{args.archive}/visualisation/"
    log.debug("Creating directory %s" % output_directory)
    os.makedirs(output_directory, exist_ok=True)

    log.info("Generating intermediate files for %s in directory %s" % (args.archive, output_directory))
    formatter: CommonOutputFormatter = CommonOutputFormatter(
        archive_directory=args.archive, filename_root=args.results_file_root
    )

    try:
        formatter.convert_all_files()
        formatter.write_output_file()
    except Exception as e:
        log.error(
            "Encountered an error parsing results in directory %s with name %s" % (args.archive, args.results_file_root)
        )
        log.exception(e)
        result = 1

    return result


if __name__ == "__main__":
    main()
