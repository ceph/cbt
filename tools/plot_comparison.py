#!/usr/bin/env -S python3 -B

"""
Draws a plot on a single set of axes of data from the two or more files specified
by --files, or all the common files between the directories specified by
--directories.
The resulting plot(s) are saved in the specified output directory.
The files must be in the correct json format, as described by CBT PR 319 
(https://github.com/ceph/cbt/pull/319)

Optionally a label for each file can be specified which is used to generate the
legend for the plot.

Usage:
        plot_comparison.py  --files=<comma_separated_list_of_files_to_compare>
                            --directories=<comma_separated_list_of_directories_to_compare>
                            --output_directory=<full_path_to_directory_to_store_plot>
                            --labels="<comma_separated_list_of_labels>

    Both files must be in the intermediate format generated from
    fio_common_output_wrapper.py


Input:
        --output_directory  [Required] The directory to write the comparison plot
                                to. If this does not exists it will be created.
                                
        --files         [Optional] A comma separated list of the full path to
                            the files, in the correct format, to plot on a
                            single set of axes. 

        --directories   [Optional] A comma separated list of directories that
                            contain a set of files, in the correct format, to
                            plot. Any files that have a common name in all the
                            directories will be plotted on a single set of axes.


            One of --files or --directories must be provided

        --labels        [Optional] The labels to use on the comparison plot for
                            the data from the corresponding file in --files. 
                            The labels are applied to the files in the order
                            they are given, so the first label would be applied
                            to the data from the first file.
        
Examples:

    Plot the results from two files, /tmp/ch_cbt_main_run/16384B_randread.json and
    /tmp/ch_cbt_sb_run/16384B_randread.json on the same axes, applying label
    'Main 2024/12/18' to the data from /tmp/ch_cbt_main_run/16384B_randread.json
    and 'ch_wip_graphing sandbox' to the data from /tmp/ch_cbt_sb_run/16384B_randread.json
         
        plot_comparison.py  --files="/tmp/ch_cbt_main_run/16384B_randread.json," \
                                    "/tmp/ch_cbt_sb_run/16384B_randread.json" \
                            --output_directory="/tmp/comparisons" \
                            --labels="Main 2024/12/18,ch_wip_graphing sandbox"

    Plot a comparison on a single axes for each common file in directories 
    /tmp/ch_cbt_main_run/visualisation and /tmp/ch_cbt_sb_run/visualisation.
    The labels used for each directory will be ch_cbt_main_run and ch_cbt_main_run

        plot_comparison.py  --directories="/tmp/ch_cbt_main_run,/tmp/ch_cbt_main_run"
                            --output_directory="/tmp/main_sb_comparisons"

    Plot a comparison between two files on the same axes, using the default
    labels

        plot_comparison.py  --files="/tmp/ch_cbt_main_run/16384B_randread.json," \
                                    "/tmp/ch_cbt_sb_run/4096B_randread.json" \
                            --output_directory="/tmp/bs_comparisons"
"""

import os
import subprocess
from argparse import ArgumentParser, Namespace
from logging import INFO, Logger, basicConfig, getLogger

from post_processing.plotter.common_format_plotter import CommonFormatPlotter
from post_processing.plotter.directory_comparison_plotter import DirectoryComparisonPlotter
from post_processing.plotter.file_comparison_plotter import FileComparisonPlotter

log: Logger = getLogger(f"{os.path.basename(__file__)}")


def main() -> int:
    """
    Main routine for the script
    """

    result: int = 0

    description: str = "Draws a plot on a single set of axes of data from the two or more files specified\n"
    description += "by --files, or all the common files between the directories specified by\n"
    description += "--directories.\n\n"
    description += "The resulting plot(s) are saved in the specified output directory.\n"
    description += "The files must be in the correct json format, as described by CBT PR 319\n"
    description += "(https://github.com/ceph/cbt/pull/319)"

    parser: ArgumentParser = ArgumentParser(description=description)

    parser.add_argument(
        "--output_directory",
        type=str,
        required=True,
        help="The directory to store the comparison plot file(s)",
    )
    parser.add_argument(
        "--files",
        type=str,
        required=False,
        default=None,
        help="A comma separated list of two or more file paths to compare",
    )
    parser.add_argument(
        "--directories",
        type=str,
        required=False,
        help="A comma separated list of two or more directories containing the files to compare",
    )
    parser.add_argument(
        "--labels",
        type=str,
        required=False,
        default="",
        help="A comma separated list of labels to use for the plots.\nMust be in the same order as the files in --files"
        + "Not used with the --direectories option",
    )

    arguments: Namespace = parser.parse_args()

    check_arguments(arguments)
    # will only create the directory if it does not already exist
    subprocess.run(f"mkdir -p -m0755 {arguments.output_directory}", shell=True)

    plotter: CommonFormatPlotter

    if arguments.files:
        comparison_files: list[str] = arguments.files.split(",")

        plotter = FileComparisonPlotter(arguments.output_directory, comparison_files)

        if arguments.labels:
            labels: list[str] = arguments.labels.split(",")
            plotter.set_labels(labels)

    if arguments.directories:
        comparison_directories: list[str] = arguments.directories.split(",")

        plotter = DirectoryComparisonPlotter(arguments.output_directory, comparison_directories)

    assert isinstance(plotter, CommonFormatPlotter)  # pyright: ignore[reportPossiblyUnboundVariable]
    try:
        plotter.draw_and_save()
    except Exception:
        log.exception("Encountered an error plotting results")
        result = 1

    return result


def check_arguments(arguments: Namespace) -> None:
    """
    Validate that the correct arguments have been passed to the script
    """
    if arguments.files and arguments.directories:
        log.error("Both --files and --directories has been specified")
        exit(1)

    if arguments.labels and not arguments.files:
        log.error("--labels has been specified without --files")
        exit(1)


def initialise_logging() -> None:
    """
    Set up all the logging for the
    """
    basicConfig(level=INFO, format="%(name)-20s: %(levelname)-8s %(message)s")


if __name__ == "__main__":
    initialise_logging()
    main()
