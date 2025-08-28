"""
A file containing the classes and code required to read two files stored in the common
intermediate format introduced in CBT PR #319 (https://github.com/ceph/cbt/pull/319)
and produce a plot of both the files on the same axes.
"""

from logging import Logger, getLogger
from pathlib import Path

import matplotlib.pyplot as plotter

from post_processing.common import (
    PLOT_FILE_EXTENSION_WITH_DOT,
    find_common_data_file_names,
    read_intermediate_file,
)
from post_processing.plotter.common_format_plotter import CommonFormatPlotter
from post_processing.types import COMMON_FORMAT_FILE_DATA_TYPE

log: Logger = getLogger("plotter")


class DirectoryComparisonPlotter(CommonFormatPlotter):
    """
    Read the intermediate data files in the common json format and produce a
    curve plot of both sets of data on the same axes. Error bars are not included
    as they seem to make the plot harder to read and compare.
    """

    def __init__(self, output_directory: str, directories: list[str]) -> None:
        self._output_directory: str = f"{output_directory}"
        self._comparison_directories: list[Path] = [Path(f"{directory}/visualisation") for directory in directories]

    def draw_and_save(self) -> None:
        # output_file_path: str = self._generate_output_file_name(files=self._comparison_directories)

        # We will only compare data for files with the same name, so find all
        # the file names that are common across all directories. Not sure this
        # is the right way though
        common_file_names: list[str] = find_common_data_file_names(self._comparison_directories)

        for file_name in common_file_names:
            output_file_path: str = self._generate_output_file_name(files=[Path(file_name)])
            for directory in self._comparison_directories:
                file_data: COMMON_FORMAT_FILE_DATA_TYPE = read_intermediate_file(f"{directory}/{file_name}")
                # we choose the last directory name for the label to apply to the data
                self._add_single_file_data(
                    plotter=plotter,
                    file_data=file_data,
                    label=f"{directory.parts[-2]}",
                )

            self._add_title(plotter=plotter, source_files=[Path(file_name)])
            self._set_axis(plotter=plotter)

            # make sure we add the legend to the plot
            plotter.legend(bbox_to_anchor=(0.5, -0.1), loc="upper center", ncol=2)  # pyright: ignore[reportUnknownMemberType]

            self._save_plot(plotter=plotter, file_path=output_file_path)
            self._clear_plot(plotter=plotter)

    def _generate_output_file_name(self, files: list[Path]) -> str:
        # we know we will only ever be passed a single file name
        output_file: str = f"{self._output_directory}/Comparison_{files[0].stem}{PLOT_FILE_EXTENSION_WITH_DOT}"

        return output_file
