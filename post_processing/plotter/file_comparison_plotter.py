"""
A file containing the classes and code required to read two files stored in the common
intermediate format introduced in CBT PR #319 (https://github.com/ceph/cbt/pull/319)
and produce a plot of both the files on the same axes.
"""

from logging import Logger, getLogger
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plotter

from post_processing.common import (
    DATA_FILE_EXTENSION_WITH_DOT,
    PLOT_FILE_EXTENSION_WITH_DOT,
    get_blocksize_percentage_operation_from_file_name,
    read_intermediate_file,
)
from post_processing.plotter.common_format_plotter import CommonFormatPlotter
from post_processing.types import CommonFormatDataType

log: Logger = getLogger("plotter")


class FileComparisonPlotter(CommonFormatPlotter):
    """
    Read the intermediate data files in the common json format and produce a
    curve plot of both sets of data on the same axes. Error bars are not included
    as they seem to make the plot harder to read and compare.
    """

    def __init__(self, output_directory: str, files: list[str]) -> None:
        self._output_directory: str = f"{output_directory}"
        self._comparison_files: list[Path] = [Path(file) for file in files]
        self._labels: Optional[list[str]] = None

    def draw_and_save(self) -> None:
        output_file_path: str = self._generate_output_file_name(files=self._comparison_files)

        for file_path in self._comparison_files:
            index: int = self._comparison_files.index(file_path)
            file_data: CommonFormatDataType = read_intermediate_file(f"{file_path}")

            operation_details: tuple[str, str, str] = get_blocksize_percentage_operation_from_file_name(
                file_name=file_path.stem
            )

            # If we have a label use it, otherwise set the label from the
            # filename. We can reliably do this as we create the file name when
            # we save the intermediate file.
            label: str = ""
            if self._labels is not None:
                label = self._labels[index]

            if label == "":
                label = " ".join(operation_details)

            self._add_single_file_data(plotter=plotter, file_data=file_data, label=label)

        # make sure we add the legend to the plot, below the chart
        plotter.legend(  # pyright: ignore[reportUnknownMemberType]
            bbox_to_anchor=(0.5, -0.1), loc="upper center", ncol=2
        )

        self._add_title(plotter=plotter, source_files=self._comparison_files)
        self._set_axis(plotter=plotter)
        self._save_plot(plotter=plotter, file_path=output_file_path)
        self._clear_plot(plotter=plotter)

    def set_labels(self, labels: list[str]) -> None:
        """
        Set the labels for the plot lines
        """
        self._labels = labels

    def _generate_output_file_name(self, files: list[Path]) -> str:
        output_file: str = f"{self._output_directory}/Comparison"

        for file_path in files:
            # get the actual file name - this will be the last part of the path
            file_name = file_path.parts[-1]
            # strip off the .json extension from each file
            file: str = file_name[: -len(DATA_FILE_EXTENSION_WITH_DOT)]

            output_file += f"_{file}"

        return f"{output_file}{PLOT_FILE_EXTENSION_WITH_DOT}"
