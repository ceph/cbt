"""
A file containing the classes and code required to read two files stored in the common
intermediate format introduced in CBT PR #319 (https://github.com/ceph/cbt/pull/319)
and produce a plot of both the files on the same axes.
"""

from logging import Logger, getLogger
from pathlib import Path

import matplotlib.pyplot as plotter
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from post_processing.common import (
    PLOT_FILE_EXTENSION_WITH_DOT,
    find_common_data_file_names,
    find_hockey_stick_visualisation_directories,
    read_intermediate_file,
)
from post_processing.plotter.common_format_plotter import CommonFormatPlotter
from post_processing.post_processing_types import CommonFormatDataType

log: Logger = getLogger("plotter")


# pylint: disable=[too-few-public-methods]
class DirectoryComparisonPlotter(CommonFormatPlotter):
    """
    Read the intermediate data files in the common json format and produce a
    curve plot of both sets of data on the same axes. Error bars are not included
    as they seem to make the plot harder to read and compare.
    """

    def __init__(self, output_directory: str, directories: list[str]) -> None:
        self._output_directory: str = f"{output_directory}"
        # Store the archive directories - we'll find visualisation dirs per operation
        self._archive_directories: list[Path] = [Path(d) for d in directories]
        super().__init__(plotter)

    def draw_and_save(self) -> None:
        # For each archive directory, find all visualisation directories
        # Group them by operation type (e.g., all 'randread/visualisation' together)
        operation_vis_dirs: dict[str, list[Path]] = {}

        for archive_dir in self._archive_directories:
            vis_dirs = find_hockey_stick_visualisation_directories(archive_dir)
            for vis_dir in vis_dirs:
                # Extract operation name (parent directory of visualisation)
                operation = vis_dir.parent.name if vis_dir.parent != archive_dir else "legacy"
                if operation not in operation_vis_dirs:
                    operation_vis_dirs[operation] = []
                operation_vis_dirs[operation].append(vis_dir)

        # For each operation type, find common files and create comparison plots
        for operation, vis_directories in operation_vis_dirs.items():
            # Only create plots if we have data from multiple archives for this operation
            if len(vis_directories) < len(self._archive_directories):
                log.warning(
                    "Skipping operation '%s' - not present in all archives (found in %d of %d)",
                    operation,
                    len(vis_directories),
                    len(self._archive_directories),
                )
                continue

            # Find files common to all archives for this operation
            common_file_names: list[str] = find_common_data_file_names(vis_directories)

            if not common_file_names:
                log.warning("No common files found for operation '%s'", operation)
                continue

            for file_name in common_file_names:
                output_file_path: str = self._generate_output_file_name(files=[Path(file_name)])

                figure: Figure
                io_axis: Axes
                figure, io_axis = self._plotter.subplots()

                for vis_dir in vis_directories:
                    file_path = vis_dir / file_name
                    if not file_path.exists():
                        continue

                    file_data: CommonFormatDataType = read_intermediate_file(f"{file_path}")
                    # Use the archive directory name (2 levels up from visualisation) for the label
                    archive_name = (
                        vis_dir.parent.parent.name if vis_dir.parent.name != "visualisation" else vis_dir.parent.name
                    )
                    self._add_single_file_data_with_optional_errorbars(
                        file_data=file_data,
                        main_axes=io_axis,
                        label=archive_name,
                        plot_error_bars=False,
                        plot_resource_usage=False,
                    )

                # make sure we add the legend to the plot
                figure.legend(  # pyright: ignore[reportUnknownMemberType, reportPossiblyUnboundVariable]
                    bbox_to_anchor=(0.5, -0.1), loc="upper center", ncol=2
                )

                self._add_title(source_files=[Path(file_name)])
                self._set_axis()

                self._save_plot(file_path=output_file_path)
                self._clear_plot()

    def _generate_output_file_name(self, files: list[Path]) -> str:
        # we know we will only ever be passed a single file name
        output_file: str = f"{self._output_directory}/Comparison_{files[0].stem}{PLOT_FILE_EXTENSION_WITH_DOT}"

        return output_file
