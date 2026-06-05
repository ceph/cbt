"""
A file containing the classes and code required to read a file stored in the common
intermediate format introduced in PR 319 (https://github.com/ceph/cbt/pull/319) and
produce a hockey-stick curve graph
"""

from pathlib import Path

import matplotlib.pyplot as plotter
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from post_processing.common import (
    DATA_FILE_EXTENSION_WITH_DOT,
    PLOT_FILE_EXTENSION,
    read_intermediate_file,
)
from post_processing.plotter.common_format_plotter import CommonFormatPlotter
from post_processing.post_processing_types import CommonFormatDataType


# pylint: disable=too-few-public-methods
class SimplePlotter(CommonFormatPlotter):
    """
    Read the intermediate data file in the common json format and produce a hockey-stick
    curve plot that includes standard deviation error bars.
    """

    def __init__(self, archive_directory: str, plot_error_bars: bool, plot_resources: bool) -> None:
        # Archive directory is the root directory for the test run
        self._archive_directory: Path = Path(archive_directory)
        # SVG files are saved to top-level visualisation directory for easy access by report generator
        self._svg_output_path: Path = self._archive_directory / "visualisation"
        self._svg_output_path.mkdir(parents=True, exist_ok=True)
        self._plot_error_bars: bool = plot_error_bars
        self._plot_resources: bool = plot_resources
        super().__init__(plotter)

    def draw_and_save(self) -> None:
        # Search recursively for JSON data files in all visualisation directories
        # This supports both legacy (archive/visualisation/) and new (archive/operation/visualisation/) structures
        json_files = list(self._archive_directory.glob(f"**/visualisation/*{DATA_FILE_EXTENSION_WITH_DOT}"))

        for file_path in json_files:
            # Skip timeseries JSON files (they have their own plotter)
            if "_timeseries" in file_path.stem:
                continue

            file_data: CommonFormatDataType = read_intermediate_file(f"{file_path}")
            output_file_path: str = self._generate_output_file_name(files=[file_path])

            figure: Figure
            io_axis: Axes
            figure, io_axis = self._plotter.subplots()

            self._add_single_file_data_with_optional_errorbars(
                file_data=file_data,
                main_axes=io_axis,
                plot_error_bars=self._plot_error_bars,
                plot_resource_usage=self._plot_resources,
            )
            self._add_title(source_files=[file_path])
            self._set_axis()

            # make sure we add the legend to the plot
            figure.legend(  # pyright: ignore[reportUnknownMemberType]
                bbox_to_anchor=(0.5, -0.1),
                loc="upper center",
                ncol=2,
            )

            self._save_plot(file_path=output_file_path)
            self._clear_plot()

    def _generate_output_file_name(self, files: list[Path]) -> str:
        """
        Generate output filename for SVG plot.

        SVG files are saved to the top-level visualisation directory
        (archive_directory/visualisation/) regardless of where the JSON
        data file is located. This ensures all plots are in one place
        for the report generator to find them.
        """
        # Extract just the filename without path and extension
        json_filename = files[0].stem  # e.g., "4k_1_randread"
        svg_filename = f"{json_filename}.{PLOT_FILE_EXTENSION}"
        # Save to top-level visualisation directory
        return str(self._svg_output_path / svg_filename)
