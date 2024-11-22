"""
A file containing the classes and code required to read a file stored in the common
intermediate format introduced in PR 319 (https://github.com/ceph/cbt/pull/319) and
produce a hockey-stick curve graph
"""

from pathlib import Path

import matplotlib.pyplot as plotter

from plotter.common_format_plotter import CommonFormatPlotter, common_format_data_type


class SimplePlotter(CommonFormatPlotter):
    """
    Read the intermediate data file in the common json format and produce a hockey-stick
    curve plot that includes standard deviation error bars.
    """

    def __init__(self, archive_directory: str) -> None:
        # A Path object for the directory where the data files are stored
        self._path: Path = Path(f"{archive_directory}/visualisation")

    def draw_and_save(self) -> None:
        for file_path in self._path.glob("*.json"):
            file_data: common_format_data_type = self._read_intermediate_file(f"{file_path}")
            output_file_path: str = self._generate_output_file_name(files=[file_path])
            self._add_single_file_data_with_errorbars(plotter=plotter, file_data=file_data)
            self._add_title(plotter=plotter, source_files=[file_path])
            self._set_axis(plotter=plotter)
            self._save_plot(plotter=plotter, file_path=output_file_path)
            self._clear_plot(plotter=plotter)

    def _generate_output_file_name(self, files: list[Path]) -> str:
        # we know we will only ever be passed a single file name
        return f"{str(files[0])[:-4]}png"
