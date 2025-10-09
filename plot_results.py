"""
A file containing the classes and code required to read a file stored in the common
intermediate format introduced in PR 319 (https://github.com/ceph/cbt/pull/319) and produce a hockey-stick curve graph
"""

import json
from logging import Logger, getLogger
from pathlib import Path
from typing import Dict, List, Union

import matplotlib.pyplot as plotter

from post_processing.common import PLOT_FILE_EXTENSION

log: Logger = getLogger("cbt")


class PlotResults:
    """
    Read the intermediate data file in the common json format and produce a hockey-stick
    curve plot that includes standard deviation error bars.
    """

    # A converted between the operation type in the intermediate file format
    # and a human-readable string that can be used in the title for the plot.
    TITLE_CONVERSION: Dict[str, str] = {
        "read": "Sequential Read",
        "write": "Sequential Write",
        "randread": "Random Read",
        "randwrite": "Random Write",
        "readwrite": "Sequential Read/Write",
        "randrw": "Random Read/Write",
    }

    def __init__(self, archive_directory: str) -> None:
        self._data_directory: str = f"{archive_directory}/visualisation"

        self._path: Path = Path(self._data_directory)

    def draw_and_save(self) -> None:
        """
        Produce the plot files for each of the intermediate data files in the given directory.
        """

        for file_path in self._path.glob("*.json"):
            file_data: Dict[str, Union[str, Dict[str, str]]] = self._read_intermediate_file(f"{file_path}")
            output_file: str = f"{str(file_path)[:-4]}{PLOT_FILE_EXTENSION}"
            plot_title: str = self._generate_plot_title(file_path.parts[-1])

            keys: List[str] = [key for key in file_data.keys() if isinstance(file_data[key], dict)]
            plot_data: Dict[str, Dict[str, str]] = {}
            sorted_plot_data: Dict[str, Dict[str, str]] = {}
            for key, data in file_data.items():
                if isinstance(data, dict):
                    plot_data[key] = data

            sorted_keys: List[str] = sorted(keys, key=int)
            for key in sorted_keys:
                sorted_plot_data[key] = plot_data[key]

            x_axis: List[Union[int, float]] = []
            y_axis: List[Union[int, float]] = []
            error_bars: List[float] = []

            log.info("converting file %s", f"{file_path}")

            for _, data in sorted_plot_data.items():
                # for blocksize less than 64K we want to use the bandwidth to plot the graphs,
                # otherwise we should use iops.
                blocksize: int = int(int(data["blocksize"]) / 1024)
                if blocksize < 64:
                    # convert bytes to Mb, not Mib, so use 1000s rather than 1024
                    x_axis.append(float(data["bandwidth_bytes"]) / (1000 * 1000))
                    plotter.xlabel("Bandwidth (MB)")  # pyright: ignore[reportUnknownMemberType]
                else:
                    x_axis.append(float(data["iops"]))
                    plotter.xlabel("IOps")  # pyright: ignore[reportUnknownMemberType]
                    # The stored values are in ns, we want to convert to ms
                y_axis.append(float(data["latency"]) / (1000 * 1000))
                plotter.ylabel("Latency (ms)")  # pyright: ignore[reportUnknownMemberType]
                error_bars.append(float(data["std_deviation"]) / (1000 * 1000))

                plotter.title(plot_title)  # pyright: ignore[reportUnknownMemberType]
                plotter.errorbar(x_axis, y_axis, error_bars, capsize=3, ecolor="red")  # pyright: ignore[reportUnknownMemberType]
                plotter.savefig(output_file, format=f"{PLOT_FILE_EXTENSION}")  # pyright: ignore[reportUnknownMemberType]
                # Now we have saved the file, clear the plot for the next file
                plotter.clf()

    def _read_intermediate_file(self, file_path: str) -> Dict[str, Union[str, Dict[str, str]]]:
        """
        Read the json data from the intermediate file and store it for processing.
        """
        data: Dict[str, Union[str, Dict[str, str]]] = {}
        # We know the file encoding as we wrote it ourselves as part of
        # common_output_format.py, so it is safe to specify here
        with open(f"{file_path}", "r", encoding="utf8") as file_data:
            data = json.load(file_data)

        return data

    def _generate_plot_title(self, source_file: str) -> str:
        """
        Given the Path object for the input file, generate the title for the
        data plot
        """
        # Strip the .json from the file name as we don't need it
        title_with_underscores: str = f"{source_file[:-5]}"
        parts: List[str] = title_with_underscores.split("_")

        # The filename is in one of 2 formats:
        #    BLOCKSIZE_OPERATION.json
        #    BLOCKSIZE_READ_WRITE_OPERATION.json
        #
        # The split on _ will mean that the last element [-1] will always be
        # the operation, and the first part [0] will be the blocksize
        title: str = f"{int(int(parts[0][:-1]) / 1024)}K "
        if len(parts) > 2:
            title += f"{parts[1]}/{parts[2]} "

        title += f"{self.TITLE_CONVERSION[parts[-1]]}"
        return title
