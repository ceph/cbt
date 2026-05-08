"""
The base class that reads a results file and converts it into the
common data format that can be plotted
"""

import re
from logging import Logger, getLogger
from pathlib import Path
from typing import Union

from post_processing.common import sum_mean_values, sum_standard_deviation_values
from post_processing.post_processing_types import IodepthDataType

# from post_processing.run_results.benchmarks.benchmark_result import BenchmarkResult
# To be removed when factory is ready
from post_processing.run_results.run_result import RunResult

# from post_processing.run_results.resources.resource_result import ResourceResult

log: Logger = getLogger(name="formatter")


class RBDFIO(RunResult):
    """
    Processes RBD FIO benchmark results and converts them to the common intermediate format.

    This class handles reading FIO JSON output files from RBD (RADOS Block Device)
    benchmark runs and aggregating results across multiple volumes.
    """

    @property
    def type(self) -> str:
        return "rbdfio"

    def _find_files_for_testrun(self, file_name_root: str) -> list[Path]:
        """
        Find all result files for a particular test run matching the file name pattern.

        Args:
            file_name_root: The base filename to search for (e.g., "json_output")

        Returns:
            List of Path objects for files matching the pattern <file_name_root>.<digit>
        """
        # We need to use a list here as we can possibly iterate over the file
        # list multiple times, and a Generator object only allows iterating
        # once
        return [
            path
            for path in self._path.glob(pattern=f"**/{file_name_root}.*")
            if re.search(rf"{file_name_root}.\d+$", f"{path}")
        ]

    def _sum_io_details(
        self, existing_values: Union[str, IodepthDataType], new_values: IodepthDataType
    ) -> IodepthDataType:
        """
        Aggregate IO statistics from multiple volumes by summing values and computing
        weighted averages for latency and standard deviation.

        Args:
            existing_values: Previously aggregated IO statistics
            new_values: New IO statistics to add to the aggregate

        Returns:
            Combined IO statistics with properly weighted latency and standard deviation
        """
        assert isinstance(existing_values, dict)
        combined_data: IodepthDataType = {}

        simple_sum_values: list[str] = ["io_bytes", "iops", "bandwidth_bytes"]

        for value in simple_sum_values:
            combined_data[value] = f"{float(existing_values[value]) + float(new_values[value])}"

        combined_data["total_ios"] = f"{int(existing_values['total_ios']) + int(new_values['total_ios'])}"

        latencies: list[float] = [float(existing_values["latency"]), float(new_values["latency"])]
        operations: list[int] = [int(existing_values["total_ios"]), int(new_values["total_ios"])]
        std_deviations: list[float] = [float(existing_values["std_deviation"]), float(new_values["std_deviation"])]

        combined_latency: float = sum_mean_values(
            latencies,
            num_ops=operations,
            total_ios=int(combined_data["total_ios"]),
        )

        combined_std_dev: float = sum_standard_deviation_values(
            std_deviations, operations, latencies, int(combined_data["total_ios"]), combined_latency
        )

        combined_data["latency"] = f"{combined_latency}"
        combined_data["std_deviation"] = f"{combined_std_dev}"

        return combined_data
