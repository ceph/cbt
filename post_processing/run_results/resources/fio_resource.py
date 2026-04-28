"""
Process the CPU statistics as provided by FIO
"""

from logging import Logger, getLogger
from pathlib import Path
from typing import Any

from post_processing.run_results.resources.resource_result import ResourceResult

log: Logger = getLogger("formatter")


class FIOResource(ResourceResult):
    """
    Processes resource usage statistics from FIO benchmark output.

    FIO includes CPU usage statistics in its JSON output, which this class
    extracts and formats for inclusion in the common intermediate format.
    """

    @property
    def source(self) -> str:
        return "fio"

    def _get_resource_output_file_from_file_path(self, file_path: Path) -> Path:
        """
        Get the path to the resource usage file.

        For FIO, resource usage details are stored in the same file as the
        benchmark results, so this simply returns the input path.

        Args:
            file_path: Path to the FIO output file

        Returns:
            The same path, as FIO stores resource data in the benchmark output file
        """
        return file_path

    def _parse(self, data: dict[str, Any]) -> None:
        """
        Extract CPU and memory usage from FIO output data.

        Combines system CPU and user CPU percentages to get total CPU usage.
        Memory usage is currently not extracted from FIO output.

        Args:
            data: Dictionary containing parsed FIO JSON output
        """
        memory_usage: float = 0.0
        cpu_usage: float = 0.0

        sys_cpu: float = float(f"{data['jobs'][0]['sys_cpu']}")
        user_cpu: float = float(f"{data['jobs'][0]['usr_cpu']}")
        cpu_usage = sys_cpu + user_cpu

        self._cpu = f"{cpu_usage:02f}"
        self._memory = f"{memory_usage:02f}"
        self._has_been_parsed = True
