"""
A base class that encapsulates the IO results from a benchmark run

This includes latency, throughput and other details
"""

import json
from abc import ABC, abstractmethod
from logging import Logger, getLogger
from pathlib import Path
from typing import Any

from post_processing.common import file_is_empty, get_blocksize
from post_processing.post_processing_types import IodepthDataType, JobsDataType

log: Logger = getLogger("formatter")


class BenchmarkResult(ABC):
    """
    This is the top level class for a benchmark run result. As each
    benchmark tool produces different output results we will need a
    sub-class for each type
    """

    def __init__(self, file_path: Path) -> None:
        self._resource_file_path: Path = file_path
        self._data: dict[str, Any] = self._read_results_from_file()
        if not self._data:
            raise ValueError(f"File {file_path} is empty")

        self._global_options: dict[str, str] = self._get_global_options(self._data["global options"])
        self._iodepth = self._get_iodepth(f"{self._data['global options']['iodepth']}")
        self._io_details: IodepthDataType = self._get_io_details(self._data["jobs"])

        self._has_been_parsed: bool = False
        self._source: str = self.source

    @property
    @abstractmethod
    def source(self) -> str:
        """
        Get the source/type identifier for the benchmark tool.

        Returns:
            A string identifier for the benchmark source (e.g., "fio", "cosbench")
        """

    @abstractmethod
    def _get_global_options(self, fio_global_options: dict[str, str]) -> dict[str, str]:
        """
        read the data from the 'global options' section of the fio output
        """

    @abstractmethod
    def _get_io_details(self, all_jobs: JobsDataType) -> dict[str, str]:
        """
        Get all the required details for the total I/O submitted by the benchmark tool
        """

    @abstractmethod
    def _get_iodepth(self, iodepth_value: str) -> str:
        """
        Checks to see if the iodepth encoded in the logfile name matches
        the iodepth in the output file. If it does, return the iodepth
        from the file, otherwise return the iodepth parsed from the
        log file path
        """

    @property
    def blocksize(self) -> str:
        return get_blocksize(f"{self._data['global options']['bs']}")

    @property
    def operation(self) -> str:
        operation: str = f"{self._data['global options']['rw']}"
        if self._global_options.get("percentage_reads", None):
            operation = (
                f"{self._global_options['percentage_reads']}_{self._global_options['percentage_writes']}_{operation}"
            )

        return operation

    @property
    def global_options(self) -> dict[str, str]:
        return self._global_options

    @property
    def iodepth(self) -> str:
        return self._iodepth

    @property
    def io_details(self) -> IodepthDataType:
        return self._io_details

    def _read_results_from_file(self) -> dict[str, Any]:
        """
        Read the data from the results file and return the results in a dict
        """
        if file_is_empty(self._resource_file_path):
            log.warning("Unable to process file %s as it is empty", self._resource_file_path)
            return {}

        try:
            with open(str(self._resource_file_path), encoding="utf8") as file:
                data: dict[str, Any] = json.load(file)

        except json.JSONDecodeError:
            log.warning("Unable to process file %s as it is not in json format", self._resource_file_path)
            return {}

        return data
