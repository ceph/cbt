"""
A base class that encapsulates the resource usage results from a benchmark run

This could be CPU, Memory etc
"""

import json
from abc import ABC, abstractmethod
from logging import Logger, getLogger
from pathlib import Path
from typing import Any

from post_processing.common import file_is_empty

log: Logger = getLogger("formatter")


class ResourceResult(ABC):
    """
    This is the top level class for a resource run result. As each
    resource monitoring tool produces different output results we will need a
    sub-class for each type
    """

    def __init__(self, file_path: Path) -> None:
        self._resource_file_path: Path = self._get_resource_output_file_from_file_path(file_path)
        self._cpu: str = ""
        self._memory: str = ""
        self._has_been_parsed: bool = False
        self._source: str = self.source

    @property
    @abstractmethod
    def source(self) -> str:
        """
        Get the source identifier for the resource monitoring tool.

        Returns:
            A string identifier for the resource monitoring source (e.g., "fio", "collectl")
        """

    @abstractmethod
    def _get_resource_output_file_from_file_path(self, file_path: Path) -> Path:
        """
        Given a particular resource file name find the corresponding
        resource usage statistics file path
        """

    @abstractmethod
    def _parse(self, data: dict[str, Any]) -> None:
        """
        Read the resource usage data from the read data and return the
        relevant resource usage statistics
        """

    @property
    def cpu(self) -> str:
        """
        getter for the CPU value
        """
        if not self._has_been_parsed:
            self._parse(self._read_results_from_file())
        return self._cpu

    @property
    def memory(self) -> str:
        """
        getter for the memory value
        """
        if not self._has_been_parsed:
            self._parse(self._read_results_from_file())
        return self._memory

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

    def get(self) -> dict[str, str]:
        """
        Add the data from the resource monitoring into the common output
        format file for this test run
        """

        if not self._has_been_parsed:
            self._parse(self._read_results_from_file())

        return {"source": self._source, "cpu": self._cpu, "memory": self._memory}
