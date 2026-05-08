"""
The base class that reads a results file and converts it into the
common data format that can be plotted
"""

from abc import ABC, abstractmethod
from logging import Logger, getLogger
from pathlib import Path
from typing import Union

from post_processing.common import file_is_empty, file_is_precondition
from post_processing.post_processing_types import (
    InternalBlocksizeDataType,
    InternalFormattedOutputType,
    InternalNumJobsDataType,
    IodepthDataType,
)

# from post_processing.run_results.benchmarks.benchmark_result import BenchmarkResult
# To be removed when factory is ready
from post_processing.run_results.benchmarks.fio import FIO
from post_processing.run_results.resources.fio_resource import FIOResource

# from post_processing.run_results.resources.resource_result import ResourceResult

log: Logger = getLogger("formatter")


class RunResult(ABC):
    """
    A result run file that needs processing
    """

    def __init__(self, directory: Path, file_name_root: str) -> None:
        self._path: Path = directory
        self._has_been_processed: bool = False

        self._files: list[Path] = self._find_files_for_testrun(file_name_root=file_name_root)
        self._processed_data: InternalFormattedOutputType = {}

    @abstractmethod
    def _find_files_for_testrun(self, file_name_root: str) -> list[Path]:
        """
        Find the relevant output files for this type of benchmark run

        These will be specific to a benchmark type or data type
        """

    @abstractmethod
    def _sum_io_details(
        self, existing_values: Union[str, IodepthDataType], new_values: IodepthDataType
    ) -> IodepthDataType:
        """
        sum the existing_values with new_values and return the result
        """

    @property
    @abstractmethod
    def type(self) -> str:
        """
        Returns the benchmark type.

        Returns:
            The benchmark type identifier (e.g., "rbdfio", "fio")
        """

    def process(self) -> None:
        """
        Convert the results data from all the individual files that make up this
        result into the standard intermediate format
        """
        number_of_volumes_for_test_run: int = len(self._files)

        if number_of_volumes_for_test_run > 0:
            self._process_test_run_files()
        else:
            log.warning("test run with directory %s has no files - not doing any conversion", self._path)

        self._has_been_processed = True

    def get(self) -> InternalFormattedOutputType:
        """
        Return the processed results
        """

        if not self._has_been_processed:
            self.process()

        return self._processed_data

    def _process_test_run_files(self) -> None:
        """
        If there is only details for a single volume then we can convert the
        data from the fio output directly into our output format
        """

        for file_path in self._files:
            if not file_is_empty(file_path):
                if not file_is_precondition(file_path):
                    log.debug("Processing file %s", file_path)
                    self._convert_file(file_path)
                else:
                    log.warning("Not processing file %s as it is from a precondition operation", file_path)
                    self._files.remove(file_path)
            else:
                log.warning("Cannot process file %s as it is empty", file_path)

    def _convert_file(self, file_path: Path) -> None:
        """
        Convert an individual benchmark result file to the common intermediate format.

        This method reads the benchmark output file, extracts IO and resource usage
        statistics, and stores them in the internal data structure organized by
        operation type, blocksize, and IO depth.

        Args:
            file_path: Path to the benchmark result file to process
        """

        # call the factory methods here to get the correct classes
        io: FIO = FIO(file_path=file_path)
        resource: FIOResource = FIOResource(file_path=file_path)

        iodepth = io.iodepth
        blocksize: str = io.blocksize
        operation: str = io.operation
        number_of_jobs: str = io.number_of_jobs
        global_details: dict[str, str] = io.global_options

        blocksize_details: InternalBlocksizeDataType = {blocksize: {}}
        iodepth_details: dict[str, dict[str, str]] = {iodepth: global_details}
        numjobs_details: InternalNumJobsDataType = {number_of_jobs: blocksize_details}

        io_details: IodepthDataType = {}

        if self._processed_data.get(operation, None):
            if self._processed_data[operation].get(number_of_jobs, None):
                if self._processed_data[operation][number_of_jobs].get(blocksize, None):
                    if self._processed_data[operation][number_of_jobs][blocksize].get(iodepth, None):
                        # we already have data here, so use it
                        log.debug("We have details for iodepth %s so using them", iodepth)
                        io_details = self._sum_io_details(
                            self._processed_data[operation][number_of_jobs][blocksize][iodepth], io.io_details
                        )

        if not io_details:
            io_details = io.io_details

        iodepth_details[iodepth].update(io_details)
        iodepth_details[iodepth].update(resource.get())
        blocksize_details[blocksize].update(iodepth_details)
        numjobs_details[number_of_jobs].update(blocksize_details)

        if self._processed_data.get(operation, None):
            if self._processed_data[operation].get(number_of_jobs, None):
                if self._processed_data[operation][number_of_jobs].get(blocksize, None):
                    self._processed_data[operation][number_of_jobs][blocksize].update(iodepth_details)
                else:
                    self._processed_data[operation][number_of_jobs].update(blocksize_details)
            else:
                self._processed_data[operation].update(numjobs_details)
        else:
            self._processed_data.update({operation: numjobs_details})
