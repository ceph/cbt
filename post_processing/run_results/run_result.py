"""
The base class that reads a results file and converts it into the
common data format that can be plotted
"""

import json
from abc import ABC, abstractmethod
from logging import Logger, getLogger
from pathlib import Path
from typing import Optional, Union, cast

from post_processing.common import file_is_empty, file_is_precondition
from post_processing.post_processing_types import (
    InternalBlocksizeDataType,
    InternalFormattedOutputType,
    InternalNumJobsDataType,
    IodepthDataType,
    TimeSeriesFormatType,
)
from post_processing.run_results.benchmark_result import BenchmarkResult
from post_processing.run_results.resource_result import ResourceResult

# from post_processing.run_results.resources.resource_result import ResourceResult

log: Logger = getLogger("formatter")


class RunResult(ABC):
    """
    A result run file that needs processing
    """

    def __init__(self, directory: Path, file_name_root: str, include_timeseries: bool = False) -> None:
        self._path: Path = directory
        self._has_been_processed: bool = False
        self._include_timeseries: bool = include_timeseries

        self._files: list[Path] = self._find_files_for_testrun(file_name_root=file_name_root)
        self._processed_data: InternalFormattedOutputType = {}
        self._timeseries_data: dict[str, TimeSeriesFormatType] = {}
        self._timeseries_by_directory: dict[Path, dict[str, TimeSeriesFormatType]] = {}

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

    @abstractmethod
    def _create_benchmark_result(self, file_path: Path) -> BenchmarkResult:
        """
        Factory method to create the appropriate BenchmarkResult subclass.

        Subclasses should implement this to return the correct benchmark result
        parser based on the benchmark type (e.g., FIO, CosBench, etc.).

        Args:
            file_path: Path to the benchmark output file

        Returns:
            BenchmarkResult subclass instance for parsing this benchmark type
        """

    @abstractmethod
    def _create_resource_result(self, file_path: Path) -> ResourceResult:
        """
        Factory method to create the appropriate ResourceResult subclass.

        Subclasses should implement this to return the correct resource result
        parser based on the benchmark type (e.g., FIOResource, etc.).

        Args:
            file_path: Path to the benchmark output file

        Returns:
            ResourceResult subclass instance for parsing resource usage
        """

    def _merge_timeseries_data(
        self,
        test_config: tuple[str, str, str, str],
        new_timeseries: TimeSeriesFormatType,
    ) -> TimeSeriesFormatType:
        """
        Merge new time-series data with existing data for the same test configuration.

        Default behavior is replacement. Subclasses such as RBDFIO can override
        this to aggregate time-series data across multiple files/volumes.

        Args:
            test_config: Tuple of (operation, blocksize, iodepth, number_of_jobs)
            new_timeseries: Newly parsed time-series data

        Returns:
            TimeSeriesFormatType to store for this configuration
        """
        operation, blocksize, iodepth, _ = test_config
        key = f"{operation}_{blocksize}_{iodepth}"
        existing_timeseries = self._timeseries_data.get(key)
        if existing_timeseries:
            log.debug("Replacing existing time-series data for %s", key)
        return new_timeseries

    def process(self) -> None:
        """
        Convert the results data from all the individual files that make up this
        result into the standard intermediate format.

        With memory-efficient approach, timeseries data is aggregated then written
        immediately after processing all files to avoid holding data in memory longer than needed.
        """
        number_of_volumes_for_test_run: int = len(self._files)

        if number_of_volumes_for_test_run > 0:
            self._process_test_run_files()
            # Write timeseries data immediately after processing all files
            # Group by aggregation directory and write each group separately
            if self._include_timeseries and self._timeseries_by_directory:
                self._write_and_clear_timeseries_by_directory()
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

    def _extract_test_configuration(self, benchmark_result: BenchmarkResult) -> tuple[str, str, str, str]:
        """
        Extract test configuration parameters from benchmark result.

        Args:
            benchmark_result: Parsed benchmark result object

        Returns:
            Tuple of (operation, blocksize, iodepth, number_of_jobs)
        """
        return (
            benchmark_result.operation,
            benchmark_result.blocksize,
            benchmark_result.iodepth,
            benchmark_result.number_of_jobs,
        )

    def _merge_io_details(
        self, test_config: tuple[str, str, str, str], new_io_details: IodepthDataType
    ) -> IodepthDataType:
        """
        Merge new IO details with existing data for the same test configuration.

        This handles cases where multiple volumes produce results for the same
        test parameters, requiring aggregation of metrics.

        Args:
            test_config: Tuple of (operation, blocksize, iodepth, number_of_jobs)
            new_io_details: New IO details to merge

        Returns:
            Merged IO details (either summed with existing or new details)
        """
        operation, blocksize, iodepth, number_of_jobs = test_config

        # Check if we have existing data for this configuration
        existing_data = self._processed_data.get(operation, {}).get(number_of_jobs, {}).get(blocksize, {}).get(iodepth)

        if existing_data:
            log.debug("We have details for iodepth %s so using them", iodepth)
            return self._sum_io_details(existing_data, new_io_details)

        return new_io_details

    def _build_test_result_data(
        self,
        test_config: tuple[str, str, str, str],
        io_details: IodepthDataType,
        global_details: dict[str, str],
        resource_data: dict[str, str],
    ) -> InternalNumJobsDataType:
        """
        Build the complete nested data structure for a test result.

        Args:
            test_config: Tuple of (operation, blocksize, iodepth, number_of_jobs)
            io_details: Merged IO performance details
            global_details: Global benchmark options
            resource_data: Resource usage statistics

        Returns:
            Nested dictionary structure: {numjobs: {blocksize: {iodepth: data}}}
        """
        _, blocksize, iodepth, number_of_jobs = test_config

        # Build from innermost to outermost level
        iodepth_data = {**global_details, **io_details, **resource_data}
        iodepth_details = {iodepth: iodepth_data}
        blocksize_details = cast(InternalBlocksizeDataType, {blocksize: iodepth_details})
        numjobs_details = cast(InternalNumJobsDataType, {number_of_jobs: blocksize_details})

        return numjobs_details

    def _update_processed_data(
        self,
        test_config: tuple[str, str, str, str],
        numjobs_details: InternalNumJobsDataType,
    ) -> None:
        """
        Update the internal processed data structure with new test results.

        Args:
            test_config: Tuple of (operation, blocksize, iodepth, number_of_jobs)
            numjobs_details: Complete test result data to merge
        """
        operation, blocksize, _, number_of_jobs = test_config

        # Extract nested structures for updating
        blocksize_details = numjobs_details[number_of_jobs]
        iodepth_details = blocksize_details[blocksize]

        # Update at the appropriate nesting level
        if operation not in self._processed_data:
            self._processed_data[operation] = numjobs_details
        elif number_of_jobs not in self._processed_data[operation]:
            self._processed_data[operation][number_of_jobs] = blocksize_details
        elif blocksize not in self._processed_data[operation][number_of_jobs]:
            self._processed_data[operation][number_of_jobs][blocksize] = iodepth_details
        else:
            self._processed_data[operation][number_of_jobs][blocksize].update(iodepth_details)

    def _process_timeseries_data(
        self, test_config: tuple[str, str, str, str], benchmark_result: BenchmarkResult
    ) -> None:
        """
        Extract and store time-series data if available, grouped by aggregation directory.

        Args:
            test_config: Tuple of (operation, blocksize, iodepth, number_of_jobs)
            benchmark_result: Parsed benchmark result object
        """
        operation, blocksize, iodepth, _ = test_config

        ts_data: Optional[TimeSeriesFormatType] = benchmark_result.get_timeseries_data()
        if ts_data:
            key = f"{operation}_{blocksize}_{iodepth}"

            # Determine aggregation directory from the file path
            file_path = benchmark_result.resource_file_path
            aggregation_directory = self._determine_aggregation_directory_from_file(file_path)

            # Initialize directory dict if needed
            if aggregation_directory not in self._timeseries_by_directory:
                self._timeseries_by_directory[aggregation_directory] = {}

            # Store existing data temporarily in _timeseries_data for merge to find it
            existing_data = self._timeseries_by_directory[aggregation_directory].get(key)
            if existing_data:
                self._timeseries_data[key] = existing_data

            # Now merge will find the existing data
            merged_ts_data = self._merge_timeseries_data(test_config, ts_data)

            # Clear temporary storage
            if key in self._timeseries_data:
                del self._timeseries_data[key]

            # Store merged result
            self._timeseries_by_directory[aggregation_directory][key] = merged_ts_data

            log.debug("Stored time-series data for %s at %s", key, aggregation_directory)
        else:
            log.debug("No time-series data available for %s %s %s", operation, blocksize, iodepth)

    def _determine_aggregation_directory_from_file(self, file_path: Path) -> Path:
        """
        Determine the correct aggregation directory from a file path.

        Looks for 'total_iodepth' or 'iodepth' in the file's parent directories.
        Prefers total_iodepth if it exists (for aggregation), otherwise uses iodepth.

        Args:
            file_path: Path to a result file

        Returns:
            Path object for the aggregation directory
        """
        path_parts = file_path.parts

        # Look for total_iodepth first (higher priority for aggregation)
        for index, part in enumerate(path_parts):
            if part.startswith("total_iodepth"):
                return Path(*path_parts[: index + 1])

        # If no total_iodepth, look for iodepth
        for index, part in enumerate(path_parts):
            if part.startswith("iodepth"):
                return Path(*path_parts[: index + 1])

        # If neither found, use the file's parent directory
        return file_path.parent

    def _write_and_clear_timeseries_by_directory(self) -> None:
        """
        Write timeseries data grouped by aggregation directory and clear memory.

        This method writes timeseries data at the correct aggregation level
        (typically total_iodepth for aggregated results, or iodepth if no
        total_iodepth exists) for each group and then clears memory.

        The aggregation of data from multiple files happens during processing
        via _merge_timeseries_data() in subclasses like RBDFIO.
        """
        if not self._timeseries_by_directory:
            return

        total_files = sum(len(ts_dict) for ts_dict in self._timeseries_by_directory.values())
        log.debug("Writing %d timeseries files across %d directories", total_files, len(self._timeseries_by_directory))

        for aggregation_dir, timeseries_dict in self._timeseries_by_directory.items():
            output_dir = aggregation_dir / "visualisation"
            output_dir.mkdir(parents=True, exist_ok=True)

            log.debug("Writing %d timeseries files to %s", len(timeseries_dict), output_dir)

            for _, ts_data in timeseries_dict.items():
                # Extract configuration from the TimeSeriesFormatType data
                operation = ts_data.get("operation", "unknown")
                blocksize = ts_data.get("blocksize", "unknown")
                numjobs = ts_data.get("numjobs", "1")
                iodepth = ts_data.get("iodepth", "1")

                filename = output_dir / f"{blocksize}_{numjobs}_{operation}_{iodepth}_timeseries.json"
                log.debug("Writing timeseries data to %s", filename)

                try:
                    with filename.open("w", encoding="utf8") as f:
                        json.dump(ts_data, f, indent=4, sort_keys=True)
                except OSError as e:
                    log.error("Failed to write timeseries file %s: %s", filename, e)

        # Clear timeseries data from memory after writing
        self._timeseries_by_directory.clear()
        log.debug("Cleared timeseries data from memory")

    def _convert_file(self, file_path: Path) -> None:
        """
        Convert an individual benchmark result file to the common intermediate format.

        This method reads the benchmark output file, extracts IO and resource usage
        statistics, and stores them in the internal data structure organized by
        operation type, blocksize, and IO depth.

        If include_timeseries is True, also extracts time-series data from log files.

        Args:
            file_path: Path to the benchmark result file to process

        Raises:
            ValueError: If benchmark or resource result creation fails
            KeyError: If required data fields are missing from results
        """
        try:
            # Use factory methods to get the correct classes
            io: BenchmarkResult = self._create_benchmark_result(file_path)
            resource: ResourceResult = self._create_resource_result(file_path)

            test_config = self._extract_test_configuration(io)

            # Merge IO details with existing data if present
            io_details = self._merge_io_details(test_config, io.io_details)

            # Build complete test result data structure
            numjobs_details = self._build_test_result_data(test_config, io_details, io.global_options, resource.get())

            # Update internal processed data
            self._update_processed_data(test_config, numjobs_details)

            # Process time-series data if requested
            if self._include_timeseries:
                self._process_timeseries_data(test_config, io)

        except (ValueError, KeyError) as e:
            log.error("Failed to convert file %s: %s", file_path, e)
            raise
        except Exception as e:
            log.exception("Unexpected error converting file %s: %s", file_path, e)
            raise
