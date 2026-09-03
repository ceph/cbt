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
from post_processing.run_results.resource_result_factory import get_all_resources

log: Logger = getLogger("cbt.formatter")


class RunResult(ABC):  # pylint: disable=too-many-instance-attributes
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
        # Tracks how many volume files have been merged per test configuration key
        # (operation, blocksize, iodepth, numjobs) — used by _merge_resource_data.
        self._resource_volume_counts: dict[tuple[str, str, str, str], int] = {}
        # Tracks how many non-zero contributions have been seen per
        # (test_config, source) pair for shared-directory sources (collectl, top).
        # Only incremented when the source returned a non-zero value, so that
        # volumes whose monitoring directory is absent (returning 0.00) are not
        # counted in the denominator and do not dilute the running average.
        self._resource_source_counts: dict[tuple[tuple[str, str, str, str], str], int] = {}

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
                    log.debug("Not processing file %s as it is from a precondition operation", file_path)
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
        resource_data: dict[str, dict[str, str]],
    ) -> InternalNumJobsDataType:
        """
        Build the complete nested data structure for a test result.

        Args:
            test_config: Tuple of (operation, blocksize, iodepth, number_of_jobs)
            io_details: Merged IO performance details
            global_details: Global benchmark options
            resource_data: Resource usage statistics with structure:
                          {"cpu": {"source1": "value1", ...}, "memory": {...}}

        Returns:
            Nested dictionary structure: {numjobs: {blocksize: {iodepth: data}}}
        """
        _, blocksize, iodepth, number_of_jobs = test_config

        # Build from innermost to outermost level
        # Merge all data including the nested resource data
        iodepth_data = {**global_details, **io_details, **resource_data}
        iodepth_details = {iodepth: iodepth_data}
        blocksize_details = cast(InternalBlocksizeDataType, {blocksize: iodepth_details})
        numjobs_details: InternalNumJobsDataType = {number_of_jobs: blocksize_details}

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

    # Sources where each volume file yields an independent CPU measurement that
    # should be summed to get the total load across all volumes.  All other
    # sources (collectl, top) capture a shared system view from one directory
    # that is read once per volume file, so they are averaged instead.
    _SUMMED_RESOURCE_SOURCES: frozenset[str] = frozenset({"fio"})

    def _collect_multi_source_resources(self, resources: list[ResourceResult]) -> dict[str, dict[str, str]]:
        """
        Collect resource data from multiple sources into nested dict format.

        Args:
            resources: List of ResourceResult instances

        Returns:
            Dict with structure: {"cpu": {"source1": "value1", ...}, "memory": {...}}
        """
        cpu_data: dict[str, str] = {}
        memory_data: dict[str, str] = {}

        for resource in resources:
            source = resource.source
            resource_dict = resource.get()

            cpu_data[source] = resource_dict.get("cpu", "0.00")
            memory_data[source] = resource_dict.get("memory", "0.00")

        return {"cpu": cpu_data, "memory": memory_data}

    def _aggregate_metric(self, source: str, prev: float, new: float, source_count: int) -> str:
        """
        Aggregate a single CPU or memory metric value for one source across volumes.

        Args:
            source: Resource source identifier (e.g. "fio", "collectl", "top")
            prev: Previously accumulated value
            new: New value from the current volume
            source_count: Number of non-zero contributions already accumulated
                          for this source (used as denominator for shared-directory
                          sources; ignored for summed sources).

        Returns:
            Aggregated value as a formatted string
        """
        if source in self._SUMMED_RESOURCE_SOURCES:
            return f"{prev + new:.2f}"
        # Shared-directory source: running_avg = (prev * n + new) / (n + 1)
        return f"{(prev * source_count + new) / (source_count + 1):.2f}"

    def _get_existing_iodepth(self, test_config: tuple[str, str, str, str]) -> Optional[Union[str, IodepthDataType]]:
        """
        Look up the already-stored iodepth entry for a test configuration.

        Args:
            test_config: Tuple of (operation, blocksize, iodepth, number_of_jobs)

        Returns:
            The stored iodepth data dict, a bare str, or None if not yet stored
        """
        operation, blocksize, iodepth, number_of_jobs = test_config
        return self._processed_data.get(operation, {}).get(number_of_jobs, {}).get(blocksize, {}).get(iodepth)

    def _merge_resource_data(
        self,
        test_config: tuple[str, str, str, str],
        new_resource_data: dict[str, dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        """
        Merge new resource data with any previously stored data for the same test
        configuration, aggregating correctly across multiple volumes.

        Aggregation strategy per source:
        - FIO: each volume file contains an independent per-job CPU measurement,
          so values are **summed** across volumes.
        - collectl / top: both point at a shared directory alongside the benchmark
          files, so every volume file in the same directory reads the same data.
          Values are **averaged** (divide running sum by volume count) so the
          final figure is not artificially inflated.

        Args:
            test_config: Tuple of (operation, blocksize, iodepth, number_of_jobs)
            new_resource_data: Resource data from the current volume file

        Returns:
            Merged resource data dict with the same structure as the input
        """
        if test_config not in self._resource_volume_counts:
            # First volume — nothing to merge yet
            return new_resource_data

        existing_iodepth = self._get_existing_iodepth(test_config)
        if not isinstance(existing_iodepth, dict):
            return new_resource_data

        raw_cpu = existing_iodepth.get("cpu", "")
        raw_mem = existing_iodepth.get("memory", "")
        existing_cpu: dict[str, str] = raw_cpu if isinstance(raw_cpu, dict) else {}
        existing_mem: dict[str, str] = raw_mem if isinstance(raw_mem, dict) else {}
        new_cpu_map = new_resource_data.get("cpu", {})
        new_mem_map = new_resource_data.get("memory", {})
        merged_cpu: dict[str, str] = {}
        merged_mem: dict[str, str] = {}

        for source in set(existing_cpu) | set(new_cpu_map):
            new_cpu_val = float(new_cpu_map.get(source, "0.00"))
            new_mem_val = float(new_mem_map.get(source, "0.00"))
            source_count: int = self._resource_source_counts.get((test_config, source), 0)

            if source not in self._SUMMED_RESOURCE_SOURCES and new_cpu_val == 0.0 and new_mem_val == 0.0:
                # Missing monitoring directory — keep the accumulated value unchanged
                # and do not increment _resource_source_counts for this source.
                merged_cpu[source] = existing_cpu.get(source, "0.00")
                merged_mem[source] = existing_mem.get(source, "0.00")
            else:
                merged_cpu[source] = self._aggregate_metric(
                    source, float(existing_cpu.get(source, "0.00")), new_cpu_val, source_count
                )
                merged_mem[source] = self._aggregate_metric(
                    source, float(existing_mem.get(source, "0.00")), new_mem_val, source_count
                )

        return {"cpu": merged_cpu, "memory": merged_mem}

    def _convert_file(self, file_path: Path) -> None:
        """
        Convert an individual benchmark result file to the common intermediate format.

        This method reads the benchmark output file, extracts IO and resource usage
        statistics, and stores them in the internal data structure organized by
        operation type, blocksize, and IO depth.

        Now supports multiple resource sources (FIO, Collectl, etc.) simultaneously.
        Resource values are aggregated across multiple volumes: sources that report
        independent per-volume measurements (e.g. FIO) are summed; sources that
        capture a shared system view (e.g. collectl, top) are averaged.

        If include_timeseries is True, also extracts time-series data from log files.

        Args:
            file_path: Path to the benchmark result file to process

        Raises:
            ValueError: If benchmark or resource result creation fails
            KeyError: If required data fields are missing from results
        """
        try:
            # Use factory method for benchmark result
            io: BenchmarkResult = self._create_benchmark_result(file_path)

            # Get ALL available resource sources using factory
            resources: list[ResourceResult] = get_all_resources(file_path)

            test_config = self._extract_test_configuration(io)

            # Merge IO details with existing data if present
            io_details = self._merge_io_details(test_config, io.io_details)

            # Collect resource data from all sources then merge with any
            # previously accumulated data for the same test configuration
            resource_data = self._collect_multi_source_resources(resources)
            resource_data = self._merge_resource_data(test_config, resource_data)

            # Build complete test result data structure
            numjobs_details = self._build_test_result_data(test_config, io_details, io.global_options, resource_data)

            # Update internal processed data
            self._update_processed_data(test_config, numjobs_details)

            # Increment the per-configuration volume counter (all sources).
            self._resource_volume_counts[test_config] = self._resource_volume_counts.get(test_config, 0) + 1

            # Increment the per-(config, source) counter only for non-zero
            # shared-directory sources, so that volumes whose monitoring
            # directory is absent are not counted in the averaging denominator.
            for source, cpu_val in resource_data.get("cpu", {}).items():
                if source not in self._SUMMED_RESOURCE_SOURCES and float(cpu_val) != 0.0:
                    key = (test_config, source)
                    self._resource_source_counts[key] = self._resource_source_counts.get(key, 0) + 1

            # Process time-series data if requested
            if self._include_timeseries:
                self._process_timeseries_data(test_config, io)

        except (ValueError, KeyError) as e:
            log.error("Failed to convert file %s: %s", file_path, e)
            raise
        except Exception as e:
            log.exception("Unexpected error converting file %s: %s", file_path, e)
            raise
