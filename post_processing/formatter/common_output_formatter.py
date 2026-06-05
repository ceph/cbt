"""
This file contains the code to convert the output of any benchmark run into
a common format that can then be used to graph the results.

Eventually the intent is to support all benchmark types, but we will start
with fio.

The output is a JSON file of the format:
{
    queue_depth: {
                    bandwidth_bytes:
                    blocksize:
                    io_bytes:
                    iops:
                    latency:
                    number_of_jobs:
                    percentage_reads:
                    percentage_writes:
                    runtime_seconds:
                    std_deviation:
                    total_ios:
                    cpu:
                    memory:

    }
    ...
    queue_depth_n {

    }
    maximum_bandwidth:
    latency_at_max_bandwidth:
    maximum_iops:
    latency_at_max_iops:
    maximum_cpu_usage:
    maximum_memory_usage:
    number_of_jobs:
    resource_type:
    benchmark:
}

The queue depth details are the sum of the details for write operation
and the details for read operations
"""

import json
import re
from pathlib import Path
from typing import Optional

from post_processing.formatter.base_formatter import BaseFormatter
from post_processing.post_processing_types import CommonFormatDataType, InternalFormattedOutputType
from post_processing.run_results.run_result_factory import get_run_result_from_directory_name


class CommonOutputFormatter(BaseFormatter):
    """
    This class contains all the common code for converting an output file in
    json format to the format we want to use to draw iops and latency
    graphs.
    We also store some additional information that may be useful for looking
    in more detail at the results
    """

    # To make sure that we read files that only contain JSON data we will look
    # at the json_output* files by default
    DEFAULT_OUTPUT_FILE_PART: str = "json_output"

    def __init__(self, archive_directory: str, filename_root: Optional[str] = None) -> None:
        super().__init__(archive_directory)
        self._filename_root: str = filename_root if filename_root else self.DEFAULT_OUTPUT_FILE_PART

        self._formatted_output: InternalFormattedOutputType = {}
        self._benchmark_types: dict[str, str] = {}  # Track benchmark type per operation

    def _merge_results(self, processed_results: InternalFormattedOutputType) -> None:
        """
        Merge processed results into the formatted output dictionary.

        This method handles merging new results into existing data, updating
        existing entries or creating new ones as needed. Performs a deep merge
        to preserve nested numjobs and blocksize data.

        Args:
            processed_results: Dictionary of processed results to merge
        """
        # We need to do a deep merge of the data here, so we need the nested blocks for the moment
        # Maybe we can get rid of them in a future re-factor
        for run_type, run_data in processed_results.items():  # pylint: disable=[too-many-nested-blocks]
            if run_type not in self._formatted_output:
                self._formatted_output[run_type] = run_data
            else:
                # Deep merge: operation -> numjobs -> blocksize
                for numjobs, numjobs_data in run_data.items():
                    if numjobs not in self._formatted_output[run_type]:
                        self._formatted_output[run_type][numjobs] = numjobs_data
                    else:
                        # Merge blocksize level
                        for blocksize, blocksize_data in numjobs_data.items():
                            if blocksize not in self._formatted_output[run_type][numjobs]:
                                self._formatted_output[run_type][numjobs][blocksize] = blocksize_data
                            else:
                                # Update existing blocksize data
                                self._formatted_output[run_type][numjobs][blocksize].update(blocksize_data)

    def _process_compatibility_mode(self) -> str:
        """
        Process test run using compatibility method for legacy directory structures.

        This handles cases where there are multiple directories for a single test run,
        which requires using the directory-level processing approach.

        Returns:
            The benchmark type identifier (e.g., "rbdfio", "fio")

        Note: This is only here for compatibility with runs that were done before the new workloads code was uploaded
        """

        results = get_run_result_from_directory_name(Path(self._directory), self._filename_root)
        results.process()
        self._merge_results(results.get())
        return results.type

    def _process_single_testrun(self, testrun_directory: Path) -> str:
        """
        Process a single test run directory and all its IO pattern subdirectories.

        This method iterates through all subdirectories in the test run directory,
        processes each one, and writes results immediately to reduce memory usage.

        Args:
            testrun_directory: Path to the test run directory

        Returns:
            The benchmark type identifier from the last processed result

        """
        benchmark_type: str = "unknown"

        for io_pattern_directory in [
            directory
            for directory in testrun_directory.iterdir()
            if directory.is_dir()
            and not f"{directory.name}".startswith(".")
            and "visualisation" not in f"{directory.name}"
        ]:
            self.log.debug("Looking at results for directory %s", io_pattern_directory)
            # Use factory method to get the correct results type based on directory name
            results = get_run_result_from_directory_name(
                directory=io_pattern_directory, file_name_root=self._filename_root
            )

            results.process()
            processed_results = results.get()

            # Memory-efficient approach: write results for this operation immediately
            # instead of accumulating everything
            self._write_operation_results(io_pattern_directory, processed_results, results.type)

            benchmark_type = results.type

        return benchmark_type

    def _add_common_metadata(self) -> None:
        """
        Add common metadata fields to the top level of each blocksize dataset.

        This method extracts metadata like number_of_jobs and benchmark type
        from the nested iodepth entries and adds them at the blocksize level
        for easier access.
        """
        for operation_type, operation_data in self._formatted_output.items():
            for _, numjobs_data in operation_data.items():
                for _, blocksize_data in numjobs_data.items():
                    # Add benchmark type
                    blocksize_data["benchmark"] = self._benchmark_types.get(operation_type, "unknown")

                    # Extract number_of_jobs from the first iodepth entry
                    # All iodepth entries should have the same number_of_jobs value
                    for _, value in blocksize_data.items():
                        if isinstance(value, dict) and "number_of_jobs" in value:
                            blocksize_data["number_of_jobs"] = value["number_of_jobs"]
                            break

    def _add_peak_metrics(self) -> None:
        """
        Add aggregate metrics (max bandwidth, IOPS, resource usage) to all test runs.

        This method calculates and adds maximum values and associated latencies
        for bandwidth and IOPS, as well as maximum resource usage, to each
        blocksize dataset in the formatted output.
        """
        for operation_type, operation_data in self._formatted_output.items():
            for _, numjobs_data in operation_data.items():
                for _, blocksize_data in numjobs_data.items():
                    # Cast to CommonFormatDataType since blocksize_data contains both
                    # string metadata and dict iodepth entries
                    max_bandwidth, max_bandwidth_latency, max_iops, max_iops_latency = (
                        self._find_maximum_bandwidth_and_iops_with_latency(blocksize_data)
                    )
                    max_cpu, max_memory = self._find_max_resource_usage(blocksize_data)
                    blocksize_data["maximum_bandwidth"] = max_bandwidth
                    blocksize_data["latency_at_max_bandwidth"] = max_bandwidth_latency
                    blocksize_data["maximum_iops"] = max_iops
                    blocksize_data["latency_at_max_iops"] = max_iops_latency
                    blocksize_data["maximum_cpu_usage"] = max_cpu
                    blocksize_data["maximum_memory_usage"] = max_memory
                    blocksize_data["benchmark"] = self._benchmark_types.get(operation_type, "unknown")

    def _write_operation_results(  # pylint: disable=too-many-locals
        self, operation_directory: Path, processed_results: InternalFormattedOutputType, benchmark_type: str
    ) -> None:
        """
        Write results for a single operation directory immediately to reduce memory usage.

        Args:
            operation_directory: Path to the operation directory (e.g., 256krandomwrite.../rbdfio/)
            processed_results: Processed results for this operation
            benchmark_type: Type of benchmark (e.g., "rbdfio", "fio")
        """
        if not processed_results:
            return

        # Create visualisation directory at operation level
        output_dir = operation_directory / "visualisation"
        output_dir.mkdir(parents=True, exist_ok=True)

        self.log.info("Writing hockey-stick data to %s", output_dir)

        # Process each operation type in the results
        for operation_type, operation_data in processed_results.items():
            # Track benchmark type
            self._benchmark_types[operation_type] = benchmark_type

            for number_of_jobs, numjobs_data in operation_data.items():
                for blocksize, blocksize_data in numjobs_data.items():
                    # Add metadata
                    blocksize_data["benchmark"] = benchmark_type
                    blocksize_data["number_of_jobs"] = number_of_jobs

                    # Add peak metrics
                    max_bandwidth, max_bandwidth_latency, max_iops, max_iops_latency = (
                        self._find_maximum_bandwidth_and_iops_with_latency(blocksize_data)
                    )
                    max_cpu, max_memory = self._find_max_resource_usage(blocksize_data)
                    blocksize_data["maximum_bandwidth"] = max_bandwidth
                    blocksize_data["latency_at_max_bandwidth"] = max_bandwidth_latency
                    blocksize_data["maximum_iops"] = max_iops
                    blocksize_data["latency_at_max_iops"] = max_iops_latency
                    blocksize_data["maximum_cpu_usage"] = max_cpu
                    blocksize_data["maximum_memory_usage"] = max_memory

                    # Write file
                    filename = output_dir / f"{blocksize}_{number_of_jobs}_{operation_type}.json"
                    self.log.debug("Writing hockey-stick data to %s", filename)

                    try:
                        with filename.open("w", encoding="utf8") as f:
                            json.dump(blocksize_data, f, indent=4, sort_keys=True)
                    except OSError as e:
                        self.log.error("Failed to write hockey-stick file %s: %s", filename, e)

    def process(self) -> None:
        """
        Process input data and convert to intermediate format.

        Convert all files in a given directory to our internal format and then
        prepare the intermediate data that can be used to produce a graph.

        Note: With memory-efficient mode, results are written immediately during
        processing rather than accumulated in memory.
        """
        self.log.info("Converting all files with name %s in directory %s", self._filename_root, self._directory)

        # Find all result files
        self.log.debug("Finding all %s* files from %s", self._filename_root, self._directory)
        file_list = [
            path
            for path in self.path.glob(f"**/{self._filename_root}.*")
            if re.search(rf"{self._filename_root}.\d+$", f"{path}")
        ]

        self._find_all_testrun_ids(file_list)

        for testrun_id in self._all_test_run_ids:
            self.log.debug("Looking at test run with id %s", testrun_id)

            testrun_directories = self._get_testrun_directories(testrun_id)

            if len(testrun_directories) > 1:
                self.log.debug(
                    "We have more than one directory for test run %s so using the compatibility method", testrun_id
                )
                self._process_compatibility_mode()
                # For compatibility mode, still need to add metadata and write
                self._add_common_metadata()
                self._add_peak_metrics()
            else:
                # Memory-efficient mode: results written during _process_single_testrun
                self._process_single_testrun(testrun_directories[0])

    def _find_maximum_bandwidth_and_iops_with_latency(
        self, test_run_data: CommonFormatDataType
    ) -> tuple[str, str, str, str]:
        """
        Find the maximum bandwidth and associated latency and the maximum iops
        and associated latency for a given test.

        Args:
            test_run_data: Dictionary containing test run data with bandwidth, iops, and latency info

        Returns:
            A tuple of (max_bandwidth, bandwidth_latency_ms, max_iops, iops_latency_ms).
            All values are returned as strings.
        """
        max_bandwidth: float = 0
        max_iops: float = 0
        bandwidth_latency_ms: float = 0
        iops_latency_ms: float = 0

        for _, data in test_run_data.items():
            if isinstance(data, dict):
                # latency is in ns, and we want to convert to ms
                if float(data["bandwidth_bytes"]) > max_bandwidth:
                    max_bandwidth = float(data["bandwidth_bytes"])
                    bandwidth_latency_ms = float(float(data["latency"]) / (1000 * 1000))
                if float(data["iops"]) > max_iops:
                    max_iops = float(data["iops"])
                    iops_latency_ms = float(float(data["latency"]) / (1000 * 1000))

        return (f"{max_bandwidth}", f"{bandwidth_latency_ms}", f"{max_iops}", f"{iops_latency_ms}")

    def _find_max_resource_usage(self, test_run_data: CommonFormatDataType) -> tuple[str, str]:
        """
        Record the maximum CPU usage and maximum memory usage for this workload.

        Args:
            test_run_data: Dictionary containing test run data with CPU and memory usage info

        Returns:
            A tuple of (max_cpu, max_memory) as strings
        """
        max_cpu: float = 0
        max_memory: float = 0

        for _, data in test_run_data.items():
            if isinstance(data, dict):
                max_cpu = max(max_cpu, float(data["cpu"]))
                # max memory here, when we start recording it

        return f"{max_cpu}", f"{max_memory}"
