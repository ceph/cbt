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
from logging import Logger, getLogger
from pathlib import Path
from pprint import pprint
from typing import Optional

from common import pdsh  # pyright: ignore[reportUnknownVariableType]
from post_processing.post_processing_types import CommonFormatDataType, InternalFormattedOutputType
from post_processing.run_results.run_result_factory import get_run_result_from_directory_name

log: Logger = getLogger(name="formatter")


class CommonOutputFormatter:
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
        self._directory: str = archive_directory
        self._filename_root: str = filename_root if filename_root else self.DEFAULT_OUTPUT_FILE_PART

        self._formatted_output: InternalFormattedOutputType = {}
        self._all_test_run_ids: set[str] = set()
        # Note that we use a set here as it does not allow duplicate entries,
        # and we do not care about ordering. It would be possible to use a List
        # and manually check for duplicates, but that seems more untidy

        self._path: Path = Path(self._directory)
        self._file_list: list[Path] = []
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
        for run_type, run_data in processed_results.items():
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

    def _get_testrun_directories(self, testrun_id: str) -> list[Path]:
        """
        Get the directories for a specific test run ID.

        When calling from CBT itself, the archive dir already includes the testrun
        directory, so we handle this case by checking if 'id-' is in the path.

        Args:
            testrun_id: The test run identifier to find directories for

        Returns:
            List of Path objects for directories matching the test run ID
        """
        if "id-" in f"{self._path}":
            return [self._path]
        return list(self._path.glob(f"**/{testrun_id}"))

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
        processes each one, and merges the results into the formatted output.

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
            log.debug("Looking at results for directory %s", io_pattern_directory)
            # Use factory method to get the correct results type based on directory name
            results = get_run_result_from_directory_name(
                directory=io_pattern_directory, file_name_root=self._filename_root
            )

            results.process()
            processed_results = results.get()
            self._merge_results(processed_results)
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

    def convert_all_files(self) -> None:
        """
        Convert all files in a given directory to our internal format and then
        write out the intermediate file that can then be used to produce a graph
        """
        log.info("Converting all files with name %s in directory %s", self._filename_root, self._directory)
        self._find_all_results_files_in_directory()
        self._find_all_testrun_ids()

        for testrun_id in self._all_test_run_ids:
            log.debug("Looking at test run with id %s", testrun_id)

            testrun_directories = self._get_testrun_directories(testrun_id)

            if len(testrun_directories) > 1:
                log.debug(
                    "We have more than one directory for test run %s so using the compatibility method", testrun_id
                )
                benchmark_type = self._process_compatibility_mode()
            else:
                benchmark_type = self._process_single_testrun(testrun_directories[0])

            # Track benchmark type for all operations processed in this test run
            for operation_type in self._formatted_output.keys():
                self._benchmark_types[operation_type] = benchmark_type

        # Add common metadata fields (benchmark type, number_of_jobs) to the test run data
        self._add_common_metadata()
        # Add the maximum values (max bandwidth, IOPS, resource usage) to the test run data
        self._add_peak_metrics()

    def write_output_file(self) -> None:
        """
        Write the formatted output to the output file in JSON format
        """

        destination_directory: str = f"{self._directory}/visualisation/"
        log.info("writing new format files to %s", destination_directory)

        if not Path(destination_directory).is_dir():
            pdsh("localhost", f"mkdir -p {destination_directory}").communicate()  # type: ignore[no-untyped-call]

        for operation_type, operation_data in self._formatted_output.items():
            for number_of_jobs, numbjob_data in operation_data.items():
                for blocksize, blocksize_data in numbjob_data.items():
                    destination_filename: str = (
                        f"{self._directory}/visualisation/{blocksize}_{number_of_jobs}_{operation_type}.json"
                    )
                    log.info("Writing formatted results to destination file %s", destination_filename)
                    with open(destination_filename, "w", encoding="utf8") as output:
                        json.dump(blocksize_data, output, indent=4, sort_keys=True)

    def _find_all_results_files_in_directory(self) -> None:
        """
        find the files of interest in the archive directory we have been given
        """
        log.debug("Finding all %s* files from %s", self._filename_root, self._directory)

        # this gives a generator where each contained object is a Path of format:
        # <self._directory>/results/<iteration>/<run_id>/json_output.<vol_id>
        self._file_list = [
            path
            for path in self._path.glob(f"**/{self._filename_root}.*")
            if re.search(rf"{self._filename_root}.\d+$", f"{path}")
        ]

    def _find_all_testrun_ids(self) -> None:
        """
        Find all the unique test run IDs in the output directory. We will need
        these to allow us to collect the data we require from a test run

        Note: This may only work for fio output runs in cbt, and we will need
        separate sub-classes for each benchmark type to be able to find and
        parse the required data
        """

        for file_path in self._file_list:
            # We know the format of the output dir is something like
            # <archive_dir>/00000000/id-ab40819c/<job_specific_details>/output.x
            #
            # We know that the output files reside in the directory structure with the
            # test run ID, so splitting up the path gives the filename as the
            # last element (-1) and the test run id directory somewhere higher up
            # the directory structure.
            # This should allow us to get test run IDs when any point in the
            # archive directory tree is passed as the archive directory
            potential_ids: list[str] = [part for part in file_path.parts if "id-" in part]
            # There is a possibility that there could be more than one id-xxxxxx string in the
            # file path, and we want only one. We choose to always take the fist one.
            # If there are none then just return the directory name above the file
            if len(potential_ids) > 0:
                testrun_id: str = potential_ids[0]
            else:
                # if we get no matches then just use the directory directly above
                # the output file
                testrun_id = file_path.parts[-2]

            self._all_test_run_ids.add(testrun_id)

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
