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
    }
    ...
    queue_depth_n {

    }
    maximum_bandwidth:
    latency_at_max_bandwidth:
    maximum_iops:
    latency_at_max_iops:
}

The queue depth details are the sum of the details for write operation
and the details for read operations
"""

import json
import re
from logging import Logger, getLogger
from pathlib import Path
from typing import Optional

# pylint: disable=[no-name-in-module]
from common import pdsh  # pyright: ignore[reportUnknownVariableType]
from post_processing.formatter.benchmark_run_result import BenchmarkRunResult
from post_processing.types import CommonFormatDataType, InternalFormattedOutputType

log: Logger = getLogger("formatter")


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
        # and manually check for duplictaes, but that seems more untidy

        self._path: Path
        self._file_list: list[Path] = []

    def convert_all_files(self) -> None:
        """
        Convert all files in a given directory to our internal format and then
        write out the intermediate file that can then be used to produce a graph
        """
        log.info("Converting all files with name %s in directory %s", self._directory, self._filename_root)
        self._find_all_results_files_in_directory()

        self._find_all_testrun_ids()
        for testrun_id in self._all_test_run_ids:
            log.debug("Looking at test run with id %s", testrun_id)

            # Actually find the test run ID directory
            testrun_directories: list[Path] = list(self._path.glob(f"**/{testrun_id}"))
            if len(testrun_directories) > 1:
                log.debug(
                    "We have more than one directory for test run %s so using the compatibility method", testrun_id
                )
                results: BenchmarkRunResult = BenchmarkRunResult(Path(self._directory), self._filename_root)

                results.process()
                self._formatted_output.update(results.get())

            else:
                testrun_directory_path: Path = testrun_directories[0]

                for io_pattern_directory in [
                    directory for directory in testrun_directory_path.iterdir() if directory.is_dir()
                ]:
                    log.debug("Looking at results for directory %s", io_pattern_directory)
                    results: BenchmarkRunResult = BenchmarkRunResult(
                        directory=io_pattern_directory, file_name_root=self._filename_root
                    )
                    results.process()
                    processed_results: InternalFormattedOutputType = results.get()
                    for run_type, run_data in processed_results.items():
                        if self._formatted_output.get(run_type, None):
                            # if run_type in self._formatted_output.keys():
                            self._formatted_output[run_type].update(run_data)
                        else:
                            self._formatted_output.update(results.get())

        # get the max bandwidth and associated latency for each test run
        for _, operation_data in self._formatted_output.items():
            for _, blocksize_data in operation_data.items():
                max_bandwidth, max_bandwidth_latency, max_iops, max_iops_latency = (
                    self._find_maximum_bandwidth_and_iops_with_latency(blocksize_data)
                )
                blocksize_data["maximum_bandwidth"] = max_bandwidth
                blocksize_data["latency_at_max_bandwidth"] = max_bandwidth_latency
                blocksize_data["maximum_iops"] = max_iops
                blocksize_data["latency_at_max_iops"] = max_iops_latency

    def write_output_file(self) -> None:
        """
        Write the formatted output to the output file in JSON format
        """

        destination_directory: str = f"{self._directory}/visualisation/"
        log.info("writing new format files to %s", destination_directory)

        if not Path(destination_directory).is_dir():
            pdsh("localhost", f"mkdir -p {destination_directory}").communicate()  # type: ignore[no-untyped-call]

        for operation_type, operation_data in self._formatted_output.items():
            for blocksize, blocksize_data in operation_data.items():
                destination_filename: str = f"{self._directory}/visualisation/{blocksize}_{operation_type}.json"
                log.info("Writing formatted results to destination file %s", destination_filename)
                with open(destination_filename, "w", encoding="utf8") as output:
                    json.dump(blocksize_data, output, indent=4, sort_keys=True)

    def _find_all_results_files_in_directory(self) -> None:
        """
        find the files of interest in the archive directory we have been given
        """
        log.debug("Finding all %s* files from %s", self._filename_root, self._directory)

        self._path = Path(self._directory)
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
        find the maximum bandwith and associated latency and the maximum iops
        and associated latency for a given test
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

    def _find_unique_results_directories(self) -> list[Path]:
        """
        Find all the unique results directories that contain data for a single
        run
        """
        unique_directories: list[Path] = []

        return unique_directories
