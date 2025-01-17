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
from logging import Logger, getLogger
from pathlib import Path
from typing import Generator, Optional

from common import pdsh  # make_remote_dir  # pyright: ignore[reportUnknownVariableType]
from post_processing.formatter.test_run_result import TestRunResult
from post_processing.types import (
    COMMON_FORMAT_FILE_DATA_TYPE,
    INTERNAL_FORMATTED_OUTPUT_TYPE,
)

log: Logger = getLogger("cbt")


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

        self._formatted_output: INTERNAL_FORMATTED_OUTPUT_TYPE = {}
        self._all_test_run_ids: set[str] = set()
        # Note that we use a set here as it does not allow duplicate entries,
        # and we do not care about ordering. It would be possible to use a List
        # and manually check for duplictaes, but that seems more untidy
        # TODO: This is the whole archive directory - what happens if I want
        # to specify a single run? How full do these get?

        self._path: Path
        self._file_list: Generator[Path, None, None]

    def convert_all_files(self) -> None:
        """
        Convert all files in a given directory to our internal format and then
        write out the intermediate file that can then be used to produce a graph
        """

        self._find_all_results_files_in_directory()

        self._find_all_testrun_ids()
        for id in self._all_test_run_ids:
            results: TestRunResult = TestRunResult(self._directory, id, self._filename_root)

            results.process()
            self._formatted_output.update(results.get())

        # get the max bandwidth and associated latency for each test run
        for operation in self._formatted_output.keys():
            for blocksize in self._formatted_output[operation].keys():
                max_bandwidth, max_bandwidth_latency, max_iops, max_iops_latency = (
                    self._find_maximum_bandwidth_and_iops_with_latency(self._formatted_output[operation][blocksize])
                )
                self._formatted_output[operation][blocksize]["maximum_bandwidth"] = max_bandwidth
                self._formatted_output[operation][blocksize]["latency_at_max_bandwidth"] = max_bandwidth_latency
                self._formatted_output[operation][blocksize]["maximum_iops"] = max_iops
                self._formatted_output[operation][blocksize]["latency_at_max_iops"] = max_iops_latency

    def write_output_file(self) -> None:
        """
        Write the formatted output to the output file in JSON format
        """
        destination_directory: str = f"{self._directory}/visualisation/"

        if not Path(destination_directory).is_dir():
            pdsh("localhost", f"mkdir -p {destination_directory}").communicate()  # type: ignore[no-untyped-call]
            # make_remote_dir(destination_directory)  # type: ignore[no-untyped-call]

        for operation_type in self._formatted_output.keys():
            for blocksize in self._formatted_output[operation_type].keys():
                destination_filename: str = f"{self._directory}/visualisation/{blocksize}_{operation_type}.json"
                log.info("Writing formatted results to destination file %s", destination_filename)
                with open(destination_filename, "w", encoding="utf8") as output:
                    json.dump(self._formatted_output[operation_type][blocksize], output, indent=4, sort_keys=True)

    def _find_all_results_files_in_directory(self) -> None:
        """
        find the files of interest in the archive directory we have been given
        """
        log.debug(
            "Finding all %s* files from %s"
            % (
                self._filename_root,
                self._directory,
            )
        )
        self._path = Path(self._directory)
        # this gives a generator where each contained object is a Path of format:
        # <self._directory>/results/<iteration>/<run_id>/json_output.<vol_id>.<hostname>
        self._file_list = self._path.glob(f"**/{self._filename_root}.?")

    def _find_all_testrun_ids(self) -> None:
        """
        Find all the unique test run IDs in the output directory. We will need
        these to allow us to collect the data we require from a test run

        Note: This may only work for fio output runs in cbt, and we will need
        separate sub-classes for each benchmark type to be able to find and
        parse the required data
        """

        for file_path in self._file_list:
            # We know that the output files reside in the directory with the
            # test run ID, so splitting up the path gives the filename as the
            # last element (-1) and the test run id directory as the penultimate
            # element (-2)
            # This should allow us to get test run IDs when any point in the
            # archive directory tree is passed as the archive directory
            self._all_test_run_ids.add(file_path.parts[-2])

    def _find_maximum_bandwidth_and_iops_with_latency(
        self, test_run_data: COMMON_FORMAT_FILE_DATA_TYPE
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
