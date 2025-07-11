"""
Data from a test run in fio output files
"""

import json
import re
from logging import Logger, getLogger
from math import sqrt
from pathlib import Path
from typing import Any, Optional, Union

from post_processing.common import recursive_search
from post_processing.types import (
    INTERNAL_BLOCKSIZE_DATA_TYPE,
    INTERNAL_FORMATTED_OUTPUT_TYPE,
    IODEPTH_DETAILS_TYPE,
    JOBS_DATA_TYPE,
)

log: Logger = getLogger("formatter")


class TestRunResult:
    def __init__(self, archive_directory: str, test_run_id: str, file_name_root: str) -> None:
        self._archive_path: Path = Path(archive_directory)
        self._id: str = test_run_id
        self._has_been_processed: bool = False

        self._files: list[Path] = self._find_files_for_testrun_with_id(
            testrun_id=test_run_id, file_name_root=file_name_root
        )
        self._processed_data: INTERNAL_FORMATTED_OUTPUT_TYPE = {}

    def have_been_processed(self) -> bool:
        """
        True if we have already processed the files for this set of results,
        otherwise False
        """
        return self._has_been_processed

    def process(self) -> None:
        """
        Convert the results data from all the individual files that make up this
        result into the standard intermediate format
        """
        number_of_volumes_for_test_run: int = len(self._files)

        if number_of_volumes_for_test_run == 0:
            log.warning("test run ID %s has no files - not doing any conversion", self._id)
            self._has_been_processed = True
            return

        else:
            self._process_test_run_files()

        self._has_been_processed = True

    def get(self) -> INTERNAL_FORMATTED_OUTPUT_TYPE:
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
            if not self._file_is_empty(file_path):
                if not self._file_is_precondition(file_path):
                    self._convert_file(file_path)
                else:
                    log.warning("Not processing file %s as it is from a precondition operation", file_path)
                    self._files.remove(file_path)
            else:
                log.warning("Cannot process file %s as it is empty", file_path)

    def _convert_file(self, file_path: Path) -> None:
        """
        convert the contents of a single output file from the benchmark into the
        JSON format we want for writing the graphs
        """

        with open(str(file_path), "r", encoding="utf8") as file:
            data: dict[str, Any] = json.load(file)
            filename: Optional[str] = recursive_search(data, "write_iops_log")
            iodepth: str = self._get_iodepth(f"{data['global options']['iodepth']}", filename)

            blocksize: str = self._get_blocksize(f"{data['global options']['bs']}")
            if re.search("\D$", blocksize):  # pyright: ignore[reportInvalidStringEscapeSequence]
                blocksize = blocksize[:-1]
            operation: str = f"{data['global options']['rw']}"
            global_details: IODEPTH_DETAILS_TYPE = self._get_global_options(data["global options"])
            blocksize_details: INTERNAL_BLOCKSIZE_DATA_TYPE = {blocksize: {}}
            iodepth_details: dict[str, IODEPTH_DETAILS_TYPE] = {iodepth: global_details}

            io_details: IODEPTH_DETAILS_TYPE = {}

            if "percentage_reads" in global_details.keys():
                operation = f"{global_details['percentage_reads']}_{global_details['percentage_writes']}_{operation}"

            if operation in self._processed_data.keys():
                if blocksize in self._processed_data[operation].keys():
                    if iodepth in self._processed_data[operation][blocksize].keys():
                        # we already have data here, so use it
                        io_details = self._sum_io_details(
                            self._processed_data[operation][blocksize][iodepth],
                            self._get_io_details(all_jobs=data["jobs"]),
                        )

            if io_details == {}:
                io_details = self._get_io_details(all_jobs=data["jobs"])

            iodepth_details[iodepth].update(io_details)
            blocksize_details[blocksize].update(iodepth_details)

            if operation in self._processed_data.keys():
                if blocksize in self._processed_data[operation].keys():
                    self._processed_data[operation][blocksize].update(iodepth_details)
                else:
                    self._processed_data[operation].update(blocksize_details)
            else:
                self._processed_data.update({operation: blocksize_details})

    def _get_global_options(self, fio_global_options: dict[str, str]) -> dict[str, str]:
        """
        read the data from the 'global options' section of the fio output
        """
        blocksize: str = self._get_blocksize(f"{fio_global_options['bs']}")
        global_options_details: dict[str, str] = {
            "number_of_jobs": f"{fio_global_options['numjobs']}",
            "runtime_seconds": f"{fio_global_options['runtime']}",
            "blocksize": blocksize,
        }

        # if rwmixread exists in the output then so does rwmixwrite
        if "rwmixread" in fio_global_options.keys():
            global_options_details["percentage_reads"] = f"{fio_global_options['rwmixread']}"
            global_options_details["percentage_writes"] = f"{fio_global_options['rwmixwrite']}"

        return global_options_details

    def _sum_io_details(
        self, existing_values: Union[str, IODEPTH_DETAILS_TYPE], new_values: IODEPTH_DETAILS_TYPE
    ) -> IODEPTH_DETAILS_TYPE:
        """
        sum the existing_values with new_values and return the result
        """
        assert isinstance(existing_values, dict)
        combined_data: IODEPTH_DETAILS_TYPE = {}

        simple_sum_values: list[str] = ["io_bytes", "iops", "bandwidth_bytes"]

        for value in simple_sum_values:
            combined_data[value] = f"{float(existing_values[value]) + float(new_values[value])}"

        combined_data["total_ios"] = f"{int(existing_values['total_ios']) + int(new_values['total_ios'])}"

        latencies: list[float] = [float(existing_values["latency"]), float(new_values["latency"])]
        operations: list[int] = [int(existing_values["total_ios"]), int(new_values["total_ios"])]
        std_deviations: list[float] = [float(existing_values["std_deviation"]), float(new_values["std_deviation"])]

        combined_latency: float = self._sum_mean_values(
            latencies,
            operations,
            int(combined_data["total_ios"]),
        )

        combined_std_dev: float = self._sum_standard_deviation_values(
            std_deviations, operations, latencies, int(combined_data["total_ios"]), combined_latency
        )

        combined_data["latency"] = f"{combined_latency}"
        combined_data["std_deviation"] = f"{combined_std_dev}"

        return combined_data

    def _find_files_for_testrun_with_id(self, testrun_id: str, file_name_root: str) -> list[Path]:
        """
        Return the files for a particular test run
        """
        # We need to use a list here as we can possibly iterate over the file
        # list multiple times, and a Generator object only allows iterating
        # once
        return [
            path
            for path in self._archive_path.glob(f"**/{testrun_id}/**/{file_name_root}.*")
            if re.search(f"{file_name_root}.\d+$", f"{path}")  # pyright: ignore[reportInvalidStringEscapeSequence]
        ]

    def _file_is_empty(self, file_path: Path) -> bool:
        """
        returns true if the input file contains no data
        """
        return file_path.stat().st_size == 0

    def _file_is_precondition(self, file_path: Path) -> bool:
        """
        Check if a file is from a precondition part of a test run
        """
        return "precond" in str(file_path)

    def _get_io_details(self, all_jobs: JOBS_DATA_TYPE) -> IODEPTH_DETAILS_TYPE:
        """
        Get all the required details for the total I/O submitted by fio.

        In the fio output the details are split by operation (read, write) so to
        get the total IO numbers we need to sum together the details for read and write.
        For a single operation e.g. read then the write details will all be 0 so
        this still gives the correct values.

        The values of interest are:
        io_bytes
        bw_bytes
        clat_ns/mean
        clat_ns/stddev
        iops
        total_ios
        """
        jobs_of_interest: list[str] = ["read", "write"]
        io_details: dict[str, str] = {}
        io_bytes: int = 0
        bw_bytes: int = 0
        latencies: list[float] = []
        operations: list[int] = []
        std_deviations: list[float] = []
        io_operations_second: float = 0
        total_ios: int = 0

        for entry in all_jobs:  # A single run in the json
            for job, job_data in entry.items():
                if job in jobs_of_interest and isinstance(job_data, dict):
                    assert isinstance(job_data["io_bytes"], int)
                    io_bytes += job_data["io_bytes"]
                    assert isinstance(job_data["bw_bytes"], int)
                    bw_bytes += job_data["bw_bytes"]
                    assert isinstance(job_data["iops"], float)
                    io_operations_second += job_data["iops"]
                    assert isinstance(job_data["total_ios"], int)
                    num_ops: int = job_data["total_ios"]
                    operations.append(num_ops)
                    total_ios += num_ops
                    assert isinstance(job_data["clat_ns"], dict)
                    latencies.append(float(job_data["clat_ns"]["mean"]))
                    std_deviations.append(float(job_data["clat_ns"]["stddev"]))

        combined_mean_latency = self._sum_mean_values(latencies, operations, total_ios)

        latency_standard_deviation = self._sum_standard_deviation_values(
            std_deviations, operations, latencies, total_ios, combined_mean_latency
        )

        io_details = {
            "io_bytes": f"{io_bytes}",
            "bandwidth_bytes": f"{bw_bytes}",
            "iops": f"{io_operations_second}",
            "latency": f"{combined_mean_latency}",
            "std_deviation": f"{latency_standard_deviation}",
            "total_ios": f"{total_ios}",
        }

        return io_details

    def _sum_mean_values(self, latencies: list[float], num_ops: list[int], total_ios: int) -> float:
        """
        Calculate the sum of mean latency values.

        As these values are means we cannot simply add them together.
        Instead we must apply the mathematical formula:

        combined mean = sum( mean * num_ops ) / total operations
        """
        weighted_latency: float = 0

        # for the combined mean we need to store mean_latency * num_ops for each
        # set of values
        for index, latency in enumerate(latencies):
            weighted_latency += latency * num_ops[index]

        combined_mean_latency: float = weighted_latency / total_ios

        return combined_mean_latency

    def _sum_standard_deviation_values(
        self,
        std_deviations: list[float],
        operations: list[int],
        latencies: list[float],
        total_ios: int,
        combined_latency: float,
    ) -> float:
        """
        Sum the standard deviations from a number of test runs

        For each run we need to calculate:
        weighted_stddev = sum ((num_ops - 1) * std_dev^2 + num_ops1 * mean_latency1^2)

        sqrt( (weighted_stddev - (total_ios)*combined_latency^2) / total_ios - 1)
        """
        weighted_stddev: float = 0

        # For standard deviation this is more complex. For each job we need to calculate:
        #    (num_ops - 1) * std_dev^2 + num_ops1 * mean_latency1^2
        for index, stddev in enumerate(std_deviations):
            weighted_stddev += (operations[index] - 1) * stddev * stddev + operations[index] * (
                latencies[index] * latencies[index]
            )

        latency_standard_deviation: float = sqrt(
            (weighted_stddev - total_ios * combined_latency * combined_latency) / (total_ios - 1)
        )

        return latency_standard_deviation

    def _get_iodepth(self, iodepth_value: str, logfile_name: Optional[str]) -> str:
        """
        Checks to see if the iodepth encoded in the logfile name matches
        the iodepth in the output file. If it does, return the iodepth
        from the file, otherwise return the iodepth parsed from the
        log file path
        """
        iodepth: int = int(iodepth_value)
        if logfile_name is not None:
            # We need to cope with both the separate workload class directory structure
            # as well as the older style non-class workload deirectory structure
            logfile_iodepth: int = 0

            # New workloads
            for value in logfile_name.split("/"):
                if "total_iodepth" in value:
                    logfile_iodepth = int(value[len("total_iodepth") + 1 :])

            # Old-style workloads
            if not logfile_iodepth:
                # the logfile name is of the format:
                #  /tmp/cbt/00000000/LibrbdFio/randwrite_1048576/iodepth-001/numjobs-001/output.0
                iodepth_start_index: int = logfile_name.find("iodepth")
                numjobs_start_index: int = logfile_name.find("numjobs")
                # an index of -1 is no match found, so do nothing
                if iodepth_start_index != -1 and numjobs_start_index != -1:
                    iodepth_end_index: int = iodepth_start_index + len("iodepth")
                    iodepth_string: str = logfile_name[iodepth_end_index + 1 : numjobs_start_index - 1]
                    logfile_iodepth = int(iodepth_string)

            if logfile_iodepth > iodepth:
                iodepth = logfile_iodepth

        return str(iodepth)

    def _get_blocksize(self, blocksize_value: str) -> str:
        """
        return a blocksize value without the units
        """
        blocksize: str = blocksize_value
        if re.search("\D+$", blocksize):  # pyright: ignore[reportInvalidStringEscapeSequence]
            blocksize = blocksize[:-1]

        return blocksize


__test__ = False
