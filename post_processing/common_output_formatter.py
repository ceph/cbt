"""
This file contains the code to convert the output of any benchmark run into
a common format that can then be used to graph the results.

Eventually the intenty is to support all benchmark types, but we will start
with fio.

The output is a JSON file of the format:
{
  operation : {
                 blocksize: {
                              maximum_bandwidth:
                              latency_at_max_bandwidth:
                              maximum_iops:
                              latency_at_max_iops:
                              queue_depth : {
                                              number_of_jobs :
                                              runtime_seconds:
                                              io_bytes:
                                              bandwidth_bytes:
                                              iops:
                                              latency:
                                              std_deviation:
                                              total_ios:
                              }
                              ...
                              queue_depth_n : {
                              }
                 }
                 ...
                 blocksize_n {
                 }
  }
  ...
  operation_n: {}
}

The queue depth details are the sum of the details for write operation
and the details for read operations
"""

import json
import os
from logging import Logger, getLogger
from math import sqrt
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union

from common import make_remote_dir  # pyright: ignore[reportUnknownVariableType]

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
    DEFAULT_OUTPUT_FILE_PART: str = "json_output*"

    def __init__(self, archive_directory: str, filename_root: Optional[str] = None) -> None:
        self._directory: str = archive_directory
        self._filename_root: str = filename_root if filename_root else self.DEFAULT_OUTPUT_FILE_PART

        self._formatted_output: Dict[str, Dict[str, Dict[str, Union[str, Dict[str, str]]]]] = {}
        self._all_test_run_ids: Set[str] = set()
        # Note that we use a set here as it does not allow duplicate entries,
        # and we do not care about ordering. It would be possible to use a List
        # and manually check for duplictaes, but that seems more untidy
        # TODO: This is the whole archive directory - what happens if I want
        # to specify a single run? How full do these get?

        self._path: Path
        self._file_list: Generator[Path]

    def convert_all_files(self) -> None:
        """
        Convert all files in a given directory to our internal format and then
        write out the intermediate file that can then be used to produce a graph

        FUTURE: This will work for a test that uses a single volume. In the
        future it needs to be extended to cope with multiple volumes in a
        single test.
        """

        self._find_all_results_files_in_directory()

        self._find_all_testrun_ids()
        for id in self._all_test_run_ids:
            for file_path in self._find_files_for_testrun_with_id(id):
                if not self._file_is_empty(str(file_path)):
                    self._convert_file(str(file_path))
                else:
                    log.warning("Cannot process file %s as it is empty", file_path)

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
            make_remote_dir(destination_directory)  # type: ignore[no-untyped-call]

        for operation_type in self._formatted_output.keys():
            for blocksize in self._formatted_output[operation_type].keys():
                destination_filename: str = f"{self._directory}/visualisation/{blocksize}_{operation_type}.json"
                log.info("Writing formatted results to destination file %s", destination_filename)
                with open(destination_filename, "w", encoding="utf8") as output:
                    json.dump(self._formatted_output[operation_type][blocksize], output, indent=4, sort_keys=True)

    def data_has_been_converted(self) -> bool:
        """
        return True if the test run data has been converted to the common
        format, else False
        """
        return self._formatted_output != {}

    def _find_all_results_files_in_directory(self) -> None:
        """
        find the files of interesting the archive directory we have been given
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
        self._file_list = self._path.glob(f"**/{self._filename_root}")

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

    def _find_files_for_testrun_with_id(self, testrun_id: str) -> Generator[Path, None, None]:
        """
        Return the files for a particular test run
        """
        return self._path.glob(f"**/{testrun_id}/{self._filename_root}")

    def _convert_file(self, file_name: str) -> None:
        """
        convert the contents of a single output file from the benchmark into the
        JSON format we want for writing the graphs
        """
        with open(file_name, "r", encoding="utf8") as file:
            data: Dict[str, Any] = json.load(file)
            iodepth: str = f"{data['global options']['iodepth']}"
            blocksize: str = f"{data['global options']['bs']}"
            operation: str = f"{data['global options']['rw']}"
            blocksize_details: Dict[str, Dict[str, Union[str, Dict[str, str]]]] = {blocksize: {}}
            iodepth_details: Dict[str, Dict[str, str]] = {iodepth: self._get_global_options(data["global options"])}

            if "percentage_reads" in iodepth_details[iodepth].keys():
                operation = f"{iodepth_details[iodepth]['percentage_reads']}_{iodepth_details[iodepth]['percentage_writes']}_{operation}"

            iodepth_details[iodepth].update(self._get_io_details(all_jobs=data["jobs"]))
            blocksize_details[blocksize].update(iodepth_details)

            if operation in self._formatted_output.keys():
                if blocksize in self._formatted_output[operation].keys():
                    self._formatted_output[operation][blocksize].update(iodepth_details)
                else:
                    self._formatted_output[operation].update(blocksize_details)
            else:
                self._formatted_output.update({operation: blocksize_details})

    def _get_global_options(self, fio_global_options: Dict[str, str]) -> Dict[str, str]:
        """
        read the data from the 'global options' section of the fio output
        """
        global_options_details: Dict[str, str] = {
            "number_of_jobs": f"{fio_global_options['numjobs']}",
            "runtime_seconds": f"{fio_global_options['runtime']}",
            "blocksize": f"{fio_global_options['bs'][:-1]}",
        }

        # if rwmixread exists in the output then so does rwmixwrite
        if "rwmixread" in fio_global_options.keys():
            global_options_details["percentage_reads"] = f"{fio_global_options['rwmixread']}"
            global_options_details["percentage_writes"] = f"{fio_global_options['rwmixwrite']}"

        return global_options_details

    def _get_io_details(
        self, all_jobs: List[Dict[str, Union[str, Dict[str, Union[int, float, Dict[str, Union[int, float]]]]]]]
    ) -> Dict[str, str]:
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
        jobs_of_interest: List[str] = ["read", "write"]
        io_details: Dict[str, str] = {}
        io_bytes: int = 0
        bw_bytes: int = 0
        weighted_latency: float = 0
        weighted_stddev: float = 0
        latency_standard_deviation: float = 0
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
                    total_ios += num_ops
                    assert isinstance(job_data["clat_ns"], dict)
                    mean: float = float(job_data["clat_ns"]["mean"])
                    stddev: float = float(job_data["clat_ns"]["stddev"])
                    # for mean latency and standard deviation we can't just combine the individual values, we have to
                    # apply the correct mathematical formula.
                    #
                    # for the combined mean we need to store mean_latency * num_ops for each
                    weighted_latency += mean * num_ops

                    # For standard deviation this is more complex. For each job we need to calculate:
                    #    (num_ops - 1) * std_dev^2 + num_ops1 * mean_latency1^2
                    weighted_stddev += (num_ops - 1) * stddev * stddev + num_ops * (mean * mean)

        # Perform the final part of the combined latency calculation
        combined_mean_latency: float = weighted_latency / total_ios

        # and the final standard deviation calculation
        # sqrt( (weighted_stddev - (total_ios)*combined_latency^2) / total_ios - 1)
        latency_standard_deviation = sqrt(
            (weighted_stddev - total_ios * combined_mean_latency * combined_mean_latency) / (total_ios - 1)
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

    def _file_is_empty(self, file_name: str) -> bool:
        """
        returns true if the input file contains no data
        """
        file_is_empty: bool = False
        try:
            file_is_empty = os.path.getsize(file_name) == 0
        except OSError:
            file_is_empty = True
        return file_is_empty

    def _find_maximum_bandwidth_and_iops_with_latency(
        self, test_run_data: Dict[str, Union[str, Dict[str, str]]]
    ) -> Tuple[str, str, str, str]:
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
