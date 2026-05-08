"""
Process data from an FIO benchmark run
"""

from logging import Logger, getLogger

from post_processing.common import get_blocksize, sum_mean_values, sum_standard_deviation_values
from post_processing.post_processing_types import (
    IodepthDataType,
    JobsDataType,
)
from post_processing.run_results.benchmarks.benchmark_result import BenchmarkResult

log: Logger = getLogger("formatter")


class FIO(BenchmarkResult):
    """
    Stores and processes the data from an FIO benchmark run
    """

    @property
    def source(self) -> str:
        return "fio"

    def _get_global_options(self, fio_global_options: dict[str, str]) -> dict[str, str]:
        """
        read the data from the 'global options' section of the fio output
        """
        blocksize: str = get_blocksize(f"{fio_global_options['bs']}")
        global_options_details: dict[str, str] = {
            "number_of_jobs": f"{fio_global_options['numjobs']}",
            "runtime_seconds": f"{fio_global_options['runtime']}",
            "blocksize": blocksize,
        }
        self._number_of_jobs = f"{fio_global_options['numjobs']}"

        # if rwmixread exists in the output then so does rwmixwrite
        if fio_global_options.get("rwmixread", None):
            # if "rwmixread" in fio_global_options.keys():
            global_options_details["percentage_reads"] = f"{fio_global_options['rwmixread']}"
            global_options_details["percentage_writes"] = f"{fio_global_options['rwmixwrite']}"

        self._global_options = global_options_details

        return global_options_details

    # pylint: disable=[too-many-locals]
    def _get_io_details(self, all_jobs: JobsDataType) -> IodepthDataType:
        """
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

        combined_mean_latency = sum_mean_values(latencies, operations, total_ios)

        latency_standard_deviation = sum_standard_deviation_values(
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

    def _get_iodepth(self, iodepth_value: str) -> str:
        log.debug("Getting iodepth from %s and %s", iodepth_value, self._resource_file_path)
        iodepth: int = int(iodepth_value)
        logfile_name: str = f"{self._resource_file_path}"

        logfile_iodepth: int = 0

        # New workloads
        for value in logfile_name.split("/"):
            if "total_iodepth" in value:
                logfile_iodepth = int(value[len("total_iodepth") + 1 :])
                break

            elif "iodepth" in value:
                logfile_iodepth = int(value[len("iodepth") + 1 :])

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

        iodepth = max(iodepth, logfile_iodepth)

        log.debug("iodepth value is %s", iodepth)
        return str(iodepth)
