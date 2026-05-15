"""
Process data from an FIO benchmark run
"""

from logging import Logger, getLogger
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from post_processing.common import get_blocksize, sum_mean_values, sum_standard_deviation_values
from post_processing.parsers.fio_log_parser import FIOLogParser
from post_processing.parsers.fio_time_series_parser import FIOTimeSeriesParser
from post_processing.parsers.timestamp_aligner import TimestampAligner
from post_processing.post_processing_types import (
    IodepthDataType,
    JobsDataType,
    TimeSeriesFormatType,
)
from post_processing.run_results.benchmark_result import BenchmarkResult

log: Logger = getLogger("formatter")

# Constants for job types to process
_READ_WRITE_JOBS: frozenset[str] = frozenset(["read", "write"])


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

    def _extract_int_metric(
        self, job_data: dict[str, Union[int, float, dict[str, Union[int, float]]]], key: str, job_type: str
    ) -> int:
        """
        Safely extract and validate an integer metric from job data.

        Args:
            job_data: Dictionary containing job metrics
            key: Metric key to extract
            job_type: Job type name for error messages

        Returns:
            Integer value of the metric

        Raises:
            ValueError: If metric is missing or not an integer
        """
        value = job_data.get(key)
        if not isinstance(value, int):
            raise ValueError(f"Missing or invalid '{key}' for {job_type} job: expected int, got {type(value).__name__}")
        return value

    def _extract_float_metric(
        self, job_data: dict[str, Union[int, float, dict[str, Union[int, float]]]], key: str, job_type: str
    ) -> float:
        """
        Safely extract and validate a float metric from job data.

        Args:
            job_data: Dictionary containing job metrics
            key: Metric key to extract
            job_type: Job type name for error messages

        Returns:
            Float value of the metric

        Raises:
            ValueError: If metric is missing or not a float
        """
        value = job_data.get(key)
        if not isinstance(value, (int, float)):
            raise ValueError(
                f"Missing or invalid '{key}' for {job_type} job: expected float, got {type(value).__name__}"
            )
        return float(value)

    # pylint: disable=[too-many-locals]
    def _get_io_details(self, all_jobs: JobsDataType) -> IodepthDataType:
        """
        Aggregate IO metrics across read and write operations from FIO output.

        FIO splits metrics by operation type (read/write), so this method combines
        them to get total IO statistics. For single-operation tests (e.g., read-only),
        the unused operation will have zero values, so summation still works correctly.

        The method calculates weighted averages for latency metrics using the formula:
            combined_mean = sum(mean_i * num_ops_i) / total_ops
            combined_stddev = sqrt((sum((n_i-1)*stddev_i^2 + n_i*mean_i^2) - N*mean_combined^2) / (N-1))

        Args:
            all_jobs: List of job dictionaries from FIO JSON output, each containing
                     'read' and 'write' keys with their respective metrics

        Returns:
            Dictionary with aggregated metrics:
            - io_bytes: Total bytes transferred
            - bandwidth_bytes: Total bandwidth in bytes/sec
            - iops: Total IO operations per second
            - latency: Weighted mean completion latency in nanoseconds
            - std_deviation: Combined standard deviation of latency
            - total_ios: Total number of IO operations

        Raises:
            ValueError: If job data structure is invalid or missing required fields
        """
        # Initialize accumulators
        io_bytes: int = 0
        bw_bytes: int = 0
        io_operations_second: float = 0.0
        total_ios: int = 0

        # Lists for weighted statistical calculations
        latencies: list[float] = []
        operations: list[int] = []
        std_deviations: list[float] = []

        # Process each job entry
        for job_entry in all_jobs:
            for job_type, job_data in job_entry.items():
                # Only process read/write operations
                if job_type not in _READ_WRITE_JOBS:
                    continue

                # Validate job data structure
                if not isinstance(job_data, dict):
                    log.warning(
                        "Skipping job '%s' in %s: expected dict, got %s",
                        job_type,
                        self._resource_file_path,
                        type(job_data).__name__,
                    )
                    continue

                try:
                    # Extract and validate metrics with proper error handling
                    io_bytes += self._extract_int_metric(job_data, "io_bytes", job_type)
                    bw_bytes += self._extract_int_metric(job_data, "bw_bytes", job_type)
                    io_operations_second += self._extract_float_metric(job_data, "iops", job_type)

                    num_ops = self._extract_int_metric(job_data, "total_ios", job_type)
                    operations.append(num_ops)
                    total_ios += num_ops

                    # Extract latency statistics
                    clat_ns = job_data.get("clat_ns")
                    if not isinstance(clat_ns, dict):
                        raise ValueError(
                            f"Missing or invalid 'clat_ns' for {job_type} job: "
                            f"expected dict, got {type(clat_ns).__name__}"
                        )

                    mean_latency = float(clat_ns.get("mean", 0))
                    stddev_latency = float(clat_ns.get("stddev", 0))

                    latencies.append(mean_latency)
                    std_deviations.append(stddev_latency)

                except (KeyError, ValueError, TypeError) as e:
                    log.error(
                        "Error processing %s job in %s: %s",
                        job_type,
                        self._resource_file_path,
                        str(e),
                    )
                    raise ValueError(f"Invalid job data structure for {job_type} in {self._resource_file_path}") from e

        # Validate we have data to process
        if total_ios == 0:
            log.warning("No IO operations found in %s", self._resource_file_path)
            return {
                "io_bytes": "0",
                "bandwidth_bytes": "0",
                "iops": "0.0",
                "latency": "0.0",
                "std_deviation": "0.0",
                "total_ios": "0",
            }

        # Calculate weighted statistics
        combined_mean_latency = sum_mean_values(latencies, operations, total_ios)
        latency_standard_deviation = sum_standard_deviation_values(
            std_deviations, operations, latencies, total_ios, combined_mean_latency
        )

        # Return aggregated metrics
        return {
            "io_bytes": str(io_bytes),
            "bandwidth_bytes": str(bw_bytes),
            "iops": str(io_operations_second),
            "latency": str(combined_mean_latency),
            "std_deviation": str(latency_standard_deviation),
            "total_ios": str(total_ios),
        }

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

    def _get_log_avg_msec(self) -> int:
        """
        Extract and validate log_avg_msec from global options.

        Returns:
            The log averaging interval in milliseconds (default: 1000)
        """
        default_value = 1000
        raw_value = self._global_options.get("log_avg_msec", default_value)

        try:
            log_avg_msec = int(raw_value)
            if log_avg_msec <= 0:
                log.warning(
                    "Invalid log_avg_msec value %d (must be positive), using default %d",
                    log_avg_msec,
                    default_value,
                )
                return default_value
            return log_avg_msec
        except (ValueError, TypeError) as e:
            log.warning(
                "Cannot convert log_avg_msec '%s' to int: %s, using default %d",
                raw_value,
                e,
                default_value,
            )
            return default_value

    def _validate_log_directory(self, log_directory: Path, base_name: str) -> bool:
        """
        Validate that the log directory exists and is accessible.

        Args:
            log_directory: Path to the log directory
            base_name: Base name for log files

        Returns:
            True if valid, False otherwise
        """
        if not log_directory.exists():
            log.warning("Log directory does not exist: %s", log_directory)
            return False

        if not log_directory.is_dir():
            log.error("Log path is not a directory: %s", log_directory)
            return False

        log.debug("Looking for time-series logs for %s in %s", base_name, log_directory)
        return True

    def _parse_metric_logs(self, log_directory: Path, base_name: str) -> Optional[dict[str, Optional[pd.DataFrame]]]:
        """
        Parse all FIO metric log files.

        Args:
            log_directory: Path to the log directory
            base_name: Base name for log files

        Returns:
            Dictionary of parsed metric data, or None if no logs found
        """
        parser = FIOLogParser()

        # Define metric types and their patterns for cleaner iteration
        metric_patterns = {
            "iops": f"{base_name}_iops.*.log",
            "clat": f"{base_name}_clat.*.log",
            "bw": f"{base_name}_bw.*.log",
            "lat": f"{base_name}_lat.*.log",
            "slat": f"{base_name}_slat.*.log",
        }

        # Parse all metric types
        parsed_data = {
            metric: parser.parse_and_combine_logs(log_directory, metric, pattern)
            for metric, pattern in metric_patterns.items()
        }

        # Early return if no logs found
        if all(dataframe is None for dataframe in parsed_data.values()):
            log.debug("No time-series log files found for %s", base_name)
            return None

        log.debug("Found time-series logs for %s, formatting data", base_name)
        return parsed_data

    def _align_throughput_metrics(
        self, parsed_data: dict[str, Optional[pd.DataFrame]], aligner: TimestampAligner
    ) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        Align throughput metrics (IOPS and bandwidth) to common time windows.

        Args:
            parsed_data: Dictionary of parsed metric data
            aligner: TimestampAligner instance

        Returns:
            Tuple of (aligned_iops, aligned_bw)
        """
        aligned_iops = None
        aligned_bw = None

        if parsed_data["iops"] is not None and not parsed_data["iops"].empty:
            aligned_iops = aligner.align_and_aggregate([parsed_data["iops"]], "iops")
            log.debug("Aligned IOPS data to %d time windows", len(aligned_iops))

        if parsed_data["bw"] is not None and not parsed_data["bw"].empty:
            aligned_bw = aligner.align_and_aggregate([parsed_data["bw"]], "bandwidth_bytes")
            log.debug("Aligned bandwidth data to %d time windows", len(aligned_bw))

        return aligned_iops, aligned_bw

    def _align_latency_metrics(
        self, parsed_data: dict[str, Optional[pd.DataFrame]], aligner: TimestampAligner
    ) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        Align latency metrics (mean and max) to common time windows.

        Args:
            parsed_data: Dictionary of parsed metric data
            aligner: TimestampAligner instance

        Returns:
            Tuple of (aligned_mean_latency, aligned_max_latency)
        """
        aligned_mean_latency = None
        aligned_max_latency = None

        if parsed_data["clat"] is not None and not parsed_data["clat"].empty:
            aligned_mean_latency = aligner.align_and_aggregate([parsed_data["clat"]], "latency_ms")
            log.debug("Aligned mean latency (clat) data to %d time windows", len(aligned_mean_latency))

        if parsed_data["lat"] is not None and not parsed_data["lat"].empty:
            aligned_max_latency = aligner.align_and_aggregate([parsed_data["lat"]], "latency_ms")
            log.debug("Aligned max latency (lat) data to %d time windows", len(aligned_max_latency))

        return aligned_mean_latency, aligned_max_latency

    def _calculate_latency_percentiles(
        self, parsed_data: dict[str, Optional[pd.DataFrame]], aligner: TimestampAligner
    ) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        Calculate latency percentiles from raw clat data.

        Args:
            parsed_data: Dictionary of parsed metric data
            aligner: TimestampAligner instance

        Returns:
            Tuple of (p50_dataframe, p95_dataframe, p99_dataframe)
        """
        p50_dataframe = None
        p95_dataframe = None
        p99_dataframe = None

        if parsed_data["clat"] is not None and not parsed_data["clat"].empty:
            log.debug("Calculating latency percentiles from clat data")
            percentiles_dataframe = aligner.calculate_percentiles([parsed_data["clat"]], percentiles=[50, 95, 99])

            if not percentiles_dataframe.empty:
                # Split into separate DataFrames for each percentile
                if "p50_latency_ms" in percentiles_dataframe.columns:
                    p50_dataframe = percentiles_dataframe[["timestamp_sec", "p50_latency_ms"]].rename(
                        columns={"p50_latency_ms": "latency_ms"}
                    )
                if "p95_latency_ms" in percentiles_dataframe.columns:
                    p95_dataframe = percentiles_dataframe[["timestamp_sec", "p95_latency_ms"]].rename(
                        columns={"p95_latency_ms": "latency_ms"}
                    )
                if "p99_latency_ms" in percentiles_dataframe.columns:
                    p99_dataframe = percentiles_dataframe[["timestamp_sec", "p99_latency_ms"]].rename(
                        columns={"p99_latency_ms": "latency_ms"}
                    )
                log.debug("Calculated percentiles for %d time windows", len(percentiles_dataframe))
            else:
                log.warning("Percentile calculation returned empty DataFrame")
        else:
            log.debug("No clat data available for percentile calculation")

        return p50_dataframe, p95_dataframe, p99_dataframe

    def get_timeseries_data(self) -> Optional[TimeSeriesFormatType]:
        """
        Parse FIO time-series logs and return formatted data.

        This method looks for FIO time-series log files matching the current
        benchmark output file prefix (for example output.0_*). If found, it parses
        that single file's logs into the TimeSeriesFormatType intermediate format.

        Returns:
            TimeSeriesFormatType with time-indexed metrics if logs exist, None otherwise
        """
        try:
            log_directory = self._resource_file_path.parent
            # Strip 'json_' prefix from filename to match actual log file names
            # e.g., 'json_output.0' -> 'output.0' to match 'output.0_iops.1.log'
            base_name = self._resource_file_path.name.replace("json_", "")

            # Validate log directory
            if not self._validate_log_directory(log_directory, base_name):
                return None

            # Parse metric logs
            parsed_data = self._parse_metric_logs(log_directory, base_name)
            if parsed_data is None:
                return None

            # Align all metrics to common time windows
            log_avg_msec = self._get_log_avg_msec()
            aligner = TimestampAligner(window_size_ms=log_avg_msec)

            # Align throughput and latency metrics
            aligned_iops, aligned_bw = self._align_throughput_metrics(parsed_data, aligner)
            aligned_mean_latency, aligned_max_latency = self._align_latency_metrics(parsed_data, aligner)

            # Calculate percentiles
            p50_dataframe, p95_dataframe, p99_dataframe = self._calculate_latency_percentiles(parsed_data, aligner)

            # Get iodepth value (prefers total_iodepth if it exists)
            iodepth_value = self._get_iodepth(self.iodepth)

            # Initialize parser with aligned data including calculated percentiles
            timeseries_parser = FIOTimeSeriesParser(
                archive_directory=str(log_directory),
                benchmark="fio",
                operation=self.operation,
                blocksize=self.blocksize,
                numjobs=self._number_of_jobs,
                iodepth=iodepth_value,
                iops_df=aligned_iops,
                bandwidth_df=aligned_bw,
                mean_latency_df=aligned_mean_latency,
                max_latency_df=aligned_max_latency,
                p50_latency_df=p50_dataframe,
                p95_latency_df=p95_dataframe,
                p99_latency_df=p99_dataframe,
                num_volumes=1,
                log_avg_msec=log_avg_msec,
            )

            timeseries_parser.process()

            # Return the formatted time-series data for aggregation at RunResult level
            return timeseries_parser.get_formatted_output()

        except (OSError, PermissionError) as e:
            log.error("File system error accessing logs for %s: %s", self._resource_file_path, e)
            return None
        except (ValueError, KeyError, AttributeError) as e:
            log.error("Data parsing error processing time-series data for %s: %s", self._resource_file_path, e)
            return None
