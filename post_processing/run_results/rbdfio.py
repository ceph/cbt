"""
The base class that reads a results file and converts it into the
common data format that can be plotted
"""

import re
from logging import Logger, getLogger
from pathlib import Path
from typing import Literal, Union, cast

from post_processing.common import sum_mean_values, sum_standard_deviation_values
from post_processing.post_processing_types import IodepthDataType, TimeSeriesDataPoint, TimeSeriesFormatType
from post_processing.run_results.benchmark_result import BenchmarkResult
from post_processing.run_results.benchmarks.fio import FIO
from post_processing.run_results.resource_result import ResourceResult
from post_processing.run_results.resources.fio_resource import FIOResource
from post_processing.run_results.run_result import RunResult

log: Logger = getLogger(name="formatter")


class RBDFIO(RunResult):
    """
    Processes RBD FIO benchmark results and converts them to the common intermediate format.

    This class handles reading FIO JSON output files from RBD (RADOS Block Device)
    benchmark runs and aggregating results across multiple volumes.
    """

    @property
    def type(self) -> str:
        return "rbdfio"

    def _find_files_for_testrun(self, file_name_root: str) -> list[Path]:
        """
        Find all result files for a particular test run matching the file name pattern.

        Args:
            file_name_root: The base filename to search for (e.g., "json_output")

        Returns:
            List of Path objects for files matching the pattern <file_name_root>.<digit>
        """
        # We need to use a list here as we can possibly iterate over the file
        # list multiple times, and a Generator object only allows iterating
        # once
        return [
            path
            for path in self._path.glob(pattern=f"**/{file_name_root}.*")
            if re.search(rf"{file_name_root}.\d+$", f"{path}")
        ]

    def _sum_io_details(
        self, existing_values: Union[str, IodepthDataType], new_values: IodepthDataType
    ) -> IodepthDataType:
        """
        Aggregate IO statistics from multiple volumes by summing values and computing
        weighted averages for latency and standard deviation.

        Args:
            existing_values: Previously aggregated IO statistics
            new_values: New IO statistics to add to the aggregate

        Returns:
            Combined IO statistics with properly weighted latency and standard deviation
        """
        assert isinstance(existing_values, dict)
        combined_data: IodepthDataType = {}

        simple_sum_values: list[str] = ["io_bytes", "iops", "bandwidth_bytes"]

        for value in simple_sum_values:
            combined_data[value] = f"{float(existing_values[value]) + float(new_values[value])}"

        combined_data["total_ios"] = f"{int(existing_values['total_ios']) + int(new_values['total_ios'])}"

        latencies: list[float] = [float(existing_values["latency"]), float(new_values["latency"])]
        operations: list[int] = [int(existing_values["total_ios"]), int(new_values["total_ios"])]
        std_deviations: list[float] = [float(existing_values["std_deviation"]), float(new_values["std_deviation"])]

        combined_latency: float = sum_mean_values(
            latencies,
            num_ops=operations,
            total_ios=int(combined_data["total_ios"]),
        )

        combined_std_dev: float = sum_standard_deviation_values(
            std_deviations, operations, latencies, int(combined_data["total_ios"]), combined_latency
        )

        combined_data["latency"] = f"{combined_latency}"
        combined_data["std_deviation"] = f"{combined_std_dev}"

        return combined_data

    def _merge_timeseries_data(  # pylint: disable=too-many-locals,too-many-statements
        self,
        test_config: tuple[str, str, str, str],
        new_timeseries: TimeSeriesFormatType,
    ) -> TimeSeriesFormatType:
        """
        Aggregate time-series data across multiple per-volume FIO result files.

        For matching timestamps:
        - Throughput metrics (IOPS, bandwidth) are summed across volumes
        - Latency metrics are weighted-averaged by IOPS (more accurate than simple average)
        - Maximum latency uses the maximum value across volumes
        """
        operation, blocksize, iodepth, _ = test_config
        key = f"{operation}_{blocksize}_{iodepth}"
        existing_timeseries = self._timeseries_data.get(key)
        if not existing_timeseries:
            return new_timeseries

        existing_points = {point["timestamp_sec"]: point for point in existing_timeseries["timeseries"]}
        new_points = {point["timestamp_sec"]: point for point in new_timeseries["timeseries"]}
        all_timestamps = sorted(set(existing_points) | set(new_points))

        combined_timeseries: list[TimeSeriesDataPoint] = []
        throughput_metrics: list[Literal["iops", "bandwidth_bytes"]] = ["iops", "bandwidth_bytes"]
        latency_metrics: list[Literal["mean_latency_ms", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms"]] = [
            "mean_latency_ms",
            "p50_latency_ms",
            "p95_latency_ms",
            "p99_latency_ms",
        ]

        for timestamp in all_timestamps:
            existing_point = existing_points.get(timestamp)
            new_point = new_points.get(timestamp)

            combined_point: TimeSeriesDataPoint = {
                "timestamp_sec": timestamp,
                "iops": 0.0,
                "bandwidth_bytes": 0.0,
                "mean_latency_ms": 0.0,
                "max_latency_ms": 0.0,
                "p50_latency_ms": 0.0,
                "p95_latency_ms": 0.0,
                "p99_latency_ms": 0.0,
                "num_samples": 0,
            }

            existing_metric_values = cast(dict[str, float], existing_point) if existing_point else {}
            new_metric_values = cast(dict[str, float], new_point) if new_point else {}

            # Sum throughput metrics (IOPS, bandwidth)
            for metric in throughput_metrics:
                combined_point[metric] = float(existing_metric_values.get(metric, 0.0)) + float(
                    new_metric_values.get(metric, 0.0)
                )

            # Calculate weighted average for latency metrics (weighted by IOPS)
            existing_iops = float(existing_metric_values.get("iops", 0.0))
            new_iops = float(new_metric_values.get("iops", 0.0))
            total_iops = existing_iops + new_iops

            if total_iops > 0:
                for latency_metric in latency_metrics:
                    existing_latency = float(existing_metric_values.get(latency_metric, 0.0))
                    new_latency = float(new_metric_values.get(latency_metric, 0.0))
                    # Weighted average: (lat1 * iops1 + lat2 * iops2) / (iops1 + iops2)
                    combined_point[latency_metric] = (
                        existing_latency * existing_iops + new_latency * new_iops
                    ) / total_iops
            else:
                # If no IOPS, use simple average (fallback)
                for latency_metric in latency_metrics:
                    existing_latency = float(existing_metric_values.get(latency_metric, 0.0))
                    new_latency = float(new_metric_values.get(latency_metric, 0.0))
                    combined_point[latency_metric] = (existing_latency + new_latency) / 2.0

            # Maximum latency should be the maximum across volumes
            combined_point["max_latency_ms"] = max(
                float(existing_metric_values.get("max_latency_ms", 0.0)),
                float(new_metric_values.get("max_latency_ms", 0.0)),
            )

            combined_point["num_samples"] = int(existing_point.get("num_samples", 0) if existing_point else 0) + int(
                new_point.get("num_samples", 0) if new_point else 0
            )

            combined_timeseries.append(combined_point)

        start_time = combined_timeseries[0]["timestamp_sec"] if combined_timeseries else 0.0
        end_time = combined_timeseries[-1]["timestamp_sec"] if combined_timeseries else 0.0

        # Calculate maximum values from the combined timeseries
        # (similar to _calculate_maximum_values in FIOTimeSeriesParser)
        if combined_timeseries:
            max_iops_point = max(combined_timeseries, key=lambda p: p["iops"])
            maximum_iops = max_iops_point["iops"]
            latency_at_max_iops = max_iops_point["mean_latency_ms"]
            timestamp_at_max_iops = max_iops_point["timestamp_sec"]

            max_bandwidth_point = max(combined_timeseries, key=lambda p: p["bandwidth_bytes"])
            maximum_bandwidth = max_bandwidth_point["bandwidth_bytes"]
            latency_at_max_bandwidth = max_bandwidth_point["mean_latency_ms"]
            timestamp_at_max_bandwidth = max_bandwidth_point["timestamp_sec"]

            max_latency_point = max(combined_timeseries, key=lambda p: p["mean_latency_ms"])
            maximum_latency = max_latency_point["mean_latency_ms"]
            timestamp_at_max_latency = max_latency_point["timestamp_sec"]
        else:
            maximum_iops = 0.0
            latency_at_max_iops = 0.0
            timestamp_at_max_iops = 0.0
            maximum_bandwidth = 0.0
            latency_at_max_bandwidth = 0.0
            timestamp_at_max_bandwidth = 0.0
            maximum_latency = 0.0
            timestamp_at_max_latency = 0.0

        return {
            "benchmark": new_timeseries["benchmark"],
            "operation": new_timeseries["operation"],
            "blocksize": new_timeseries["blocksize"],
            "numjobs": new_timeseries["numjobs"],
            "iodepth": new_timeseries.get("iodepth", iodepth),
            "metadata": {
                "start_time_epoch": start_time,
                "end_time_epoch": end_time,
                "duration_seconds": end_time - start_time,
                "num_volumes": int(existing_timeseries["metadata"]["num_volumes"])
                + int(new_timeseries["metadata"]["num_volumes"]),
                "sampling_interval_ms": int(new_timeseries["metadata"]["sampling_interval_ms"]),
                "log_avg_msec": int(new_timeseries["metadata"]["log_avg_msec"]),
            },
            "timeseries": combined_timeseries,
            "maximum_iops": f"{maximum_iops:.0f}",
            "maximum_bandwidth": f"{maximum_bandwidth:.0f}",
            "latency_at_max_iops": f"{latency_at_max_iops:.6f}",
            "latency_at_max_bandwidth": f"{latency_at_max_bandwidth:.6f}",
            "timestamp_at_max_iops": f"{timestamp_at_max_iops:.6f}",
            "timestamp_at_max_bandwidth": f"{timestamp_at_max_bandwidth:.6f}",
            "maximum_latency": f"{maximum_latency:.6f}",
            "timestamp_at_max_latency": f"{timestamp_at_max_latency:.6f}",
            "maximum_cpu_usage": "0.00",  # TODO: Add CPU/memory tracking
            "maximum_memory_usage": "0.00",
        }

    def _create_benchmark_result(self, file_path: Path) -> BenchmarkResult:
        """
        Factory method to create FIO benchmark result parser.

        RBDFIO uses FIO as the underlying benchmark tool, so this returns
        a FIO instance to parse the benchmark output.

        Args:
            file_path: Path to the FIO JSON output file

        Returns:
            FIO instance for parsing the benchmark results
        """
        return FIO(file_path=file_path)

    def _create_resource_result(self, file_path: Path) -> ResourceResult:
        """
        Factory method to create FIO resource result parser.

        RBDFIO uses FIO's resource monitoring, so this returns a FIOResource
        instance to parse CPU and memory usage from the output.

        Args:
            file_path: Path to the FIO JSON output file

        Returns:
            FIOResource instance for parsing resource usage statistics
        """
        return FIOResource(file_path=file_path)
