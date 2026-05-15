"""
Parser for converting aligned time-series data to plotting format.

This module provides the FIOTimeSeriesParser class which converts
pandas DataFrames from FIOLogParser into the TimeSeriesFormatType
intermediate format suitable for plotting.
"""

from collections.abc import Mapping
from logging import Logger, getLogger
from typing import Optional, Union, cast

import pandas as pd

from post_processing.post_processing_types import (
    TimeSeriesDataPoint,
    TimeSeriesFormatType,
    TimeSeriesMetadata,
)

log: Logger = getLogger("parser")


class FIOTimeSeriesParser:  # pylint: disable=too-many-instance-attributes
    """
    Parse aligned time-series data for plotting.

    Takes pandas DataFrames from FIOLogParser and converts them to
    the TimeSeriesFormatType intermediate format. This format includes
    all metrics (IOPS, bandwidth, latency) at each timestamp along with
    metadata about the test run.
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        self,
        archive_directory: str,
        benchmark: str = "fio",
        operation: str = "unknown",
        blocksize: str = "unknown",
        numjobs: str = "1",
        iodepth: str = "1",
        iops_df: Optional[pd.DataFrame] = None,
        bandwidth_df: Optional[pd.DataFrame] = None,
        mean_latency_df: Optional[pd.DataFrame] = None,
        max_latency_df: Optional[pd.DataFrame] = None,
        p50_latency_df: Optional[pd.DataFrame] = None,
        p95_latency_df: Optional[pd.DataFrame] = None,
        p99_latency_df: Optional[pd.DataFrame] = None,
        num_volumes: int = 1,
        log_avg_msec: int = 1000,
    ) -> None:
        """
        Initialize the parser with all required data.

        Args:
            archive_directory: Directory containing benchmark results
            benchmark: Name of the benchmark (default: "fio")
            operation: Operation type (e.g., "randread", "randwrite", "randrw")
            blocksize: Block size used in test (e.g., "4k", "128k")
            numjobs: Number of jobs used in test (default: "1")
            iodepth: IO depth (total_iodepth if exists, otherwise iodepth) (default: "1")
            iops_df: DataFrame with columns [timestamp_sec, iops]
            bandwidth_df: DataFrame with columns [timestamp_sec, bandwidth_bytes]
            mean_latency_df: DataFrame with columns [timestamp_sec, latency_ms]
            max_latency_df: DataFrame with columns [timestamp_sec, latency_ms]
            p50_latency_df: DataFrame with columns [timestamp_sec, latency_ms]
            p95_latency_df: DataFrame with columns [timestamp_sec, latency_ms]
            p99_latency_df: DataFrame with columns [timestamp_sec, latency_ms]
            num_volumes: Number of volumes in the test
            log_avg_msec: FIO log averaging interval in milliseconds
        """
        self._directory = archive_directory
        self._benchmark = benchmark
        self._operation = operation
        self._blocksize = blocksize
        self._numjobs = numjobs
        self._iodepth = iodepth
        self._formatted_output: Optional[TimeSeriesFormatType] = None

        # Store input data directly
        self._dataframes = {
            "iops": iops_df,
            "bandwidth": bandwidth_df,
            "mean_latency": mean_latency_df,
            "max_latency": max_latency_df,
            "p50_latency": p50_latency_df,
            "p95_latency": p95_latency_df,
            "p99_latency": p99_latency_df,
        }
        self._num_volumes = num_volumes
        self._log_avg_msec = log_avg_msec

    def process(self) -> None:
        """
        Process input data and convert to intermediate format.

        This method processes the stored input data and converts it to
        the TimeSeriesFormatType format. Results are stored internally
        for aggregation at RunResult level (needed for RBDFIO with multiple volumes).
        """
        self._formatted_output = self._format_time_series(
            dataframes=self._dataframes,
            num_volumes=self._num_volumes,
            log_avg_msec=self._log_avg_msec,
        )

    def get_formatted_output(self) -> Optional[TimeSeriesFormatType]:
        """
        Get the formatted time-series output.

        Returns:
            The formatted time-series data, or None if process() hasn't been called
            or if no valid data was available.
        """
        return self._formatted_output

    def _format_time_series(
        self,
        dataframes: Mapping[str, Optional[pd.DataFrame]],
        num_volumes: int = 1,
        log_avg_msec: int = 1000,
    ) -> Optional[TimeSeriesFormatType]:
        """
        Format aligned time-series data into TimeSeriesFormatType.

        Args:
            dataframes: Dictionary with keys: 'iops', 'bandwidth', 'mean_latency',
                       'max_latency', 'p50_latency', 'p95_latency', 'p99_latency'.
                       Each value is an optional DataFrame with appropriate columns.
            num_volumes: Number of volumes in the test
            log_avg_msec: FIO log averaging interval in milliseconds

        Returns:
            TimeSeriesFormatType with all metrics, or None if no valid data
        """
        # Merge all DataFrames on timestamp
        merged_df = self._merge_dataframes(dataframes)

        if merged_df is None or merged_df.empty:
            log.warning(
                "No valid time-series data to format for %s %s %s",
                self._benchmark,
                self._operation,
                self._blocksize,
            )
            return None

        # Create metadata
        metadata = self._create_metadata(merged_df, num_volumes, log_avg_msec)

        # Convert DataFrame rows to TimeSeriesDataPoint list
        timeseries = self._create_timeseries_points(merged_df)

        # Calculate maximum values for report generation
        maximum_values = self._calculate_maximum_values(timeseries)

        result: TimeSeriesFormatType = {
            "benchmark": self._benchmark,
            "operation": self._operation,
            "blocksize": self._blocksize,
            "numjobs": self._numjobs,
            "iodepth": self._iodepth,
            "metadata": metadata,
            "timeseries": timeseries,
            "maximum_iops": maximum_values["maximum_iops"],
            "maximum_bandwidth": maximum_values["maximum_bandwidth"],
            "latency_at_max_iops": maximum_values["latency_at_max_iops"],
            "latency_at_max_bandwidth": maximum_values["latency_at_max_bandwidth"],
            "timestamp_at_max_iops": maximum_values["timestamp_at_max_iops"],
            "timestamp_at_max_bandwidth": maximum_values["timestamp_at_max_bandwidth"],
            "maximum_latency": maximum_values["maximum_latency"],
            "timestamp_at_max_latency": maximum_values["timestamp_at_max_latency"],
            "maximum_cpu_usage": maximum_values["maximum_cpu_usage"],
            "maximum_memory_usage": maximum_values["maximum_memory_usage"],
        }

        log.info(
            "Formatted %d time-series data points for %s %s %s",
            len(timeseries),
            self._benchmark,
            self._operation,
            self._blocksize,
        )

        return result

    def _merge_dataframes(  # pylint: disable=too-many-branches
        self,
        dataframes: Mapping[str, Optional[pd.DataFrame]],
    ) -> Optional[pd.DataFrame]:
        """
        Merge all metric DataFrames on timestamp.

        Args:
            dataframes: Dictionary with keys: 'iops', 'bandwidth', 'mean_latency',
                       'max_latency', 'p50_latency', 'p95_latency', 'p99_latency'

        Returns:
            Merged DataFrame with all metrics, or None if no data
        """
        # Early validation
        if not dataframes:
            log.error("Empty dataframes dictionary provided")
            return None

        # Metric configuration: (dict_key, source_column, target_column)
        metric_configs = [
            ("iops", "iops", "iops"),
            ("bandwidth", "bandwidth_bytes", "bandwidth_bytes"),
            ("mean_latency", "latency_ms", "mean_latency_ms"),
            ("max_latency", "latency_ms", "max_latency_ms"),
            ("p50_latency", "latency_ms", "p50_latency_ms"),
            ("p95_latency", "latency_ms", "p95_latency_ms"),
            ("p99_latency", "latency_ms", "p99_latency_ms"),
        ]

        # Collect valid DataFrames with column validation
        valid_dataframes: list[tuple[str, pd.DataFrame, str, str]] = []

        for key, source_column, target_column in metric_configs:
            dataframe = dataframes.get(key)
            if dataframe is not None and not dataframe.empty:
                # Validate required columns exist
                if "timestamp_sec" not in dataframe.columns or source_column not in dataframe.columns:
                    log.warning(
                        "DataFrame for '%s' missing required columns (timestamp_sec, %s), skipping",
                        key,
                        source_column,
                    )
                    continue
                valid_dataframes.append((key, dataframe, source_column, target_column))

        if not valid_dataframes:
            log.error("No valid data in any DataFrame")
            return None

        # Initialize with first valid DataFrame
        key, dataframe, source_column, target_column = valid_dataframes[0]

        # Aggregate across directions if direction column exists
        if "direction" in dataframe.columns:
            base_dataframe = dataframe.groupby("timestamp_sec", as_index=False)[[source_column]].sum()
            log.debug("Aggregated '%s' across directions: %d rows", key, len(base_dataframe))
        else:
            base_dataframe = dataframe[["timestamp_sec", source_column]].copy()

        if source_column != target_column:
            base_dataframe.rename(columns={source_column: target_column}, inplace=True)

        log.debug("Starting merge with '%s' as base (%d rows)", key, len(base_dataframe))

        # Merge remaining DataFrames
        # Strategy: Use outer join but track which metrics have data at each timestamp
        for key, dataframe, source_column, target_column in valid_dataframes[1:]:
            try:
                # Aggregate across directions if direction column exists
                if "direction" in dataframe.columns:
                    temp_df = dataframe.groupby("timestamp_sec", as_index=False)[[source_column]].sum()
                    log.debug("Aggregated '%s' across directions: %d rows", key, len(temp_df))
                else:
                    temp_df = dataframe[["timestamp_sec", source_column]].copy()

                if source_column != target_column:
                    temp_df.rename(columns={source_column: target_column}, inplace=True)

                # Use outer join to preserve all timestamps
                base_dataframe = base_dataframe.merge(temp_df, on="timestamp_sec", how="outer", validate="one_to_one")

                log.debug(
                    "Merged '%s' (%d rows), result now has %d rows",
                    key,
                    len(temp_df),
                    len(base_dataframe),
                )
            except pd.errors.MergeError as merge_error:
                log.error(
                    "Failed to merge DataFrame for '%s': %s. Skipping this metric.",
                    key,
                    str(merge_error),
                )
                continue

        # Sort by timestamp for chronological order
        base_dataframe.sort_values("timestamp_sec", inplace=True)
        base_dataframe.reset_index(drop=True, inplace=True)

        # Identify throughput columns (IOPS and bandwidth) - these are the primary metrics
        metric_columns = [column for column in base_dataframe.columns if column != "timestamp_sec"]
        throughput_columns = [column for column in metric_columns if column in ["iops", "bandwidth_bytes"]]

        if metric_columns:
            # Strategy: Remove rows where throughput metrics (IOPS/bandwidth) are missing or zero
            # This prevents artificial zero spikes in plots caused by timestamp misalignment
            # between different log files (e.g., _iops.log vs _clat.log)

            if throughput_columns:
                # Drop rows where ALL throughput columns are NaN
                base_dataframe.dropna(subset=throughput_columns, how="all", inplace=True)

                # Fill remaining NaN values in throughput columns with 0.0
                base_dataframe[throughput_columns] = base_dataframe[throughput_columns].fillna(0.0)

                # Now drop rows where ALL throughput columns are actually zero
                # These are timestamps from latency logs that don't have corresponding IOPS/bandwidth data
                throughput_mask = (base_dataframe[throughput_columns] != 0).any(axis=1)
                base_dataframe = base_dataframe[throughput_mask].copy()

                log.debug(
                    "Removed rows with zero/missing throughput, %d rows remain",
                    len(base_dataframe),
                )

            # For remaining rows, fill any NaN values in other metrics with 0.0
            base_dataframe[metric_columns] = base_dataframe[metric_columns].fillna(0.0)

            base_dataframe.reset_index(drop=True, inplace=True)

        log.info(
            "Successfully merged %d DataFrames into %d rows with %d metrics (filtered zero throughput)",
            len(valid_dataframes),
            len(base_dataframe),
            len(metric_columns),
        )

        return base_dataframe

    def _create_metadata(self, df: pd.DataFrame, num_volumes: int, log_avg_msec: int) -> TimeSeriesMetadata:
        """
        Create metadata from the DataFrame.

        Args:
            df: Merged DataFrame with timestamp_sec column
            num_volumes: Number of volumes in test
            log_avg_msec: FIO log averaging interval

        Returns:
            TimeSeriesMetadata with test information
        """
        start_time = float(df["timestamp_sec"].min())
        end_time = float(df["timestamp_sec"].max())
        duration = end_time - start_time

        metadata: TimeSeriesMetadata = {
            "start_time_epoch": start_time,
            "end_time_epoch": end_time,
            "duration_seconds": duration,
            "num_volumes": num_volumes,
            "sampling_interval_ms": log_avg_msec,
            "log_avg_msec": log_avg_msec,
        }

        return metadata

    def _create_timeseries_points(self, df: pd.DataFrame) -> list[TimeSeriesDataPoint]:
        """
        Convert DataFrame rows to TimeSeriesDataPoint list.

        Args:
            df: Merged DataFrame with all metrics

        Returns:
            List of TimeSeriesDataPoint dictionaries
        """
        timeseries: list[TimeSeriesDataPoint] = []

        for _, row in df.iterrows():
            row_dict = cast(dict[str, Union[float, int]], row.to_dict())
            point: TimeSeriesDataPoint = {
                "timestamp_sec": float(row_dict.get("timestamp_sec", 0.0)),
                "iops": float(row_dict.get("iops", 0.0)),
                "bandwidth_bytes": float(row_dict.get("bandwidth_bytes", 0.0)),
                "mean_latency_ms": float(row_dict.get("mean_latency_ms", 0.0)),
                "max_latency_ms": float(row_dict.get("max_latency_ms", 0.0)),
                "p50_latency_ms": float(row_dict.get("p50_latency_ms", 0.0)),
                "p95_latency_ms": float(row_dict.get("p95_latency_ms", 0.0)),
                "p99_latency_ms": float(row_dict.get("p99_latency_ms", 0.0)),
                "num_samples": int(row_dict.get("num_samples", 1)),
            }
            timeseries.append(point)

        return timeseries

    def _calculate_maximum_values(self, timeseries: list[TimeSeriesDataPoint]) -> dict[str, str]:
        """
        Calculate maximum values from time-series data points.

        This follows the same pattern as the hockey-stick intermediate format,
        pre-calculating maximum values for efficient report generation.

        Args:
            timeseries: List of time-series data points

        Returns:
            Dictionary with maximum values as strings, including timestamps
        """
        if not timeseries:
            return {
                "maximum_iops": "0",
                "maximum_bandwidth": "0",
                "latency_at_max_iops": "0.0",
                "latency_at_max_bandwidth": "0.0",
                "timestamp_at_max_iops": "0.0",
                "timestamp_at_max_bandwidth": "0.0",
                "maximum_latency": "0.0",
                "timestamp_at_max_latency": "0.0",
                "maximum_cpu_usage": "0.0",
                "maximum_memory_usage": "0.0",
            }

        # Find maximum IOPS and corresponding latency and timestamp
        maximum_iops_point = max(timeseries, key=lambda p: p["iops"])
        maximum_iops = maximum_iops_point["iops"]
        latency_at_max_iops = maximum_iops_point["mean_latency_ms"]
        timestamp_at_max_iops = maximum_iops_point["timestamp_sec"]

        # Find maximum bandwidth and corresponding latency and timestamp
        maximum_bandwidth_point = max(timeseries, key=lambda p: p["bandwidth_bytes"])
        maximum_bandwidth = maximum_bandwidth_point["bandwidth_bytes"]
        latency_at_max_bandwidth = maximum_bandwidth_point["mean_latency_ms"]
        timestamp_at_max_bandwidth = maximum_bandwidth_point["timestamp_sec"]

        # Find maximum latency and its timestamp
        # Use max_latency_ms which represents the maximum latency observed in each time window
        maximum_latency_point = max(timeseries, key=lambda p: p["max_latency_ms"])
        maximum_latency = maximum_latency_point["max_latency_ms"]
        timestamp_at_max_latency = maximum_latency_point["timestamp_sec"]

        # CPU and memory are not yet available in time-series data
        maximum_cpu_usage = 0.0
        maximum_memory_usage = 0.0

        return {
            "maximum_iops": f"{maximum_iops:.0f}",
            "maximum_bandwidth": f"{maximum_bandwidth:.0f}",
            "latency_at_max_iops": f"{latency_at_max_iops:.6f}",
            "latency_at_max_bandwidth": f"{latency_at_max_bandwidth:.6f}",
            "timestamp_at_max_iops": f"{timestamp_at_max_iops:.1f}",
            "timestamp_at_max_bandwidth": f"{timestamp_at_max_bandwidth:.1f}",
            "maximum_latency": f"{maximum_latency:.6f}",
            "timestamp_at_max_latency": f"{timestamp_at_max_latency:.1f}",
            "maximum_cpu_usage": f"{maximum_cpu_usage:.2f}",
            "maximum_memory_usage": f"{maximum_memory_usage:.2f}",
        }


# Made with Bob
