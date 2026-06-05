"""
Timestamp alignment and aggregation for time-series data.

This module provides functionality to align timestamps from multiple volumes
using time-window binning and aggregate metrics appropriately.
"""

from logging import Logger, getLogger
from typing import Literal, Optional

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame

log: Logger = getLogger("parser")


class TimestampAligner:
    """
    Align and aggregate time-series data from multiple volumes.

    When testing multiple volumes, timestamps may not align exactly due to
    slight timing differences in when each volume starts logging. This class
    uses time-window binning to group data points into fixed time intervals
    and aggregate them appropriately.

    For example, with a 1-second window:
    - All data points between 0.0-1.0 seconds are grouped together
    - IOPS values are summed across volumes
    - Latency values are averaged or max'd as appropriate
    """

    def __init__(self, window_size_ms: int = 1000) -> None:
        """
        Initialize the timestamp aligner.

        Args:
            window_size_sec: Size of time windows in seconds (default: 1000ms = 1s)
        """
        self._window_size_sec = window_size_ms / 1000.0
        log.debug("Initialized TimestampAligner with window size %dms", window_size_ms)

    def align_and_aggregate(
        self,
        dataframes: list[Optional[pd.DataFrame]],
        metric_column: str,
        aggregation: Literal["sum", "mean", "max"] = "sum",
    ) -> pd.DataFrame:
        """
        Align timestamps using time windows and aggregate metric values.

        Args:
            dataframes: List of DataFrames from different volumes, each with
                       columns [timestamp_sec, <metric_column>, ...]
            metric_column: Name of the column to aggregate (e.g., 'iops', 'latency_ms')
            aggregation: How to aggregate values:
                        - 'sum': Add values (use for IOPS, bandwidth)
                        - 'mean': Average values (use for mean latency)
                        - 'max': Take maximum (use for max latency)

        Returns:
            DataFrame with columns [timestamp_sec, <metric_column>, num_samples]
            where timestamp_sec is the start of each time window
        """
        # Validate aggregation method early
        valid_aggregations = {"sum", "mean", "max"}
        if aggregation not in valid_aggregations:
            raise ValueError(f"Unknown aggregation method: {aggregation}. Must be one of {valid_aggregations}")

        if not dataframes:
            log.warning("No dataframes provided for alignment")
            return pd.DataFrame(columns=["timestamp_sec", metric_column, "num_samples"])

        # Filter out None dataframes
        valid_dfs: list[DataFrame] = [df for df in dataframes if df is not None and not df.empty]
        if not valid_dfs:
            log.warning("All dataframes are None or empty")
            return pd.DataFrame(columns=["timestamp_sec", metric_column, "num_samples"])

        # Combine all dataframes
        combined = pd.concat(valid_dfs, ignore_index=True)
        log.debug("Combined %d dataframes into %d total samples", len(valid_dfs), len(combined))

        # Create time windows using vectorized floor division
        combined["time_window"] = (combined["timestamp_sec"] // self._window_size_sec) * self._window_size_sec

        # Aggregate by time window and count samples in single operation
        grouped = combined.groupby("time_window")
        result = grouped[metric_column].agg(aggregation)
        counts = grouped.size()

        # Build result dataframe
        result_df = pd.DataFrame(
            {"timestamp_sec": result.index, metric_column: result.values, "num_samples": counts.values}
        ).reset_index(drop=True)

        log.debug("Aggregated into %d time windows using %s", len(result_df), aggregation)
        return result_df

    def calculate_percentiles(
        self, dataframes: list[pd.DataFrame], percentiles: Optional[list[float]] = None
    ) -> pd.DataFrame:
        """
        Calculate latency percentiles per time window.

        Args:
            dataframes: List of DataFrames with latency data, each with
                       columns [timestamp_sec, latency_ms, ...]
            percentiles: List of percentiles to calculate (e.g., [50, 95, 99])
                        Defaults to [50, 95, 99] if not provided

        Returns:
            DataFrame with columns [timestamp_sec, p50_latency_ms, p95_latency_ms, ...]
        """
        if percentiles is None:
            percentiles = [50, 95, 99]

        if not dataframes:
            log.warning("No dataframes provided for percentile calculation")
            return pd.DataFrame(columns=["timestamp_sec"])

        # Filter out None dataframes
        valid_dfs = [df for df in dataframes if df is not None and not df.empty]
        if not valid_dfs:
            log.warning("All dataframes are None or empty")
            return pd.DataFrame(columns=["timestamp_sec"])

        # Combine all dataframes
        combined = pd.concat(valid_dfs, ignore_index=True)

        # Create time windows using vectorized floor division (consistent with align_and_aggregate)
        combined["time_window"] = (combined["timestamp_sec"] // self._window_size_sec) * self._window_size_sec

        # Calculate percentiles per window
        quantiles = [p / 100.0 for p in percentiles]
        result = (
            combined.groupby("time_window")["latency_ms"]
            .quantile(quantiles)  # type: ignore[arg-type]
            .unstack(fill_value=np.nan)  # type: ignore[arg-type]
        )

        # Rename columns and reset index with timestamp
        result.columns = [f"p{int(q * 100)}_latency_ms" for q in result.columns]
        result_df = result.reset_index(names="timestamp_sec")

        log.debug("Calculated percentiles %s for %d time windows", percentiles, len(result_df))
        return result_df

    def merge_metrics(self, *dataframes: pd.DataFrame) -> pd.DataFrame:
        """
        Merge multiple metric dataframes on timestamp_sec.

        All dataframes should have a 'timestamp_sec' column. This method
        performs an outer join to include all timestamps from all dataframes.

        Args:
            *dataframes: Variable number of DataFrames to merge

        Returns:
            Merged DataFrame with all metrics aligned by timestamp
        """
        if not dataframes:
            return pd.DataFrame()

        # Filter out None and empty dataframes
        valid_dfs = [df for df in dataframes if df is not None and not df.empty]
        if not valid_dfs:
            return pd.DataFrame()

        # Start with the first dataframe
        result = valid_dfs[0].copy()

        # Merge each subsequent dataframe
        for df in valid_dfs[1:]:
            result = pd.merge(result, df, on="timestamp_sec", how="outer", suffixes=("", "_dup"))

            # Remove duplicate columns (keep the first occurrence)
            result = result.loc[:, ~result.columns.str.endswith("_dup")]

        # Sort by timestamp
        result = result.sort_values("timestamp_sec").reset_index(drop=True)

        log.debug("Merged %d dataframes into %d rows", len(valid_dfs), len(result))
        return result


# Made with Bob
