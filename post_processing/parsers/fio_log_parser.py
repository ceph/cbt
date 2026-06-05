"""
Parser for FIO time-series log files.

This module provides functionality to parse FIO's time-series log files
(_iops, _clat, _bw, _lat) into pandas DataFrames for further processing.
"""

from logging import Logger, getLogger
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from pandas.core.frame import DataFrame

log: Logger = getLogger("parser")


class FIOLogParser:
    """
    Parse FIO time-series log files into pandas DataFrames.

    FIO generates time-series logs when the --write_iops_log, --write_bw_log,
    and --write_lat_log options are used. These logs have a consistent format:
    timestamp_ms, value, direction, block_offset

    This parser converts these logs into DataFrames with appropriate units
    (seconds for time, milliseconds for latency, bytes for bandwidth).
    """

    def parse_iops_log(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Parse FIO _iops.log file.

        Format: timestamp_ms, iops, direction, block_size, offset

        Args:
            file_path: Path to the _iops.log file

        Returns:
            DataFrame with columns [timestamp_sec, iops, direction] or None if error
        """
        try:
            data_framef = pd.read_csv(
                file_path,
                names=["timestamp_ms", "iops", "direction", "block_size", "offset"],
                skipinitialspace=True,
            )
            # Handle empty file - return empty DataFrame
            if data_framef.empty:
                log.debug("Empty IOPS log file %s", file_path)
                return pd.DataFrame(columns=["timestamp_sec", "iops", "direction"])
            # Validate numeric columns
            data_framef["timestamp_ms"] = pd.to_numeric(data_framef["timestamp_ms"], errors="coerce")
            data_framef["iops"] = pd.to_numeric(data_framef["iops"], errors="coerce")
            # Check if we have any valid data after coercion
            if data_framef["timestamp_ms"].isna().all() or data_framef["iops"].isna().all():
                log.error("No valid numeric data in IOPS log %s", file_path)
                return None
            data_framef["timestamp_sec"] = data_framef["timestamp_ms"] / 1000.0
            result: DataFrame = data_framef[["timestamp_sec", "iops", "direction"]].copy()
            log.debug("Parsed %d IOPS samples from %s", len(result), file_path)
            return result
        except (FileNotFoundError, pd.errors.ParserError, ValueError, TypeError) as e:
            log.error("Error parsing IOPS log %s: %s", file_path, e)
            return None

    def parse_clat_log(self, file_path: Path) -> Optional[DataFrame]:
        """
        Parse FIO _clat.log file (completion latency).

        Format: timestamp_ms, latency_ns, direction, block_size, offset

        Args:
            file_path: Path to the _clat.log file

        Returns:
            DataFrame with columns [timestamp_sec, latency_ms, direction] or None if error
        """
        try:
            df: DataFrame = pd.read_csv(
                file_path,
                names=["timestamp_ms", "latency_ns", "direction", "block_size", "offset"],
                skipinitialspace=True,
            )
            # Handle empty file - return empty DataFrame
            if df.empty:
                log.debug("Empty clat log file %s", file_path)
                return pd.DataFrame(columns=["timestamp_sec", "latency_ms", "direction"])
            # Validate numeric columns
            df["timestamp_ms"] = pd.to_numeric(df["timestamp_ms"], errors="coerce")
            df["latency_ns"] = pd.to_numeric(df["latency_ns"], errors="coerce")
            # Check if we have any valid data after coercion
            if df["timestamp_ms"].isna().all() or df["latency_ns"].isna().all():
                log.error("No valid numeric data in clat log %s", file_path)
                return None
            df["timestamp_sec"] = df["timestamp_ms"] / 1000.0
            df["latency_ms"] = df["latency_ns"] / 1_000_000.0
            result: DataFrame = df[["timestamp_sec", "latency_ms", "direction"]].copy()
            log.debug("Parsed %d latency samples from %s", len(result), file_path)
            return result
        except (FileNotFoundError, pd.errors.ParserError, ValueError, TypeError) as e:
            log.error("Error parsing clat log %s: %s", file_path, e)
            return None

    def parse_lat_log(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Parse FIO _lat.log file (total latency).

        Format: timestamp_ms, latency_ns, direction, block_size, offset

        Args:
            file_path: Path to the _lat.log file

        Returns:
            DataFrame with columns [timestamp_sec, latency_ms, direction] or None if error
        """
        # Same format as clat
        return self.parse_clat_log(file_path)

    def parse_bw_log(self, file_path: Path) -> Optional[DataFrame]:
        """
        Parse FIO _bw.log file (bandwidth).

        Format: timestamp_ms, bandwidth_kb, direction, block_size, offset

        Args:
            file_path: Path to the _bw.log file

        Returns:
            DataFrame with columns [timestamp_sec, bandwidth_bytes, direction] or None if error
        """
        try:
            dataframe: DataFrame = pd.read_csv(
                file_path,
                names=["timestamp_ms", "bandwidth_kb", "direction", "block_size", "offset"],
                skipinitialspace=True,
            )
            # Handle empty file - return empty DataFrame
            if dataframe.empty:
                log.debug("Empty bandwidth log file %s", file_path)
                return pd.DataFrame(columns=["timestamp_sec", "bandwidth_bytes", "direction"])
            # Validate numeric columns
            dataframe["timestamp_ms"] = pd.to_numeric(dataframe["timestamp_ms"], errors="coerce")
            dataframe["bandwidth_kb"] = pd.to_numeric(dataframe["bandwidth_kb"], errors="coerce")
            # Check if we have any valid data after coercion
            if dataframe["timestamp_ms"].isna().all() or dataframe["bandwidth_kb"].isna().all():
                log.error("No valid numeric data in bandwidth log %s", file_path)
                return None
            dataframe["timestamp_sec"] = dataframe["timestamp_ms"] / 1000.0
            dataframe["bandwidth_bytes"] = dataframe["bandwidth_kb"] * 1024
            result: DataFrame = dataframe[["timestamp_sec", "bandwidth_bytes", "direction"]].copy()
            log.debug("Parsed %d bandwidth samples from %s", len(result), file_path)
            return result
        except (FileNotFoundError, pd.errors.ParserError, ValueError, TypeError) as e:
            log.error("Error parsing bandwidth log %s: %s", file_path, e)
            return None

    def parse_slat_log(self, file_path: Path) -> Optional[DataFrame]:
        """
        Parse FIO _slat.log file (submission latency).

        Format: timestamp_ms, latency_ns, direction, block_size, offset

        Args:
            file_path: Path to the _slat.log file

        Returns:
            DataFrame with columns [timestamp_sec, latency_ms, direction] or None if error
        """
        # Same format as clat
        return self.parse_clat_log(file_path)

    def parse_and_combine_logs(  # pylint: disable=too-many-locals
        self, directory: Path, log_type: str, pattern: str = "*"
    ) -> Optional[DataFrame]:
        """
        Parse and combine multiple FIO log files from a directory.

        This method finds all matching log files in a directory (e.g., output.0_clat.1.log,
        output.1_clat.1.log, etc.), parses each one, and combines them by summing values
        at matching timestamps.

        Args:
            directory: Directory containing the log files
            log_type: Type of log to parse ('iops', 'clat', 'lat', 'bw', 'slat')
            pattern: Glob pattern to match files (default: "*")

        Returns:
            Combined DataFrame with aggregated values, or None if error

        Example:
            parser = FIOLogParser()
            # Combine all clat logs matching output.*_clat.1.log
            combined = parser.parse_and_combine_logs(
                Path("/path/to/logs"), "clat", "output.*_clat.1.log"
            )
        """
        # Map log type to parser method
        parser_methods: dict[str, Callable[..., Optional[DataFrame]]] = {
            "iops": self.parse_iops_log,
            "clat": self.parse_clat_log,
            "lat": self.parse_lat_log,
            "bw": self.parse_bw_log,
            "slat": self.parse_slat_log,
        }

        if log_type not in parser_methods:
            log.error("Invalid log_type '%s'. Must be one of: %s", log_type, list(parser_methods.keys()))
            return None

        parser_method: Callable[..., Optional[DataFrame]] = parser_methods[log_type]

        # Find all matching log files
        try:
            log_files = sorted(directory.glob(pattern))
        except OSError as e:
            log.error("Error finding log files in %s with pattern %s: %s", directory, pattern, e)
            return None

        if not log_files:
            log.warning("No log files found in %s matching pattern %s", directory, pattern)
            return None

        log.info("Found %d log files to combine: %s", len(log_files), [f.name for f in log_files])

        # Determine the value column name based on log type
        value_columns: dict[str, str] = {
            "iops": "iops",
            "clat": "latency_ms",
            "lat": "latency_ms",
            "bw": "bandwidth_bytes",
            "slat": "latency_ms",
        }
        value_col = value_columns[log_type]

        # Parse all files and aggregate each one individually first
        # This handles duplicate timestamps within a single file
        aggregated_dataframes: list[pd.DataFrame] = []
        for log_file in log_files:
            dataframe: Optional[DataFrame] = parser_method(log_file)
            if dataframe is not None and not dataframe.empty:
                # Aggregate duplicate timestamps within this file first
                # Group by timestamp and direction, sum the values
                aggregated_dataframe = dataframe.groupby(["timestamp_sec", "direction"], as_index=False)[
                    [value_col]
                ].sum()
                aggregated_dataframes.append(aggregated_dataframe)
                log.debug(
                    "Aggregated %d samples to %d unique timestamps in %s",
                    len(dataframe),
                    len(aggregated_dataframe),
                    log_file.name,
                )
            else:
                log.warning("Skipping empty or invalid log file: %s", log_file)

        if not aggregated_dataframes:
            log.error("No valid data found in any log files")
            return None

        # Now combine the pre-aggregated dataframes across files
        # Each file now has unique timestamps, so we can safely sum across files
        combined: DataFrame = pd.concat(aggregated_dataframes, ignore_index=True)

        # Group by timestamp and direction again to combine across files
        result: DataFrame = combined.groupby(["timestamp_sec", "direction"], as_index=False)[[value_col]].sum()
        result = result.sort_values(by=["timestamp_sec"]).reset_index(drop=True)

        log.info(
            "Combined %d log files into %d aggregated samples for %s",
            len(aggregated_dataframes),
            len(result),
            log_type,
        )

        return result


# Made with Bob
