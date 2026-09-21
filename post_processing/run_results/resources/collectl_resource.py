"""
Process CPU statistics from Collectl monitoring output.

Collectl is a system monitoring tool that captures detailed performance metrics.
This module parses collectl's semicolon-separated CPU output files to extract
CPU usage statistics.
"""

from logging import Logger, getLogger
from pathlib import Path
from typing import Any

from post_processing.run_results.resource_result import ResourceResult

log: Logger = getLogger("formatter")


class CollectlResource(ResourceResult):
    """
    Processes resource usage statistics from Collectl monitoring output.

    Collectl produces detailed system monitoring data in semicolon-separated
    format. This class extracts CPU usage by aggregating across all cores
    and time samples.

    The collectl output files are expected to be in a 'collectl' subdirectory
    relative to the benchmark output file, with filenames matching the pattern:
    <hostname>-<date>.cpu
    """

    @property
    def source(self) -> str:
        """Return the source identifier for this resource parser."""
        return "collectl"

    def _get_resource_output_file_from_file_path(self, file_path: Path) -> Path:
        """
        Locate the collectl CPU file from the benchmark output file path.

        Args:
            file_path: Path to benchmark output (e.g., .../json_output.0)

        Returns:
            Path to collectl .cpu file

        Raises:
            FileNotFoundError: If collectl directory or CPU file not found
        """
        collectl_dir = file_path.parent / "collectl"

        if not collectl_dir.exists():
            raise FileNotFoundError(f"Collectl directory not found: {collectl_dir}")

        # Find .cpu files (format: hostname-YYYYMMDD.cpu)
        cpu_files = list(collectl_dir.glob("*.cpu"))

        if not cpu_files:
            raise FileNotFoundError(f"No .cpu files found in {collectl_dir}")

        if len(cpu_files) > 1:
            log.warning("Multiple .cpu files found in %s, using first: %s", collectl_dir, cpu_files[0])

        return cpu_files[0]

    def _parse(self, data: dict[str, Any]) -> None:
        """
        Parse collectl CPU data and calculate average CPU usage.

        Reads the semicolon-separated CPU file, extracts per-core metrics,
        and calculates the average total CPU usage across all cores and time samples.

        Args:
            data: Not used for collectl (reads directly from file)
        """
        try:
            cpu_usage = self._parse_cpu_file()
            self._cpu = f"{cpu_usage:.2f}"
            self._memory = "0.00"  # Memory not implemented yet
            self._has_been_parsed = True
        except Exception as e:
            log.error("Failed to parse collectl data from %s: %s", self._resource_file_path, e)
            self._cpu = "0.00"
            self._memory = "0.00"
            self._has_been_parsed = True

    def _parse_cpu_file(self) -> float:
        """
        Parse collectl .cpu file and calculate average CPU usage.

        Returns:
            Average CPU usage percentage across all cores and time samples
        """
        with open(self._resource_file_path, encoding="utf-8") as f:
            lines = f.readlines()

        # Find header line (starts with #Date;Time;)
        header_line = None
        data_start_idx = 0

        for idx, line in enumerate(lines):
            if line.startswith("#Date;Time;"):
                header_line = line
                data_start_idx = idx + 1
                break

        if not header_line:
            raise ValueError("Could not find header line in collectl CPU file")

        # Parse header to find CPU column indices
        headers = header_line.strip().split(";")
        cpu_indices = self._find_cpu_columns(headers)

        if not cpu_indices["user"] and not cpu_indices["totl"]:
            raise ValueError("No CPU columns found in collectl header")

        # Parse data lines and calculate average
        total_cpu_samples: list[float] = []

        for line in lines[data_start_idx:]:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            values = line.split(";")
            if len(values) < len(headers):
                log.warning("Skipping malformed line: %s", line)
                continue

            # Calculate total CPU for this sample (sum across all cores)
            sample_cpu = self._calculate_sample_cpu(values, cpu_indices)
            total_cpu_samples.append(sample_cpu)

        if not total_cpu_samples:
            log.warning("No valid CPU samples found in %s", self._resource_file_path)
            return 0.0

        # Return average CPU across all samples
        avg_cpu = sum(total_cpu_samples) / len(total_cpu_samples)
        log.debug("Parsed %d CPU samples, average: %.2f%%", len(total_cpu_samples), avg_cpu)

        return avg_cpu

    def _find_cpu_columns(self, headers: list[str]) -> dict[str, list[int]]:
        """
        Find column indices for CPU metrics (User%, Sys%, etc.) for each core.

        Args:
            headers: List of column headers from collectl file

        Returns:
            Dict mapping metric names to lists of column indices
        """
        cpu_columns: dict[str, list[int]] = {"user": [], "sys": [], "totl": []}

        for idx, header in enumerate(headers):
            # Match patterns like [CPU:0]User%, [CPU:15]Sys%, etc.
            if "User%" in header:
                cpu_columns["user"].append(idx)
            elif "Sys%" in header:
                cpu_columns["sys"].append(idx)
            elif "Totl%" in header:
                cpu_columns["totl"].append(idx)

        return cpu_columns

    def _calculate_sample_cpu(self, values: list[str], cpu_indices: dict[str, list[int]]) -> float:
        """
        Calculate total CPU usage for a single time sample.

        Args:
            values: List of values from a data line
            cpu_indices: Dict of CPU column indices

        Returns:
            Average CPU usage across all cores for this sample
        """
        # If Totl% is available, use it directly
        if cpu_indices["totl"]:
            totals = [float(values[idx]) for idx in cpu_indices["totl"] if idx < len(values)]
            return sum(totals) / len(totals) if totals else 0.0

        # Otherwise calculate from User% + Sys%
        num_cores = len(cpu_indices["user"])
        if num_cores == 0:
            return 0.0

        core_totals: list[float] = []
        for core_idx in range(num_cores):
            user_idx = cpu_indices["user"][core_idx]
            sys_idx = cpu_indices["sys"][core_idx]

            if user_idx < len(values) and sys_idx < len(values):
                user = float(values[user_idx])
                sys = float(values[sys_idx])
                core_totals.append(user + sys)

        return sum(core_totals) / len(core_totals) if core_totals else 0.0

    def _read_results_from_file(self) -> dict[str, Any]:
        """
        Override parent method - collectl doesn't use JSON format.

        Returns:
            Empty dict (parsing happens in _parse_cpu_file)
        """
        return {}


# Made with Bob
