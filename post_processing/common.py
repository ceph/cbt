"""
This file contains any common methods for post-processing tasks such as
drawing plots or producing reports
"""

import json
import re
from datetime import datetime
from logging import Logger, getLogger
from math import sqrt
from pathlib import Path
from re import Pattern
from typing import Any, Callable, Optional, Union, cast

from post_processing.post_processing_types import CommonFormatDataType

log: Logger = getLogger("cbt")

# A conversion between the operation type in the intermediate file format
# and a human-readable string that can be used in the title for the plot.
TITLE_CONVERSION: dict[str, str] = {
    "read": "Sequential Read",
    "write": "Sequential Write",
    "randread": "Random Read",
    "randwrite": "Random Write",
    "readwrite": "Sequential Read/Write",
    "randrw": "Random Read/Write",
}

# Common file extensions
PLOT_FILE_EXTENSION: str = "svg"
DATA_FILE_EXTENSION: str = "json"
PLOT_FILE_EXTENSION_WITH_DOT: str = f".{PLOT_FILE_EXTENSION}"
DATA_FILE_EXTENSION_WITH_DOT: str = f".{DATA_FILE_EXTENSION}"

# Common conversion factors
KB_CONVERSION_FACTOR: int = 1024

# Regex patterns for stripping confidential data
_IPV4_PATTERN: Pattern[str] = re.compile(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b")
_IPV6_PATTERN: Pattern[str] = re.compile(
    r"\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|"
    r"\s::(?:[0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}|"
    r"\b[0-9a-fA-F]{1,4}::(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}|"
    r"\b[0-9a-fA-F]{1,4}:[0-9a-fA-F]{1,4}::(?:[0-9a-fA-F]{1,4}:){0,4}[0-9a-fA-F]{1,4}|"
    r"\b(?:[0-9a-fA-F]{1,4}:){0,2}[0-9a-fA-F]{1,4}::(?:[0-9a-fA-F]{1,4}:){0,3}[0-9a-fA-F]{1,4}|"
    r"\b(?:[0-9a-fA-F]{1,4}:){0,3}[0-9a-fA-F]{1,4}::(?:[0-9a-fA-F]{1,4}:){0,2}[0-9a-fA-F]{1,4}|"
    r"\b(?:[0-9a-fA-F]{1,4}:){0,4}[0-9a-fA-F]{1,4}::(?:[0-9a-fA-F]{1,4}:)?[0-9a-fA-F]{1,4}|"
    r"\b(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}::[0-9a-fA-F]{1,4}|"
    r"\b(?:[0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}::"
)
_HOSTNAME_PATTERN: Pattern[str] = re.compile(
    r"(?:^|\s)([a-z0-9-]{1,61}\.(?:[a-z0-9-]{1,61}\.){0,6}[a-z0-9-]{1,61})(?=\s|$|[,:\[\]\"'])",
    re.IGNORECASE | re.MULTILINE,
)


def get_blocksize_percentage_operation_numjobs_from_file_name(file_name: str) -> tuple[str, str, str, str]:
    """
        Return the blocksize , operation and read/write percentage from the
    filename

    The filename is in one of 2 formats:
        BLOCKSIZE_NUMJOBS_OPERATION.json
        BLOCKSIZE_NUMJOBS_READ_WRITE_OPERATION.json
    """
    file_parts: list[str] = file_name.split("_")

    # The split on _ will mean that the last element [-1] will always be
    # the operation, the first part [0] will be the blocksize, and the
    # second part [1] will be the number of jobs
    operation: str = f"{TITLE_CONVERSION[file_parts[-1]]}"
    blocksize: str = get_blocksize(f"{file_parts[0]}")
    blocksize = f"{int(int(blocksize) / 1024)}K"
    number_of_jobs: str = file_parts[1]
    read_percent: str = ""

    # If there are more than 3 parts, we have read/write percentages
    # Format: BLOCKSIZE_NUMJOBS_READ_WRITE_OPERATION
    # Parts:  [0]       [1]     [2]  [3]    [4]
    if len(file_parts) > 3:
        read_percent = f"{file_parts[2]}/{file_parts[3]} "

    return (blocksize, read_percent, operation, number_of_jobs)


def read_intermediate_file(file_path: str) -> CommonFormatDataType:
    """
    Read the json data from the common intermediate file and store it for processing.
    """
    data: CommonFormatDataType = {}
    # We know the file encoding as we wrote it ourselves as part of
    # common_output_format.py, so it is safe to specify here

    try:
        with open(file_path, encoding="utf8") as file_data:
            data = json.load(file_data)
    except FileNotFoundError:
        log.exception("File %s does not exist", file_path)
    except OSError:
        log.error("Error reading file %s", file_path)

    return data


def _extract_metrics_from_intermediate_file(
    file_path: Path, metric_keys: list[str], format_function: Optional[list[Callable[[str], str]]] = None
) -> tuple[str, ...]:
    """
    Generic function to extract metrics from an intermediate format file.

    Args:
        file_path:          Path to the intermediate format data file
        metric_keys:        List of keys to extract from the data
        format_function:    Optional list of formatting functions to apply to each metric.
                                If None, values are returned as-is. List must match length of metric_keys.

    Returns:
        Tuple of extracted and formatted metric values as strings
    """
    data: CommonFormatDataType = read_intermediate_file(file_path=f"{file_path}")

    results: list[str] = []
    for index, key in enumerate(metric_keys):
        value = data.get(key, "0")
        assert isinstance(value, str)

        # Apply formatting function if provided
        if format_function and index < len(format_function):
            formatted_value = format_function[index](value)
        else:
            formatted_value = value

        results.append(formatted_value)

    return tuple(results)


def get_latency_throughput_from_file(file_path: Path) -> tuple[str, str]:
    """
    Reads the data stored in the intermediate file format and returns the
    maximum throughput in either iops or MB/s, and the latency in ms
    recorded for that throughput
    """
    data: CommonFormatDataType = read_intermediate_file(file_path=f"{file_path}")

    # The blocksize will be the same for every data point in the file.
    # We can therefore read the blocksize from the first data point
    keys: list[str] = list(data)
    key_data: Union[str, dict[str, str]] = data[keys[0]]
    assert isinstance(key_data, dict)
    blocksize: int = int(int(key_data["blocksize"]) / 1024)
    throughput_key: str = "maximum_iops"
    latency_key: str = "latency_at_max_iops"
    throughput = data[throughput_key]
    assert isinstance(throughput, str)
    max_throughput: float = float(throughput)
    throughput_type: str = "IOps"
    if blocksize >= 64:
        throughput_key = "maximum_bandwidth"
        throughput = data[throughput_key]
        assert isinstance(throughput, str)
        max_throughput = float(float(throughput) / (1000 * 1000))
        throughput_type = "MB/s"
        latency_key = "latency_at_max_bandwidth"

    latency = data[latency_key]
    assert isinstance(latency, str)
    latency_at_maximum_throughput: float = float(latency)

    return (f"{max_throughput:.0f} {throughput_type}", f"{latency_at_maximum_throughput:.1f}")


def get_resource_details_from_file(file_path: Path) -> tuple[str, str]:
    """
    Return the max CPU and max memory value from an intermediate file.

    Args:
        file_path: Path to the intermediate format data file

    Returns:
        A tuple of (max_cpu, max_memory) as formatted strings
    """

    # Use the generic extraction function with formatting
    def format_cpu(value: str) -> str:
        return f"{float(value):.2f}"

    def format_memory(value: str) -> str:
        return f"{float(value):.2f}"

    result = _extract_metrics_from_intermediate_file(
        file_path=file_path,
        metric_keys=["maximum_cpu_usage", "maximum_memory_usage"],
        format_function=[format_cpu, format_memory],
    )
    # Cast to the specific tuple type for type safety
    return cast(tuple[str, str], result)


def strip_confidential_data_from_yaml(yaml_data: str) -> str:
    """
    Remove any confidential data from a string of yaml files and replaces
    the confidential data with a string that still allows identification
    of unique assets e.g.

    all references to a single server hostname will be replaced with:
    --- server1 ---

    IP addresses cannot reliable be resolved, so they are replaced with
    --- IP Address ---

    Unfortunately this cannot be linked to a server hostname at this time

    Currently handles hostnames, IPv4 addresses and IPv6 addresses
    """
    # Replace all IPv4 addresses
    filtered_text: str = _IPV4_PATTERN.sub("--- IP Address ---", yaml_data)

    # Replace all IPv6 addresses
    filtered_text = _IPV6_PATTERN.sub("--- IP Address ---", filtered_text)

    # Replace hostnames with numbered identifiers using a callback
    hostname_map: dict[str, str] = {}

    def replace_hostname(match: re.Match[str]) -> str:
        # Group 1 contains the hostname, group 0 includes leading whitespace
        hostname = match.group(1)
        if hostname not in hostname_map:
            hostname_map[hostname] = f"--- server{len(hostname_map) + 1} ---"
        # Preserve any leading whitespace from the original match
        leading = match.group(0)[: match.start(1) - match.start(0)]
        return leading + hostname_map[hostname]

    filtered_text = _HOSTNAME_PATTERN.sub(replace_hostname, filtered_text)

    return filtered_text


def find_common_data_file_names(directories: list[Path]) -> list[str]:
    """
    Find a list of file names from the provided directories.

    For a single archive (multiple subdirectories from the same parent),
    returns all unique file names found across all subdirectories.
    For multiple archives (comparison), returns only files that
    exist in ALL provided directories.

    Note: Excludes time-series files (*_timeseries.json) as they have
    a different data structure and are handled by TimeSeriesReportGenerator.
    """
    if not directories:
        return []

    # Collect all file names from all directories, excluding time-series files
    all_files: set[str] = set()
    for directory in directories:
        files = set(
            path.parts[-1]
            for path in directory.glob(f"*{DATA_FILE_EXTENSION_WITH_DOT}")
            if not path.stem.endswith("_timeseries")
        )
        all_files.update(files)

    # If we only have one directory, return all files found
    if len(directories) == 1:
        return sorted(list(all_files))

    # Check if all directories share a common ancestor (same archive)
    # by checking if they all have the same great-great-grandparent
    # (archive_dir/results/00000000/id-xxx/workload/visualisation)
    try:
        ancestors = [directory.parents[3] for directory in directories if len(directory.parents) > 3]
        if ancestors and all(ancestor == ancestors[0] for ancestor in ancestors):
            # All from same archive - return all unique files
            return sorted(list(all_files))
    except (IndexError, AttributeError):
        pass

    # For multiple archives, check if files exist in ALL directories
    common_files: set[str] = set()
    for file_name in all_files:
        # Check if this file exists in all of the directories
        found_count = sum(1 for directory in directories if (directory / file_name).exists())
        # Only include if found in ALL directories
        if found_count == len(directories):
            common_files.add(file_name)

    return sorted(list(common_files))


def calculate_percent_difference_to_baseline(baseline: str, comparison: str) -> str:
    """
    Compare a value to a baseline and calculate the percentage difference
    from that baseline value
    """

    baseline_value: float = float(baseline.split(" ")[0])
    comparison_value: float = float(comparison.split(" ")[0])

    percentage_difference: float = (comparison_value - baseline_value) / baseline_value

    # format the value as a percentage, to 0 decimal places
    return f"{percentage_difference:.0%}"  #


def get_date_time_string() -> str:
    """
    Get the string for the current date and time
    """
    current_datetime: datetime = datetime.now()

    # Convert to string
    datetime_string: str = current_datetime.strftime("%y%m%d_%H%M%S")
    return datetime_string


def recursive_search(data_to_search: dict[str, Any], search_key: str) -> Optional[str]:
    """
    Recursively search through a python dictionary for a particular key, and
    return the value stored at that key

    This can handle a dictionary containing other dictionaries and lists
    """
    # Note: As we don't know the structure the data will take we need to type
    # the dictioary as Any.

    for key, value in data_to_search.items():
        if key == search_key:
            log.debug("Returning %s for key %s from %s", value, key, data_to_search)
            return f"{value}"
        if isinstance(value, list):
            for item in value:  # pyright: ignore[reportUnknownVariableType]
                if isinstance(item, dict):
                    result = recursive_search(item, search_key)  # pyright: ignore[reportUnknownArgumentType]
                    if result is not None:
                        return result
        if isinstance(value, dict):
            result = recursive_search(value, search_key)  # pyright: ignore[reportUnknownArgumentType]
            if result is not None:
                return result

    return None


def get_blocksize(blocksize_value: str) -> str:
    """
    Extract the numeric blocksize value from a string, removing any unit suffix.

    Args:
        blocksize_value: Blocksize string that may include a unit suffix (e.g., "4K", "1024")

    Returns:
        The numeric blocksize value as a string without units

    Example:
        >>> get_blocksize("4K")
        "4"
        >>> get_blocksize("1024")
        "1024"
    """
    blocksize: str = blocksize_value
    if re.search(r"\D$", blocksize):
        blocksize = blocksize[:-1]

    return blocksize


def sum_standard_deviation_values(
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


def file_is_empty(file_path: Path) -> bool:
    """
    returns true if the input file contains no data
    """
    return file_path.stat().st_size == 0


def file_is_precondition(file_path: Path) -> bool:
    """
    Check if a file is from a precondition part of a test run
    """
    return "precond" in str(file_path)


def sum_mean_values(latencies: list[float], num_ops: list[int], total_ios: int) -> float:
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


def _find_visualisation_directories_with_filter(
    archive_directory: Path,
    filter_function: Callable[[Path], bool],
    sort_function: Callable[[list[Path]], list[Path]] = lambda paths: sorted(paths, key=str),
) -> list[Path]:
    """
    Generic function to find visualisation directories based on a filter function.

    Args:
        archive_directory: Root directory to search for visualisation directories
        filter_function: Function that takes a Path and returns True if it should be included
        sort_function: Function to sort the resulting paths (default: sort by string representation)

    Returns:
        List of Path objects for visualisation directories that match the filter
    """
    visualisation_directories = []

    # Search for all visualisation directories recursively
    for visualisation_directory in archive_directory.glob("**/visualisation"):
        if filter_function(visualisation_directory):
            visualisation_directories.append(visualisation_directory)

    return sort_function(visualisation_directories)


def find_hockey_stick_visualisation_directories(archive_directory: Path) -> list[Path]:
    """
    Find visualisation directories containing hockey-stick (common format) JSON data.

    Hockey-stick data is written at the operation level, e.g.:
    - archive_directory/randread/visualisation/ (new structure)
    - archive_directory/randwrite/visualisation/ (new structure)
    - archive_directory/visualisation/ (legacy structure with JSON files)
    - archive_directory/**/visualisation/ (deeply nested legacy CBT structure)

    Only returns directories that actually contain .json files.

    Args:
        archive_directory: Root directory to search for visualisation directories

    Returns:
        List of Path objects for visualisation directories that contain JSON files
    """
    # First check for legacy structure: visualisation directly under archive directory
    legacy_visualisation_directory = archive_directory / "visualisation"
    if legacy_visualisation_directory.exists() and legacy_visualisation_directory.is_dir():
        # Only include if there are non-timeseries JSON files
        json_files = [
            filename
            for filename in legacy_visualisation_directory.glob(f"*{DATA_FILE_EXTENSION_WITH_DOT}")
            if not filename.stem.endswith("_timeseries")
        ]
        if json_files:
            # Legacy structure with data - use only this
            return [legacy_visualisation_directory]

    # Look for new structure: visualisation directories under operation directories
    # Operation directories are direct children of the archive directory
    visualisation_directories = []
    for operation_dir in archive_directory.iterdir():
        if operation_dir.is_dir() and not operation_dir.name.startswith("."):
            visualisation_directory = operation_dir / "visualisation"
            if visualisation_directory.exists() and visualisation_directory.is_dir():
                # Only include if there are non-timeseries JSON files
                json_files = [
                    filename
                    for filename in visualisation_directory.glob(f"*{DATA_FILE_EXTENSION_WITH_DOT}")
                    if not filename.stem.endswith("_timeseries")
                ]
                if json_files:
                    visualisation_directories.append(visualisation_directory)

    # If we found visualisation directories at top level, return them
    if visualisation_directories:
        return sorted(visualisation_directories, key=str)

    # If no visualisation directories found at top level, search recursively
    # This handles deeply nested CBT structures
    def has_json_files(visualisation_directory: Path) -> bool:
        """Filter function: check if directory contains non-timeseries JSON files and is not the legacy dir."""
        if visualisation_directory == legacy_visualisation_directory:
            return False
        # Only count non-timeseries JSON files
        json_files = [
            filename
            for filename in visualisation_directory.glob(f"*{DATA_FILE_EXTENSION_WITH_DOT}")
            if not filename.stem.endswith("_timeseries")
        ]
        return len(json_files) > 0

    return _find_visualisation_directories_with_filter(
        archive_directory=archive_directory,
        filter_function=has_json_files,
        sort_function=lambda paths: sorted(paths, key=str),
    )


def find_timeseries_visualisation_directories(archive_directory: Path) -> list[Path]:
    """
    Find visualisation directories containing timeseries data.

    Timeseries data is written at the iodepth/total_iodepth level, e.g.:
    - archive_directory/randread/total_iodepth-256/visualisation/
    - archive_directory/randread/iodepth-32/visualisation/

    Args:
        archive_directory: Root directory to search for visualisation directories

    Returns:
        List of Path objects for iodepth-level visualisation directories
    """

    def is_iodepth_directory(visualisation_directory: Path) -> bool:
        """Filter function: check if parent is an iodepth or total_iodepth directory."""
        parent_name = visualisation_directory.parent.name
        return parent_name.startswith(("iodepth-", "total_iodepth-"))

    def sort_by_priority(paths: list[Path]) -> list[Path]:
        """Sort by priority: total_iodepth > iodepth, then by path."""

        def sort_key(path: Path) -> tuple[int, str]:
            parent_name = path.parent.name
            if parent_name.startswith("total_iodepth"):
                return (0, str(path))
            # iodepth
            return (1, str(path))

        return sorted(paths, key=sort_key)

    return _find_visualisation_directories_with_filter(
        archive_directory=archive_directory, filter_function=is_iodepth_directory, sort_function=sort_by_priority
    )
