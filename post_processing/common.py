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
from typing import Any, Optional, Union

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


def get_blocksize_percentage_operation_from_file_name(file_name: str) -> tuple[str, str, str]:
    """
        Return the blocksize , operation and read/write percentage from the
    filename

    The filename is in one of 2 formats:
        BLOCKSIZE_OPERATION.json
        BLOCKSIZE_READ_WRITE_OPERATION.json
    """
    file_parts: list[str] = file_name.split("_")

    # The split on _ will mean that the last element [-1] will always be
    # the operation, and the first part [0] will be the blocksize
    operation: str = f"{TITLE_CONVERSION[file_parts[-1]]}"
    blocksize: str = get_blocksize(f"{file_parts[0]}")
    blocksize = f"{int(int(blocksize) / 1024)}K"
    read_percent: str = ""

    if len(file_parts) > 2:
        read_percent = f"{file_parts[1]}/{file_parts[2]} "

    return (blocksize, read_percent, operation)


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
    data: CommonFormatDataType = read_intermediate_file(file_path=f"{file_path}")

    max_cpu: float = float(f"{data.get('maximum_cpu_usage', '0')}")
    max_memory: float = float(f"{data.get('maximum_memory_usage', '0')}")
    return f"{max_cpu:.2f}", f"{max_memory:.2f}"


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
    Find a list of file names that are common to all directories in
    a list of directories.
    """
    common_files: set[str] = set(path.parts[-1] for path in directories[0].glob(f"*{DATA_FILE_EXTENSION_WITH_DOT}"))

    # first find all the common paths between all the directories
    for index in range(1, (len(directories))):
        files: set[str] = set(path.parts[-1] for path in directories[index].glob(f"*{DATA_FILE_EXTENSION_WITH_DOT}"))
        common_files = common_files.intersection(files)

    return list(common_files)


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
