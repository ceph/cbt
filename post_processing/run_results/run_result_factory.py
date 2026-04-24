"""
Factory for creating the appropriate RunResult subclass based on benchmark type.

This module provides a factory method that determines which RunResult implementation
to use based on the directory name, which should contain the benchmark type identifier.
"""

from logging import Logger, getLogger
from pathlib import Path

from post_processing.run_results.rbdfio import RBDFIO
from post_processing.run_results.run_result import RunResult

log: Logger = getLogger(name="formatter")


# Map benchmark types to their RunResult classes
# Currently only RBDFIO is implemented, but this can be extended
BENCHMARK_TYPE_MAP: dict[str, type[RunResult]] = {
    "rbdfio": RBDFIO,
    "librbdfio": RBDFIO,  # LibrbdFio uses same result format as RbdFio
    "kvmrbdfio": RBDFIO,  # KvmRbdFio uses same result format as RbdFio
    "rawfio": RBDFIO,  # RawFio uses same result format as RbdFio
    # Add more mappings as new RunResult subclasses are implemented
    # 'radosbench': RadosBench,
    # 'cosbench': CosBench,
    # 'hsbench': HsBench,
    # 'getput': GetPut,
    # etc.
}


def get_run_result_from_directory_name(directory: Path, file_name_root: str) -> RunResult:
    """
    Create the appropriate RunResult subclass based on the directory name.

    The directory name should contain the benchmark type name (e.g., 'rbdfio',
    'librbdfio', 'kvmrbdfio', 'rawfio', 'radosbench', etc.) which corresponds
    to the benchmark class name.

    Args:
        directory: Path to the directory containing benchmark results
        file_name_root: Root name of the result files to process

    Returns:
        An instance of the appropriate RunResult subclass

    Note:
        If the benchmark type cannot be determined from the directory name,
        a NotImplementedError will be raised
    """
    directory_str: str = ""
    # We need to look at the directory that is inside the directory we are passed.
    # For example, if we are passed /tmp/results/id-1234/precondition, we need to look at
    # /tmp/results/id-1234/precondition/rbdfio to determine the benchmark type.
    for entry in directory.iterdir():
        if entry.is_dir():
            directory_str = str(entry).lower()
            break

    # Try to find a matching benchmark type in the directory path
    for benchmark_type, result_class in BENCHMARK_TYPE_MAP.items():
        if benchmark_type in directory_str:
            log.debug("Creating %s result processor for directory %s", result_class.__name__, directory)
            return result_class(directory=directory, file_name_root=file_name_root)

    raise NotImplementedError(f"Could not determine benchmark type from directory {directory}, ")


# Made with Bob
