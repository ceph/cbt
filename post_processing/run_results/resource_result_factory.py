"""
Factory for discovering and creating all available resource result parsers.

This module provides functionality to automatically discover and instantiate
all available resource monitoring parsers (FIO, Collectl, etc.) for a given
benchmark output file.
"""

from logging import Logger, getLogger
from pathlib import Path

from post_processing.run_results.resource_result import ResourceResult
from post_processing.run_results.resources.collectl_resource import CollectlResource
from post_processing.run_results.resources.fio_resource import FIOResource

log: Logger = getLogger("formatter")


def get_all_resources(file_path: Path) -> list[ResourceResult]:
    """
    Discover and instantiate all available resource parsers for a benchmark file.

    This function attempts to create resource parsers for all available monitoring
    sources. It will try FIO (embedded in benchmark output) and Collectl (separate
    monitoring files) if available.

    Args:
        file_path: Path to the benchmark output file (e.g., json_output.0)

    Returns:
        List of ResourceResult instances for all available sources.
        Returns empty list if no sources are available.
    """
    resources: list[ResourceResult] = []

    # Always try FIO resource (embedded in benchmark output)
    try:
        fio_resource = FIOResource(file_path)
        resources.append(fio_resource)
        log.debug("Added FIO resource for %s", file_path)
    except Exception as e:
        log.warning("Could not create FIO resource for %s: %s", file_path, e)

    # Check for collectl data
    collectl_dir = file_path.parent / "collectl"
    if collectl_dir.exists() and collectl_dir.is_dir():
        try:
            collectl_resource = CollectlResource(file_path)
            resources.append(collectl_resource)
            log.debug("Added Collectl resource for %s", file_path)
        except Exception as e:
            log.warning("Could not create Collectl resource for %s: %s", file_path, e)

    if not resources:
        log.error("No resource parsers available for %s", file_path)

    return resources


# Made with Bob
