"""
Base class for all formatters.

This module provides the BaseFormatter abstract class that defines
the common API for all formatter implementations.
"""

import re
from abc import ABC, abstractmethod
from logging import Logger, getLogger
from pathlib import Path
from re import Pattern

from post_processing.common import split_group_and_host
from post_processing.post_processing_types import DirectoryGroup

# The test run directory is named by benchmark.py as "id-" followed by the first
# 8 characters of a sha1 hex digest
_TESTRUN_ID_PATTERN: Pattern[str] = re.compile(r"id-[0-9a-f]{8}")


class BaseFormatter(ABC):
    """
    Base class for all formatters.

    Formatters convert benchmark output into intermediate formats
    for report generation. With the memory-efficient approach,
    data is written during process() to avoid accumulating large
    datasets in memory.
    """

    def __init__(self, archive_directory: str) -> None:
        """
        Initialize formatter.

        Args:
            archive_directory: Directory containing benchmark results
        """
        self._directory = archive_directory
        self._log: Logger = getLogger("cbt.formatter")
        self._all_test_run_ids: set[str] = set()

    @property
    def log(self) -> Logger:
        """Get the logger instance"""
        return self._log

    @property
    def path(self) -> Path:
        """
        Get Path object for archive directory.

        Returns:
            Path object representing the archive directory
        """
        return Path(self._directory)

    def _ensure_output_directory(self, directory: Path) -> None:
        """
        Ensure output directory exists, creating it if necessary.

        This method creates the directory and any necessary parent directories.
        If the directory already exists, no action is taken.

        Args:
            directory: Path object for directory to create
        """
        if not directory.exists():
            self.log.debug("Creating output directory: %s", directory)
            directory.mkdir(parents=True, exist_ok=True)

    def _get_testrun_directories(self, testrun_id: str) -> list[Path]:
        """
        Get the directories for a specific test run ID.

        When calling from CBT itself, the archive dir already includes the testrun
        directory, so we handle this case by checking if an 'id-xxxxxxxx' directory
        is in the path.

        Args:
            testrun_id: The test run identifier to find directories for

        Returns:
            List of Path objects for directories matching the test run ID
        """
        if any(_TESTRUN_ID_PATTERN.fullmatch(part) for part in self.path.parts):
            return [self.path]
        return list(self.path.glob(f"**/{testrun_id}"))

    def _get_io_pattern_directories(self, testrun_directory: Path, file_name_root: str) -> list[Path]:
        """
        Get the directories inside a test run directory that contain benchmark results.

        A test run directory also holds the output of the resource monitoring tools
        (for example collectl.<host> and pool_monitoring.<host>). These contain no
        benchmark result files so cannot be processed as an I/O pattern directory.
        Only directories that contain at least one <file_name_root>.<number> file are
        returned. Hidden directories and visualisation directories are always skipped.

        Args:
            testrun_directory: Path to the test run directory
            file_name_root: Root name of the result files (e.g. "json_output")

        Returns:
            List of Path objects for the directories that contain result files
        """
        io_pattern_directories: list[Path] = []

        for directory in self._get_candidate_directories(testrun_directory):
            if self._directory_has_result_files(directory, file_name_root):
                io_pattern_directories.append(directory)
            else:
                self.log.debug("Skipping directory %s as it contains no %s files", directory, file_name_root)

        if not io_pattern_directories:
            self.log.warning("No I/O Pattern directories found that contain files")
        return io_pattern_directories

    def _group_io_pattern_directories(self, testrun_directory: Path, file_name_root: str) -> list[DirectoryGroup]:
        """
        Group the directories in a test run directory by the workload they belong to.

        In workloads mode rpdcp appends the host name to each directory, so a workload that
        ran on several nodes is spread over <workload>.<host1>, <workload>.<host2> and so on.
        Directories sharing the same base workload name form a group. The primary of a group
        is the first (sorted) directory that contains result files; it is never a
        monitoring-only directory. Other directories with result files are result peers and
        directories without result files are monitoring peers. A group with no directory
        containing result files (for example idle_monitoring.<host>) is dropped.

        Directories are split into workload and host on the first dot, so base workload names
        are assumed not to contain dots. A workload whose name does contain a dot would be
        grouped under the wrong base name; resolving that needs the known workload or host
        names to be passed to split_group_and_host().

        Args:
            testrun_directory: Path to the test run directory
            file_name_root: Root name of the result files (e.g. "json_output")

        Returns:
            List of DirectoryGroup, sorted by base workload name
        """
        members_by_workload: dict[str, list[Path]] = {}

        for directory in self._get_candidate_directories(testrun_directory):
            workload, _ = split_group_and_host(directory.name)
            members_by_workload.setdefault(workload, []).append(directory)

        groups: list[DirectoryGroup] = []

        for workload, members in sorted(members_by_workload.items()):
            result_members: list[Path] = [
                member for member in members if self._directory_has_result_files(member, file_name_root)
            ]
            if not result_members:
                self.log.debug("Skipping %s as none of its directories contain %s files", workload, file_name_root)
                continue

            groups.append(
                DirectoryGroup(
                    primary=result_members[0],
                    result_peers=result_members[1:],
                    monitoring_peers=[member for member in members if member not in result_members],
                )
            )

        if not groups:
            self.log.warning("No I/O Pattern directories found that contain files")
        return groups

    @staticmethod
    def _get_candidate_directories(testrun_directory: Path) -> list[Path]:
        """
        Get the sorted, non-hidden, non-visualisation directories inside a test run directory.

        The result is sorted because iterdir() order is arbitrary and everything built from
        these directories (primary selection in particular) must be deterministic.

        The sort key is the directory name, which is only equivalent to sorting by full path
        because every entry is a direct child of the same parent directory.
        """
        return sorted(
            (
                directory
                for directory in testrun_directory.iterdir()
                if directory.is_dir() and not directory.name.startswith(".") and "visualisation" not in directory.name
            ),
            key=lambda directory: directory.name,
        )

    @staticmethod
    def _directory_has_result_files(directory: Path, file_name_root: str) -> bool:
        """Return True if the directory contains at least one <file_name_root>.<number> file."""
        return any(path.suffix[1:].isdigit() for path in directory.glob(f"**/{file_name_root}.*"))

    def _find_all_testrun_ids(self, file_list: list[Path]) -> None:
        """
        Find all the unique test run IDs from a list of file paths.

        Populates self._all_test_run_ids with unique test run ID strings.

        Args:
            file_list: List of Path objects for result files

        Note: This may only work for fio output runs in cbt, and we will need
        separate sub-classes for each benchmark type to be able to find and
        parse the required data
        """
        for file_path in file_list:
            # We know the format of the output dir is something like
            # <archive_dir>/00000000/id-ab40819c/<job_specific_details>/output.x
            #
            # We know that the output files reside in the directory structure with the
            # test run ID, so splitting up the path gives the filename as the
            # last element (-1) and the test run id directory somewhere higher up
            # the directory structure.
            # This should allow us to get test run IDs when any point in the
            # archive directory tree is passed as the archive directory
            potential_ids: list[str] = [part for part in file_path.parts if _TESTRUN_ID_PATTERN.fullmatch(part)]
            # There is a possibility that there could be more than one id-xxxxxxxx string in the
            # file path, and we want only one. We choose to always take the first one.
            # If there are none then just return the directory name above the file
            if len(potential_ids) > 0:
                testrun_id: str = potential_ids[0]
            else:
                # if we get no matches then just use the directory directly above
                # the output file
                testrun_id = file_path.parts[-2]

            self._all_test_run_ids.add(testrun_id)

    @abstractmethod
    def process(self) -> None:
        """
        Process input data and convert to intermediate format.

        This method reads benchmark output files and converts them
        to the formatter's target format. With the memory-efficient
        approach, data is written immediately during processing to
        avoid accumulating large datasets in memory.
        """


# Made with Bob
