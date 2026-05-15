"""
Base class for all formatters.

This module provides the BaseFormatter abstract class that defines
the common API for all formatter implementations.
"""

from abc import ABC, abstractmethod
from logging import Logger, getLogger
from pathlib import Path


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
        self._log: Logger = getLogger("formatter")
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
        directory, so we handle this case by checking if 'id-' is in the path.

        Args:
            testrun_id: The test run identifier to find directories for

        Returns:
            List of Path objects for directories matching the test run ID
        """
        if "id-" in f"{self.path}":
            return [self.path]
        return list(self.path.glob(f"**/{testrun_id}"))

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
            potential_ids: list[str] = [part for part in file_path.parts if "id-" in part]
            # There is a possibility that there could be more than one id-xxxxxx string in the
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
