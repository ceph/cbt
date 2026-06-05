"""
Formatter for converting time-series log files to plotting format.

This module provides the TimeSeriesOutputFormatter class which automatically
discovers and processes time-series log files using the same factory pattern
as CommonOutputFormatter.
"""

from pathlib import Path
from typing import Optional

from post_processing.formatter.base_formatter import BaseFormatter
from post_processing.post_processing_types import TimeSeriesFormatType
from post_processing.run_results.run_result_factory import get_run_result_from_directory_name


class TimeSeriesOutputFormatter(BaseFormatter):
    """
    Automatically discover and process time-series log files.

    This class follows the same pattern as CommonOutputFormatter:
    - Uses factory pattern to get appropriate RunResult subclass
    - Delegates time-series processing to BenchmarkResult classes
    - Provides simple process() and write_output() interface
    """

    # To make sure that we read files that only contain JSON data we will look
    # at the json_output* files by default
    DEFAULT_OUTPUT_FILE_PART: str = "json_output"

    def __init__(self, archive_directory: str, filename_root: Optional[str] = None) -> None:
        """
        Initialize the time series output formatter.

        Args:
            archive_directory: Directory containing benchmark results
            filename_root: Root name for output files (default: "json_output")
        """
        super().__init__(archive_directory)
        self._filename_root: str = filename_root if filename_root else self.DEFAULT_OUTPUT_FILE_PART
        self._timeseries_data: dict[str, TimeSeriesFormatType] = {}
        self._benchmark_types: dict[str, str] = {}  # Track benchmark type per operation

    def _process_compatibility_mode(self) -> str:
        """
        Process test run using compatibility method for legacy directory structures.

        This handles cases where there are multiple directories for a single test run,
        which requires using the directory-level processing approach.

        Returns:
            The benchmark type identifier (e.g., "rbdfio", "fio")
        """
        results = get_run_result_from_directory_name(
            Path(self._directory), self._filename_root, include_timeseries=True
        )
        results.process()

        # Note: Timeseries data is now written directly to disk during RunResult.process()
        # to reduce memory usage. We no longer accumulate it here.

        return results.type

    def _process_single_testrun(self, testrun_directory: Path) -> str:
        """
        Process a single test run directory and all its IO pattern subdirectories.

        This method iterates through all subdirectories in the test run directory,
        processes each one with time-series enabled, and collects the results.

        Args:
            testrun_directory: Path to the test run directory

        Returns:
            The benchmark type identifier from the last processed result
        """
        benchmark_type: str = "unknown"

        for io_pattern_directory in [
            directory
            for directory in testrun_directory.iterdir()
            if directory.is_dir()
            and not f"{directory.name}".startswith(".")
            and "visualisation" not in f"{directory.name}"
        ]:
            self.log.debug("Looking at results for directory %s", io_pattern_directory)

            # Use factory method to get the correct results type based on directory name
            # Enable time-series processing
            results = get_run_result_from_directory_name(
                directory=io_pattern_directory, file_name_root=self._filename_root, include_timeseries=True
            )

            results.process()

            # Note: Timeseries data is now written directly to disk during RunResult.process()
            # to reduce memory usage. We no longer accumulate it here.

            benchmark_type = results.type

        return benchmark_type

    def process(self) -> None:
        """
        Process input data and convert to intermediate format.

        Discovers all test runs in the archive directory, processes them using
        the factory pattern to get appropriate RunResult subclasses, and extracts
        time-series data from each.
        """
        self.log.info("Processing time-series data from %s", self._directory)

        # Find all result files
        file_list = [
            path for path in self.path.glob(f"**/{self._filename_root}.*") if path.name.split(".")[-1].isdigit()
        ]
        self._find_all_testrun_ids(file_list)

        if not self._all_test_run_ids:
            self.log.warning("No test runs found in %s", self._directory)
            return

        for testrun_id in self._all_test_run_ids:
            self.log.debug("Looking at test run with id %s", testrun_id)

            testrun_directories = self._get_testrun_directories(testrun_id)

            if len(testrun_directories) > 1:
                self.log.debug(
                    "We have more than one directory for test run %s so using the compatibility method", testrun_id
                )
                benchmark_type = self._process_compatibility_mode()
            else:
                benchmark_type = self._process_single_testrun(testrun_directories[0])

            # Track benchmark type for all operations processed in this test run
            for key in self._timeseries_data:
                if key not in self._benchmark_types:
                    self._benchmark_types[key] = benchmark_type


# Made with Bob
