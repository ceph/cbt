"""
Unit tests for the TimeSeriesOutputFormatter class.
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from post_processing.formatter.time_series_output_formatter import (
    TimeSeriesOutputFormatter,
)


class TestTimeSeriesOutputFormatterInitialization(unittest.TestCase):
    """Test TimeSeriesOutputFormatter initialization"""

    def test_initialization_with_defaults(self) -> None:
        """Test initialization with default filename root"""
        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test")

        self.assertEqual(formatter._directory, "/tmp/test")
        self.assertEqual(formatter._filename_root, "json_output")
        self.assertEqual(formatter._timeseries_data, {})
        self.assertEqual(formatter._all_test_run_ids, set())
        self.assertEqual(formatter._benchmark_types, {})

    def test_initialization_with_custom_filename_root(self) -> None:
        """Test initialization with custom filename root"""
        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test", filename_root="custom_output")

        self.assertEqual(formatter._filename_root, "custom_output")


class TestGetTestrunDirectories(unittest.TestCase):
    """Test _get_testrun_directories method"""

    def test_with_id_in_path(self) -> None:
        """Test when 'id-' is in the archive directory path"""
        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test/id-12345")

        directories = formatter._get_testrun_directories("id-12345")

        self.assertEqual(len(directories), 1)
        self.assertEqual(directories[0], Path("/tmp/test/id-12345"))

    @patch("pathlib.Path.glob")
    def test_without_id_in_path(self, mock_glob: Mock) -> None:
        """Test when 'id-' is not in the archive directory path"""
        mock_glob.return_value = [Path("/tmp/test/results/id-12345")]

        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test")
        directories = formatter._get_testrun_directories("id-12345")

        mock_glob.assert_called_once_with("**/id-12345")
        self.assertEqual(len(directories), 1)


class TestFindAllTestrunIds(unittest.TestCase):
    """Test _find_all_testrun_ids method"""

    def test_find_ids_with_id_prefix(self) -> None:
        """Test finding test run IDs with 'id-' prefix"""
        file_list = [
            Path("/tmp/test/results/id-12345/rbdfio/json_output.0"),
            Path("/tmp/test/results/id-12345/rbdfio/json_output.1"),
            Path("/tmp/test/results/id-67890/rbdfio/json_output.0"),
        ]

        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test")
        formatter._find_all_testrun_ids(file_list)

        self.assertEqual(len(formatter._all_test_run_ids), 2)
        self.assertIn("id-12345", formatter._all_test_run_ids)
        self.assertIn("id-67890", formatter._all_test_run_ids)

    def test_find_ids_without_id_prefix(self) -> None:
        """Test finding test run IDs without 'id-' prefix"""
        file_list = [
            Path("/tmp/test/some_directory/workload1/json_output.0"),
            Path("/tmp/test/some_directory/workload2/json_output.0"),
        ]

        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test")
        formatter._find_all_testrun_ids(file_list)

        # When no 'id-' prefix, uses directory directly above the file
        self.assertEqual(len(formatter._all_test_run_ids), 2)
        self.assertIn("workload1", formatter._all_test_run_ids)
        self.assertIn("workload2", formatter._all_test_run_ids)


class TestProcessCompatibilityMode(unittest.TestCase):
    """Test _process_compatibility_mode method"""

    @patch("post_processing.formatter.time_series_output_formatter.get_run_result_from_directory_name")
    def test_process_compatibility_mode(self, mock_factory: Mock) -> None:
        """Test processing in compatibility mode"""
        # Create mock RunResult
        mock_result = MagicMock()
        mock_result.type = "rbdfio"
        # With new memory-efficient approach, timeseries data is written during process()
        # and get_timeseries() returns empty dict
        mock_result.get_timeseries.return_value = {}
        mock_factory.return_value = mock_result

        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test")
        benchmark_type = formatter._process_compatibility_mode()

        # Verify factory was called with include_timeseries=True
        mock_factory.assert_called_once_with(Path("/tmp/test"), "json_output", include_timeseries=True)
        mock_result.process.assert_called_once()
        # get_timeseries() is no longer called in the new memory-efficient approach

        self.assertEqual(benchmark_type, "rbdfio")
        # With new approach, timeseries_data should be empty (written during process())
        self.assertEqual(len(formatter._timeseries_data), 0)


class TestProcessSingleTestrun(unittest.TestCase):
    """Test _process_single_testrun method"""

    @patch("post_processing.formatter.time_series_output_formatter.get_run_result_from_directory_name")
    @patch("pathlib.Path.iterdir")
    def test_process_single_testrun(self, mock_iterdir: Mock, mock_factory: Mock) -> None:
        """Test processing a single test run"""
        # Create mock directories
        mock_dir1 = MagicMock()
        mock_dir1.is_dir.return_value = True
        mock_dir1.name = "randread_4k"

        mock_dir2 = MagicMock()
        mock_dir2.is_dir.return_value = True
        mock_dir2.name = "randwrite_4k"

        mock_iterdir.return_value = [mock_dir1, mock_dir2]

        # Create mock RunResult
        mock_result = MagicMock()
        mock_result.type = "rbdfio"
        mock_result.get_timeseries.return_value = {"test_key": {"operation": "randread", "blocksize": "4k"}}
        mock_factory.return_value = mock_result

        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test")
        testrun_dir = Path("/tmp/test/id-12345")

        benchmark_type = formatter._process_single_testrun(testrun_dir)

        # Verify factory was called twice (once for each directory)
        self.assertEqual(mock_factory.call_count, 2)
        self.assertEqual(mock_result.process.call_count, 2)
        self.assertEqual(benchmark_type, "rbdfio")


class TestProcess(unittest.TestCase):
    """Test process method"""

    @patch.object(TimeSeriesOutputFormatter, "_process_single_testrun")
    @patch.object(TimeSeriesOutputFormatter, "_get_testrun_directories")
    @patch.object(TimeSeriesOutputFormatter, "_find_all_testrun_ids")
    def test_process_with_test_runs(
        self,
        mock_find_ids: Mock,
        mock_get_dirs: Mock,
        mock_process_testrun: Mock,
    ) -> None:
        """Test processing with test runs found"""
        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test")

        # Mock finding test run IDs
        def set_test_run_ids(file_list: list[Path]) -> None:
            formatter._all_test_run_ids = {"id-12345"}

        mock_find_ids.side_effect = set_test_run_ids

        # Mock getting directories
        mock_get_dirs.return_value = [Path("/tmp/test/id-12345")]

        # Mock processing
        mock_process_testrun.return_value = "rbdfio"

        formatter.process()

        mock_find_ids.assert_called_once()
        mock_get_dirs.assert_called_once_with("id-12345")
        mock_process_testrun.assert_called_once()

    @patch.object(TimeSeriesOutputFormatter, "_find_all_testrun_ids")
    def test_process_with_no_test_runs(self, mock_find_ids: Mock) -> None:
        """Test processing when no test runs are found"""
        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test")

        # Mock finding no test run IDs
        def set_empty_test_run_ids(file_list: list[Path]) -> None:
            formatter._all_test_run_ids = set()

        mock_find_ids.side_effect = set_empty_test_run_ids

        formatter.process()

        mock_find_ids.assert_called_once()
        # Should return early without processing


class TestIntegration(unittest.TestCase):
    """Integration tests for TimeSeriesOutputFormatter"""

    @patch("post_processing.formatter.time_series_output_formatter.get_run_result_from_directory_name")
    @patch("pathlib.Path.glob")
    @patch("pathlib.Path.iterdir")
    def test_full_workflow(
        self,
        mock_iterdir: Mock,
        mock_glob: Mock,
        mock_factory: Mock,
    ) -> None:
        """Test complete workflow with memory-efficient processing"""
        # Setup mocks
        mock_glob.return_value = [
            Path("/tmp/test/id-12345/rbdfio/json_output.0"),
        ]

        mock_dir = MagicMock()
        mock_dir.is_dir.return_value = True
        mock_dir.name = "randread_4k"
        mock_iterdir.return_value = [mock_dir]

        mock_result = MagicMock()
        mock_result.type = "rbdfio"
        # With new memory-efficient approach, timeseries data is written during process()
        mock_factory.return_value = mock_result

        # Run workflow - data is written during process()
        formatter = TimeSeriesOutputFormatter(archive_directory="/tmp/test")
        formatter.process()

        # Verify
        mock_factory.assert_called()
        mock_result.process.assert_called()
        # With new approach, timeseries_data should be empty (written during process())
        self.assertEqual(len(formatter._timeseries_data), 0)


if __name__ == "__main__":
    unittest.main()

# Made with Bob
