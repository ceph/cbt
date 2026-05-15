"""
Unit tests for RunResult directory detection and timeseries writing.

Tests verify that timeseries and hockey-stick data are written at the
correct directory levels in the hierarchy.
"""

import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from post_processing.post_processing_types import TimeSeriesFormatType
from post_processing.run_results.run_result import RunResult


class TestRunResultDirectoryDetection(unittest.TestCase):
    """Test directory detection for timeseries output"""

    def setUp(self):
        """Set up test fixtures"""
        # Create a mock RunResult subclass
        self.mock_run_result = MagicMock(spec=RunResult)
        self.mock_run_result._determine_aggregation_directory_from_file = (
            RunResult._determine_aggregation_directory_from_file.__get__(self.mock_run_result)
        )

    def test_determine_aggregation_directory_from_file_with_total_iodepth(self):
        """Test detection of total_iodepth directory from file path"""
        file_path = Path(
            "/home/user/results/00000000/id-806228fa/seq8kwrite.host.com/"
            "rbdfio/numjobs-001/total_iodepth-256/iodepth-000032/json_output.0"
        )

        result = self.mock_run_result._determine_aggregation_directory_from_file(file_path)

        expected = Path(
            "/home/user/results/00000000/id-806228fa/seq8kwrite.host.com/"
            "rbdfio/numjobs-001/total_iodepth-256"
        )
        self.assertEqual(result, expected)

    def test_determine_aggregation_directory_from_file_with_iodepth_only(self):
        """Test detection of iodepth directory when no total_iodepth exists"""
        file_path = Path(
            "/home/user/results/00000000/id-123/test/rbdfio/"
            "numjobs-001/iodepth-032/json_output.0"
        )

        result = self.mock_run_result._determine_aggregation_directory_from_file(file_path)

        expected = Path("/home/user/results/00000000/id-123/test/rbdfio/numjobs-001/iodepth-032")
        self.assertEqual(result, expected)

    def test_determine_aggregation_directory_from_file_no_depth_dirs(self):
        """Test fallback to parent directory when no depth directories exist"""
        file_path = Path("/home/user/results/00000000/id-123/test/rbdfio/numjobs-001/json_output.0")

        result = self.mock_run_result._determine_aggregation_directory_from_file(file_path)

        expected = Path("/home/user/results/00000000/id-123/test/rbdfio/numjobs-001")
        self.assertEqual(result, expected)

    def test_determine_aggregation_directory_from_file_prefers_total_iodepth(self):
        """Test that total_iodepth is preferred over iodepth"""
        # Path with both total_iodepth and iodepth
        file_path = Path(
            "/home/user/results/rbdfio/numjobs-001/total_iodepth-256/"
            "iodepth-000032/json_output.0"
        )

        result = self.mock_run_result._determine_aggregation_directory_from_file(file_path)

        # Should stop at total_iodepth, not continue to iodepth
        expected = Path("/home/user/results/rbdfio/numjobs-001/total_iodepth-256")
        self.assertEqual(result, expected)


class TestRunResultTimeseriesWriting(unittest.TestCase):
    """Test timeseries data writing at correct directory levels"""

    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = Path("/tmp/test_run_result")

    @patch("json.dump")
    @patch("post_processing.run_results.run_result.Path.open")
    @patch("post_processing.run_results.run_result.Path.mkdir")
    def test_write_timeseries_by_directory_groups_correctly(self, mock_mkdir, mock_open, mock_json_dump):
        """Test that timeseries data is grouped and written by directory"""
        # Create a mock RunResult
        mock_result = MagicMock(spec=RunResult)
        mock_result._timeseries_by_directory = {
            Path("/test/total_iodepth-256"): {
                "randread_4k_256": {
                    "benchmark": "fio",
                    "operation": "randread",
                    "blocksize": "4k",
                    "numjobs": "1",
                    "metadata": {},
                    "timeseries": [],
                }
            },
            Path("/test/total_iodepth-128"): {
                "randwrite_4k_128": {
                    "benchmark": "fio",
                    "operation": "randwrite",
                    "blocksize": "4k",
                    "numjobs": "1",
                    "metadata": {},
                    "timeseries": [],
                }
            },
        }

        # Call the actual method
        RunResult._write_and_clear_timeseries_by_directory(mock_result)

        # Verify mkdir was called for each directory
        self.assertEqual(mock_mkdir.call_count, 2)

        # Verify json.dump was called twice (once per file)
        self.assertEqual(mock_json_dump.call_count, 2)

        # Verify data was cleared
        self.assertEqual(len(mock_result._timeseries_by_directory), 0)

    @patch("json.dump")
    @patch("builtins.open")
    def test_write_timeseries_creates_visualisation_subdirectory(self, mock_open, mock_json_dump):
        """Test that visualisation subdirectory is created at aggregation level"""
        import tempfile
        import shutil
        
        # Use a real temporary directory for this test
        test_dir = Path(tempfile.mkdtemp())
        try:
            agg_dir = test_dir / "total_iodepth-256"
            agg_dir.mkdir(parents=True)
            
            mock_result = MagicMock(spec=RunResult)
            mock_result._timeseries_by_directory = {
                agg_dir: {
                    "test_key": {
                        "benchmark": "fio",
                        "operation": "randread",
                        "blocksize": "4k",
                        "numjobs": "1",
                        "metadata": {},
                        "timeseries": [],
                    }
                }
            }

            RunResult._write_and_clear_timeseries_by_directory(mock_result)

            # Verify visualisation subdirectory was created
            expected_dir = agg_dir / "visualisation"
            self.assertTrue(expected_dir.exists(), f"Expected {expected_dir} to exist")
            self.assertTrue(expected_dir.is_dir(), f"Expected {expected_dir} to be a directory")
        finally:
            # Clean up
            shutil.rmtree(test_dir, ignore_errors=True)


class TestRunResultIntegration(unittest.TestCase):
    """Integration tests for directory detection and writing"""

    def test_timeseries_grouping_by_file_location(self):
        """Test that files from different directories are grouped separately"""
        # This would be an integration test that processes actual files
        # and verifies they're written to the correct locations
        # Placeholder for now - would need actual test data
        pass


if __name__ == "__main__":
    unittest.main()

# Made with Bob
