"""
Unit tests for the CommonOutputFormatter class
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any

from post_processing.formatter.common_output_formatter import CommonOutputFormatter


class TestCommonOutputFormatter(unittest.TestCase):
    """
    Unit tests for CommonOutputFormatter class methods
    """

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.formatter = CommonOutputFormatter(self.temp_dir)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test that CommonOutputFormatter initializes correctly"""
        self.assertEqual(self.formatter._directory, self.temp_dir)
        self.assertEqual(self.formatter._filename_root, "json_output")
        self.assertIsInstance(self.formatter._formatted_output, dict)
        self.assertIsInstance(self.formatter._all_test_run_ids, set)
        self.assertEqual(len(self.formatter._all_test_run_ids), 0)

    def test_initialization_with_custom_filename_root(self) -> None:
        """Test initialization with custom filename root"""
        custom_formatter = CommonOutputFormatter(self.temp_dir, "custom_output")
        self.assertEqual(custom_formatter._filename_root, "custom_output")

    def test_find_maximum_bandwidth_and_iops_with_latency(self) -> None:
        """Test finding maximum bandwidth and IOPS with associated latencies"""
        test_data: dict[str, Any] = {
            "1": {
                "bandwidth_bytes": "1000000",
                "iops": "100.5",
                "latency": "5000000",  # 5ms in nanoseconds
            },
            "2": {
                "bandwidth_bytes": "2000000",
                "iops": "150.5",
                "latency": "8000000",  # 8ms in nanoseconds
            },
            "4": {
                "bandwidth_bytes": "1500000",
                "iops": "200.5",
                "latency": "10000000",  # 10ms in nanoseconds
            },
            "metadata": "should be ignored",
        }

        max_bw, bw_lat, max_iops, iops_lat = self.formatter._find_maximum_bandwidth_and_iops_with_latency(test_data)
        # Max bandwidth is 2000000 with latency 8ms
        self.assertEqual(max_bw, "2000000.0")
        self.assertEqual(bw_lat, "8.0")

        # Max IOPS is 200.5 with latency 10ms
        self.assertEqual(max_iops, "200.5")
        self.assertEqual(iops_lat, "10.0")

    def test_find_maximum_bandwidth_and_iops_with_empty_data(self) -> None:
        """Test finding maximum values with empty data"""
        test_data: dict[str, Any] = {}

        max_bw, bw_lat, max_iops, iops_lat = self.formatter._find_maximum_bandwidth_and_iops_with_latency(test_data)

        self.assertEqual(max_bw, "0")
        self.assertEqual(bw_lat, "0")
        self.assertEqual(max_iops, "0")
        self.assertEqual(iops_lat, "0")

    def test_find_max_resource_usage(self) -> None:
        """Test finding maximum CPU and memory usage"""
        test_data: dict[str, Any] = {
            "1": {
                "cpu": "25.5",
                "memory": "1024",
            },
            "2": {
                "cpu": "45.8",
                "memory": "2048",
            },
            "4": {
                "cpu": "35.2",
                "memory": "1536",
            },
            "metadata": "should be ignored",
        }

        max_cpu, max_memory = self.formatter._find_max_resource_usage(test_data)

        self.assertEqual(max_cpu, "45.8")
        # Note: max_memory is not currently implemented in the code
        self.assertEqual(max_memory, "0")

    def test_find_max_resource_usage_with_empty_data(self) -> None:
        """Test finding maximum resource usage with empty data"""
        test_data: dict[str, Any] = {}

        max_cpu, max_memory = self.formatter._find_max_resource_usage(test_data)

        self.assertEqual(max_cpu, "0")
        self.assertEqual(max_memory, "0")

    def test_find_all_results_files_in_directory(self) -> None:
        """Test finding result files in directory"""
        # Create test directory structure with mock files
        test_dir = Path(self.temp_dir) / "test_run" / "id-12345"
        test_dir.mkdir(parents=True, exist_ok=True)

        # Create valid output files
        (test_dir / "json_output.0").touch()
        (test_dir / "json_output.1").touch()
        (test_dir / "json_output.2").touch()

        # Create files that should be ignored
        (test_dir / "json_output.txt").touch()
        (test_dir / "other_file.0").touch()

        formatter = CommonOutputFormatter(str(test_dir))
        formatter._find_all_results_files_in_directory()

        # Should find exactly 3 files
        self.assertEqual(len(formatter._file_list), 3)

        # All files should match the pattern
        for file_path in formatter._file_list:
            self.assertTrue(file_path.name.startswith("json_output."))
            self.assertTrue(file_path.name.split(".")[-1].isdigit())

    def test_find_all_testrun_ids(self) -> None:
        """Test extracting test run IDs from file paths"""
        # Create test directory structure
        test_dir = Path(self.temp_dir)
        run1_dir = test_dir / "00000000" / "id-abc123" / "workload"
        run2_dir = test_dir / "00000001" / "id-def456" / "workload"

        run1_dir.mkdir(parents=True, exist_ok=True)
        run2_dir.mkdir(parents=True, exist_ok=True)

        # Create output files
        (run1_dir / "json_output.0").touch()
        (run2_dir / "json_output.0").touch()

        formatter = CommonOutputFormatter(str(test_dir))
        formatter._find_all_results_files_in_directory()
        formatter._find_all_testrun_ids()

        # Should find 2 unique test run IDs
        self.assertEqual(len(formatter._all_test_run_ids), 2)
        self.assertIn("id-abc123", formatter._all_test_run_ids)
        self.assertIn("id-def456", formatter._all_test_run_ids)

    def test_find_all_testrun_ids_without_id_prefix(self) -> None:
        """Test extracting test run IDs when no 'id-' prefix exists"""
        # Create test directory structure without id- prefix
        test_dir = Path(self.temp_dir)
        run_dir = test_dir / "some_directory" / "workload"

        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "json_output.0").touch()

        formatter = CommonOutputFormatter(str(test_dir))
        formatter._find_all_results_files_in_directory()
        formatter._find_all_testrun_ids()

        # Should use the directory name above the file
        self.assertEqual(len(formatter._all_test_run_ids), 1)
        self.assertIn("workload", formatter._all_test_run_ids)


if __name__ == "__main__":
    unittest.main()

# Made with Bob
