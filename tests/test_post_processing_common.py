"""
Unit tests for the post_processing/common.py module
"""

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from post_processing.common import (
    TITLE_CONVERSION,
    calculate_percent_difference_to_baseline,
    file_is_empty,
    file_is_precondition,
    find_common_data_file_names,
    get_blocksize,
    get_blocksize_percentage_operation_from_file_name,
    get_date_time_string,
    get_latency_throughput_from_file,
    get_resource_details_from_file,
    read_intermediate_file,
    recursive_search,
    strip_confidential_data_from_yaml,
    sum_mean_values,
    sum_standard_deviation_values,
)


class TestCommonFunctions(unittest.TestCase):
    """Test cases for common.py utility functions"""

    def test_get_blocksize_percentage_operation_from_file_name_simple(self) -> None:
        """Test parsing simple filename format: BLOCKSIZE_OPERATION"""
        blocksize, read_percent, operation = get_blocksize_percentage_operation_from_file_name("4096_read")
        self.assertEqual(blocksize, "4K")
        self.assertEqual(read_percent, "")
        self.assertEqual(operation, "Sequential Read")

    def test_get_blocksize_percentage_operation_from_file_name_with_percentage(self) -> None:
        """Test parsing filename with read/write percentage: BLOCKSIZE_READ_WRITE_OPERATION"""
        blocksize, read_percent, operation = get_blocksize_percentage_operation_from_file_name("4096_70_30_randrw")
        self.assertEqual(blocksize, "4K")
        self.assertEqual(read_percent, "70/30 ")
        self.assertEqual(operation, "Random Read/Write")

    def test_get_blocksize_percentage_operation_from_file_name_randwrite(self) -> None:
        """Test parsing randwrite operation"""
        blocksize, read_percent, operation = get_blocksize_percentage_operation_from_file_name("8192_randwrite")
        self.assertEqual(blocksize, "8K")
        self.assertEqual(read_percent, "")
        self.assertEqual(operation, "Random Write")

    def test_read_intermediate_file_success(self) -> None:
        """Test successfully reading an intermediate JSON file"""
        test_data: dict[str, Any] = {"key1": "value1", "key2": {"nested": "data"}}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_data, f)
            temp_path = f.name

        try:
            result = read_intermediate_file(temp_path)
            self.assertEqual(result, test_data)
        finally:
            Path(temp_path).unlink()

    def test_read_intermediate_file_not_found(self) -> None:
        """Test reading a non-existent file returns empty dict"""
        result = read_intermediate_file("/nonexistent/path/file.json")
        self.assertEqual(result, {})

    def test_get_latency_throughput_from_file_small_blocksize(self) -> None:
        """Test getting latency/throughput for small blocksize (< 64K) - returns IOPS"""
        test_data: dict[str, Any] = {
            "test_key": {"blocksize": "4096"},
            "maximum_iops": "1000.5",
            "latency_at_max_iops": "5.5",
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_data, f)
            temp_path = Path(f.name)

        try:
            throughput, latency = get_latency_throughput_from_file(temp_path)
            self.assertEqual(throughput, "1000 IOps")
            self.assertEqual(latency, "5.5")
        finally:
            temp_path.unlink()

    def test_get_latency_throughput_from_file_large_blocksize(self) -> None:
        """Test getting latency/throughput for large blocksize (>= 64K) - returns MB/s"""
        test_data: dict[str, Any] = {
            "test_key": {"blocksize": "65536"},  # 64K
            "maximum_iops": "1000",  # Still needed even though we use bandwidth
            "maximum_bandwidth": "100000000",  # 100MB in bytes
            "latency_at_max_iops": "5.0",
            "latency_at_max_bandwidth": "10.5",
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_data, f)
            temp_path = Path(f.name)

        try:
            throughput, latency = get_latency_throughput_from_file(temp_path)
            self.assertEqual(throughput, "100 MB/s")
            self.assertEqual(latency, "10.5")
        finally:
            temp_path.unlink()

    def test_get_resource_details_from_file(self) -> None:
        """Test extracting CPU and memory usage from file"""
        test_data = {
            "maximum_cpu_usage": "45.67",
            "maximum_memory_usage": "2048.89",
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_data, f)
            temp_path = Path(f.name)

        try:
            cpu, memory = get_resource_details_from_file(temp_path)
            self.assertEqual(cpu, "45.67")
            self.assertEqual(memory, "2048.89")
        finally:
            temp_path.unlink()

    def test_get_resource_details_from_file_missing_keys(self) -> None:
        """Test extracting resource details when keys are missing"""
        test_data: dict[str, str] = {}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(test_data, f)
            temp_path = Path(f.name)

        try:
            cpu, memory = get_resource_details_from_file(temp_path)
            self.assertEqual(cpu, "0.00")
            self.assertEqual(memory, "0.00")
        finally:
            temp_path.unlink()

    def test_strip_confidential_data_from_yaml_ipv4(self) -> None:
        """Test stripping IPv4 addresses from YAML"""
        yaml_data = "server: 192.168.1.100\nbackup: 10.0.0.1"
        result = strip_confidential_data_from_yaml(yaml_data)
        self.assertNotIn("192.168.1.100", result)
        self.assertNotIn("10.0.0.1", result)
        self.assertIn("--- IP Address --", result)

    def test_strip_confidential_data_from_yaml_hostname(self) -> None:
        """Test stripping hostnames from YAML"""
        yaml_data = "server: server1.example.com\nbackup: server2.example.com"
        result = strip_confidential_data_from_yaml(yaml_data)
        self.assertNotIn("server1.example.com", result)
        self.assertNotIn("server2.example.com", result)
        self.assertIn("--- server1 ---", result)
        self.assertIn("--- server2 ---", result)

    def test_strip_confidential_data_from_yaml_mixed(self) -> None:
        """Test stripping both IPs and hostnames"""
        yaml_data = "server: 192.168.1.1 host.example.com"
        result = strip_confidential_data_from_yaml(yaml_data)
        self.assertNotIn("192.168.1.1", result)
        self.assertNotIn("host.example.com", result)

    def test_find_common_data_file_names(self) -> None:
        """Test finding common files across multiple directories"""
        with tempfile.TemporaryDirectory() as temp_dir:
            dir1 = Path(temp_dir) / "dir1"
            dir2 = Path(temp_dir) / "dir2"
            dir3 = Path(temp_dir) / "dir3"

            dir1.mkdir()
            dir2.mkdir()
            dir3.mkdir()

            # Create common files
            (dir1 / "file1.json").touch()
            (dir1 / "file2.json").touch()
            (dir1 / "file3.json").touch()

            (dir2 / "file1.json").touch()
            (dir2 / "file2.json").touch()
            (dir2 / "unique.json").touch()

            (dir3 / "file1.json").touch()
            (dir3 / "file2.json").touch()

            common_files = find_common_data_file_names([dir1, dir2, dir3])

            self.assertEqual(len(common_files), 2)
            self.assertIn("file1.json", common_files)
            self.assertIn("file2.json", common_files)
            self.assertNotIn("file3.json", common_files)
            self.assertNotIn("unique.json", common_files)

    def test_calculate_percent_difference_to_baseline(self) -> None:
        """Test calculating percentage difference"""
        result = calculate_percent_difference_to_baseline("100 IOps", "150 IOps")
        self.assertEqual(result, "50%")

        result = calculate_percent_difference_to_baseline("200 MB/s", "180 MB/s")
        self.assertEqual(result, "-10%")

        result = calculate_percent_difference_to_baseline("100 IOps", "100 IOps")
        self.assertEqual(result, "0%")

    def test_get_date_time_string(self) -> None:
        """Test getting formatted date/time string"""
        result = get_date_time_string()
        # Should be in format YYMMDD_HHMMSS
        self.assertEqual(len(result), 13)
        self.assertTrue(result[6] == "_")
        self.assertTrue(result[:6].isdigit())
        self.assertTrue(result[7:].isdigit())

    def test_recursive_search_simple(self) -> None:
        """Test recursive search in simple dictionary"""
        data = {"key1": "value1", "key2": "value2"}
        result = recursive_search(data, "key1")
        self.assertEqual(result, "value1")

    def test_recursive_search_nested(self) -> None:
        """Test recursive search in nested dictionary"""
        data = {"level1": {"level2": {"target_key": "found_value"}}}
        result = recursive_search(data, "target_key")
        self.assertEqual(result, "found_value")

    def test_recursive_search_with_list(self) -> None:
        """Test recursive search with list containing dictionaries"""
        data = {"items": [{"name": "item1"}, {"target_key": "found_in_list"}]}
        result = recursive_search(data, "target_key")
        self.assertEqual(result, "found_in_list")

    def test_recursive_search_not_found(self) -> None:
        """Test recursive search when key doesn't exist"""
        data = {"key1": "value1"}
        result = recursive_search(data, "nonexistent")
        self.assertIsNone(result)

    def test_get_blocksize_with_unit(self) -> None:
        """Test extracting blocksize from string with unit suffix"""
        self.assertEqual(get_blocksize("4K"), "4")
        self.assertEqual(get_blocksize("8M"), "8")
        self.assertEqual(get_blocksize("16G"), "16")

    def test_get_blocksize_without_unit(self) -> None:
        """Test extracting blocksize from numeric string"""
        self.assertEqual(get_blocksize("4096"), "4096")
        self.assertEqual(get_blocksize("8192"), "8192")

    def test_sum_standard_deviation_values(self) -> None:
        """Test summing standard deviations from multiple runs"""
        std_deviations = [1.0, 2.0, 1.5]
        operations = [100, 200, 150]
        latencies = [5.0, 6.0, 5.5]
        total_ios = 450
        combined_latency = 5.611111  # weighted average

        result = sum_standard_deviation_values(std_deviations, operations, latencies, total_ios, combined_latency)

        # Result should be a positive float
        self.assertIsInstance(result, float)
        self.assertGreater(result, 0)

    def test_file_is_empty_true(self) -> None:
        """Test detecting empty file"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = Path(f.name)

        try:
            self.assertTrue(file_is_empty(temp_path))
        finally:
            temp_path.unlink()

    def test_file_is_empty_false(self) -> None:
        """Test detecting non-empty file"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("some content")
            temp_path = Path(f.name)

        try:
            self.assertFalse(file_is_empty(temp_path))
        finally:
            temp_path.unlink()

    def test_file_is_precondition_true(self) -> None:
        """Test detecting precondition file"""
        path = Path("/some/path/precond_test.json")
        self.assertTrue(file_is_precondition(path))

    def test_file_is_precondition_false(self) -> None:
        """Test detecting non-precondition file"""
        path = Path("/some/path/regular_test.json")
        self.assertFalse(file_is_precondition(path))

    def test_sum_mean_values(self) -> None:
        """Test calculating combined mean from multiple means"""
        latencies = [5.0, 6.0, 7.0]
        num_ops = [100, 200, 300]
        total_ios = 600

        result = sum_mean_values(latencies, num_ops, total_ios)

        # Expected: (5*100 + 6*200 + 7*300) / 600 = 3800/600 = 6.333...
        self.assertAlmostEqual(result, 6.333333, places=5)

    def test_sum_mean_values_equal_weights(self) -> None:
        """Test combined mean with equal weights"""
        latencies = [4.0, 6.0, 8.0]
        num_ops = [100, 100, 100]
        total_ios = 300

        result = sum_mean_values(latencies, num_ops, total_ios)

        # Expected: (4 + 6 + 8) / 3 = 6.0
        self.assertAlmostEqual(result, 6.0, places=5)

    def test_title_conversion_dict(self) -> None:
        """Test that TITLE_CONVERSION contains expected mappings"""
        self.assertEqual(TITLE_CONVERSION["read"], "Sequential Read")
        self.assertEqual(TITLE_CONVERSION["write"], "Sequential Write")
        self.assertEqual(TITLE_CONVERSION["randread"], "Random Read")
        self.assertEqual(TITLE_CONVERSION["randwrite"], "Random Write")
        self.assertEqual(TITLE_CONVERSION["readwrite"], "Sequential Read/Write")
        self.assertEqual(TITLE_CONVERSION["randrw"], "Random Read/Write")


if __name__ == "__main__":
    unittest.main()

# Made with Bob
