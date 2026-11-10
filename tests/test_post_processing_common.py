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
    find_iodepth_directory,
    find_path_in_ancestors,
    get_blocksize,
    get_blocksize_percentage_operation_numjobs_from_file_name,
    get_date_time_string,
    get_iodepth_from_path,
    get_latency_throughput_from_file,
    get_resource_details_from_file,
    read_intermediate_file,
    recursive_search,
    strip_confidential_data_from_yaml,
    sum_mean_values,
    sum_standard_deviation_values,
)


class TestCommonFunctions(unittest.TestCase):  # pylint: disable=too-many-public-methods
    """Test cases for common.py utility functions"""

    def test_get_blocksize_percentage_operation_from_file_name_simple(self) -> None:
        """Test parsing simple filename format: BLOCKSIZE_NUMJOBS_OPERATION"""
        blocksize, read_percent, operation, number_of_jobs = get_blocksize_percentage_operation_numjobs_from_file_name(
            "4096_1_read"
        )
        self.assertEqual(blocksize, "4K")
        self.assertEqual(read_percent, "")
        self.assertEqual(operation, "Sequential Read")
        self.assertEqual(number_of_jobs, "1")

    def test_get_blocksize_percentage_operation_from_file_name_with_percentage(self) -> None:
        """Test parsing filename with read/write percentage: BLOCKSIZE_NUMJOBS_READ_WRITE_OPERATION"""
        blocksize, read_percent, operation, number_of_jobs = get_blocksize_percentage_operation_numjobs_from_file_name(
            "4096_1_70_30_randrw"
        )
        self.assertEqual(blocksize, "4K")
        self.assertEqual(read_percent, "70/30 ")
        self.assertEqual(operation, "Random Read/Write")
        self.assertEqual(number_of_jobs, "1")

    def test_get_blocksize_percentage_operation_from_file_name_randwrite(self) -> None:
        """Test parsing randwrite operation"""
        blocksize, read_percent, operation, number_of_jobs = get_blocksize_percentage_operation_numjobs_from_file_name(
            "8192_2_randwrite"
        )
        self.assertEqual(blocksize, "8K")
        self.assertEqual(read_percent, "")
        self.assertEqual(operation, "Random Write")
        self.assertEqual(number_of_jobs, "2")

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
            # pylint cannot see that the cast() in get_resource_details_from_file gives a 2-tuple
            cpu, memory = get_resource_details_from_file(temp_path)  # pylint: disable=unbalanced-tuple-unpacking
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
            # pylint cannot see that the cast() in get_resource_details_from_file gives a 2-tuple
            cpu, memory = get_resource_details_from_file(temp_path)  # pylint: disable=unbalanced-tuple-unpacking
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

    def test_find_common_data_file_names_is_independent_of_path_depth(self) -> None:
        """Result must not depend on how deeply the directories are nested (e.g. macOS temp paths)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            deep_root = Path(temp_dir) / "a" / "b" / "c" / "d" / "e"
            directories = [deep_root / name for name in ("dir1", "dir2")]
            for directory in directories:
                directory.mkdir(parents=True)
            (directories[0] / "file1.json").touch()
            (directories[0] / "only_first.json").touch()
            (directories[1] / "file1.json").touch()

            common_files = find_common_data_file_names(directories)

            self.assertEqual(common_files, ["file1.json"], "Only files present in every directory should be returned")

    def test_find_common_data_file_names_unions_directories_within_one_archive(self) -> None:
        """Directories belonging to the same archive are merged; archives are intersected"""
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            archive_a = root / "archA"
            archive_b = root / "archB"
            a_read = archive_a / "randread" / "visualisation"
            a_write = archive_a / "randwrite" / "visualisation"
            b_read = archive_b / "randread" / "visualisation"
            b_write = archive_b / "randwrite" / "visualisation"
            for directory in (a_read, a_write, b_read, b_write):
                directory.mkdir(parents=True)

            (a_read / "4096_1_randread.json").touch()
            (a_write / "4096_1_randwrite.json").touch()
            (a_write / "only_in_a.json").touch()
            (b_read / "4096_1_randread.json").touch()
            (b_write / "4096_1_randwrite.json").touch()

            common_files = find_common_data_file_names(
                [a_read, a_write, b_read, b_write], archive_directories=[archive_a, archive_b]
            )

            self.assertEqual(
                common_files,
                ["4096_1_randread.json", "4096_1_randwrite.json"],
                "Files should be unioned within an archive and intersected across archives",
            )

    def test_find_common_data_file_names_single_archive_returns_union(self) -> None:
        """A single archive with several operation directories returns every file"""
        with tempfile.TemporaryDirectory() as temp_dir:
            archive = Path(temp_dir) / "archA"
            read_dir = archive / "randread" / "visualisation"
            write_dir = archive / "randwrite" / "visualisation"
            read_dir.mkdir(parents=True)
            write_dir.mkdir(parents=True)
            (read_dir / "a.json").touch()
            (write_dir / "b.json").touch()

            common_files = find_common_data_file_names([read_dir, write_dir], archive_directories=[archive])

            self.assertEqual(common_files, ["a.json", "b.json"])

    def test_find_common_data_file_names_excludes_timeseries_files(self) -> None:
        """Time-series files are never returned"""
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir) / "dir1"
            directory.mkdir()
            (directory / "file1.json").touch()
            (directory / "file1_timeseries.json").touch()

            self.assertEqual(find_common_data_file_names([directory]), ["file1.json"])

    def test_find_common_data_file_names_timeseries_only(self) -> None:
        """With timeseries=True only the time-series files are returned"""
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir) / "dir1"
            directory.mkdir()
            (directory / "file1.json").touch()
            (directory / "file1_timeseries.json").touch()

            self.assertEqual(find_common_data_file_names([directory], timeseries=True), ["file1_timeseries.json"])

    def test_find_path_in_ancestors_finds_path_in_an_ancestor_directory(self) -> None:
        """The path is found in an ancestor, whatever the name of that ancestor or its depth"""
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir) / "not_called_c_b_t"
            header = repo_root / "include" / "header.tex"
            header.parent.mkdir(parents=True)
            header.touch()
            start = repo_root / "a" / "b" / "c"
            start.mkdir(parents=True)

            self.assertEqual(find_path_in_ancestors(start, "include/header.tex"), header)

    def test_find_path_in_ancestors_checks_the_start_directory_itself(self) -> None:
        """The start directory is searched as well as its ancestors"""
        with tempfile.TemporaryDirectory() as temp_dir:
            header = Path(temp_dir) / "include" / "header.tex"
            header.parent.mkdir()
            header.touch()

            self.assertEqual(find_path_in_ancestors(Path(temp_dir), "include/header.tex"), header)

    def test_find_path_in_ancestors_returns_nearest_match(self) -> None:
        """If several ancestors contain the path the nearest one is returned"""
        with tempfile.TemporaryDirectory() as temp_dir:
            outer = Path(temp_dir) / "include" / "header.tex"
            inner = Path(temp_dir) / "inner" / "include" / "header.tex"
            for header in (outer, inner):
                header.parent.mkdir(parents=True)
                header.touch()

            self.assertEqual(find_path_in_ancestors(inner.parent.parent / "sub", "include/header.tex"), inner)

    def test_find_path_in_ancestors_returns_none_when_not_found(self) -> None:
        """None is returned when no ancestor contains the path"""
        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertIsNone(find_path_in_ancestors(Path(temp_dir), "include/does_not_exist_anywhere.tex"))

    def test_find_iodepth_directory_prefers_total_iodepth(self) -> None:
        """total_iodepth is returned in preference to iodepth, and the path stops at that directory"""
        file_path = Path("/results/00000000/id-806228fa/wl/rbdfio/numjobs-001/total_iodepth-256/iodepth-000032/out.0")

        self.assertEqual(
            find_iodepth_directory(file_path),
            Path("/results/00000000/id-806228fa/wl/rbdfio/numjobs-001/total_iodepth-256"),
        )

    def test_find_iodepth_directory_prefers_total_iodepth_wherever_it_is(self) -> None:
        """total_iodepth wins even if an iodepth directory comes earlier in the path"""
        file_path = Path("/results/iodepth-000032/total_iodepth-256/out.0")

        self.assertEqual(find_iodepth_directory(file_path), Path("/results/iodepth-000032/total_iodepth-256"))

    def test_find_iodepth_directory_iodepth_only(self) -> None:
        """Old style paths only have an iodepth directory"""
        file_path = Path("/results/rbdfio/iodepth-016/numjobs-001/output.0")

        self.assertEqual(find_iodepth_directory(file_path), Path("/results/rbdfio/iodepth-016"))

    def test_find_iodepth_directory_relative_path(self) -> None:
        """Relative paths are handled, including an iodepth directory as the first component"""
        self.assertEqual(find_iodepth_directory(Path("iodepth-8/output.0")), Path("iodepth-8"))

    def test_find_iodepth_directory_returns_none_when_there_is_no_iodepth_directory(self) -> None:
        """None is returned if the path has no iodepth directory"""
        self.assertIsNone(find_iodepth_directory(Path("/results/rbdfio/numjobs-001/output.0")))

    def test_find_iodepth_directory_ignores_look_alike_directories(self) -> None:
        """Only directories of exactly the form [total_]iodepth-<digits> are iodepth directories"""
        for name in (
            "my_iodepth_tests",
            "iodepth",
            "total_iodepth",
            "iodepth-",
            "iodepth-abc",
            "iodepth-12-old",
            "iodepthfoo-12",
            "my_total_iodepth-12",
            "iodepth_12",
        ):
            with self.subTest(directory=name):
                self.assertIsNone(find_iodepth_directory(Path("/results") / name / "numjobs-001" / "output.0"))

    def test_get_iodepth_from_path_returns_the_digits_unchanged(self) -> None:
        """The value is returned as it appears in the path, including any zero padding"""
        self.assertEqual(get_iodepth_from_path(Path("/r/total_iodepth-256/iodepth-000032/out.0")), "256")
        self.assertEqual(get_iodepth_from_path(Path("/r/iodepth-000032/out.0")), "000032")

    def test_get_iodepth_from_path_returns_none_when_there_is_no_iodepth_directory(self) -> None:
        """None is returned if the path has no iodepth directory"""
        self.assertIsNone(get_iodepth_from_path(Path("/r/my_iodepth_tests/numjobs-001/out.0")))

    def test_find_common_data_file_names_no_directories(self) -> None:
        """No directories gives no files"""
        self.assertEqual(find_common_data_file_names([]), [])

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

    def test_sum_standard_deviation_values_single_io_returns_zero(self) -> None:
        """A single I/O has no spread, and must not divide by (total_ios - 1) == 0"""
        result = sum_standard_deviation_values(
            std_deviations=[0.0], operations=[1], latencies=[1000000.0], total_ios=1, combined_latency=1000000.0
        )

        self.assertEqual(result, 0.0, "A single I/O should have a standard deviation of 0")

    def test_sum_standard_deviation_values_one_io_split_across_jobs_returns_zero(self) -> None:
        """One I/O in total, spread over a read job and an empty write job, must not divide by zero"""
        result = sum_standard_deviation_values(
            std_deviations=[0.0, 0.0],
            operations=[1, 0],
            latencies=[1000000.0, 0.0],
            total_ios=1,
            combined_latency=1000000.0,
        )

        self.assertEqual(result, 0.0, "One I/O in total should have a standard deviation of 0")

    def test_sum_standard_deviation_values_zero_total_ios_returns_zero(self) -> None:
        """No I/O at all must not divide by (total_ios - 1) == -1 and take the root of a negative number"""
        result = sum_standard_deviation_values(
            std_deviations=[0.0], operations=[0], latencies=[0.0], total_ios=0, combined_latency=0.0
        )

        self.assertEqual(result, 0.0, "No I/O should give a standard deviation of 0")

    def test_sum_standard_deviation_values_identical_latencies_returns_zero(self) -> None:
        """
        Floating point rounding can make the value under the square root very slightly
        negative when every I/O has the same latency. Each of these latency / I/O count
        pairs raised ValueError("math domain error") before the radicand was clamped.
        """
        for latency, total_ios in [(33333.3, 1000), (33333.3, 100000), (33333.3, 1000000), (123456.789, 100000)]:
            with self.subTest(latency=latency, total_ios=total_ios):
                combined_latency = sum_mean_values([latency], [total_ios], total_ios)

                result = sum_standard_deviation_values(
                    std_deviations=[0.0],
                    operations=[total_ios],
                    latencies=[latency],
                    total_ios=total_ios,
                    combined_latency=combined_latency,
                )

                self.assertEqual(result, 0.0, "Identical latencies should give a standard deviation of 0")

    def test_sum_standard_deviation_values_single_job_returns_its_own_deviation(self) -> None:
        """Combining one job with itself must give back that job's standard deviation"""
        total_ios = 1000000
        latency = 250000.0
        std_deviation = 40000.0

        result = sum_standard_deviation_values(
            std_deviations=[std_deviation],
            operations=[total_ios],
            latencies=[latency],
            total_ios=total_ios,
            combined_latency=sum_mean_values([latency], [total_ios], total_ios),
        )

        self.assertAlmostEqual(result, std_deviation, places=3)

    def test_sum_standard_deviation_values_two_jobs_matches_pooled_deviation(self) -> None:
        """
        Compare the combined value with the sample standard deviation of the whole data set.

        Job A is the values 1, 2, 3 and job B is the values 4, 5, 6, so the combined
        data set 1..6 has a sample standard deviation of sqrt(3.5).
        """
        result = sum_standard_deviation_values(
            std_deviations=[1.0, 1.0],
            operations=[3, 3],
            latencies=[2.0, 5.0],
            total_ios=6,
            combined_latency=3.5,
        )

        self.assertAlmostEqual(result, 3.5**0.5, places=9)

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
