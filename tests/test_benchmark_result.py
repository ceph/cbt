"""
Unit tests for the post_processing/run_results module classes
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any

from post_processing.run_results.benchmarks.benchmark_result import BenchmarkResult


class ConcreteBenchmarkResult(BenchmarkResult):
    """Concrete implementation of BenchmarkResult for testing"""

    @property
    def source(self) -> str:
        return "test_benchmark"

    def _get_global_options(self, fio_global_options: dict[str, str]) -> dict[str, str]:
        return {"test_option": "test_value"}

    def _get_io_details(self, all_jobs: list[dict[str, Any]]) -> dict[str, str]:
        return {"test_io": "test_value"}

    def _get_iodepth(self, iodepth_value: str) -> str:
        return iodepth_value


class TestBenchmarkResult(unittest.TestCase):
    """Test cases for BenchmarkResult base class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = Path(self.temp_dir) / "test_output.json"

        # Create test FIO output data
        self.test_data: dict[str, Any] = {
            "global options": {"bs": "4096", "rw": "read", "iodepth": "1", "numjobs": "1", "runtime": "60"},
            "jobs": [
                {
                    "read": {
                        "io_bytes": 1000000,
                        "bw_bytes": 16666,
                        "iops": 4.0,
                        "total_ios": 244,
                        "clat_ns": {"mean": 5000000.0, "stddev": 500000.0},
                    }
                }
            ],
        }

        with open(self.test_file, "w") as f:
            json.dump(self.test_data, f)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test BenchmarkResult initialization"""
        result = ConcreteBenchmarkResult(self.test_file)

        self.assertEqual(result._resource_file_path, self.test_file)
        self.assertIsInstance(result._data, dict)
        self.assertFalse(result._has_been_parsed)

    def test_blocksize_property(self) -> None:
        """Test blocksize property extraction"""
        result = ConcreteBenchmarkResult(self.test_file)

        self.assertEqual(result.blocksize, "4096")

    def test_operation_property_simple(self) -> None:
        """Test operation property for simple operation"""
        result = ConcreteBenchmarkResult(self.test_file)

        self.assertEqual(result.operation, "read")

    def test_operation_property_with_percentages(self) -> None:
        """Test operation property with read/write percentages"""
        # Modify test data to include rwmix
        data_with_mix: dict[str, Any] = {
            "global options": {
                "bs": "4096",
                "rw": "randrw",
                "iodepth": "1",
                "numjobs": "1",
                "runtime": "60",
                "rwmixread": "70",
                "rwmixwrite": "30",
            },
            "jobs": self.test_data["jobs"],
        }

        test_file_mix = Path(self.temp_dir) / "test_mix.json"
        with open(test_file_mix, "w") as f:
            json.dump(data_with_mix, f)

        # Need to create a concrete class that handles percentages
        class TestBenchmarkWithMix(ConcreteBenchmarkResult):
            def _get_global_options(self, fio_global_options: dict[str, str]) -> dict[str, str]:
                options = {"test_option": "test_value"}
                if fio_global_options.get("rwmixread"):
                    options["percentage_reads"] = fio_global_options["rwmixread"]
                    options["percentage_writes"] = fio_global_options["rwmixwrite"]
                return options

        result = TestBenchmarkWithMix(test_file_mix)

        self.assertEqual(result.operation, "70_30_randrw")

    def test_global_options_property(self) -> None:
        """Test global_options property"""
        result = ConcreteBenchmarkResult(self.test_file)

        self.assertIsInstance(result.global_options, dict)
        self.assertEqual(result.global_options["test_option"], "test_value")

    def test_iodepth_property(self) -> None:
        """Test iodepth property"""
        result = ConcreteBenchmarkResult(self.test_file)

        self.assertEqual(result.iodepth, "1")

    def test_io_details_property(self) -> None:
        """Test io_details property"""
        result = ConcreteBenchmarkResult(self.test_file)

        self.assertIsInstance(result.io_details, dict)
        self.assertEqual(result.io_details["test_io"], "test_value")

    def test_read_results_from_empty_file(self) -> None:
        """Test reading from empty file raises KeyError"""
        empty_file = Path(self.temp_dir) / "empty.json"
        empty_file.touch()

        # Empty file should raise KeyError when trying to access 'global options'
        with self.assertRaises(ValueError):
            ConcreteBenchmarkResult(empty_file)

    def test_read_results_from_invalid_json(self) -> None:
        """Test reading from file with invalid JSON raises ValueError"""
        invalid_file = Path(self.temp_dir) / "invalid.json"
        with open(invalid_file, "w") as f:
            f.write("not valid json {")

        # Invalid JSON returns empty dict, which raises ValueError
        with self.assertRaises(ValueError):
            ConcreteBenchmarkResult(invalid_file)


# Made with Bob
