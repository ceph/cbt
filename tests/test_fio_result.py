"""
Unit tests for the post_processing/run_results FIO resuly module class
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

from post_processing.run_results.benchmarks.fio import FIO


class TestFIO(unittest.TestCase):
    """Test cases for FIO class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = Path(self.temp_dir) / "fio_output.json"

        self.test_data = {
            "global options": {"bs": "4K", "rw": "randread", "iodepth": "4", "numjobs": "2", "runtime": "60"},
            "jobs": [
                {
                    "read": {
                        "io_bytes": 1000000,
                        "bw_bytes": 16666,
                        "iops": 100.5,
                        "total_ios": 244,
                        "clat_ns": {"mean": 5000000.0, "stddev": 500000.0},
                    },
                    "write": {
                        "io_bytes": 0,
                        "bw_bytes": 0,
                        "iops": 0.0,
                        "total_ios": 0,
                        "clat_ns": {"mean": 0.0, "stddev": 0.0},
                    },
                }
            ],
        }

        with open(self.test_file, "w") as f:
            json.dump(self.test_data, f)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_source_property(self) -> None:
        """Test source property returns 'fio'"""
        fio = FIO(self.test_file)

        self.assertEqual(fio.source, "fio")

    def test_get_global_options(self) -> None:
        """Test extracting global options"""
        fio = FIO(self.test_file)

        options = fio.global_options

        self.assertEqual(options["number_of_jobs"], "2")
        self.assertEqual(options["runtime_seconds"], "60")
        self.assertEqual(options["blocksize"], "4")

    def test_get_global_options_with_rwmix(self) -> None:
        """Test extracting global options with rwmix"""
        data_with_mix: dict[str, Any] = {
            "global options": {
                "bs": "4K",
                "rw": "randread",
                "iodepth": "4",
                "numjobs": "2",
                "runtime": "60",
                "rwmixread": "70",
                "rwmixwrite": "30",
            },
            "jobs": self.test_data["jobs"],
        }

        test_file_mix = Path(self.temp_dir) / "fio_mix.json"
        with open(test_file_mix, "w") as f:
            json.dump(data_with_mix, f)

        fio = FIO(test_file_mix)

        self.assertEqual(fio.global_options["percentage_reads"], "70")
        self.assertEqual(fio.global_options["percentage_writes"], "30")

    def test_get_io_details(self) -> None:
        """Test extracting IO details"""
        fio = FIO(self.test_file)

        io_details = fio.io_details

        self.assertIn("io_bytes", io_details)
        self.assertIn("bandwidth_bytes", io_details)
        self.assertIn("iops", io_details)
        self.assertIn("latency", io_details)
        self.assertIn("std_deviation", io_details)
        self.assertIn("total_ios", io_details)

        self.assertEqual(io_details["io_bytes"], "1000000")
        self.assertEqual(io_details["bandwidth_bytes"], "16666")

    def test_get_iodepth_from_value(self) -> None:
        """Test getting iodepth from value"""
        fio = FIO(self.test_file)

        self.assertEqual(fio.iodepth, "4")

    def test_get_iodepth_from_filename_new_style(self) -> None:
        """Test extracting iodepth from new-style filename"""
        test_file = Path(self.temp_dir) / "total_iodepth-8" / "fio_output.json"
        test_file.parent.mkdir(parents=True)

        with open(test_file, "w") as f:
            json.dump(self.test_data, f)

        fio = FIO(test_file)

        # Should use max of file value (4) and filename value (8)
        self.assertEqual(fio.iodepth, "8")

    def test_get_iodepth_from_filename_old_style(self) -> None:
        """Test extracting iodepth from old-style filename"""
        test_file = Path(self.temp_dir) / "iodepth-016" / "numjobs-001" / "output.0"
        test_file.parent.mkdir(parents=True)

        with open(test_file, "w") as f:
            json.dump(self.test_data, f)

        fio = FIO(test_file)

        # Should use max of file value (4) and filename value (16)
        self.assertEqual(fio.iodepth, "16")


# Made with Bob
