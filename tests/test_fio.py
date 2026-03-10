"""
Unit tests for the FIO benchmark result class
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import unittest
from pathlib import Path
from typing import ClassVar, Union

from post_processing.run_results.benchmarks.fio import FIO


class MockFIO(FIO):
    """Mock FIO class for testing that doesn't require a file"""

    def __init__(self) -> None:
        # Skip the parent __init__ to avoid file reading
        self._resource_file_path = Path("/tmp/test")
        self._data = {}
        self._global_options = {}
        self._iodepth = "1"
        self._io_details = {}
        self._has_been_parsed = False
        self._source = "fio"


class TestFIO(unittest.TestCase):
    """
    Unit tests for FIO benchmark result parsing methods
    """

    read_data: ClassVar[dict[str, Union[int, float, dict[str, Union[int, float]]]]] = {
        "io_bytes": 440397824,
        "bw_bytes": 34982748,
        "iops": 8539.518627,
        "total_ios": 107504,
        "clat_ns": {
            "mean": 1977392.018174,
            "stddev": 9231257.966646,
        },
    }
    write_data: ClassVar[dict[str, Union[int, float, dict[str, Union[int, float]]]]] = {
        "io_bytes": 480681984,
        "bw_bytes": 35640393,
        "iops": 8700.155705,
        "total_ios": 117339,
        "clat_ns": {
            "mean": 1825254.043151,
            "stddev": 10820490.136089,
        },
    }
    global_options_data: ClassVar[dict[str, str]] = {
        "rw": "write",
        "runtime": "90",
        "numjobs": "1",
        "bs": "4096B",
        "iodepth": "16",
    }

    job_data: ClassVar[list[dict[str, Union[str, dict[str, Union[int, float, dict[str, Union[int, float]]]]]]]] = [
        {},
        {"read": read_data, "write": write_data},
    ]

    def setUp(self) -> None:
        print("setting up tests")
        self.test_run_results = MockFIO()

    def test_do_nothing(self) -> None:
        """
        A test that does nothing to verify that the unit tests
        can be run as expected
        """
        print("This test case should pass")
        value = True
        self.assertTrue(value)

    def test_global_options_parsing(self) -> None:
        """
        Check the parsing of global_options
        """

        output = self.test_run_results._get_global_options(self.global_options_data)

        expected_output: dict[str, str] = {
            "number_of_jobs": self.global_options_data["numjobs"],
            "runtime_seconds": self.global_options_data["runtime"],
            "blocksize": self.global_options_data["bs"][:-1],
        }

        self.assertDictEqual(output, expected_output)

    def test_extended_global_oprtions_parsing(self) -> None:
        """
        Check the parsing of extended global_options
        """
        extended_test_data: dict[str, str] = self.global_options_data.copy()
        extended_test_data.update({"rwmixread": "70", "rwmixwrite": "30"})
        output = self.test_run_results._get_global_options(extended_test_data)

        expected_output: dict[str, str] = {
            "number_of_jobs": extended_test_data["numjobs"],
            "runtime_seconds": extended_test_data["runtime"],
            "blocksize": self.global_options_data["bs"][:-1],
            "percentage_reads": extended_test_data["rwmixread"],
            "percentage_writes": extended_test_data["rwmixwrite"],
        }

        self.assertDictEqual(output, expected_output)

    def test_read_parsing(self) -> None:
        """
        Make sure we pull the correct details from the read data
        """
        read_job_details: list[dict[str, Union[str, dict[str, Union[int, float, dict[str, Union[int, float]]]]]]] = [
            {"read": self.read_data}
        ]
        output = self.test_run_results._get_io_details(read_job_details)

        assert isinstance(self.read_data["io_bytes"], int)
        assert isinstance(self.read_data["bw_bytes"], int)
        assert isinstance(self.read_data["iops"], float)
        expected_output: dict[str, str] = {
            "io_bytes": f"{self.read_data['io_bytes']}",
            "bandwidth_bytes": f"{self.read_data['bw_bytes']}",
            "iops": f"{self.read_data['iops']}",
        }

        for key in expected_output.keys():
            self.assertEqual(expected_output[key], output[key])

    def test_write_parsing(self) -> None:
        """
        Make sure we pull the correct details from the read data
        """
        write_job_details: list[dict[str, Union[str, dict[str, Union[int, float, dict[str, Union[int, float]]]]]]] = [
            {"write": self.write_data}
        ]
        output = self.test_run_results._get_io_details(write_job_details)

        assert isinstance(self.write_data["io_bytes"], int)
        assert isinstance(self.write_data["bw_bytes"], int)
        assert isinstance(self.write_data["iops"], float)
        expected_output: dict[str, str] = {
            "io_bytes": f"{self.write_data['io_bytes']}",
            "bandwidth_bytes": f"{self.write_data['bw_bytes']}",
            "iops": f"{self.write_data['iops']}",
        }

        for key in expected_output.keys():
            self.assertEqual(expected_output[key], output[key])

    def test_read_and_write_parsing(self) -> None:
        """
        Make sure we pull the correct details from the read data
        """

        output = self.test_run_results._get_io_details(self.job_data)

        assert isinstance(self.write_data["io_bytes"], int)
        assert isinstance(self.read_data["io_bytes"], int)
        io: str = str(int(self.write_data["io_bytes"]) + int(self.read_data["io_bytes"]))

        assert isinstance(self.write_data["bw_bytes"], int)
        assert isinstance(self.read_data["bw_bytes"], int)
        bw: str = str(int(self.write_data["bw_bytes"]) + int(self.read_data["bw_bytes"]))

        assert isinstance(self.write_data["iops"], float)
        assert isinstance(self.read_data["iops"], float)
        iops: str = str(float(self.write_data["iops"]) + float(self.read_data["iops"]))

        expected_output: dict[str, str] = {
            "io_bytes": io,
            "bandwidth_bytes": bw,
            "iops": iops,
        }

        for key in expected_output.keys():
            self.assertEqual(expected_output[key], output[key])
