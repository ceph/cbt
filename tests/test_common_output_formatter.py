"""
Unit tests for the CommonOutputFormatter class
"""

import unittest
from typing import Dict, List, Union

from post_processing.formatter.common_output_formatter import CommonOutputFormatter
from post_processing.formatter.benchmark_run_result import BenchmarkRunResult


# pyright: ignore[reportPrivateUsage]
class TestCommonOutputFormatter(unittest.TestCase):
    """
    A basic unit test to check our unit test infrastructure is
     working as expected
    """

    read_data: Dict[str, Union[int, float, Dict[str, Union[int, float]]]] = {
        "io_bytes": 440397824,
        "bw_bytes": 34982748,
        "iops": 8539.518627,
        "total_ios": 107504,
        "clat_ns": {
            "mean": 1977392.018174,
            "stddev": 9231257.966646,
        },
    }
    write_data: Dict[str, Union[int, float, Dict[str, Union[int, float]]]] = {
        "io_bytes": 480681984,
        "bw_bytes": 35640393,
        "iops": 8700.155705,
        "total_ios": 117339,
        "clat_ns": {
            "mean": 1825254.043151,
            "stddev": 10820490.136089,
        },
    }
    global_options_data: Dict[str, str] = {
        "rw": "write",
        "runtime": "90",
        "numjobs": "1",
        "bs": "4096B",
        "iodepth": "16",
    }

    job_data: List[Dict[str, Union[str, Dict[str, Union[int, float, Dict[str, Union[int, float]]]]]]] = [
        {},
        {"read": read_data, "write": write_data},
    ]

    def setUp(self) -> None:
        print("setting up tests")
        self.formatter = CommonOutputFormatter("/tmp")
        self.test_run_results = BenchmarkRunResult("/tmp", "unit_tests", "output")

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

        output = self.test_run_results._get_global_options(self.global_options_data)  # pyright: ignore[reportPrivateUsage]

        expected_output: Dict[str, str] = {
            "number_of_jobs": self.global_options_data["numjobs"],
            "runtime_seconds": self.global_options_data["runtime"],
            "blocksize": self.global_options_data["bs"][:-1],
        }

        self.assertDictEqual(output, expected_output)

    def test_extended_global_oprtions_parsing(self) -> None:
        """
        Check the parsing of extended global_options
        """
        extended_test_data: Dict[str, str] = self.global_options_data.copy()
        extended_test_data.update({"rwmixread": "70", "rwmixwrite": "30"})
        output = self.test_run_results._get_global_options(extended_test_data)  # pyright: ignore[reportPrivateUsage]

        expected_output: Dict[str, str] = {
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
        read_job_details: List[Dict[str, Union[str, Dict[str, Union[int, float, Dict[str, Union[int, float]]]]]]] = [
            {"read": self.read_data}
        ]
        output = self.test_run_results._get_io_details(read_job_details)  # pyright: ignore[reportPrivateUsage]

        assert isinstance(self.read_data["io_bytes"], int)
        assert isinstance(self.read_data["bw_bytes"], int)
        assert isinstance(self.read_data["iops"], float)
        expected_output: Dict[str, str] = {
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
        write_job_details: List[Dict[str, Union[str, Dict[str, Union[int, float, Dict[str, Union[int, float]]]]]]] = [
            {"write": self.write_data}
        ]
        output = self.test_run_results._get_io_details(write_job_details)  # pyright: ignore[reportPrivateUsage]

        assert isinstance(self.write_data["io_bytes"], int)
        assert isinstance(self.write_data["bw_bytes"], int)
        assert isinstance(self.write_data["iops"], float)
        expected_output: Dict[str, str] = {
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

        output = self.test_run_results._get_io_details(self.job_data)  # pyright: ignore[reportPrivateUsage]

        assert isinstance(self.write_data["io_bytes"], int)
        assert isinstance(self.read_data["io_bytes"], int)
        io: str = str(int(self.write_data["io_bytes"]) + int(self.read_data["io_bytes"]))

        assert isinstance(self.write_data["bw_bytes"], int)
        assert isinstance(self.read_data["bw_bytes"], int)
        bw: str = str(int(self.write_data["bw_bytes"]) + int(self.read_data["bw_bytes"]))

        assert isinstance(self.write_data["iops"], float)
        assert isinstance(self.read_data["iops"], float)
        iops: str = str(float(self.write_data["iops"]) + float(self.read_data["iops"]))

        expected_output: Dict[str, str] = {
            "io_bytes": io,
            "bandwidth_bytes": bw,
            "iops": iops,
        }

        for key in expected_output.keys():
            self.assertEqual(expected_output[key], output[key])
