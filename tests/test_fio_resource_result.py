"""
Unit tests for the post_processing FIO resource result module class
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

from post_processing.run_results.resources.fio_resource import FIOResource


class TestFIOResource(unittest.TestCase):
    """Test cases for FIOResource class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = Path(self.temp_dir) / "fio_output.json"

        self.test_data = {"jobs": [{"sys_cpu": 25.5, "usr_cpu": 30.2}]}

        with open(self.test_file, "w") as f:
            json.dump(self.test_data, f)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_source_property(self) -> None:
        """Test source property returns 'fio'"""
        resource = FIOResource(self.test_file)

        self.assertEqual(resource.source, "fio")

    def test_get_resource_output_file_from_file_path(self) -> None:
        """Test that resource file path is same as input path"""
        resource = FIOResource(self.test_file)

        self.assertEqual(resource._resource_file_path, self.test_file)

    def test_parse_cpu_usage(self) -> None:
        """Test parsing CPU usage from FIO output"""
        resource = FIOResource(self.test_file)

        cpu = resource.cpu

        # Should be sum of sys_cpu (25.5) and usr_cpu (30.2) = 55.7
        self.assertAlmostEqual(float(cpu), 55.7, places=1)

    def test_parse_memory_usage(self) -> None:
        """Test parsing memory usage (currently returns 0)"""
        resource = FIOResource(self.test_file)

        memory = resource.memory

        # Memory is not currently extracted from FIO output
        self.assertEqual(float(memory), 0.0)

    def test_get_method(self) -> None:
        """Test get method returns formatted resource data"""
        resource = FIOResource(self.test_file)

        data = resource.get()

        self.assertEqual(data["source"], "fio")
        self.assertIn("cpu", data)
        self.assertIn("memory", data)
        self.assertAlmostEqual(float(data["cpu"]), 55.7, places=1)


# Made with Bob
