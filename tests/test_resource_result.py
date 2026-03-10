"""
Unit tests for the post_processing/run_results resource result class
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

from post_processing.run_results.resources.resource_result import ResourceResult


class ConcreteResourceResult(ResourceResult):
    """Concrete implementation of ResourceResult for testing"""

    @property
    def source(self) -> str:
        return "test_resource"

    def _get_resource_output_file_from_file_path(self, file_path: Path) -> Path:
        return file_path

    def _parse(self, data: dict[str, Any]) -> None:
        self._cpu = "50.0"
        self._memory = "1024.0"
        self._has_been_parsed = True


class TestResourceResult(unittest.TestCase):
    """Test cases for ResourceResult base class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file = Path(self.temp_dir) / "resource_output.json"

        self.test_data = {"cpu_usage": 50.5, "memory_usage": 2048}

        with open(self.test_file, "w") as f:
            json.dump(self.test_data, f)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test ResourceResult initialization"""
        result = ConcreteResourceResult(self.test_file)

        self.assertEqual(result._resource_file_path, self.test_file)
        self.assertFalse(result._has_been_parsed)

    def test_cpu_property(self) -> None:
        """Test CPU property triggers parsing"""
        result = ConcreteResourceResult(self.test_file)

        cpu = result.cpu

        self.assertEqual(cpu, "50.0")
        self.assertTrue(result._has_been_parsed)

    def test_memory_property(self) -> None:
        """Test memory property triggers parsing"""
        result = ConcreteResourceResult(self.test_file)

        memory = result.memory

        self.assertEqual(memory, "1024.0")
        self.assertTrue(result._has_been_parsed)

    def test_get_method(self) -> None:
        """Test get method returns formatted dict"""
        result = ConcreteResourceResult(self.test_file)

        data = result.get()

        self.assertIn("source", data)
        self.assertIn("cpu", data)
        self.assertIn("memory", data)
        self.assertEqual(data["source"], "test_resource")
        self.assertEqual(data["cpu"], "50.0")
        self.assertEqual(data["memory"], "1024.0")

    def test_read_results_from_empty_file(self) -> None:
        """Test reading from empty file"""
        empty_file = Path(self.temp_dir) / "empty.json"
        empty_file.touch()

        result = ConcreteResourceResult(empty_file)
        data = result._read_results_from_file()

        self.assertEqual(data, {})


# Made with Bob
