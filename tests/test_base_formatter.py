"""
Unit tests for the BaseFormatter class
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import shutil
import tempfile
import unittest
from pathlib import Path

from post_processing.formatter.base_formatter import BaseFormatter


class ConcreteFormatter(BaseFormatter):
    """Concrete implementation of BaseFormatter for testing"""

    def process(self) -> None:
        """Dummy implementation"""

    def write_output(self) -> None:
        """Dummy implementation"""


class TestBaseFormatter(unittest.TestCase):
    """
    Unit tests for BaseFormatter class methods
    """

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.formatter = ConcreteFormatter(self.temp_dir)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test that BaseFormatter initializes correctly"""
        self.assertEqual(self.formatter._directory, self.temp_dir)  # pylint: disable=protected-access
        self.assertIsNotNone(self.formatter.log)

    def test_log_property(self) -> None:
        """Test that log property returns a logger"""
        logger = self.formatter.log
        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, "formatter")

    def test_path_property(self) -> None:
        """Test that path property returns a Path object"""
        path = self.formatter.path
        self.assertIsInstance(path, Path)
        self.assertEqual(str(path), self.temp_dir)

    def test_path_property_with_nested_directory(self) -> None:
        """Test path property with nested directory structure"""
        nested_dir = Path(self.temp_dir) / "nested" / "directory"
        nested_dir.mkdir(parents=True, exist_ok=True)

        formatter = ConcreteFormatter(str(nested_dir))
        path = formatter.path

        self.assertIsInstance(path, Path)
        self.assertEqual(path, nested_dir)
        self.assertTrue(path.exists())

    def test_path_property_consistency(self) -> None:
        """Test that path property returns consistent results"""
        path1 = self.formatter.path
        path2 = self.formatter.path

        self.assertEqual(path1, path2)
        self.assertIsInstance(path1, Path)
        self.assertIsInstance(path2, Path)

    def test_ensure_output_directory_creates_directory(self) -> None:
        """Test that _ensure_output_directory creates a new directory"""
        new_dir = Path(self.temp_dir) / "output" / "nested"
        self.assertFalse(new_dir.exists())

        self.formatter._ensure_output_directory(new_dir)  # pylint: disable=protected-access

        self.assertTrue(new_dir.exists())
        self.assertTrue(new_dir.is_dir())

    def test_ensure_output_directory_with_existing_directory(self) -> None:
        """Test that _ensure_output_directory handles existing directories"""
        existing_dir = Path(self.temp_dir) / "existing"
        existing_dir.mkdir(parents=True, exist_ok=True)
        self.assertTrue(existing_dir.exists())

        # Should not raise an error
        self.formatter._ensure_output_directory(existing_dir)  # pylint: disable=protected-access

        self.assertTrue(existing_dir.exists())
        self.assertTrue(existing_dir.is_dir())

    def test_ensure_output_directory_creates_parent_directories(self) -> None:
        """Test that _ensure_output_directory creates parent directories"""
        nested_dir = Path(self.temp_dir) / "level1" / "level2" / "level3"
        self.assertFalse(nested_dir.exists())
        self.assertFalse(nested_dir.parent.exists())

        self.formatter._ensure_output_directory(nested_dir)  # pylint: disable=protected-access

        self.assertTrue(nested_dir.exists())
        self.assertTrue(nested_dir.parent.exists())
        self.assertTrue(nested_dir.parent.parent.exists())


if __name__ == "__main__":
    unittest.main()

# Made with Bob
