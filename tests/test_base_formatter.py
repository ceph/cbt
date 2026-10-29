"""
Unit tests for the BaseFormatter class
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module
# pylint: disable=protected-access


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
        self.assertEqual(logger.name, "cbt.formatter")

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


class TestGetIoPatternDirectories(unittest.TestCase):
    """
    Unit tests for BaseFormatter._get_io_pattern_directories
    """

    def setUp(self) -> None:
        """Set up a test run directory that will hold workload and monitoring directories"""
        self.temp_dir = tempfile.mkdtemp()
        self.testrun = Path(self.temp_dir) / "id-94a6db48"
        self.testrun.mkdir()
        self.formatter = ConcreteFormatter(self.temp_dir)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _make_workload(self, name: str) -> Path:
        """Create a workload directory containing a json_output.0 result file"""
        workload = self.testrun / name
        (workload / "librbdfio").mkdir(parents=True)
        (workload / "librbdfio" / "json_output.0").write_text("{}", encoding="utf-8")
        return workload

    def test_directories_with_result_files_are_returned(self) -> None:
        """Workload directories containing json_output.<n> files are returned"""
        workload = self._make_workload("workload1")

        result = self.formatter._get_io_pattern_directories(self.testrun, "json_output")

        self.assertEqual(result, [workload])

    def test_monitoring_directories_without_result_files_are_skipped(self) -> None:
        """Directories such as collectl.<host> and pool_monitoring.<host> hold no results so are skipped"""
        workload = self._make_workload("workload1")
        collectl_dir = self.testrun / "collectl.host1.example.com"
        collectl_dir.mkdir()
        (collectl_dir / "host1.cpu").write_text("data", encoding="utf-8")
        (self.testrun / "pool_monitoring.host1.example.com").mkdir()

        result = self.formatter._get_io_pattern_directories(self.testrun, "json_output")

        self.assertEqual(result, [workload])

    def test_directory_with_non_numeric_suffix_is_skipped(self) -> None:
        """Files such as json_output.txt are not result files"""
        other = self.testrun / "other"
        other.mkdir()
        (other / "json_output.txt").write_text("x", encoding="utf-8")

        result = self.formatter._get_io_pattern_directories(self.testrun, "json_output")

        self.assertEqual(result, [])

    def test_hidden_and_visualisation_directories_are_skipped(self) -> None:
        """Hidden and visualisation directories are never returned, even if they hold result files"""
        for name in (".hidden", "visualisation"):
            skipped = self.testrun / name
            skipped.mkdir()
            (skipped / "json_output.0").write_text("{}", encoding="utf-8")

        result = self.formatter._get_io_pattern_directories(self.testrun, "json_output")

        self.assertEqual(result, [])

    def test_plain_files_are_ignored(self) -> None:
        """Files directly inside the test run directory are not returned"""
        (self.testrun / "json_output.0").write_text("{}", encoding="utf-8")

        result = self.formatter._get_io_pattern_directories(self.testrun, "json_output")

        self.assertEqual(result, [])


class TestTestrunIdMatching(unittest.TestCase):
    """
    The test run directory is named id-<8 hex characters> by benchmark.py. Only
    path components of exactly that form are test run IDs, not any component
    that happens to contain the text "id-".
    """

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_find_all_testrun_ids_ignores_directories_containing_id_dash(self) -> None:
        """Directories such as raid-config or valid-data before the real ID must not be used as the ID"""
        formatter = ConcreteFormatter("/tmp/android-build")
        file_list = [Path("/tmp/android-build/valid-data/results/00000000/id-94a6db48/workload/json_output.0")]

        formatter._find_all_testrun_ids(file_list)

        self.assertEqual(formatter._all_test_run_ids, {"id-94a6db48"})

    def test_find_all_testrun_ids_uses_first_matching_id(self) -> None:
        """If there is more than one id-<hex> component the first is used"""
        formatter = ConcreteFormatter("/tmp/test")
        file_list = [Path("/tmp/test/id-94a6db48/workload/id-0123abcd/json_output.0")]

        formatter._find_all_testrun_ids(file_list)

        self.assertEqual(formatter._all_test_run_ids, {"id-94a6db48"})

    def test_find_all_testrun_ids_rejects_malformed_ids(self) -> None:
        """Components that only look similar to an ID fall back to the directory above the file"""
        formatter = ConcreteFormatter("/tmp/test")
        file_list = [
            Path("/tmp/test/id-94a6db4/workload1/json_output.0"),  # too short
            Path("/tmp/test/id-94a6db489/workload2/json_output.0"),  # too long
            Path("/tmp/test/id-94A6DB48/workload3/json_output.0"),  # not lower case hex
            Path("/tmp/test/id-94a6dbzz/workload4/json_output.0"),  # not hex
            Path("/tmp/test/xid-94a6db48/workload5/json_output.0"),  # extra prefix
        ]

        formatter._find_all_testrun_ids(file_list)

        self.assertEqual(formatter._all_test_run_ids, {"workload1", "workload2", "workload3", "workload4", "workload5"})

    def test_get_testrun_directories_when_archive_directory_is_the_testrun_directory(self) -> None:
        """When the archive directory already is the id-<hex> directory it is returned as is"""
        testrun_directory = Path(self.temp_dir) / "results" / "00000000" / "id-94a6db48"
        testrun_directory.mkdir(parents=True)

        formatter = ConcreteFormatter(str(testrun_directory))

        self.assertEqual(formatter._get_testrun_directories("id-94a6db48"), [testrun_directory])

    def test_get_testrun_directories_not_confused_by_id_dash_in_archive_path(self) -> None:
        """A look-alike such as 'valid-archive' in the archive path must not stop the test run search"""
        archive = Path(self.temp_dir) / "valid-archive"
        testrun_directory = archive / "results" / "00000000" / "id-94a6db48"
        testrun_directory.mkdir(parents=True)

        formatter = ConcreteFormatter(str(archive))

        self.assertEqual(formatter._get_testrun_directories("id-94a6db48"), [testrun_directory])


if __name__ == "__main__":
    unittest.main()

# Made with Bob
