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
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

from post_processing.formatter.base_formatter import BaseFormatter
from post_processing.post_processing_types import DirectoryGroup

_ORIGINAL_ITERDIR = Path.iterdir


def _reverse_sorted_iterdir(self: Path) -> Iterator[Path]:
    """Stand-in for Path.iterdir() that returns entries in reverse sorted order, so a missing sort is detected"""
    return iter(sorted(_ORIGINAL_ITERDIR(self), reverse=True))


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

    def test_results_are_sorted(self) -> None:
        """Directories are returned in sorted order, independent of iterdir() order"""
        workloads = [self._make_workload(name) for name in ("w.client2", "w.client10", "w.client1")]

        with patch.object(Path, "iterdir", _reverse_sorted_iterdir):
            result = self.formatter._get_io_pattern_directories(self.testrun, "json_output")

        self.assertEqual(result, sorted(workloads), "Result directories must be returned in sorted order")


class TestGroupIoPatternDirectories(unittest.TestCase):
    """
    Unit tests for BaseFormatter._group_io_pattern_directories
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
        (workload / "rbdfio").mkdir(parents=True)
        (workload / "rbdfio" / "json_output.0").write_text("{}", encoding="utf-8")
        return workload

    def _make_monitoring(self, name: str) -> Path:
        """Create a directory that holds monitoring data only (no result files)"""
        monitoring = self.testrun / name
        (monitoring / "collectl").mkdir(parents=True)
        (monitoring / "collectl" / "host.cpu").write_text("data", encoding="utf-8")
        return monitoring

    def _group(self) -> list[DirectoryGroup]:
        return self.formatter._group_io_pattern_directories(self.testrun, "json_output")

    def test_peers_with_same_workload_are_clustered(self) -> None:
        """<workload>.<nodeA> and <workload>.<nodeB> form one group"""
        node_a = self._make_workload("randwrite_4k.nodeA")
        node_b = self._make_workload("randwrite_4k.nodeB")

        groups = self._group()

        self.assertEqual(len(groups), 1, "Both hosts must be in a single group")
        self.assertEqual(groups[0].primary, node_a)
        self.assertEqual(groups[0].result_peers, [node_b])
        self.assertEqual(groups[0].monitoring_peers, [])

    def test_different_workloads_form_different_groups(self) -> None:
        """Different base workload names are never clustered together"""
        read = self._make_workload("randread_4k.nodeA")
        write = self._make_workload("randwrite_4k.nodeA")

        groups = self._group()

        self.assertEqual([group.primary for group in groups], [read, write])

    def test_order_is_sorted_and_independent_of_iterdir_order(self) -> None:
        """Primary selection and group order are deterministic regardless of iterdir() order"""
        for name in ("w.nodeC", "w.nodeA", "w.nodeB"):
            self._make_workload(name)

        with patch.object(Path, "iterdir", _reverse_sorted_iterdir):
            groups = self._group()

        self.assertEqual(groups[0].primary, self.testrun / "w.nodeA")
        self.assertEqual(groups[0].result_peers, [self.testrun / "w.nodeB", self.testrun / "w.nodeC"])

    def test_groups_are_ordered_by_workload_not_by_host_suffix(self) -> None:
        """Group order follows the workload name; characters in the host suffix must not affect it"""
        # By primary directory name "w-x.h" sorts before "w.h" because "-" sorts before ".", but
        # by workload "w" sorts before "w-x".
        workload_w = self._make_workload("w.h")
        workload_wx = self._make_workload("w-x.h")

        groups = self._group()

        self.assertEqual(
            [group.primary for group in groups],
            [workload_w, workload_wx],
            "Groups must be ordered by workload name, not by the full primary directory name",
        )

    def test_primary_ordering_is_lexicographic(self) -> None:
        """client10 sorts before client2 (lexicographic, not numeric) so becomes the primary"""
        self._make_workload("w.client2")
        self._make_workload("w.client10")

        groups = self._group()

        self.assertEqual(groups[0].primary, self.testrun / "w.client10")
        self.assertEqual(groups[0].result_peers, [self.testrun / "w.client2"])

    def test_primary_is_never_a_monitoring_only_directory(self) -> None:
        """A monitoring-only directory that sorts first is attached as a peer, not made primary"""
        monitoring = self._make_monitoring("w.aaa_osd")
        client = self._make_workload("w.zzz_client")

        groups = self._group()

        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0].primary, client)
        self.assertEqual(groups[0].result_peers, [])
        self.assertEqual(groups[0].monitoring_peers, [monitoring])

    def test_groups_without_result_bearing_member_are_dropped(self) -> None:
        """Monitoring-only groups such as idle_monitoring.<host> are dropped"""
        workload = self._make_workload("w.nodeA")
        self._make_monitoring("idle_monitoring.nodeA")
        self._make_monitoring("pool_monitoring.nodeA")
        (self.testrun / "recovery_pool_monitoring.nodeA").mkdir()

        groups = self._group()

        self.assertEqual([group.primary for group in groups], [workload])

    def test_single_suffixed_host_forms_single_member_group(self) -> None:
        """A one-host group with a host suffix behaves like the old single directory"""
        workload = self._make_workload("w.nodeA")

        groups = self._group()

        self.assertEqual(groups, [DirectoryGroup(workload, [], [])])

    def test_unsuffixed_directory_forms_single_member_group(self) -> None:
        """A directory without a host suffix is a single-member group"""
        workload = self._make_workload("workload1")

        groups = self._group()

        self.assertEqual(groups, [DirectoryGroup(workload, [], [])])

    def test_legacy_monitoring_directories_are_dropped_not_treated_as_workloads(self) -> None:
        """Legacy collectl.<host> / top.<host> directories have no result files so are dropped"""
        workload = self._make_workload("w.nodeA")
        for name in ("collectl.nodeA", "top.nodeA"):
            directory = self.testrun / name
            directory.mkdir()
            (directory / "nodeA.cpu").write_text("data", encoding="utf-8")

        groups = self._group()

        self.assertEqual([group.primary for group in groups], [workload])

    def test_hidden_and_visualisation_directories_are_skipped(self) -> None:
        """Hidden and visualisation directories are never grouped"""
        workload = self._make_workload("w.nodeA")
        for name in (".hidden", "visualisation"):
            skipped = self.testrun / name
            skipped.mkdir()
            (skipped / "json_output.0").write_text("{}", encoding="utf-8")

        groups = self._group()

        self.assertEqual([group.primary for group in groups], [workload])

    def test_host_names_containing_dots_group_together(self) -> None:
        """FQDN and IPv4 host suffixes are all grouped under the base workload name"""
        fqdn = self._make_workload("w.client1.example.com")
        ipv4 = self._make_workload("w.10.0.0.2")

        groups = self._group()

        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0].primary, ipv4, "10.0.0.2 sorts before client1.example.com")
        self.assertEqual(groups[0].result_peers, [fqdn])

    def test_node_that_is_both_client_and_osd_is_a_result_peer(self) -> None:
        """A directory with results and monitoring data is a result peer, not a monitoring peer"""
        client = self._make_workload("w.nodeA")
        client_osd = self._make_workload("w.nodeB")
        (client_osd / "rbdfio" / "collectl").mkdir()
        (client_osd / "rbdfio" / "collectl" / "nodeB.cpu").write_text("data", encoding="utf-8")
        osd = self._make_monitoring("w.nodeC")

        groups = self._group()

        self.assertEqual(groups, [DirectoryGroup(client, [client_osd], [osd])])

    def test_empty_testrun_directory_returns_no_groups(self) -> None:
        """No directories means no groups"""
        self.assertEqual(self._group(), [])


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
