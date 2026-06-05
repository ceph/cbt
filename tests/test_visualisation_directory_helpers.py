"""
Unit tests for visualisation directory helper functions in post_processing/common.py
"""

import shutil
import tempfile
import unittest
from pathlib import Path

from post_processing.common import (
    find_hockey_stick_visualisation_directories,
    find_timeseries_visualisation_directories,
)


class TestVisualisationDirectoryHelpers(unittest.TestCase):
    """Test cases for visualisation directory helper functions"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.archive_dir = Path(self.temp_dir) / "test_archive"
        self.archive_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_find_hockey_stick_legacy_structure(self) -> None:
        """Test finding hockey-stick directories in legacy structure"""
        # Create legacy structure: archive/visualisation/
        legacy_vis = self.archive_dir / "visualisation"
        legacy_vis.mkdir(parents=True)
        (legacy_vis / "4k_1_randread.json").touch()

        result = find_hockey_stick_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], legacy_vis)

    def test_find_hockey_stick_new_structure(self) -> None:
        """Test finding hockey-stick directories in new structure"""
        # Create new structure: archive/operation/visualisation/
        randread_vis = self.archive_dir / "randread" / "visualisation"
        randread_vis.mkdir(parents=True)
        (randread_vis / "4k_1_randread.json").touch()

        randwrite_vis = self.archive_dir / "randwrite" / "visualisation"
        randwrite_vis.mkdir(parents=True)
        (randwrite_vis / "4k_1_randwrite.json").touch()

        result = find_hockey_stick_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 2)
        self.assertIn(randread_vis, result)
        self.assertIn(randwrite_vis, result)

    def test_find_hockey_stick_prefers_legacy(self) -> None:
        """Test that legacy structure with data takes precedence if both exist"""
        # Create both structures
        legacy_vis = self.archive_dir / "visualisation"
        legacy_vis.mkdir(parents=True)
        
        # Add a JSON file to legacy directory
        (legacy_vis / "test.json").touch()

        new_vis = self.archive_dir / "randread" / "visualisation"
        new_vis.mkdir(parents=True)
        # Add JSON file to new structure too
        (new_vis / "test2.json").touch()

        result = find_hockey_stick_visualisation_directories(self.archive_dir)

        # Should only return legacy structure when it has data
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], legacy_vis)
    
    def test_find_hockey_stick_empty_legacy_with_new_structure(self) -> None:
        """Test that empty legacy directory doesn't prevent finding new structure"""
        # Create both structures
        legacy_vis = self.archive_dir / "visualisation"
        legacy_vis.mkdir(parents=True)

        new_vis = self.archive_dir / "randread" / "visualisation"
        new_vis.mkdir(parents=True)
        # Add JSON file to new structure
        (new_vis / "test.json").touch()

        result = find_hockey_stick_visualisation_directories(self.archive_dir)

        # Should only return new structure since legacy is empty
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], new_vis)

    def test_find_hockey_stick_empty_directory(self) -> None:
        """Test finding hockey-stick directories in empty archive"""
        result = find_hockey_stick_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 0)

    def test_find_hockey_stick_ignores_hidden_dirs(self) -> None:
        """Test that hidden directories are ignored"""
        # Create hidden directory
        hidden_vis = self.archive_dir / ".hidden" / "visualisation"
        hidden_vis.mkdir(parents=True)

        result = find_hockey_stick_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 0)

    def test_find_timeseries_with_total_iodepth(self) -> None:
        """Test finding timeseries directories under total_iodepth"""
        # Create structure: archive/operation/total_iodepth-X/visualisation/
        vis_dir = self.archive_dir / "randread" / "total_iodepth-256" / "visualisation"
        vis_dir.mkdir(parents=True)
        (vis_dir / "randread_4k_256_timeseries.json").touch()

        result = find_timeseries_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], vis_dir)

    def test_find_timeseries_with_iodepth(self) -> None:
        """Test finding timeseries directories under iodepth"""
        # Create structure: archive/operation/iodepth-X/visualisation/
        vis_dir = self.archive_dir / "randread" / "iodepth-32" / "visualisation"
        vis_dir.mkdir(parents=True)
        (vis_dir / "randread_4k_32_timeseries.json").touch()

        result = find_timeseries_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], vis_dir)

    def test_find_timeseries_prioritizes_total_iodepth(self) -> None:
        """Test that total_iodepth directories are prioritized over iodepth"""
        # Create both types
        total_iodepth_vis = self.archive_dir / "randread" / "total_iodepth-256" / "visualisation"
        total_iodepth_vis.mkdir(parents=True)

        iodepth_vis = self.archive_dir / "randread" / "iodepth-32" / "visualisation"
        iodepth_vis.mkdir(parents=True)

        result = find_timeseries_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 2)
        # total_iodepth should come first
        self.assertEqual(result[0], total_iodepth_vis)
        self.assertEqual(result[1], iodepth_vis)

    def test_find_timeseries_multiple_operations(self) -> None:
        """Test finding timeseries directories across multiple operations"""
        # Create multiple operations with timeseries data
        randread_vis = self.archive_dir / "randread" / "total_iodepth-256" / "visualisation"
        randread_vis.mkdir(parents=True)

        randwrite_vis = self.archive_dir / "randwrite" / "iodepth-32" / "visualisation"
        randwrite_vis.mkdir(parents=True)

        result = find_timeseries_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 2)
        self.assertIn(randread_vis, result)
        self.assertIn(randwrite_vis, result)

    def test_find_timeseries_ignores_operation_level(self) -> None:
        """Test that operation-level visualisation directories are ignored"""
        # Create operation-level visualisation (for hockey-stick data)
        operation_vis = self.archive_dir / "randread" / "visualisation"
        operation_vis.mkdir(parents=True)

        result = find_timeseries_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 0)

    def test_find_timeseries_empty_directory(self) -> None:
        """Test finding timeseries directories in empty archive"""
        result = find_timeseries_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 0)

    def test_find_timeseries_ignores_legacy_structure(self) -> None:
        """Test that legacy visualisation directory is ignored for timeseries"""
        # Create legacy structure
        legacy_vis = self.archive_dir / "visualisation"
        legacy_vis.mkdir(parents=True)

        result = find_timeseries_visualisation_directories(self.archive_dir)

        self.assertEqual(len(result), 0)


if __name__ == "__main__":
    unittest.main()

# Made with Bob
