"""
Unit tests for the post_processing/reports module classes
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
from unittest.mock import MagicMock, patch

from post_processing.reports.simple_report_generator import SimpleReportGenerator


class TestSimpleReportGenerator(unittest.TestCase):
    """Test cases for SimpleReportGenerator class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.archive_dir = Path(self.temp_dir) / "test_archive"
        
        # Use new nested structure: operation/visualisation/
        self.vis_dir = self.archive_dir / "read" / "visualisation"
        self.vis_dir.mkdir(parents=True)

        # Create test data files with actual JSON content
        # Format: {blocksize}_{numjobs}_{operation}.json
        test_data = {"data": [{"x": 1, "y": 100}]}
        with open(self.vis_dir / "4096_1_read.json", "w") as f:
            json.dump(test_data, f)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_generate_report_title(self) -> None:
        """Test generating report title"""
        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        title = generator._generate_report_title()

        self.assertIn("Performance Report", title)
        self.assertIn("test-archive", title)

    def test_generate_report_name(self) -> None:
        """Test generating report name with timestamp"""
        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        report_name = generator._generate_report_name()

        self.assertTrue(report_name.startswith("performance_report_"))
        self.assertTrue(report_name.endswith(".md"))
        # Should contain timestamp in format YYMMDD_HHMMSS
        self.assertIn("_", report_name)

    @patch("post_processing.reports.simple_report_generator.SimplePlotter")
    def test_copy_images_creates_plots_if_missing(self, mock_plotter_class: MagicMock) -> None:
        """Test that _copy_images creates plots if they don't exist"""
        output_dir = f"{self.temp_dir}/output"

        mock_plotter = MagicMock()
        mock_plotter_class.return_value = mock_plotter

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        generator._copy_images()

        # Should create plotter and call draw_and_save
        mock_plotter_class.assert_called_once()
        mock_plotter.draw_and_save.assert_called_once()

    def test_find_and_sort_file_paths(self) -> None:
        """Test finding and sorting file paths"""
        # Create multiple files
        # Format: {blocksize}_{numjobs}_{operation}.json
        (self.vis_dir / "8192_1_write.json").touch()
        (self.vis_dir / "16384_1_read.json").touch()

        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        paths = generator._find_and_sort_file_paths(paths=[self.vis_dir], search_pattern="*.json", index=0)

        self.assertEqual(len(paths), 3)
        # Should be sorted by blocksize
        self.assertTrue(str(paths[0]).endswith("4096_1_read.json"))

    def test_new_structure_with_operation_directories(self) -> None:
        """Test report generation with new directory structure (operation/visualisation)"""
        # Clean up legacy structure
        shutil.rmtree(self.vis_dir, ignore_errors=True)
        
        # Create new structure: archive/operation/visualisation/
        randread_vis = self.archive_dir / "randread" / "visualisation"
        randread_vis.mkdir(parents=True)
        test_data = {"data": [{"x": 1, "y": 100}]}
        with open(randread_vis / "4096_1_randread.json", "w") as f:
            json.dump(test_data, f)
        
        randwrite_vis = self.archive_dir / "randwrite" / "visualisation"
        randwrite_vis.mkdir(parents=True)
        with open(randwrite_vis / "8192_1_randwrite.json", "w") as f:
            json.dump(test_data, f)
        
        output_dir = f"{self.temp_dir}/output"
        
        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )
        
        # With new nested structure, _data_directories contains one entry per operation
        # So with 2 operations (randread, randwrite), we have 2 data directories
        self.assertEqual(len(generator._data_directories), 2)
        self.assertIn(randread_vis, generator._data_directories)
        self.assertIn(randwrite_vis, generator._data_directories)
    
    @patch("post_processing.reports.simple_report_generator.SimplePlotter")
    def test_new_structure_creates_plots_per_operation(self, mock_plotter_class: MagicMock) -> None:
        """Test that plots are created for new structure with multiple operations"""
        # Clean up legacy structure
        shutil.rmtree(self.vis_dir, ignore_errors=True)
        
        # Create new structure with multiple operations
        randread_vis = self.archive_dir / "randread" / "visualisation"
        randread_vis.mkdir(parents=True)
        test_data = {"data": [{"x": 1, "y": 100}]}
        with open(randread_vis / "4096_1_randread.json", "w") as f:
            json.dump(test_data, f)
        
        randwrite_vis = self.archive_dir / "randwrite" / "visualisation"
        randwrite_vis.mkdir(parents=True)
        with open(randwrite_vis / "8192_1_randwrite.json", "w") as f:
            json.dump(test_data, f)
        
        output_dir = f"{self.temp_dir}/output"
        
        mock_plotter = MagicMock()
        mock_plotter_class.return_value = mock_plotter
        
        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )
        
        generator._copy_images()
        
        # With new nested structure, SimplePlotter is called once per operation directory
        # So with 2 operations (randread, randwrite), it should be called twice
        self.assertEqual(mock_plotter_class.call_count, 2)
        self.assertEqual(mock_plotter.draw_and_save.call_count, 2)


# Made with Bob
