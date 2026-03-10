"""
Unit tests for the post_processing/reports module classes
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

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
        self.vis_dir = self.archive_dir / "visualisation"
        self.vis_dir.mkdir(parents=True)

        # Create test data files
        (self.vis_dir / "4096_read.json").touch()

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
        (self.vis_dir / "8192_write.json").touch()
        (self.vis_dir / "16384_read.json").touch()

        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        paths = generator._find_and_sort_file_paths(paths=[self.vis_dir], search_pattern="*.json", index=0)

        self.assertEqual(len(paths), 3)
        # Should be sorted by blocksize
        self.assertTrue(str(paths[0]).endswith("4096_read.json"))


# Made with Bob
