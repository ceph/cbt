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

from post_processing.reports.report_generator import ReportGenerator
from post_processing.reports.simple_report_generator import SimpleReportGenerator


class TestReportGenerator(unittest.TestCase):
    """Test cases for ReportGenerator base class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.archive_dir = Path(self.temp_dir) / "archive"
        self.vis_dir = self.archive_dir / "visualisation"
        self.vis_dir.mkdir(parents=True)

        # Create some test data files
        (self.vis_dir / "4096_read.json").touch()
        (self.vis_dir / "8192_write.json").touch()

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test ReportGenerator initialization"""
        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
            no_error_bars=False,
            force_refresh=False,
            plot_resources=False,
        )

        self.assertTrue(generator._plot_error_bars)
        self.assertFalse(generator._force_refresh)
        self.assertFalse(generator._plot_resources)
        self.assertEqual(len(generator._archive_directories), 1)
        self.assertEqual(len(generator._data_directories), 1)

    def test_initialization_with_no_error_bars(self) -> None:
        """Test initialization with no_error_bars=True"""
        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
            no_error_bars=True,
            force_refresh=False,
            plot_resources=False,
        )

        self.assertFalse(generator._plot_error_bars)

    def test_initialization_with_plot_resources(self) -> None:
        """Test initialization with plot_resources=True"""
        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
            no_error_bars=False,
            force_refresh=False,
            plot_resources=True,
        )

        self.assertTrue(generator._plot_resources)

    def test_build_strings_replace_underscores(self) -> None:
        """Test that build strings replace underscores with hyphens"""
        archive_with_underscores = Path(self.temp_dir) / "test_archive_name"
        vis_dir = archive_with_underscores / "visualisation"
        vis_dir.mkdir(parents=True)
        (vis_dir / "4096_read.json").touch()

        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(archive_with_underscores)],
            output_directory=output_dir,
        )

        self.assertEqual(generator._build_strings[0], "test-archive-name")
        self.assertNotIn("_", generator._build_strings[0])

    def test_find_files_with_filename(self) -> None:
        """Test finding files with specific filename"""
        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        files = generator._find_files_with_filename("4096_read")

        self.assertEqual(len(files), 1)
        self.assertTrue(str(files[0]).endswith("4096_read.json"))

    def test_sort_list_of_paths(self) -> None:
        """Test sorting paths by numeric blocksize"""
        # Create files with different blocksizes
        (self.vis_dir / "16384_read.json").touch()
        (self.vis_dir / "1024_read.json").touch()

        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        paths = list(self.vis_dir.glob("*.json"))
        sorted_paths = generator._sort_list_of_paths(paths, index=0)

        # Should be sorted by blocksize: 1024, 4096, 8192, 16384
        self.assertTrue(str(sorted_paths[0]).endswith("1024_read.json"))
        self.assertTrue(str(sorted_paths[-1]).endswith("16384_read.json"))

    def test_generate_plot_directory_name(self) -> None:
        """Test generating unique plot directory name"""
        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        plot_dir_name = generator._generate_plot_directory_name()

        self.assertTrue(plot_dir_name.startswith(f"{output_dir}/plots."))
        # Should have timestamp appended
        self.assertGreater(len(plot_dir_name), len(f"{output_dir}/plots."))

    def test_constants(self) -> None:
        """Test ReportGenerator constants"""
        self.assertEqual(ReportGenerator.MARKDOWN_FILE_EXTENSION, "md")
        self.assertEqual(ReportGenerator.PDF_FILE_EXTENSION, "pdf")
        self.assertEqual(ReportGenerator.BASE_HEADER_FILE_PATH, "include/performance_report.tex")


# Made with Bob
