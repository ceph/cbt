"""
Unit tests for the post_processing/reports comparison report class
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

from post_processing.reports.comparison_report_generator import ComparisonReportGenerator


class TestComparisonReportGenerator(unittest.TestCase):
    """Test cases for ComparisonReportGenerator class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()

        # Create two archive directories for comparison
        self.archive1 = Path(self.temp_dir) / "baseline"
        self.archive2 = Path(self.temp_dir) / "comparison"

        self.vis1 = self.archive1 / "visualisation"
        self.vis2 = self.archive2 / "visualisation"

        self.vis1.mkdir(parents=True)
        self.vis2.mkdir(parents=True)

        # Create matching data files in both
        (self.vis1 / "4096_read.json").touch()
        (self.vis2 / "4096_read.json").touch()

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization_with_multiple_archives(self) -> None:
        """Test initialization with multiple archive directories"""
        output_dir = f"{self.temp_dir}/output"

        generator = ComparisonReportGenerator(
            archive_directories=[str(self.archive1), str(self.archive2)],
            output_directory=output_dir,
        )

        self.assertEqual(len(generator._archive_directories), 2)
        self.assertEqual(len(generator._data_directories), 2)

    def test_generate_report_title(self) -> None:
        """Test generating comparison report title"""
        output_dir = f"{self.temp_dir}/output"

        generator = ComparisonReportGenerator(
            archive_directories=[str(self.archive1), str(self.archive2)],
            output_directory=output_dir,
        )

        title = generator._generate_report_title()

        self.assertIn("Comparitive Performance Report", title)
        self.assertIn("baseline", title)
        self.assertIn("comparison", title)
        self.assertIn(" vs ", title)

    def test_generate_report_name(self) -> None:
        """Test generating comparison report name"""
        output_dir = f"{self.temp_dir}/output"

        generator = ComparisonReportGenerator(
            archive_directories=[str(self.archive1), str(self.archive2)],
            output_directory=output_dir,
        )

        report_name = generator._generate_report_name()

        self.assertTrue(report_name.startswith("comparitive_performance_report_"))
        self.assertTrue(report_name.endswith(".md"))

    def test_find_and_sort_file_paths_multiple_directories(self) -> None:
        """Test finding files across multiple directories"""
        # Create additional files
        (self.vis1 / "8192_write.json").touch()
        (self.vis2 / "8192_write.json").touch()

        output_dir = f"{self.temp_dir}/output"

        generator = ComparisonReportGenerator(
            archive_directories=[str(self.archive1), str(self.archive2)],
            output_directory=output_dir,
        )

        paths = generator._find_and_sort_file_paths(paths=[self.vis1, self.vis2], search_pattern="*.json", index=0)

        # Should find files from both directories
        self.assertEqual(len(paths), 4)

    @patch("post_processing.reports.comparison_report_generator.DirectoryComparisonPlotter")
    def test_copy_images_creates_comparison_plots(self, mock_plotter_class: MagicMock) -> None:
        """Test that _copy_images creates comparison plots"""
        output_dir = f"{self.temp_dir}/output"

        mock_plotter = MagicMock()
        mock_plotter_class.return_value = mock_plotter

        generator = ComparisonReportGenerator(
            archive_directories=[str(self.archive1), str(self.archive2)],
            output_directory=output_dir,
        )

        generator._copy_images()

        # Should create comparison plotter
        mock_plotter_class.assert_called_once()
        mock_plotter.draw_and_save.assert_called_once()

    def test_generate_table_headers_two_directories(self) -> None:
        """Test generating table headers for two directories"""
        output_dir = f"{self.temp_dir}/output"

        generator = ComparisonReportGenerator(
            archive_directories=[str(self.archive1), str(self.archive2)],
            output_directory=output_dir,
        )

        header, justification = generator._generate_table_headers()

        # Should include baseline directory name
        self.assertIn("baseline", header)
        # Should include comparison directory name
        self.assertIn("comparison", header)
        # Should have percentage change columns
        self.assertIn("%change", header)
        
        # Test justification string for two directories
        # Format: | :--- | ---: | ---: | ---: | ---: |
        # (left-aligned first column, right-aligned for baseline, comparison, %change throughput, %change latency)
        self.assertEqual(justification, "| :--- | ---: | ---: | ---: | ---: |")

    def test_generate_table_headers_multiple_directories(self) -> None:
        """Test generating table headers for more than two directories"""
        archive3 = Path(self.temp_dir) / "comparison2"
        vis3 = archive3 / "visualisation"
        vis3.mkdir(parents=True)
        (vis3 / "4096_read.json").touch()

        output_dir = f"{self.temp_dir}/output"

        generator = ComparisonReportGenerator(
            archive_directories=[str(self.archive1), str(self.archive2), str(archive3)],
            output_directory=output_dir,
        )

        header, justification = generator._generate_table_headers()

        # Should have all directory names
        self.assertIn("baseline", header)
        self.assertIn("comparison", header)
        self.assertIn("comparison2", header)
        
        # Test justification string for multiple directories (3+ total)
        # Format: | :--- | ---: | ---: | ---: | ---: | ---: |
        # (left-aligned first column, right-aligned for baseline, then comparison + %change for each comparison dir)
        self.assertEqual(justification, "| :--- | ---: | ---: | ---: | ---: | ---: |")

    @patch("subprocess.check_output")
    def test_yaml_file_has_more_than_20_differences_true(self, mock_check_output: MagicMock) -> None:
        """Test detecting significant differences between yaml files"""
        # Mock diff output showing 25 differences
        mock_check_output.return_value = b"25\n"

        output_dir = f"{self.temp_dir}/output"

        generator = ComparisonReportGenerator(
            archive_directories=[str(self.archive1), str(self.archive2)],
            output_directory=output_dir,
        )

        file1 = Path(self.temp_dir) / "file1.yaml"
        file2 = Path(self.temp_dir) / "file2.yaml"
        file1.touch()
        file2.touch()

        result = generator._yaml_file_has_more_that_20_differences(file1, file2)

        self.assertTrue(result)

    @patch("subprocess.check_output")
    def test_yaml_file_has_more_than_20_differences_false(self, mock_check_output: MagicMock) -> None:
        """Test detecting minor differences between yaml files"""
        # Mock diff output showing 10 differences
        mock_check_output.return_value = b"10\n"

        output_dir = f"{self.temp_dir}/output"

        generator = ComparisonReportGenerator(
            archive_directories=[str(self.archive1), str(self.archive2)],
            output_directory=output_dir,
        )

        file1 = Path(self.temp_dir) / "file1.yaml"
        file2 = Path(self.temp_dir) / "file2.yaml"
        file1.touch()
        file2.touch()

        result = generator._yaml_file_has_more_that_20_differences(file1, file2)

        self.assertFalse(result)


# Made with Bob
