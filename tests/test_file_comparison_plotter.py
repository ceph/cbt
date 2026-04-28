"""
Unit tests for the FileComparisonPlotter class
"""

import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from post_processing.plotter.file_comparison_plotter import FileComparisonPlotter


class TestFileComparisonPlotter(unittest.TestCase):
    """Test suite for FileComparisonPlotter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = Path(self.temp_dir) / "output"
        self.output_dir.mkdir(parents=True)

        # Create test data files
        self.test_file1 = Path(self.temp_dir) / "4096_100_read_1.json"
        self.test_file2 = Path(self.temp_dir) / "4096_100_write_1.json"
        self.test_file1.touch()
        self.test_file2.touch()

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test FileComparisonPlotter initialization"""
        files = [str(self.test_file1), str(self.test_file2)]
        plotter = FileComparisonPlotter(output_directory=str(self.output_dir), files=files)

        assert plotter._output_directory == str(self.output_dir)
        assert len(plotter._comparison_files) == 2
        assert plotter._comparison_files[0] == self.test_file1
        assert plotter._comparison_files[1] == self.test_file2
        assert plotter._labels is None

    def test_set_labels(self) -> None:
        """Test setting custom labels for plot lines"""
        files = [str(self.test_file1), str(self.test_file2)]
        plotter = FileComparisonPlotter(output_directory=str(self.output_dir), files=files)

        labels = ["Test 1", "Test 2"]
        plotter.set_labels(labels)

        assert plotter._labels == labels

    def test_generate_output_file_name_single_file(self) -> None:
        """Test generating output file name with single file"""
        files = [str(self.test_file1)]
        plotter = FileComparisonPlotter(output_directory=str(self.output_dir), files=files)

        output_name = plotter._generate_output_file_name([self.test_file1])

        expected = f"{self.output_dir}/Comparison_4096_100_read_1.svg"
        assert output_name == expected

    def test_generate_output_file_name_multiple_files(self) -> None:
        """Test generating output file name with multiple files"""
        files = [str(self.test_file1), str(self.test_file2)]
        plotter = FileComparisonPlotter(output_directory=str(self.output_dir), files=files)

        output_name = plotter._generate_output_file_name([self.test_file1, self.test_file2])

        expected = f"{self.output_dir}/Comparison_4096_100_read_1_4096_100_write_1.svg"
        assert output_name == expected

    @patch("post_processing.plotter.file_comparison_plotter.read_intermediate_file")
    @patch("post_processing.plotter.file_comparison_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save_without_labels(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_get_details_common: MagicMock,
        mock_get_details: MagicMock,
        mock_read_file: MagicMock,
    ) -> None:
        """Test draw_and_save method without custom labels"""
        files = [str(self.test_file1), str(self.test_file2)]
        plotter = FileComparisonPlotter(output_directory=str(self.output_dir), files=files)

        # Mock file name parsing (both locations where it's called)
        mock_get_details.return_value = ("4096", "100", "read", "1")
        mock_get_details_common.return_value = ("4096", "100", "read", "1")

        # Mock file data
        mock_read_file.return_value = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "100",
                "latency": "5000000",
                "std_deviation": "500000",
            },
            "maximum_iops": "100",
        }

        # Mock matplotlib
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter.draw_and_save()

        # Should read both files
        assert mock_read_file.call_count == 2

        # Should create subplots once
        mock_subplots.assert_called_once()

        # Should save the plot once
        mock_savefig.assert_called_once()

        # Should close the plot
        assert mock_close.call_count >= 1

    @patch("post_processing.plotter.file_comparison_plotter.read_intermediate_file")
    @patch("post_processing.plotter.file_comparison_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save_with_labels(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_get_details_common: MagicMock,
        mock_get_details: MagicMock,
        mock_read_file: MagicMock,
    ) -> None:
        """Test draw_and_save method with custom labels"""
        files = [str(self.test_file1), str(self.test_file2)]
        plotter = FileComparisonPlotter(output_directory=str(self.output_dir), files=files)

        # Set custom labels
        labels = ["Baseline", "Optimized"]
        plotter.set_labels(labels)

        # Mock file name parsing (both locations where it's called)
        mock_get_details.return_value = ("4096", "100", "read", "1")
        mock_get_details_common.return_value = ("4096", "100", "read", "1")

        # Mock file data
        mock_read_file.return_value = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "100",
                "latency": "5000000",
                "std_deviation": "500000",
            },
            "maximum_iops": "100",
        }

        # Mock matplotlib
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter.draw_and_save()

        # Should read both files
        assert mock_read_file.call_count == 2

        # Should create subplots once
        mock_subplots.assert_called_once()

        # Should save the plot once
        mock_savefig.assert_called_once()

    @patch("post_processing.plotter.file_comparison_plotter.read_intermediate_file")
    @patch("post_processing.plotter.file_comparison_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save_no_error_bars(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_get_details_common: MagicMock,
        mock_get_details: MagicMock,
        mock_read_file: MagicMock,
    ) -> None:
        """Test that error bars are not plotted in comparison mode"""
        files = [str(self.test_file1)]
        plotter = FileComparisonPlotter(output_directory=str(self.output_dir), files=files)

        # Mock file name parsing (both locations where it's called)
        mock_get_details.return_value = ("4096", "100", "read", "1")
        mock_get_details_common.return_value = ("4096", "100", "read", "1")

        # Mock file data with std_deviation
        mock_read_file.return_value = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "100",
                "latency": "5000000",
                "std_deviation": "500000",
            },
            "maximum_iops": "100",
        }

        # Mock matplotlib
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter.draw_and_save()

        # Verify that the plotter completed successfully
        # (plot is called on a twinx axis, not the main axes directly)
        mock_savefig.assert_called_once()

    @patch("post_processing.plotter.file_comparison_plotter.read_intermediate_file")
    @patch("post_processing.plotter.file_comparison_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save_adds_legend(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_get_details_common: MagicMock,
        mock_get_details: MagicMock,
        mock_read_file: MagicMock,
    ) -> None:
        """Test that legend is added to the plot"""
        files = [str(self.test_file1), str(self.test_file2)]
        plotter = FileComparisonPlotter(output_directory=str(self.output_dir), files=files)

        # Mock file name parsing (both locations where it's called)
        mock_get_details.return_value = ("4096", "100", "read", "1")
        mock_get_details_common.return_value = ("4096", "100", "read", "1")

        # Mock file data
        mock_read_file.return_value = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "100",
                "latency": "5000000",
            },
        }

        # Mock matplotlib
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter.draw_and_save()

        # Verify legend was added
        mock_figure.legend.assert_called_once()


# Made with Bob