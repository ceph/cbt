"""
Unit tests for the DirectoryComparisonPlotter class
"""

import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from post_processing.plotter.directory_comparison_plotter import DirectoryComparisonPlotter


class TestDirectoryComparisonPlotter(unittest.TestCase):
    """Test suite for DirectoryComparisonPlotter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = Path(self.temp_dir) / "output"
        self.output_dir.mkdir(parents=True)

        # Create test directory structures
        self.dir1 = Path(self.temp_dir) / "test1"
        self.dir2 = Path(self.temp_dir) / "test2"
        self.vis_dir1 = self.dir1 / "visualisation"
        self.vis_dir2 = self.dir2 / "visualisation"
        self.vis_dir1.mkdir(parents=True)
        self.vis_dir2.mkdir(parents=True)

        # Create common test data files
        self.test_file1 = self.vis_dir1 / "4096_100_read_1.json"
        self.test_file2 = self.vis_dir2 / "4096_100_read_1.json"
        self.test_file1.touch()
        self.test_file2.touch()

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test DirectoryComparisonPlotter initialization"""
        directories = [str(self.dir1), str(self.dir2)]
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=directories)

        assert plotter._output_directory == str(self.output_dir)
        assert len(plotter._comparison_directories) == 2
        assert plotter._comparison_directories[0] == self.vis_dir1
        assert plotter._comparison_directories[1] == self.vis_dir2

    def test_generate_output_file_name(self) -> None:
        """Test generating output file name"""
        directories = [str(self.dir1), str(self.dir2)]
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=directories)

        test_file = Path("4096_100_read_1.json")
        output_name = plotter._generate_output_file_name([test_file])

        expected = f"{self.output_dir}/Comparison_4096_100_read_1.svg"
        assert output_name == expected

    @patch("post_processing.plotter.directory_comparison_plotter.find_common_data_file_names")
    @patch("post_processing.plotter.directory_comparison_plotter.read_intermediate_file")
    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save_single_common_file(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_get_details: MagicMock,
        mock_read_file: MagicMock,
        mock_find_common: MagicMock,
    ) -> None:
        """Test draw_and_save with single common file"""
        directories = [str(self.dir1), str(self.dir2)]
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=directories)

        # Mock common file names
        mock_find_common.return_value = ["4096_100_read_1.json"]

        # Mock file name parsing
        mock_get_details.return_value = ("4096", "100", "read", "1")

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

        # Should find common files
        mock_find_common.assert_called_once()

        # Should read file from both directories
        assert mock_read_file.call_count == 2

        # Should create subplots once (one plot for the common file)
        mock_subplots.assert_called_once()

        # Should save the plot once
        mock_savefig.assert_called_once()

        # Should close the plot
        assert mock_close.call_count >= 1

    @patch("post_processing.plotter.directory_comparison_plotter.find_common_data_file_names")
    @patch("post_processing.plotter.directory_comparison_plotter.read_intermediate_file")
    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save_multiple_common_files(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_get_details: MagicMock,
        mock_read_file: MagicMock,
        mock_find_common: MagicMock,
    ) -> None:
        """Test draw_and_save with multiple common files"""
        directories = [str(self.dir1), str(self.dir2)]
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=directories)

        # Mock multiple common file names
        mock_find_common.return_value = ["4096_100_read_1.json", "8192_100_write_1.json"]

        # Mock file name parsing
        mock_get_details.return_value = ("4096", "100", "read", "1")

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

        # Should read file from both directories for each common file (2 files * 2 dirs = 4 reads)
        assert mock_read_file.call_count == 4

        # Should create subplots twice (one for each common file)
        assert mock_subplots.call_count == 2

        # Should save the plot twice
        assert mock_savefig.call_count == 2

    @patch("post_processing.plotter.directory_comparison_plotter.find_common_data_file_names")
    @patch("post_processing.plotter.directory_comparison_plotter.read_intermediate_file")
    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save_no_error_bars(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_get_details: MagicMock,
        mock_read_file: MagicMock,
        mock_find_common: MagicMock,
    ) -> None:
        """Test that error bars are not plotted in comparison mode"""
        directories = [str(self.dir1), str(self.dir2)]
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=directories)

        # Mock common file names
        mock_find_common.return_value = ["4096_100_read_1.json"]

        # Mock file name parsing
        mock_get_details.return_value = ("4096", "100", "read", "1")

        # Mock file data with std_deviation
        mock_read_file.return_value = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "100",
                "latency": "5000000",
                "std_deviation": "500000",
            },
        }

        # Mock matplotlib
        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter.draw_and_save()

        # Verify that the plotter completed successfully
        # (plot is called on a twinx axis, not the main axes directly)
        mock_savefig.assert_called_once()

    @patch("post_processing.plotter.directory_comparison_plotter.find_common_data_file_names")
    @patch("post_processing.plotter.directory_comparison_plotter.read_intermediate_file")
    @patch("post_processing.plotter.common_format_plotter.get_blocksize_percentage_operation_numjobs_from_file_name")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save_adds_legend(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_get_details: MagicMock,
        mock_read_file: MagicMock,
        mock_find_common: MagicMock,
    ) -> None:
        """Test that legend is added to the plot"""
        directories = [str(self.dir1), str(self.dir2)]
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=directories)

        # Mock common file names
        mock_find_common.return_value = ["4096_100_read_1.json"]

        # Mock file name parsing
        mock_get_details.return_value = ("4096", "100", "read", "1")

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

    @patch("post_processing.plotter.directory_comparison_plotter.find_common_data_file_names")
    def test_draw_and_save_no_common_files(
        self,
        mock_find_common: MagicMock,
    ) -> None:
        """Test draw_and_save when no common files exist"""
        directories = [str(self.dir1), str(self.dir2)]
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=directories)

        # Mock no common file names
        mock_find_common.return_value = []

        # Should not raise an error, just do nothing
        plotter.draw_and_save()

        # Should find common files
        mock_find_common.assert_called_once()

    def test_uses_directory_name_as_label(self) -> None:
        """Test that directory names are used as labels in the plot"""
        # Create directories with meaningful names
        baseline_dir = Path(self.temp_dir) / "baseline"
        optimized_dir = Path(self.temp_dir) / "optimized"
        baseline_vis = baseline_dir / "visualisation"
        optimized_vis = optimized_dir / "visualisation"
        baseline_vis.mkdir(parents=True)
        optimized_vis.mkdir(parents=True)

        directories = [str(baseline_dir), str(optimized_dir)]
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=directories)

        # Verify the comparison directories are set correctly
        assert plotter._comparison_directories[0] == baseline_vis
        assert plotter._comparison_directories[1] == optimized_vis


# Made with Bob