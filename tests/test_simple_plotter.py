"""
Unit tests for the post_processing/plotter simple_plotter module class
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

from post_processing.plotter.simple_plotter import SimplePlotter


class TestSimplePlotter(unittest.TestCase):
    """Test cases for SimplePlotter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.archive_dir = Path(self.temp_dir) / "archive"
        self.vis_dir = self.archive_dir / "visualisation"
        self.vis_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self) -> None:
        """Test SimplePlotter initialization"""
        plotter = SimplePlotter(archive_directory=str(self.archive_dir), plot_error_bars=True, plot_resources=False)

        self.assertEqual(plotter._path, self.vis_dir)
        self.assertTrue(plotter._plot_error_bars)
        self.assertFalse(plotter._plot_resources)

    def test_initialization_with_different_options(self) -> None:
        """Test SimplePlotter initialization with different options"""
        plotter = SimplePlotter(archive_directory=str(self.archive_dir), plot_error_bars=False, plot_resources=True)

        self.assertFalse(plotter._plot_error_bars)
        self.assertTrue(plotter._plot_resources)

    def test_generate_output_file_name(self) -> None:
        """Test generating output file name"""
        plotter = SimplePlotter(archive_directory=str(self.archive_dir), plot_error_bars=True, plot_resources=False)

        input_file = Path("/path/to/4096_read.json")
        output_name = plotter._generate_output_file_name([input_file])

        self.assertEqual(output_name, "/path/to/4096_read.svg")

    @patch("post_processing.plotter.simple_plotter.read_intermediate_file")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_read_file: MagicMock,
    ) -> None:
        """Test draw_and_save method"""
        # Create test data file
        test_file = self.vis_dir / "4096_read.json"
        test_file.touch()

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

        plotter = SimplePlotter(archive_directory=str(self.archive_dir), plot_error_bars=True, plot_resources=False)

        plotter.draw_and_save()

        # Should read the file
        mock_read_file.assert_called_once()

        # Should create subplots
        mock_subplots.assert_called_once()

        # Should save the plot
        mock_savefig.assert_called_once()

        # Should close the plot (may be called once or twice depending on matplotlib version)
        self.assertGreaterEqual(mock_close.call_count, 1)

    @patch("post_processing.plotter.simple_plotter.read_intermediate_file")
    @patch("matplotlib.pyplot.subplots")
    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.close")
    def test_draw_and_save_multiple_files(
        self,
        mock_close: MagicMock,
        mock_savefig: MagicMock,
        mock_subplots: MagicMock,
        mock_read_file: MagicMock,
    ) -> None:
        """Test draw_and_save with multiple data files"""
        # Create multiple test data files
        (self.vis_dir / "4096_read.json").touch()
        (self.vis_dir / "8192_write.json").touch()

        mock_read_file.return_value = {
            "1": {
                "blocksize": "4096",
                "bandwidth_bytes": "1000000",
                "iops": "100",
                "latency": "5000000",
                "std_deviation": "500000",
            },
        }

        mock_figure = MagicMock()
        mock_axes = MagicMock()
        mock_subplots.return_value = (mock_figure, mock_axes)

        plotter = SimplePlotter(archive_directory=str(self.archive_dir), plot_error_bars=False, plot_resources=False)

        plotter.draw_and_save()

        # Should process both files
        self.assertEqual(mock_read_file.call_count, 2)
        self.assertEqual(mock_savefig.call_count, 2)
        # close may be called 2-4 times depending on matplotlib version
        self.assertGreaterEqual(mock_close.call_count, 2)


# Made with Bob
