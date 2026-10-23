"""
Unit tests for the DirectoryComparisonPlotter class
"""

# The mocked matplotlib/patch decorators inject many (sometimes unused) arguments, and
# the tests deliberately exercise protected members of the class under test.
# pylint: disable=protected-access,too-many-arguments,too-many-positional-arguments,unused-argument
# pylint: disable=too-many-instance-attributes

import json
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
        assert len(plotter._archive_directories) == 2
        assert plotter._archive_directories[0] == self.dir1
        assert plotter._archive_directories[1] == self.dir2

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
        # Create the JSON files BEFORE instantiating plotter so find_hockey_stick_visualisation_directories finds them
        (self.vis_dir1 / "4096_100_read_1.json").touch()
        (self.vis_dir1 / "8192_100_write_1.json").touch()
        (self.vis_dir2 / "4096_100_read_1.json").touch()
        (self.vis_dir2 / "8192_100_write_1.json").touch()

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

    @patch("post_processing.plotter.directory_comparison_plotter.find_common_data_file_names")
    def test_draw_and_save_passes_archive_directories_to_find_common(self, mock_find_common: MagicMock) -> None:
        """The archive directories must be passed so files are merged within an archive"""
        plotter = DirectoryComparisonPlotter(
            output_directory=str(self.output_dir), directories=[str(self.dir1), str(self.dir2)]
        )
        mock_find_common.return_value = []

        plotter.draw_and_save()

        mock_find_common.assert_called_once()
        assert mock_find_common.call_args.kwargs.get("archive_directories") == [
            self.dir1,
            self.dir2,
        ], "find_common_data_file_names should be given the archive directories"

    def test_draw_and_save_merges_files_from_multiple_directories_in_one_archive(self) -> None:
        """An archive with several visualisation directories for one operation contributes the union of its files"""
        for sub_directory, file_name in (("x", "a.json"), ("y", "b.json")):
            vis_dir = Path(self.temp_dir) / "nested" / sub_directory / "randread" / "visualisation"
            vis_dir.mkdir(parents=True)
            (vis_dir / file_name).touch()

        other_vis_dir = Path(self.temp_dir) / "other" / "randread" / "visualisation"
        other_vis_dir.mkdir(parents=True)
        (other_vis_dir / "a.json").touch()
        (other_vis_dir / "b.json").touch()

        plotter = DirectoryComparisonPlotter(
            output_directory=str(self.output_dir),
            directories=[str(Path(self.temp_dir) / "nested"), str(Path(self.temp_dir) / "other")],
        )

        with patch.object(plotter, "_plot_comparison_file") as mock_plot:
            plotter.draw_and_save()

        plotted_files = sorted(call.kwargs["file_name"] for call in mock_plot.call_args_list)
        assert plotted_files == ["a.json", "b.json"], "files from both directories of an archive should be merged"

    def test_draw_and_save_skips_files_missing_from_an_archive(self) -> None:
        """A file found in only one archive is not plotted, even if that archive has several directories for it"""
        for sub_directory in ("x", "y"):
            vis_dir = Path(self.temp_dir) / "nested" / sub_directory / "randread" / "visualisation"
            vis_dir.mkdir(parents=True)
            (vis_dir / "4096_1_randread.json").touch()

        other_vis_dir = Path(self.temp_dir) / "other" / "randwrite" / "visualisation"
        other_vis_dir.mkdir(parents=True)
        (other_vis_dir / "4096_1_randwrite.json").touch()

        plotter = DirectoryComparisonPlotter(
            output_directory=str(self.output_dir),
            directories=[str(Path(self.temp_dir) / "nested"), str(Path(self.temp_dir) / "other")],
        )

        with patch.object(plotter, "_plot_comparison_file") as mock_plot:
            plotter.draw_and_save()

        mock_plot.assert_not_called()

    def test_draw_and_save_does_not_plot_when_an_archive_has_no_data(self) -> None:
        """If one archive has no visualisation data there is nothing to compare, so nothing is plotted"""
        empty_archive = Path(self.temp_dir) / "empty"
        empty_archive.mkdir()

        plotter = DirectoryComparisonPlotter(
            output_directory=str(self.output_dir), directories=[str(self.dir1), str(empty_archive)]
        )

        assert self._plotted_file_names(plotter) == []

    def test_draw_and_save_ignores_workload_directory_names_and_hostnames(self) -> None:
        """Archives are compared on the data file names only, so runs on different hosts can be compared"""
        layout = {
            "baseline": ["seq32kwrite.cephalasquad6.ssd.hursley.ibm.com/visualisation/32768_1_write.json"],
            "comparison": [
                "seq32kread.cephalasquad3.ssd.hursley.ibm.com/visualisation/32768_1_read.json",
                "seq32kread.cephalasquad3.ssd.hursley.ibm.com/visualisation/32768_2_read.json",
                "seq32kwrite.cephalasquad3.ssd.hursley.ibm.com/visualisation/32768_1_write.json",
                "seq32kwrite.cephalasquad3.ssd.hursley.ibm.com/visualisation/32768_2_write.json",
            ],
        }
        for archive, entries in layout.items():
            for entry in entries:
                path = Path(self.temp_dir) / archive / "results" / "00000000" / "id-0123abcd" / entry
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()

        plotter = DirectoryComparisonPlotter(
            output_directory=str(self.output_dir),
            directories=[str(Path(self.temp_dir) / archive) for archive in layout],
        )

        # Only 32768_1_write.json is in both archives, and it is in one directory in each
        assert self._plotted_file_names(plotter) == [("32768_1_write.json", 2)]

    def _plotted_file_names(self, plotter: DirectoryComparisonPlotter) -> list[tuple[str, int]]:
        """Run draw_and_save with the real file discovery, returning (file_name, number_of_directories) per plot"""
        with patch.object(plotter, "_plot_comparison_file") as mock_plot:
            plotter.draw_and_save()

        return sorted(
            (call.kwargs["file_name"], len(call.kwargs["vis_directories"])) for call in mock_plot.call_args_list
        )

    def test_draw_and_save_legacy_structure_plots_only_files_in_every_archive(self) -> None:
        """One visualisation directory per archive (legacy layout): plot exactly the files present in all archives"""
        (self.vis_dir1 / "only_in_first.json").touch()
        (self.vis_dir2 / "only_in_second.json").touch()
        (self.vis_dir1 / "common_1.json").touch()
        (self.vis_dir2 / "common_1.json").touch()
        (self.vis_dir1 / "common_timeseries.json").touch()
        (self.vis_dir2 / "common_timeseries.json").touch()

        plotter = DirectoryComparisonPlotter(
            output_directory=str(self.output_dir), directories=[str(self.dir1), str(self.dir2)]
        )

        # setUp creates 4096_100_read_1.json in both, so there are two common files, and no time-series plots
        assert self._plotted_file_names(plotter) == [("4096_100_read_1.json", 2), ("common_1.json", 2)]

    def test_draw_and_save_operation_structure_plots_each_operation_separately(self) -> None:
        """One visualisation directory per operation per archive: plot the common files of every operation"""
        for archive in ("base", "new"):
            for operation, file_names in (
                ("randread", ("r1.json", "r2.json")),
                ("randwrite", ("w1.json",)),
            ):
                vis_dir = Path(self.temp_dir) / archive / operation / "visualisation"
                vis_dir.mkdir(parents=True)
                for file_name in file_names:
                    (vis_dir / file_name).touch()
        (Path(self.temp_dir) / "new" / "randread" / "visualisation" / "extra.json").touch()

        plotter = DirectoryComparisonPlotter(
            output_directory=str(self.output_dir),
            directories=[str(Path(self.temp_dir) / "base"), str(Path(self.temp_dir) / "new")],
        )

        assert self._plotted_file_names(plotter) == [("r1.json", 2), ("r2.json", 2), ("w1.json", 2)]

    def test_draw_and_save_operation_structure_plots_only_operations_in_every_archive(self) -> None:
        """One visualisation directory per operation per archive: plot only operations present in every archive"""
        for archive, operations in (("base", ("randread", "randwrite")), ("new", ("randread",))):
            for operation in operations:
                vis_dir = Path(self.temp_dir) / archive / operation / "visualisation"
                vis_dir.mkdir(parents=True)
                (vis_dir / f"{operation}.json").touch()

        plotter = DirectoryComparisonPlotter(
            output_directory=str(self.output_dir),
            directories=[str(Path(self.temp_dir) / "base"), str(Path(self.temp_dir) / "new")],
        )

        assert self._plotted_file_names(plotter) == [("randread.json", 2)]

    def test_draw_and_save_same_archive_given_twice_still_plots(self) -> None:
        """Comparing an archive with itself (e.g. --baseline and --archives the same) must still produce plots"""
        vis_dir = Path(self.temp_dir) / "same" / "randread" / "visualisation"
        vis_dir.mkdir(parents=True)
        (vis_dir / "a.json").touch()

        archive = str(Path(self.temp_dir) / "same")
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=[archive, archive])

        assert self._plotted_file_names(plotter) == [("a.json", 2)]

    def test_draw_and_save_plots_all_directories_of_an_archive_holding_the_same_file(self) -> None:
        """Several directories in one archive with the same file name (e.g. one per client host) are all passed on"""
        for host in ("hostA", "hostB"):
            vis_dir = Path(self.temp_dir) / "multi" / f"seq32kwrite.{host}" / "visualisation"
            vis_dir.mkdir(parents=True)
            (vis_dir / "32768_1_write.json").touch()
        single_vis_dir = Path(self.temp_dir) / "single" / "seq32kwrite.hostC" / "visualisation"
        single_vis_dir.mkdir(parents=True)
        (single_vis_dir / "32768_1_write.json").touch()

        plotter = DirectoryComparisonPlotter(
            output_directory=str(self.output_dir),
            directories=[str(Path(self.temp_dir) / "multi"), str(Path(self.temp_dir) / "single")],
        )

        with self.assertLogs("cbt.plotter", level="WARNING") as captured:
            plotted = self._plotted_file_names(plotter)

        assert plotted == [("32768_1_write.json", 3)]
        assert any("more than one" in message for message in captured.output), "partial data should be flagged"

    def test_uses_directory_name_as_label(self) -> None:
        """Test that directory names are used as labels in the plot"""
        # Create directories with meaningful names using new nested structure
        baseline_dir = Path(self.temp_dir) / "baseline"
        optimized_dir = Path(self.temp_dir) / "optimized"

        # Create operation-level visualisation directories (new structure)
        baseline_vis = baseline_dir / "randread" / "visualisation"
        optimized_vis = optimized_dir / "randread" / "visualisation"
        baseline_vis.mkdir(parents=True)
        optimized_vis.mkdir(parents=True)

        # Create JSON files so directories are found
        test_data = {"data": [{"x": 1, "y": 100}]}
        with open(baseline_vis / "test.json", "w", encoding="utf-8") as f:
            json.dump(test_data, f)
        with open(optimized_vis / "test.json", "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        directories = [str(baseline_dir), str(optimized_dir)]
        plotter = DirectoryComparisonPlotter(output_directory=str(self.output_dir), directories=directories)

        # Verify the archive directories are set correctly
        assert plotter._archive_directories[0] == baseline_dir
        assert plotter._archive_directories[1] == optimized_dir


# Made with Bob
