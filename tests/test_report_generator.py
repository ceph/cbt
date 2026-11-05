"""
Unit tests for the post_processing/reports module classes
"""

# pyright: strict, reportPrivateUsage=false
# pylint: disable=protected-access
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
        """Set up test fixtures with new nested structure"""
        self.temp_dir = tempfile.mkdtemp()
        self.archive_dir = Path(self.temp_dir) / "archive"

        # Create new nested structure: operation/visualisation/
        read_vis_dir = self.archive_dir / "read" / "visualisation"
        write_vis_dir = self.archive_dir / "write" / "visualisation"
        read_vis_dir.mkdir(parents=True)
        write_vis_dir.mkdir(parents=True)

        # Also create top-level visualisation for SVG files
        self.vis_dir = self.archive_dir / "visualisation"
        self.vis_dir.mkdir(parents=True)

        # Create some test data files in nested structure
        # Format: {blocksize}_{numjobs}_{operation}.json
        (read_vis_dir / "4096_1_read.json").touch()
        (write_vis_dir / "8192_1_write.json").touch()

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
        # With new nested structure, we have 2 operation directories (read and write)
        self.assertEqual(len(generator._data_directories), 2)

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

        # Create new nested structure
        read_vis_dir = archive_with_underscores / "read" / "visualisation"
        read_vis_dir.mkdir(parents=True)
        # Format: {blocksize}_{numjobs}_{operation}.json
        (read_vis_dir / "4096_1_read.json").touch()

        # Also create top-level visualisation for SVG files
        top_vis_dir = archive_with_underscores / "visualisation"
        top_vis_dir.mkdir(parents=True)

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

        files = generator._find_files_with_filename("4096_1_read")

        self.assertEqual(len(files), 1)
        self.assertTrue(str(files[0]).endswith("4096_1_read.json"))

    def test_sort_list_of_paths(self) -> None:
        """Test sorting paths by numeric blocksize"""
        # Create files with different blocksizes in new nested structure
        # Format: {blocksize}_{numjobs}_{operation}.json
        read_vis_dir = self.archive_dir / "read" / "visualisation"
        read_vis_dir.mkdir(parents=True, exist_ok=True)
        (read_vis_dir / "16384_1_read.json").touch()
        (read_vis_dir / "1024_1_read.json").touch()

        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=output_dir,
        )

        paths = list(read_vis_dir.glob("*.json"))
        sorted_paths = generator._sort_list_of_paths(paths, index=0)

        # Should be sorted by blocksize: 1024, 4096, 8192, 16384
        self.assertTrue(str(sorted_paths[0]).endswith("1024_1_read.json"))
        self.assertTrue(str(sorted_paths[-1]).endswith("16384_1_read.json"))

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

    def test_force_refresh_allows_legacy_visualisation_directory(self) -> None:
        """Test that force_refresh bypasses legacy visualisation directory rejection"""
        legacy_archive = Path(self.temp_dir) / "legacy_archive"
        legacy_vis_dir = legacy_archive / "visualisation"
        legacy_vis_dir.mkdir(parents=True)
        (legacy_vis_dir / "4096_1_read.json").touch()

        output_dir = f"{self.temp_dir}/output"

        generator = SimpleReportGenerator(
            archive_directories=[str(legacy_archive)],
            output_directory=output_dir,
            force_refresh=True,
        )

        self.assertTrue(generator._force_refresh)
        self.assertEqual(generator._data_directories, [legacy_vis_dir])

    def test_constants(self) -> None:
        """Test ReportGenerator constants"""
        self.assertEqual(ReportGenerator.MARKDOWN_FILE_EXTENSION, "md")
        self.assertEqual(ReportGenerator.PDF_FILE_EXTENSION, "pdf")
        self.assertEqual(ReportGenerator.BASE_HEADER_FILE_PATH, "include/performance_report.tex")

    def test_find_configuration_yaml_file_in_results_subdir(self) -> None:
        """Test finding cbt_config.yaml inside results subdirectory"""
        results_dir = self.archive_dir / "results"
        results_dir.mkdir(parents=True)
        config_file = results_dir / "cbt_config.yaml"
        config_file.touch()

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=f"{self.temp_dir}/output",
        )

        found = generator._find_configuration_yaml_file(self.archive_dir)
        self.assertEqual(found, config_file)

    def test_find_configuration_yaml_file_directly_in_archive_dir(self) -> None:
        """Test finding cbt_config.yaml directly in archive directory"""
        config_file = self.archive_dir / "cbt_config.yaml"
        config_file.touch()

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=f"{self.temp_dir}/output",
        )

        found = generator._find_configuration_yaml_file(self.archive_dir)
        self.assertEqual(found, config_file)

    def test_find_configuration_yaml_file_when_archive_is_nested_in_results(self) -> None:
        """Test finding cbt_config.yaml when archive directory is nested inside results tree"""
        results_dir = self.archive_dir / "results"
        results_dir.mkdir(parents=True)
        config_file = results_dir / "cbt_config.yaml"
        config_file.touch()

        nested_archive = results_dir / "00000000" / "id-12345"
        nested_vis = nested_archive / "read" / "visualisation"
        nested_vis.mkdir(parents=True)
        (nested_vis / "4096_1_read.json").touch()

        generator = SimpleReportGenerator(
            archive_directories=[str(nested_archive)],
            output_directory=f"{self.temp_dir}/output",
        )

        found = generator._find_configuration_yaml_file(nested_archive)
        self.assertEqual(found, config_file)

    def test_find_configuration_yaml_file_does_not_leak_sibling_archives(self) -> None:
        """Test that searching for YAML does not find files from sibling archives under a shared parent"""
        shared_parent = Path(self.temp_dir) / "results"
        archive1 = shared_parent / "run1"
        archive2 = shared_parent / "run2"

        # Create nested visualisation dirs
        for archive in (archive1, archive2):
            vis = archive / "read" / "visualisation"
            vis.mkdir(parents=True)
            (vis / "4096_1_read.json").touch()

        # Archive2 has a cbt_config.yaml, Archive1 does not
        results2 = archive2 / "results"
        results2.mkdir(parents=True)
        (results2 / "cbt_config.yaml").touch()

        generator = SimpleReportGenerator(
            archive_directories=[str(archive1)],
            output_directory=f"{self.temp_dir}/output",
        )

        # Searching for archive1 should not return archive2's yaml
        found = generator._find_configuration_yaml_file(archive1)
        self.assertIsNone(found)

    def test_find_configuration_yaml_files_returns_at_most_one_per_archive(self) -> None:
        """Test that _find_configuration_yaml_files returns exactly one YAML per archive directory"""
        archive2 = Path(self.temp_dir) / "archive2"
        vis2 = archive2 / "read" / "visualisation"
        vis2.mkdir(parents=True)
        (vis2 / "4096_1_read.json").touch()

        results1 = self.archive_dir / "results"
        results1.mkdir(parents=True)
        config1 = results1 / "cbt_config.yaml"
        config1.touch()

        results2 = archive2 / "results"
        results2.mkdir(parents=True)
        config2 = results2 / "cbt_config.yaml"
        config2.touch()

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir), str(archive2)],
            output_directory=f"{self.temp_dir}/output",
        )

        found_files = generator._find_configuration_yaml_files()
        self.assertEqual(found_files, [config1, config2])

    def test_find_configuration_yaml_file_in_results_subdir_of_ancestor_directory(self) -> None:
        """Test finding cbt_config.yaml in results/ of an ancestor that is not itself named results"""
        run_dir = Path(self.temp_dir) / "run"
        config_file = run_dir / "results" / "cbt_config.yaml"
        config_file.parent.mkdir(parents=True)
        config_file.touch()

        nested_archive = run_dir / "sub" / "archive"
        nested_vis = nested_archive / "read" / "visualisation"
        nested_vis.mkdir(parents=True)
        (nested_vis / "4096_1_read.json").touch()

        generator = SimpleReportGenerator(
            archive_directories=[str(nested_archive)],
            output_directory=f"{self.temp_dir}/output",
        )

        found = generator._find_configuration_yaml_file(nested_archive)
        self.assertEqual(found, config_file)

    def test_find_configuration_yaml_file_fallback_searches_within_directory(self) -> None:
        """Test the fallback finds cbt_config.yaml in a non-standard location below the archive directory"""
        config_file = self.archive_dir / "a" / "b" / "cbt_config.yaml"
        config_file.parent.mkdir(parents=True)
        config_file.touch()

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=f"{self.temp_dir}/output",
        )

        found = generator._find_configuration_yaml_file(self.archive_dir)
        self.assertEqual(found, config_file)

    def test_find_configuration_yaml_file_fallback_selects_shallowest_file(self) -> None:
        """Test the fallback selects the cbt_config.yaml closest to the top of the archive directory"""
        deep_config = self.archive_dir / "a" / "b" / "c" / "cbt_config.yaml"
        shallow_config = self.archive_dir / "x" / "cbt_config.yaml"
        for config_file in (deep_config, shallow_config):
            config_file.parent.mkdir(parents=True)
            config_file.touch()

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=f"{self.temp_dir}/output",
        )

        found = generator._find_configuration_yaml_file(self.archive_dir)
        self.assertEqual(found, shallow_config)

    def test_find_configuration_yaml_file_returns_none_when_directory_does_not_exist(self) -> None:
        """Test that None is returned when the archive directory does not exist"""
        missing_dir = Path(self.temp_dir) / "does_not_exist"

        generator = SimpleReportGenerator(
            archive_directories=[str(self.archive_dir)],
            output_directory=f"{self.temp_dir}/output",
        )

        self.assertIsNone(generator._find_configuration_yaml_file(missing_dir))
