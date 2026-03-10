"""
Unit tests for the post_processing/report.py module
"""

# pyright: strict, reportPrivateUsage=false
#
# We are OK to ignore private use in unit tests as the whole point of the tests
# is to validate the functions contained in the module

import shutil
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

from post_processing.post_processing_types import ReportOptions
from post_processing.report import Report, parse_namespace_to_options


class TestParseNamespaceToOptions(unittest.TestCase):
    """Test cases for parse_namespace_to_options function"""

    def test_parse_namespace_simple_report(self) -> None:
        """Test parsing namespace for simple (non-comparison) report"""
        args = Namespace(
            archive="/path/to/archive",
            output_directory="/output",
            results_file_root="results",
            create_pdf=True,
            force_refresh=False,
        )

        options = parse_namespace_to_options(args, comparison_report=False)

        self.assertEqual(options.archives, ["/path/to/archive"])
        self.assertEqual(options.output_directory, "/output")
        self.assertEqual(options.results_file_root, "results")
        self.assertTrue(options.create_pdf)
        self.assertFalse(options.force_refresh)
        self.assertFalse(options.no_error_bars)
        self.assertFalse(options.comparison)
        self.assertFalse(options.plot_resources)

    def test_parse_namespace_comparison_report(self) -> None:
        """Test parsing namespace for comparison report"""
        args = Namespace(
            baseline="/path/to/baseline",
            archives="/path/to/archive1,/path/to/archive2",
            output_directory="/output",
            results_file_root="results",
            create_pdf=False,
            force_refresh=True,
        )

        options = parse_namespace_to_options(args, comparison_report=True)

        self.assertEqual(len(options.archives), 3)
        self.assertEqual(options.archives[0], "/path/to/baseline")
        self.assertEqual(options.archives[1], "/path/to/archive1")
        self.assertEqual(options.archives[2], "/path/to/archive2")
        self.assertTrue(options.comparison)
        self.assertTrue(options.force_refresh)

    def test_parse_namespace_with_no_error_bars(self) -> None:
        """Test parsing namespace with no_error_bars attribute"""
        args = Namespace(
            archive="/path/to/archive",
            output_directory="/output",
            results_file_root="results",
            create_pdf=False,
            force_refresh=False,
            no_error_bars=True,
        )

        options = parse_namespace_to_options(args, comparison_report=False)

        self.assertTrue(options.no_error_bars)

    def test_parse_namespace_with_plot_resources(self) -> None:
        """Test parsing namespace with plot_resources attribute"""
        args = Namespace(
            archive="/path/to/archive",
            output_directory="/output",
            results_file_root="results",
            create_pdf=False,
            force_refresh=False,
            plot_resources=True,
        )

        options = parse_namespace_to_options(args, comparison_report=False)

        self.assertTrue(options.plot_resources)

    def test_parse_namespace_without_optional_attributes(self) -> None:
        """Test parsing namespace without optional attributes"""
        args = Namespace(
            archive="/path/to/archive",
            output_directory="/output",
            results_file_root="results",
            create_pdf=False,
            force_refresh=False,
        )

        options = parse_namespace_to_options(args, comparison_report=False)

        # Should default to False when attributes don't exist
        self.assertFalse(options.no_error_bars)
        self.assertFalse(options.plot_resources)


class TestReport(unittest.TestCase):
    """Test cases for Report class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.options = ReportOptions(
            archives=[self.temp_dir],
            output_directory=f"{self.temp_dir}/output",
            results_file_root="test_results",
            create_pdf=False,
            force_refresh=False,
            no_error_bars=False,
            comparison=False,
            plot_resources=False,
        )

    def tearDown(self) -> None:
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_report_initialization(self) -> None:
        """Test Report class initialization"""
        report = Report(self.options)

        self.assertEqual(report._options, self.options)
        self.assertEqual(report.result_code, 0)

    def test_result_code_property(self) -> None:
        """Test result_code property"""
        report = Report(self.options)

        self.assertEqual(report.result_code, 0)

        # Simulate error by setting internal result code
        report._result_code = 1
        self.assertEqual(report.result_code, 1)

    @patch("post_processing.report.os.makedirs")
    @patch("post_processing.report.CommonOutputFormatter")
    @patch("post_processing.report.SimpleReportGenerator")
    def test_generate_simple_report_success(
        self,
        mock_simple_generator: MagicMock,
        mock_formatter: MagicMock,
        mock_makedirs: MagicMock,
    ) -> None:
        """Test successful generation of simple report"""
        # Setup mocks
        mock_formatter_instance = MagicMock()
        mock_formatter.return_value = mock_formatter_instance

        mock_generator_instance = MagicMock()
        mock_simple_generator.return_value = mock_generator_instance

        # Create report with existing visualisation directory
        vis_dir = Path(self.temp_dir) / "visualisation"
        vis_dir.mkdir()
        (vis_dir / "dummy.json").touch()

        report = Report(self.options)
        report.generate()

        # Should create output directory
        mock_makedirs.assert_called()

        # Should not create formatter since visualisation directory exists and not force_refresh
        mock_formatter.assert_not_called()

        # Should create report generator
        mock_simple_generator.assert_called_once()
        mock_generator_instance.create_report.assert_called_once()

        # Should not create PDF
        mock_generator_instance.save_as_pdf.assert_not_called()

        # Result code should be 0
        self.assertEqual(report.result_code, 0)

    @patch("post_processing.report.os.makedirs")
    @patch("post_processing.report.os.path.exists")
    @patch("post_processing.report.os.listdir")
    @patch("post_processing.report.CommonOutputFormatter")
    @patch("post_processing.report.SimpleReportGenerator")
    def test_generate_with_intermediate_file_creation(
        self,
        mock_simple_generator: MagicMock,
        mock_formatter: MagicMock,
        mock_listdir: MagicMock,
        mock_exists: MagicMock,
        mock_makedirs: MagicMock,
    ) -> None:
        """Test report generation when intermediate files need to be created"""
        # Setup mocks - visualisation directory doesn't exist
        mock_exists.return_value = False
        mock_listdir.return_value = []

        mock_formatter_instance = MagicMock()
        mock_formatter.return_value = mock_formatter_instance

        mock_generator_instance = MagicMock()
        mock_simple_generator.return_value = mock_generator_instance

        report = Report(self.options)
        report.generate()

        # Should create formatter and convert files
        mock_formatter.assert_called_once()
        mock_formatter_instance.convert_all_files.assert_called_once()
        mock_formatter_instance.write_output_file.assert_called_once()

    @patch("post_processing.report.os.makedirs")
    @patch("post_processing.report.CommonOutputFormatter")
    @patch("post_processing.report.ComparisonReportGenerator")
    def test_generate_comparison_report(
        self,
        mock_comparison_generator: MagicMock,
        mock_formatter: MagicMock,
        mock_makedirs: MagicMock,
    ) -> None:
        """Test generation of comparison report"""
        comparison_options = ReportOptions(
            archives=[self.temp_dir, f"{self.temp_dir}/archive2"],
            output_directory=f"{self.temp_dir}/output",
            results_file_root="test_results",
            create_pdf=False,
            force_refresh=False,
            no_error_bars=False,
            comparison=True,
            plot_resources=False,
        )

        # Create visualisation directories
        for archive in comparison_options.archives:
            vis_dir = Path(archive) / "visualisation"
            vis_dir.mkdir(parents=True, exist_ok=True)
            (vis_dir / "dummy.json").touch()

        mock_generator_instance = MagicMock()
        mock_comparison_generator.return_value = mock_generator_instance

        report = Report(comparison_options)
        report.generate()

        # Should create comparison report generator
        mock_comparison_generator.assert_called_once()
        mock_generator_instance.create_report.assert_called_once()

    @patch("post_processing.report.os.makedirs")
    @patch("post_processing.report.CommonOutputFormatter")
    @patch("post_processing.report.SimpleReportGenerator")
    def test_generate_with_pdf_creation(
        self,
        mock_simple_generator: MagicMock,
        mock_formatter: MagicMock,
        mock_makedirs: MagicMock,
    ) -> None:
        """Test report generation with PDF creation"""
        pdf_options = ReportOptions(
            archives=[self.temp_dir],
            output_directory=f"{self.temp_dir}/output",
            results_file_root="test_results",
            create_pdf=True,
            force_refresh=False,
            no_error_bars=False,
            comparison=False,
            plot_resources=False,
        )

        # Create visualisation directory
        vis_dir = Path(self.temp_dir) / "visualisation"
        vis_dir.mkdir()
        (vis_dir / "dummy.json").touch()

        mock_generator_instance = MagicMock()
        mock_simple_generator.return_value = mock_generator_instance

        report = Report(pdf_options)
        report.generate()

        # Should create PDF
        mock_generator_instance.save_as_pdf.assert_called_once()

    @patch("post_processing.report.os.makedirs")
    @patch("post_processing.report.CommonOutputFormatter")
    @patch("post_processing.report.SimpleReportGenerator")
    def test_generate_with_exception_no_throw(
        self,
        mock_simple_generator: MagicMock,
        mock_formatter: MagicMock,
        mock_makedirs: MagicMock,
    ) -> None:
        """Test report generation when exception occurs and throw_exception=False"""
        # Create visualisation directory
        vis_dir = Path(self.temp_dir) / "visualisation"
        vis_dir.mkdir()
        (vis_dir / "dummy.json").touch()

        # Make report generator raise exception
        mock_generator_instance = MagicMock()
        mock_generator_instance.create_report.side_effect = Exception("Test error")
        mock_simple_generator.return_value = mock_generator_instance

        report = Report(self.options)
        report.generate(throw_exception=False)

        # Should set result code to 1
        self.assertEqual(report.result_code, 1)

    @patch("post_processing.report.os.makedirs")
    @patch("post_processing.report.CommonOutputFormatter")
    @patch("post_processing.report.SimpleReportGenerator")
    def test_generate_with_exception_throw(
        self,
        mock_simple_generator: MagicMock,
        mock_formatter: MagicMock,
        mock_makedirs: MagicMock,
    ) -> None:
        """Test report generation when exception occurs and throw_exception=True"""
        # Create visualisation directory
        vis_dir = Path(self.temp_dir) / "visualisation"
        vis_dir.mkdir()
        (vis_dir / "dummy.json").touch()

        # Make report generator raise exception
        mock_generator_instance = MagicMock()
        test_exception = Exception("Test error")
        mock_generator_instance.create_report.side_effect = test_exception
        mock_simple_generator.return_value = mock_generator_instance

        report = Report(self.options)

        # Should re-raise exception
        with self.assertRaises(Exception) as context:
            report.generate(throw_exception=True)

        self.assertEqual(str(context.exception), "Test error")
        self.assertEqual(report.result_code, 1)

    @patch("post_processing.report.os.makedirs")
    @patch("post_processing.report.os.path.exists")
    @patch("post_processing.report.os.listdir")
    @patch("post_processing.report.CommonOutputFormatter")
    @patch("post_processing.report.SimpleReportGenerator")
    def test_generate_with_force_refresh(
        self,
        mock_simple_generator: MagicMock,
        mock_formatter: MagicMock,
        mock_listdir: MagicMock,
        mock_exists: MagicMock,
        mock_makedirs: MagicMock,
    ) -> None:
        """Test report generation with force_refresh=True"""
        refresh_options = ReportOptions(
            archives=[self.temp_dir],
            output_directory=f"{self.temp_dir}/output",
            results_file_root="test_results",
            create_pdf=False,
            force_refresh=True,
            no_error_bars=False,
            comparison=False,
            plot_resources=False,
        )

        # Visualisation directory exists with files
        mock_exists.return_value = True
        mock_listdir.return_value = ["existing_file.json"]

        mock_formatter_instance = MagicMock()
        mock_formatter.return_value = mock_formatter_instance

        mock_generator_instance = MagicMock()
        mock_simple_generator.return_value = mock_generator_instance

        report = Report(refresh_options)
        report.generate()

        # Should still create formatter and regenerate files
        mock_formatter.assert_called_once()
        mock_formatter_instance.convert_all_files.assert_called_once()
        mock_formatter_instance.write_output_file.assert_called_once()

    @patch("post_processing.report.os.makedirs")
    @patch("post_processing.report.os.path.exists")
    @patch("post_processing.report.os.listdir")
    @patch("post_processing.report.CommonOutputFormatter")
    def test_generate_intermediate_files_exception(
        self,
        mock_formatter: MagicMock,
        mock_listdir: MagicMock,
        mock_exists: MagicMock,
        mock_makedirs: MagicMock,
    ) -> None:
        """Test exception handling during intermediate file generation"""
        mock_exists.return_value = False

        # Make formatter raise exception
        mock_formatter_instance = MagicMock()
        mock_formatter_instance.convert_all_files.side_effect = Exception("Conversion error")
        mock_formatter.return_value = mock_formatter_instance

        report = Report(self.options)

        # Should catch and re-raise exception
        with self.assertRaises(Exception):
            report.generate(throw_exception=True)


# Made with Bob
