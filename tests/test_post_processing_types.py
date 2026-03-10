"""
Unit tests for the post_processing/post_processing_types.py module
"""

import unittest

from post_processing.post_processing_types import (
    CommonFormatDataType,
    CPUPlotType,
    HandlerType,
    InternalBlocksizeDataType,
    InternalFormattedOutputType,
    IodepthDataType,
    JobsDataType,
    PlotDataType,
    ReportOptions,
)


class TestCPUPlotType(unittest.TestCase):
    """Test cases for CPUPlotType enum"""

    def test_cpu_plot_type_values(self) -> None:
        """Test that CPUPlotType enum has expected values"""
        self.assertEqual(CPUPlotType.NOCPU.value, 0)
        self.assertEqual(CPUPlotType.OVERALL.value, 1)
        self.assertEqual(CPUPlotType.OSD.value, 2)
        self.assertEqual(CPUPlotType.FIO.value, 3)
        self.assertEqual(CPUPlotType.NODES.value, 4)

    def test_cpu_plot_type_members(self) -> None:
        """Test that CPUPlotType has all expected members"""
        members = [member.name for member in CPUPlotType]
        self.assertIn("NOCPU", members)
        self.assertIn("OVERALL", members)
        self.assertIn("OSD", members)
        self.assertIn("FIO", members)
        self.assertIn("NODES", members)
        self.assertEqual(len(members), 5)

    def test_cpu_plot_type_comparison(self) -> None:
        """Test CPUPlotType enum comparison"""
        self.assertEqual(CPUPlotType.NOCPU, CPUPlotType.NOCPU)
        self.assertNotEqual(CPUPlotType.NOCPU, CPUPlotType.OVERALL)

    def test_cpu_plot_type_from_value(self) -> None:
        """Test creating CPUPlotType from value"""
        self.assertEqual(CPUPlotType(0), CPUPlotType.NOCPU)
        self.assertEqual(CPUPlotType(1), CPUPlotType.OVERALL)
        self.assertEqual(CPUPlotType(2), CPUPlotType.OSD)

    def test_cpu_plot_type_from_name(self) -> None:
        """Test accessing CPUPlotType by name"""
        self.assertEqual(CPUPlotType["NOCPU"], CPUPlotType.NOCPU)
        self.assertEqual(CPUPlotType["OVERALL"], CPUPlotType.OVERALL)


class TestReportOptions(unittest.TestCase):
    """Test cases for ReportOptions NamedTuple"""

    def test_report_options_creation(self) -> None:
        """Test creating ReportOptions instance"""
        options = ReportOptions(
            archives=["archive1", "archive2"],
            output_directory="/output",
            results_file_root="results",
            create_pdf=True,
            force_refresh=False,
            no_error_bars=True,
            comparison=False,
            plot_resources=True,
        )

        self.assertEqual(options.archives, ["archive1", "archive2"])
        self.assertEqual(options.output_directory, "/output")
        self.assertEqual(options.results_file_root, "results")
        self.assertTrue(options.create_pdf)
        self.assertFalse(options.force_refresh)
        self.assertTrue(options.no_error_bars)
        self.assertFalse(options.comparison)
        self.assertTrue(options.plot_resources)

    def test_report_options_immutable(self) -> None:
        """Test that ReportOptions is immutable"""
        options = ReportOptions(
            archives=["archive1"],
            output_directory="/output",
            results_file_root="results",
            create_pdf=True,
            force_refresh=False,
            no_error_bars=False,
            comparison=False,
            plot_resources=False,
        )

        with self.assertRaises(AttributeError):
            options.archives = ["new_archive"]  # type: ignore

    def test_report_options_access_by_index(self) -> None:
        """Test accessing ReportOptions fields by index"""
        options = ReportOptions(
            archives=["archive1"],
            output_directory="/output",
            results_file_root="results",
            create_pdf=True,
            force_refresh=False,
            no_error_bars=False,
            comparison=False,
            plot_resources=False,
        )

        self.assertEqual(options[0], ["archive1"])
        self.assertEqual(options[1], "/output")
        self.assertEqual(options[2], "results")
        self.assertTrue(options[3])

    def test_report_options_unpack(self) -> None:
        """Test unpacking ReportOptions"""
        options = ReportOptions(
            archives=["archive1"],
            output_directory="/output",
            results_file_root="results",
            create_pdf=True,
            force_refresh=False,
            no_error_bars=False,
            comparison=False,
            plot_resources=False,
        )

        (archives, output_dir, results_root, create_pdf, force_refresh, no_error_bars, comparison, plot_resources) = (
            options
        )

        self.assertEqual(archives, ["archive1"])
        self.assertEqual(output_dir, "/output")
        self.assertEqual(results_root, "results")
        self.assertTrue(create_pdf)

    def test_report_options_as_dict(self) -> None:
        """Test converting ReportOptions to dict"""
        options = ReportOptions(
            archives=["archive1"],
            output_directory="/output",
            results_file_root="results",
            create_pdf=True,
            force_refresh=False,
            no_error_bars=False,
            comparison=False,
            plot_resources=False,
        )

        options_dict = options._asdict()

        self.assertEqual(options_dict["archives"], ["archive1"])
        self.assertEqual(options_dict["output_directory"], "/output")
        self.assertEqual(options_dict["results_file_root"], "results")
        self.assertTrue(options_dict["create_pdf"])

    def test_report_options_replace(self) -> None:
        """Test replacing fields in ReportOptions"""
        options = ReportOptions(
            archives=["archive1"],
            output_directory="/output",
            results_file_root="results",
            create_pdf=True,
            force_refresh=False,
            no_error_bars=False,
            comparison=False,
            plot_resources=False,
        )

        new_options = options._replace(create_pdf=False, force_refresh=True)

        self.assertFalse(new_options.create_pdf)
        self.assertTrue(new_options.force_refresh)
        # Original should be unchanged
        self.assertTrue(options.create_pdf)
        self.assertFalse(options.force_refresh)


class TestTypeAliases(unittest.TestCase):
    """Test cases for type aliases"""

    def test_handler_type_structure(self) -> None:
        """Test HandlerType type alias structure"""
        handler: HandlerType = {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
            }
        }

        self.assertIsInstance(handler, dict)
        self.assertIsInstance(handler["console"], dict)
        self.assertIsInstance(handler["console"]["class"], str)

    def test_iodepth_data_type_structure(self) -> None:
        """Test IodepthDataType type alias structure"""
        iodepth_data: IodepthDataType = {
            "1": "100",
            "2": "200",
            "4": "400",
        }

        self.assertIsInstance(iodepth_data, dict)
        for key, value in iodepth_data.items():
            self.assertIsInstance(key, str)
            self.assertIsInstance(value, str)

    def test_common_format_data_type_structure(self) -> None:
        """Test CommonFormatDataType type alias structure"""
        common_data: CommonFormatDataType = {
            "1": {"bandwidth": "1000", "iops": "100"},
            "metadata": "test",
        }

        self.assertIsInstance(common_data, dict)

    def test_internal_blocksize_data_type_structure(self) -> None:
        """Test InternalBlocksizeDataType type alias structure"""
        blocksize_data: InternalBlocksizeDataType = {
            "4096": {
                "1": {"bandwidth": "1000"},
                "metadata": "test",
            }
        }

        self.assertIsInstance(blocksize_data, dict)

    def test_internal_formatted_output_type_structure(self) -> None:
        """Test InternalFormattedOutputType type alias structure"""
        formatted_output: InternalFormattedOutputType = {
            "read": {
                "4096": {
                    "1": {"bandwidth": "1000"},
                }
            }
        }

        self.assertIsInstance(formatted_output, dict)

    def test_plot_data_type_structure(self) -> None:
        """Test PlotDataType type alias structure"""
        plot_data: PlotDataType = {
            "series1": {
                "x": "1,2,3",
                "y": "10,20,30",
            }
        }

        self.assertIsInstance(plot_data, dict)
        self.assertIsInstance(plot_data["series1"], dict)

    def test_jobs_data_type_structure(self) -> None:
        """Test JobsDataType type alias structure"""
        jobs_data: JobsDataType = [
            {
                "jobname": "test_job",
                "read": {
                    "iops": 100,
                    "bw": 1000,
                    "lat_ns": {
                        "mean": 5000,
                    },
                },
            }
        ]

        self.assertIsInstance(jobs_data, list)
        self.assertIsInstance(jobs_data[0], dict)


class TestTypeAliasUsage(unittest.TestCase):
    """Test practical usage of type aliases"""

    def test_nested_common_format_data(self) -> None:
        """Test nested structure of CommonFormatDataType"""
        data: CommonFormatDataType = {
            "1": {"bandwidth_bytes": "1000000", "iops": "100"},
            "2": {"bandwidth_bytes": "2000000", "iops": "200"},
            "maximum_iops": "200",
            "maximum_bandwidth": "2000000",
        }

        # Should be able to access nested data
        iodepth_1 = data["1"]
        self.assertIsInstance(iodepth_1, dict)

        # Should be able to access string values
        max_iops = data["maximum_iops"]
        self.assertIsInstance(max_iops, str)

    def test_handler_type_multiple_handlers(self) -> None:
        """Test HandlerType with multiple handlers"""
        handlers: HandlerType = {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "console",
            },
            "file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": "/tmp/test.log",
            },
        }

        self.assertEqual(len(handlers), 2)
        self.assertIn("console", handlers)
        self.assertIn("file", handlers)


# Made with Bob
