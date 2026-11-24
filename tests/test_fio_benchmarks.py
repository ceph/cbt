"""
Comprehensive tests for post_processing/run_results/benchmarks/fio.py

Tests cover error handling, edge cases, and time-series data processing.
"""

import json
import logging
import tempfile
from pathlib import Path
from typing import Any, Union
from unittest.mock import Mock, patch

import pytest

from post_processing.post_processing_types import TimeSeriesFormatType
from post_processing.run_results.benchmarks.fio import FIO


class TestFIOExtractMetrics:
    """Test metric extraction helper methods."""

    def test_extract_int_metric_success(self) -> None:
        """Test successful integer metric extraction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            job_data: dict[str, Union[int, float, dict[str, Union[int, float]]]] = {"io_bytes": 1000, "total_ios": 50}

            result = fio._extract_int_metric(job_data, "io_bytes", "read")
            assert result == 1000

    def test_extract_int_metric_missing(self) -> None:
        """Test integer metric extraction with missing key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            job_data: dict[str, Union[int, float, dict[str, Union[int, float]]]] = {"io_bytes": 1000}

            with pytest.raises(ValueError, match="Missing or invalid 'total_ios'"):
                fio._extract_int_metric(job_data, "total_ios", "read")

    def test_extract_int_metric_wrong_type(self) -> None:
        """Test integer metric extraction with wrong type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            job_data: dict[str, Union[int, float, dict[str, Union[int, float]]]] = {"io_bytes": "not_an_int"}  # type: ignore[dict-item]

            with pytest.raises(ValueError, match="expected int, got str"):
                fio._extract_int_metric(job_data, "io_bytes", "read")

    def test_extract_float_metric_success(self) -> None:
        """Test successful float metric extraction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.5,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            job_data: dict[str, Union[int, float, dict[str, Union[int, float]]]] = {"iops": 10.5}

            result = fio._extract_float_metric(job_data, "iops", "read")
            assert result == 10.5

    def test_extract_float_metric_from_int(self) -> None:
        """Test float metric extraction accepts integers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            job_data: dict[str, Union[int, float, dict[str, Union[int, float]]]] = {"iops": 10}

            result = fio._extract_float_metric(job_data, "iops", "read")
            assert result == 10.0

    def test_extract_float_metric_wrong_type(self) -> None:
        """Test float metric extraction with wrong type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            job_data: dict[str, Union[int, float, dict[str, Union[int, float]]]] = {"iops": "not_a_float"}  # type: ignore[dict-item]

            with pytest.raises(ValueError, match="expected float, got str"):
                fio._extract_float_metric(job_data, "iops", "read")


class TestFIOGetIODetails:
    """Test _get_io_details method with various scenarios."""

    def test_get_io_details_with_invalid_job_data(self) -> None:
        """Test IO details extraction with invalid job data structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": "not_a_dict",  # Invalid: should be dict
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            # Should handle invalid data gracefully and return zero values
            io_details = fio.io_details
            assert io_details["total_ios"] == "0"

    def test_get_io_details_missing_clat_ns(self) -> None:
        """Test IO details extraction with missing clat_ns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            # Missing clat_ns
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            with pytest.raises(ValueError, match="Invalid job data structure"):
                FIO(test_file)

    def test_get_io_details_zero_total_ios(self) -> None:
        """Test IO details when total_ios is zero."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            io_details = fio.io_details

            assert io_details["total_ios"] == "0"
            assert io_details["iops"] == "0.0"
            assert io_details["latency"] == "0.0"

    def test_get_io_details_with_extra_job_keys(self) -> None:
        """Test IO details extraction ignores non-read/write keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "jobname": "test_job",  # Should be ignored
                        "sys_cpu": 5.5,  # Should be ignored
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            io_details = fio.io_details

            # Should process successfully, ignoring extra keys
            assert io_details["total_ios"] == "50"


# Three samples 100 ms apart, as fio writes them when log_avg_msec is 100
_SAMPLES_100MS_APART: tuple[int, ...] = (150, 250, 350)


def _write_fio_output(directory: Path, **global_options: str) -> Path:
    """
    Write a single job FIO json output file called output.0 and return its path.

    The global options are those of a 32k sequential read at iodepth 4, plus any given as keywords.
    """
    output = {
        "global options": {
            "bs": "32768",
            "rw": "read",
            "iodepth": "4",
            "numjobs": "1",
            "runtime": "90",
            **global_options,
        },
        "jobs": [
            {
                "read": {
                    "io_bytes": 1000,
                    "bw_bytes": 100,
                    "iops": 10.0,
                    "total_ios": 50,
                    "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                },
                "write": {
                    "io_bytes": 0,
                    "bw_bytes": 0,
                    "iops": 0.0,
                    "total_ios": 0,
                    "clat_ns": {"mean": 0.0, "stddev": 0.0},
                },
            }
        ],
    }
    output_file = directory / "output.0"
    output_file.write_text(json.dumps(output), encoding="utf8")
    return output_file


def _write_timeseries_logs(directory: Path, timestamps_ms: tuple[int, ...]) -> None:
    """Write iops, bw and clat logs for output.0 with one sample at each of the given times."""
    for metric, value in (("iops", 1500), ("bw", 100), ("clat", 5000000)):
        lines = [f"{timestamp}, {value}, 0, 32768, 0" for timestamp in timestamps_ms]
        (directory / f"output.0_{metric}.1.log").write_text("\n".join(lines) + "\n", encoding="utf8")


def _get_timeseries(directory: Path, timestamps_ms: tuple[int, ...], **global_options: str) -> TimeSeriesFormatType:
    """Build an FIO result from output.0 and its logs and return the time series it produces."""
    output_file = _write_fio_output(directory, **global_options)
    _write_timeseries_logs(directory, timestamps_ms)

    timeseries = FIO(output_file).get_timeseries_data()

    assert timeseries is not None, "Expected time series data for a directory with logs"
    return timeseries


class TestFIOGetLogAvgMsec:
    """
    Test that the log averaging interval fio was run with sets the time series window.

    Samples written by fio with log_avg_msec=100 are already averages over 100 ms, so they must not
    be summed into larger windows.
    """

    def test_log_avg_msec_is_kept_in_global_options(self, tmp_path: Path) -> None:
        """log_avg_msec is part of the global options fio reports, so it is available from the result."""
        fio = FIO(_write_fio_output(tmp_path, log_avg_msec="100"))

        assert fio.global_options["log_avg_msec"] == "100"

    def test_window_is_the_log_avg_msec_that_fio_ran_with(self, tmp_path: Path) -> None:
        """With log_avg_msec 100 each sample is its own window, rather than all three being summed."""
        timeseries = _get_timeseries(tmp_path, _SAMPLES_100MS_APART, log_avg_msec="100")

        assert timeseries["metadata"]["log_avg_msec"] == 100
        assert len(timeseries["timeseries"]) == 3
        assert [point["iops"] for point in timeseries["timeseries"]] == [1500, 1500, 1500]

    def test_window_defaults_to_one_second_when_log_avg_msec_was_not_set(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Without log_avg_msec fio logs every I/O; the default window sums them, and that is not an error."""
        with caplog.at_level(logging.WARNING, logger="cbt.formatter"):
            timeseries = _get_timeseries(tmp_path, _SAMPLES_100MS_APART)

        assert timeseries["metadata"]["log_avg_msec"] == 1000
        assert len(timeseries["timeseries"]) == 1
        assert timeseries["timeseries"][0]["iops"] == 4500
        assert "log_avg_msec" not in caplog.text, "A missing log_avg_msec must not produce a warning"

    @pytest.mark.parametrize(
        "value, warning",
        [
            ("-100", "Invalid log_avg_msec value -100 (must be positive), using default 1000"),
            ("0", "Invalid log_avg_msec value 0 (must be positive), using default 1000"),
            ("invalid", "Cannot convert log_avg_msec 'invalid' to int"),
        ],
        ids=["negative", "zero", "not_a_number"],
    )
    def test_invalid_log_avg_msec_falls_back_to_the_default_and_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture, value: str, warning: str
    ) -> None:
        """A log_avg_msec that is not a positive integer is replaced by 1000, with a warning saying so."""
        with caplog.at_level(logging.WARNING, logger="cbt.formatter"):
            timeseries = _get_timeseries(tmp_path, _SAMPLES_100MS_APART, log_avg_msec=value)

        assert timeseries["metadata"]["log_avg_msec"] == 1000
        assert warning in caplog.text


class TestFIOGetTimeseriesData:
    """Test get_timeseries_data method."""

    def test_get_timeseries_data_no_logs(self) -> None:
        """Test timeseries data when no log files exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            result = fio.get_timeseries_data()

            # Should return None when no logs found
            assert result is None

    def test_get_timeseries_data_directory_not_exists(self) -> None:
        """Test timeseries data when directory doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            fio = FIO(test_file)
            # Modify the path to non-existent directory
            fio._resource_file_path = Path("/nonexistent/path/test.json")

            result = fio.get_timeseries_data()

            # Should return None gracefully
            assert result is None

    @pytest.mark.parametrize(
        "test_file_name",
        [
            "output.0",
            "json_output.0",
        ],
    )
    @patch("post_processing.run_results.benchmarks.fio.FIOLogParser")
    @patch("post_processing.run_results.benchmarks.fio.FIOTimeSeriesParser")
    def test_get_timeseries_data_with_parameterized_file_patterns(
        self, mock_parser_ts_class: Any, mock_parser_class: Any, test_file_name: str
    ) -> None:
        """Test timeseries data processing for supported result file naming patterns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            test_file = log_dir / test_file_name

            # Create mock log files with standard naming: output.X_*.log
            (log_dir / "output.0_iops.1.log").touch()
            (log_dir / "output.0_clat.1.log").touch()
            (log_dir / "output.0_bw.1.log").touch()

            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randread",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            # Setup mocks
            mock_parser = Mock()
            mock_parser.parse_and_combine_logs.return_value = Mock()  # Return non-None DataFrame
            mock_parser_class.return_value = mock_parser

            mock_timeseries_parser = Mock()
            mock_formatted_output: Any = {"test": "data"}
            mock_timeseries_parser.get_formatted_output.return_value = mock_formatted_output
            mock_parser_ts_class.return_value = mock_timeseries_parser

            fio = FIO(test_file)
            result = fio.get_timeseries_data()

            # Should process and return the formatted output
            assert result == mock_formatted_output
            mock_timeseries_parser.process.assert_called_once()
            parser_call_patterns = [call.args[2] for call in mock_parser.parse_and_combine_logs.call_args_list]
            assert "output.0_iops.*.log" in parser_call_patterns
            assert "output.0_clat.*.log" in parser_call_patterns
            assert "output.0_bw.*.log" in parser_call_patterns
            assert all("output.1_" not in pattern for pattern in parser_call_patterns)

            if test_file_name.startswith("json_"):
                assert all("json_output" not in pattern for pattern in parser_call_patterns)

    @patch("post_processing.run_results.benchmarks.fio.TimestampAligner")
    @patch("post_processing.run_results.benchmarks.fio.FIOLogParser")
    @patch("post_processing.run_results.benchmarks.fio.FIOTimeSeriesParser")
    def test_get_timeseries_data_with_percentile_calculation(
        self, mock_parser_ts_class: Any, mock_parser_class: Any, mock_aligner_class: Any
    ) -> None:
        """Test that percentiles are calculated from clat data."""
        import pandas as pd

        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            test_file = log_dir / "output.0"

            # Create mock log files
            (log_dir / "output.0_iops.1.log").touch()
            (log_dir / "output.0_clat.1.log").touch()
            (log_dir / "output.0_bw.1.log").touch()

            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randwrite",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                    "log_avg_msec": "1000",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                        "write": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            # Setup mock parser to return clat data
            mock_parser = Mock()
            mock_clat_df = pd.DataFrame(
                {"timestamp_sec": [1.0, 2.0, 3.0], "latency_ms": [10.0, 15.0, 20.0], "direction": [1, 1, 1]}
            )
            mock_parser.parse_and_combine_logs.side_effect = lambda dir, metric, pattern: (
                mock_clat_df if metric == "clat" else None
            )
            mock_parser_class.return_value = mock_parser

            # Setup mock aligner to return aligned data and percentile data
            mock_aligner = Mock()

            # Mock aligned data (returned by align_and_aggregate)
            mock_aligned_df = pd.DataFrame({"timestamp_sec": [1.0, 2.0, 3.0], "latency_ms": [10.0, 15.0, 20.0]})
            mock_aligner.align_and_aggregate.return_value = mock_aligned_df

            # Mock percentile data (returned by calculate_percentiles)
            mock_percentiles_df = pd.DataFrame(
                {
                    "timestamp_sec": [1.0, 2.0, 3.0],
                    "p50_latency_ms": [12.0, 16.0, 21.0],
                    "p95_latency_ms": [18.0, 22.0, 28.0],
                    "p99_latency_ms": [19.0, 23.0, 29.0],
                }
            )
            mock_aligner.calculate_percentiles.return_value = mock_percentiles_df
            mock_aligner_class.return_value = mock_aligner

            # Setup mock timeseries parser
            mock_timeseries_parser = Mock()
            mock_formatted_output: Any = {"test": "data"}
            mock_timeseries_parser.get_formatted_output.return_value = mock_formatted_output
            mock_parser_ts_class.return_value = mock_timeseries_parser

            fio = FIO(test_file)
            result = fio.get_timeseries_data()

            # Verify TimestampAligner was created with correct window size
            mock_aligner_class.assert_called_once_with(window_size_ms=1000)

            # Verify calculate_percentiles was called with clat data
            mock_aligner.calculate_percentiles.assert_called_once()
            call_args = mock_aligner.calculate_percentiles.call_args
            assert len(call_args[0][0]) == 1  # One dataframe in list
            pd.testing.assert_frame_equal(call_args[0][0][0], mock_clat_df)
            assert call_args[1]["percentiles"] == [50, 95, 99]

            # Verify FIOTimeSeriesParser was called with percentile dataframes
            parser_init_call = mock_parser_ts_class.call_args
            assert parser_init_call[1]["p50_latency_df"] is not None
            assert parser_init_call[1]["p95_latency_df"] is not None
            assert parser_init_call[1]["p99_latency_df"] is not None

            # Verify the percentile dataframes have correct structure
            p50_df = parser_init_call[1]["p50_latency_df"]
            assert "timestamp_sec" in p50_df.columns
            assert "latency_ms" in p50_df.columns

            assert result == mock_formatted_output

    @patch("post_processing.run_results.benchmarks.fio.FIOLogParser")
    @patch("post_processing.run_results.benchmarks.fio.FIOTimeSeriesParser")
    def test_get_timeseries_data_no_clat_data_for_percentiles(
        self, mock_parser_ts_class: Any, mock_parser_class: Any
    ) -> None:
        """Test that percentiles are None when no clat data available."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            test_file = log_dir / "output.0"

            # Create mock log files (but no clat)
            (log_dir / "output.0_iops.1.log").touch()
            (log_dir / "output.0_bw.1.log").touch()

            test_data = {
                "global options": {
                    "bs": "4K",
                    "rw": "randwrite",
                    "iodepth": "4",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                        "write": {
                            "io_bytes": 1000,
                            "bw_bytes": 100,
                            "iops": 10.0,
                            "total_ios": 50,
                            "clat_ns": {"mean": 5000.0, "stddev": 500.0},
                        },
                    }
                ],
            }

            with open(test_file, "w") as f:
                json.dump(test_data, f)

            # Setup mock parser to return None for clat
            mock_parser = Mock()
            mock_parser.parse_and_combine_logs.side_effect = lambda dir, metric, pattern: (
                None if metric == "clat" else Mock()
            )
            mock_parser_class.return_value = mock_parser

            # Setup mock timeseries parser
            mock_timeseries_parser = Mock()
            mock_formatted_output: Any = {"test": "data"}
            mock_timeseries_parser.get_formatted_output.return_value = mock_formatted_output
            mock_parser_ts_class.return_value = mock_timeseries_parser

            fio = FIO(test_file)
            result = fio.get_timeseries_data()

            # Verify FIOTimeSeriesParser was called with None percentile dataframes
            parser_init_call = mock_parser_ts_class.call_args
            assert parser_init_call[1]["p50_latency_df"] is None
            assert parser_init_call[1]["p95_latency_df"] is None
            assert parser_init_call[1]["p99_latency_df"] is None

            assert result == mock_formatted_output


# Made with Bob
