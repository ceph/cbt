"""
Tests for the RunResult base class.

This module tests the abstract base class functionality and helper methods
that are inherited by concrete implementations like RBDFIO.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from post_processing.post_processing_types import IodepthDataType
from post_processing.run_results.benchmark_result import BenchmarkResult
from post_processing.run_results.rbdfio import RBDFIO


class TestRunResultInitialization:
    """Test RunResult initialization through concrete subclass."""

    def test_initialization_basic(self):
        """Test basic initialization of RunResult."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            assert result._path == path
            assert result._has_been_processed is False
            assert result._include_timeseries is False
            assert isinstance(result._files, list)
            assert isinstance(result._processed_data, dict)
            assert isinstance(result._timeseries_data, dict)

    def test_initialization_with_timeseries(self):
        """Test initialization with timeseries enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output", include_timeseries=True)

            assert result._include_timeseries is True


class TestProcessMethod:
    """Test the process() method."""

    def test_process_with_no_files(self):
        """Test process() when no files are found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            # No files created, so _files should be empty
            result.process()

            assert result._has_been_processed is True
            assert len(result._processed_data) == 0

    def test_process_with_files(self):
        """Test process() with valid files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data
            test_data = {
                "global options": {
                    "bs": "4096",
                    "rw": "randread",
                    "iodepth": "32",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000000000,
                            "bw_bytes": 16666666,
                            "iops": 4000.0,
                            "total_ios": 244140,
                            "clat_ns": {"mean": 8000000.0, "stddev": 500000.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                        "sys_cpu": 5.5,
                        "usr_cpu": 10.2,
                    }
                ],
            }

            # Create test file
            test_file = path / "json_output.0"
            with open(test_file, "w") as f:
                json.dump(test_data, f)

            result = RBDFIO(path, "json_output")
            result.process()

            assert result._has_been_processed is True
            assert len(result._processed_data) > 0


class TestGetMethods:
    """Test get() and get_timeseries() methods."""

    def test_get_without_processing(self):
        """Test get() automatically processes if not done."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            assert result._has_been_processed is False

            data = result.get()

            assert result._has_been_processed is True
            assert isinstance(data, dict)

    def test_get_after_processing(self):
        """Test get() returns data after processing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            result.process()
            data = result.get()

            assert isinstance(data, dict)

    # Tests for get_timeseries() removed - with memory-efficient approach,
    # timeseries data is written immediately during process() and not stored in memory


class TestProcessTestRunFiles:
    """Test _process_test_run_files() method."""

    def test_process_empty_file(self):
        """Test processing skips empty files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create empty file
            empty_file = path / "json_output.0"
            empty_file.touch()

            result = RBDFIO(path, "json_output")
            result.process()

            # Should complete without error, but no data processed
            assert result._has_been_processed is True

    def test_process_precondition_file(self):
        """Test processing skips precondition files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data
            test_data = {
                "global options": {
                    "bs": "4096",
                    "rw": "randread",
                    "iodepth": "32",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "jobname": "precondition",
                        "read": {
                            "io_bytes": 1000000000,
                            "bw_bytes": 16666666,
                            "iops": 4000.0,
                            "total_ios": 244140,
                            "clat_ns": {"mean": 8000000.0, "stddev": 500000.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                        "sys_cpu": 5.5,
                        "usr_cpu": 10.2,
                    }
                ],
            }

            # Create precondition file
            precond_file = path / "json_output.0"
            with open(precond_file, "w") as f:
                json.dump(test_data, f)

            result = RBDFIO(path, "json_output")
            result.process()

            # Should complete without error
            assert result._has_been_processed is True


class TestExtractTestConfiguration:
    """Test _extract_test_configuration() method."""

    def test_extract_configuration(self):
        """Test extracting configuration from benchmark result."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            # Create mock benchmark result
            mock_benchmark = Mock(spec=BenchmarkResult)
            mock_benchmark.operation = "randread"
            mock_benchmark.blocksize = "4096"
            mock_benchmark.iodepth = "32"
            mock_benchmark.number_of_jobs = "1"

            config = result._extract_test_configuration(mock_benchmark)

            assert config == ("randread", "4096", "32", "1")


class TestMergeIODetails:
    """Test _merge_io_details() method."""

    def test_merge_with_no_existing_data(self):
        """Test merge when no existing data exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            test_config = ("randread", "4096", "32", "1")
            new_io_details: IodepthDataType = {
                "io_bytes": "1000000000",
                "iops": "4000.0",
                "bandwidth_bytes": "16666666",
                "total_ios": "244140",
                "latency": "8.0",
                "std_deviation": "0.5",
            }

            merged = result._merge_io_details(test_config, new_io_details)

            # Should return new_io_details unchanged
            assert merged == new_io_details

    def test_merge_with_existing_data(self):
        """Test merge when existing data exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            # Set up existing data
            test_config = ("randread", "4096", "32", "1")
            existing_io: IodepthDataType = {
                "io_bytes": "1000000000",
                "iops": "4000.0",
                "bandwidth_bytes": "16666666",
                "total_ios": "244140",
                "latency": "8.0",
                "std_deviation": "0.5",
            }

            result._processed_data = {"randread": {"1": {"4096": {"32": existing_io}}}}

            new_io_details: IodepthDataType = {
                "io_bytes": "1000000000",
                "iops": "4000.0",
                "bandwidth_bytes": "16666666",
                "total_ios": "244140",
                "latency": "8.0",
                "std_deviation": "0.5",
            }

            merged = result._merge_io_details(test_config, new_io_details)

            # Should sum the values
            assert float(merged["io_bytes"]) == 2000000000.0
            assert float(merged["iops"]) == 8000.0


class TestConvertFile:
    """Test _convert_file() method."""

    def test_convert_file_success(self):
        """Test successful file conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data
            test_data = {
                "global options": {
                    "bs": "4096",
                    "rw": "randread",
                    "iodepth": "32",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000000000,
                            "bw_bytes": 16666666,
                            "iops": 4000.0,
                            "total_ios": 244140,
                            "clat_ns": {"mean": 8000000.0, "stddev": 500000.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                        "sys_cpu": 5.5,
                        "usr_cpu": 10.2,
                    }
                ],
            }

            test_file = path / "json_output.0"
            with open(test_file, "w") as f:
                json.dump(test_data, f)

            result = RBDFIO(path, "json_output")
            result._convert_file(test_file)

            # Should have processed data
            assert len(result._processed_data) > 0

    def test_convert_file_with_error(self):
        """Test file conversion with error handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create invalid JSON file
            test_file = path / "json_output.0"
            with open(test_file, "w") as f:
                f.write("invalid json")

            result = RBDFIO(path, "json_output")

            # Should raise an exception
            with pytest.raises(Exception):
                result._convert_file(test_file)


class TestIntegration:
    """Integration tests for RunResult."""

    def test_full_workflow_single_volume(self):
        """Test complete workflow with single volume."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data
            test_data = {
                "global options": {
                    "bs": "4096",
                    "rw": "randread",
                    "iodepth": "32",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000000000,
                            "bw_bytes": 16666666,
                            "iops": 4000.0,
                            "total_ios": 244140,
                            "clat_ns": {"mean": 8000000.0, "stddev": 500000.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                        "sys_cpu": 5.5,
                        "usr_cpu": 10.2,
                    }
                ],
            }

            test_file = path / "json_output.0"
            with open(test_file, "w") as f:
                json.dump(test_data, f)

            result = RBDFIO(path, "json_output")
            data = result.get()

            assert "randread" in data
            assert "1" in data["randread"]
            assert "4096" in data["randread"]["1"]
            assert "32" in data["randread"]["1"]["4096"]

    def test_full_workflow_multiple_volumes(self):
        """Test complete workflow with multiple volumes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data for multiple volumes
            test_data = {
                "global options": {
                    "bs": "4096",
                    "rw": "randread",
                    "iodepth": "32",
                    "numjobs": "1",
                    "runtime": "60",
                },
                "jobs": [
                    {
                        "read": {
                            "io_bytes": 1000000000,
                            "bw_bytes": 16666666,
                            "iops": 4000.0,
                            "total_ios": 244140,
                            "clat_ns": {"mean": 8000000.0, "stddev": 500000.0},
                        },
                        "write": {
                            "io_bytes": 0,
                            "bw_bytes": 0,
                            "iops": 0.0,
                            "total_ios": 0,
                            "clat_ns": {"mean": 0.0, "stddev": 0.0},
                        },
                        "sys_cpu": 5.5,
                        "usr_cpu": 10.2,
                    }
                ],
            }

            # Create multiple volume files
            for i in range(3):
                test_file = path / f"json_output.{i}"
                with open(test_file, "w") as f:
                    json.dump(test_data, f)

            result = RBDFIO(path, "json_output")
            data = result.get()

            # Data should be aggregated from all volumes
            assert "randread" in data
            # Navigate through nested structure
            randread_data = data["randread"]
            assert isinstance(randread_data, dict)
            numjobs_data = randread_data["1"]
            assert isinstance(numjobs_data, dict)
            blocksize_data = numjobs_data["4096"]
            assert isinstance(blocksize_data, dict)
            iodepth_data = blocksize_data["32"]
            assert isinstance(iodepth_data, dict)
            iops_value = float(iodepth_data["iops"])
            # Should be sum of 3 volumes: 4000 * 3 = 12000
            assert iops_value == pytest.approx(12000.0, rel=0.01)


class TestMergeResourceData:
    """Test _merge_resource_data() aggregates CPU/memory correctly across volumes."""

    def _make_rbdfio(self, tmpdir: str) -> RBDFIO:
        return RBDFIO(Path(tmpdir), "json_output")

    def test_first_volume_returns_unchanged(self):
        """First volume has no existing data — resource data is returned as-is."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")
            resource_data = {"cpu": {"fio": "15.70"}, "memory": {"fio": "0.00"}}

            merged = result._merge_resource_data(test_config, resource_data)

            assert merged == resource_data

    def test_fio_cpu_is_summed_across_volumes(self):
        """FIO CPU values should be summed because each volume file is independent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")

            # Simulate first volume already stored
            result._processed_data = {
                "randread": {"1": {"4096": {"32": {"cpu": {"fio": "10.00"}, "memory": {"fio": "0.00"}}}}}
            }
            result._resource_volume_counts[test_config] = 1

            resource_data = {"cpu": {"fio": "15.00"}, "memory": {"fio": "0.00"}}
            merged = result._merge_resource_data(test_config, resource_data)

            assert float(merged["cpu"]["fio"]) == pytest.approx(25.00)

    def test_shared_source_cpu_is_averaged_across_volumes(self):
        """collectl/top CPU should be averaged because they share one directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")

            # One volume already stored, count == 1
            result._processed_data = {
                "randread": {"1": {"4096": {"32": {"cpu": {"collectl": "40.00"}, "memory": {"collectl": "0.00"}}}}}
            }
            result._resource_volume_counts[test_config] = 1
            result._resource_source_counts[(test_config, "collectl")] = 1

            resource_data = {"cpu": {"collectl": "60.00"}, "memory": {"collectl": "0.00"}}
            merged = result._merge_resource_data(test_config, resource_data)

            # (40 * 1 + 60) / 2 = 50
            assert float(merged["cpu"]["collectl"]) == pytest.approx(50.00)

    def test_shared_source_average_uses_volume_count(self):
        """Running average uses the stored volume count for a third+ volume."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")

            # Two volumes already processed, running average is 50, source_count == 2
            result._processed_data = {
                "randread": {"1": {"4096": {"32": {"cpu": {"top": "50.00"}, "memory": {"top": "0.00"}}}}}
            }
            result._resource_volume_counts[test_config] = 2
            result._resource_source_counts[(test_config, "top")] = 2

            resource_data = {"cpu": {"top": "62.00"}, "memory": {"top": "0.00"}}
            merged = result._merge_resource_data(test_config, resource_data)

            # (50 * 2 + 62) / 3 = 162 / 3 = 54
            assert float(merged["cpu"]["top"]) == pytest.approx(54.00)

    def test_multiple_sources_merged_independently(self):
        """FIO is summed while collectl is averaged in the same merge call."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")

            result._processed_data = {
                "randread": {
                    "1": {
                        "4096": {
                            "32": {
                                "cpu": {"fio": "10.00", "collectl": "40.00"},
                                "memory": {"fio": "0.00", "collectl": "0.00"},
                            }
                        }
                    }
                }
            }
            result._resource_volume_counts[test_config] = 1
            result._resource_source_counts[(test_config, "collectl")] = 1

            resource_data = {
                "cpu": {"fio": "15.00", "collectl": "60.00"},
                "memory": {"fio": "0.00", "collectl": "0.00"},
            }
            merged = result._merge_resource_data(test_config, resource_data)

            assert float(merged["cpu"]["fio"]) == pytest.approx(25.00)  # summed
            assert float(merged["cpu"]["collectl"]) == pytest.approx(50.00)  # averaged


class TestResourceAggregationIntegration:
    """Integration tests confirming CPU is correctly aggregated across multiple volumes."""

    def _make_volume_data(self, sys_cpu: float = 5.0, usr_cpu: float = 10.0) -> dict:
        return {
            "global options": {
                "bs": "4096",
                "rw": "randread",
                "iodepth": "32",
                "numjobs": "1",
                "runtime": "60",
            },
            "jobs": [
                {
                    "read": {
                        "io_bytes": 1000000000,
                        "bw_bytes": 16666666,
                        "iops": 4000.0,
                        "total_ios": 244140,
                        "clat_ns": {"mean": 8000000.0, "stddev": 500000.0},
                    },
                    "write": {
                        "io_bytes": 0,
                        "bw_bytes": 0,
                        "iops": 0.0,
                        "total_ios": 0,
                        "clat_ns": {"mean": 0.0, "stddev": 0.0},
                    },
                    "sys_cpu": sys_cpu,
                    "usr_cpu": usr_cpu,
                }
            ],
        }

    def test_fio_cpu_summed_across_three_volumes(self):
        """FIO CPU from three identical volumes should appear as 3x the per-volume normalised value."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            sys_cpu, usr_cpu = 5.5, 10.2
            cpu_count = 4
            per_volume_cpu = (sys_cpu + usr_cpu) / cpu_count  # normalised per-volume value

            for i in range(3):
                (path / f"json_output.{i}").write_text(
                    json.dumps(self._make_volume_data(sys_cpu=sys_cpu, usr_cpu=usr_cpu))
                )

            with patch("post_processing.run_results.resources.fio_resource.os.cpu_count", return_value=cpu_count):
                result = RBDFIO(path, "json_output")
                data = result.get()
            iodepth_entry = data["randread"]["1"]["4096"]["32"]

            fio_cpu = float(iodepth_entry["cpu"]["fio"])
            assert fio_cpu == pytest.approx(per_volume_cpu * 3, rel=0.01)

    def test_single_volume_fio_cpu_unchanged(self):
        """Single-volume result should have exactly the per-volume normalised FIO CPU value."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            sys_cpu, usr_cpu = 5.5, 10.2
            cpu_count = 4
            per_volume_cpu = (sys_cpu + usr_cpu) / cpu_count

            (path / "json_output.0").write_text(json.dumps(self._make_volume_data(sys_cpu=sys_cpu, usr_cpu=usr_cpu)))

            with patch("post_processing.run_results.resources.fio_resource.os.cpu_count", return_value=cpu_count):
                result = RBDFIO(path, "json_output")
                data = result.get()
            iodepth_entry = data["randread"]["1"]["4096"]["32"]

            fio_cpu = float(iodepth_entry["cpu"]["fio"])
            assert fio_cpu == pytest.approx(per_volume_cpu, rel=0.01)

    def test_volume_counter_not_stored_in_processed_data(self):
        """The internal volume counter must not appear anywhere in the output data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            for i in range(2):
                (path / f"json_output.{i}").write_text(json.dumps(self._make_volume_data()))

            result = RBDFIO(path, "json_output")
            data = result.get()
            iodepth_entry = data["randread"]["1"]["4096"]["32"]

            # Counter lives in _resource_volume_counts, not inside _processed_data
            assert "_resource_volume_count" not in iodepth_entry
            assert "_resource_volume_count" not in iodepth_entry.get("cpu", {})


# Made with Bob
