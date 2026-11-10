"""
Tests for the RunResult base class.

This module tests the abstract base class functionality and helper methods
that are inherited by concrete implementations like RBDFIO.
"""

# pylint: disable=protected-access,too-many-locals,too-few-public-methods

import json
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from post_processing.post_processing_types import InternalFormattedOutputType, IodepthDataType, IoDetailsType
from post_processing.run_results.benchmark_result import BenchmarkResult
from post_processing.run_results.rbdfio import RBDFIO


class TestRunResultInitialization:
    """Test RunResult initialization through concrete subclass."""

    def test_initialization_basic(self) -> None:
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

    def test_initialization_with_timeseries(self) -> None:
        """Test initialization with timeseries enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output", include_timeseries=True)

            assert result._include_timeseries is True


class TestProcessMethod:
    """Test the process() method."""

    def test_process_with_no_files(self) -> None:
        """Test process() when no files are found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            # No files created, so _files should be empty
            result.process()

            assert result._has_been_processed is True
            assert len(result._processed_data) == 0

    def test_process_with_files(self) -> None:
        """Test process() with valid files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data
            test_data: dict[str, Any] = {
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
            with open(test_file, "w", encoding="utf-8") as f:
                json.dump(test_data, f)

            result = RBDFIO(path, "json_output")
            result.process()

            assert result._has_been_processed is True
            assert len(result._processed_data) > 0


class TestGetMethods:
    """Test get() and get_timeseries() methods."""

    def test_get_without_processing(self) -> None:
        """Test get() automatically processes if not done."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            assert result._has_been_processed is False

            data = result.get()

            assert result._has_been_processed is True
            assert isinstance(data, dict)

    def test_get_after_processing(self) -> None:
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

    def test_process_empty_file(self) -> None:
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

    def test_process_precondition_file(self) -> None:
        """Test processing skips precondition files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data
            test_data: dict[str, Any] = {
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
            with open(precond_file, "w", encoding="utf-8") as f:
                json.dump(test_data, f)

            result = RBDFIO(path, "json_output")
            result.process()

            # Should complete without error
            assert result._has_been_processed is True


class TestExtractTestConfiguration:
    """Test _extract_test_configuration() method."""

    def test_extract_configuration(self) -> None:
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

    def test_merge_with_no_existing_data(self) -> None:
        """Test merge when no existing data exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            result = RBDFIO(path, "json_output")

            test_config = ("randread", "4096", "32", "1")
            new_io_details: IoDetailsType = {
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

    def test_merge_with_existing_data(self) -> None:
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

            existing_processed_data: InternalFormattedOutputType = {"randread": {"1": {"4096": {"32": existing_io}}}}
            result._processed_data = existing_processed_data

            new_io_details: IoDetailsType = {
                "io_bytes": "1000000000",
                "iops": "4000.0",
                "bandwidth_bytes": "16666666",
                "total_ios": "244140",
                "latency": "8.0",
                "std_deviation": "0.5",
            }

            merged = result._merge_io_details(test_config, new_io_details)

            # Should sum the values
            io_bytes_val = merged["io_bytes"]
            iops_val = merged["iops"]
            assert isinstance(io_bytes_val, str)
            assert isinstance(iops_val, str)
            assert float(io_bytes_val) == 2000000000.0
            assert float(iops_val) == 8000.0


class TestConvertFile:
    """Test _convert_file() method."""

    def test_convert_file_success(self) -> None:
        """Test successful file conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data
            test_data: dict[str, Any] = {
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
            with open(test_file, "w", encoding="utf-8") as f:
                json.dump(test_data, f)

            result = RBDFIO(path, "json_output")
            result._convert_file(test_file)

            # Should have processed data
            assert len(result._processed_data) > 0

    def test_convert_file_with_error(self) -> None:
        """Test file conversion with error handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create invalid JSON file
            test_file = path / "json_output.0"
            with open(test_file, "w", encoding="utf-8") as f:
                f.write("invalid json")

            result = RBDFIO(path, "json_output")

            # Invalid JSON is read as an empty result, which BenchmarkResult rejects
            with pytest.raises(ValueError, match="is empty"):
                result._convert_file(test_file)


class TestIntegration:
    """Integration tests for RunResult."""

    def test_full_workflow_single_volume(self) -> None:
        """Test complete workflow with single volume."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data
            test_data: dict[str, Any] = {
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
            with open(test_file, "w", encoding="utf-8") as f:
                json.dump(test_data, f)

            result = RBDFIO(path, "json_output")
            data = result.get()

            assert "randread" in data
            assert "1" in data["randread"]
            assert "4096" in data["randread"]["1"]
            assert "32" in data["randread"]["1"]["4096"]

    def test_full_workflow_multiple_volumes(self) -> None:
        """Test complete workflow with multiple volumes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            # Create test data for multiple volumes
            test_data: dict[str, Any] = {
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
                with open(test_file, "w", encoding="utf-8") as f:
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
            iops_raw = iodepth_data["iops"]
            assert isinstance(iops_raw, str)
            iops_value = float(iops_raw)
            # Should be sum of 3 volumes: 4000 * 3 = 12000
            assert iops_value == pytest.approx(12000.0, rel=0.01)


class TestMergeResourceData:
    """Test _merge_resource_data() aggregates CPU/memory correctly across volumes."""

    def _make_rbdfio(self, tmpdir: str) -> RBDFIO:
        return RBDFIO(Path(tmpdir), "json_output")

    def test_first_volume_returns_unchanged(self) -> None:
        """First volume has no existing data — resource data is returned as-is."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")
            resource_data: dict[str, dict[str, str]] = {"cpu": {"fio": "15.70"}, "memory": {"fio": "0.00"}}

            merged = result._merge_resource_data(test_config, resource_data)

            assert merged == resource_data

    def test_fio_cpu_is_summed_across_volumes(self) -> None:
        """FIO CPU values should be summed because each volume file is independent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")

            # Simulate first volume already stored
            stored_data: InternalFormattedOutputType = {
                "randread": {"1": {"4096": {"32": {"cpu": {"fio": "10.00"}, "memory": {"fio": "0.00"}}}}}
            }
            result._processed_data = stored_data
            result._resource_volume_counts[test_config] = 1

            resource_data: dict[str, dict[str, str]] = {"cpu": {"fio": "15.00"}, "memory": {"fio": "0.00"}}
            merged = result._merge_resource_data(test_config, resource_data)

            assert float(merged["cpu"]["fio"]) == pytest.approx(25.00)

    def test_shared_source_cpu_is_averaged_across_volumes(self) -> None:
        """collectl/top CPU should be averaged because they share one directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")

            # One volume already stored, count == 1
            stored_data: InternalFormattedOutputType = {
                "randread": {"1": {"4096": {"32": {"cpu": {"collectl": "40.00"}, "memory": {"collectl": "0.00"}}}}}
            }
            result._processed_data = stored_data
            result._resource_volume_counts[test_config] = 1
            result._resource_source_counts[(test_config, "collectl")] = 1

            resource_data: dict[str, dict[str, str]] = {"cpu": {"collectl": "60.00"}, "memory": {"collectl": "0.00"}}
            merged = result._merge_resource_data(test_config, resource_data)

            # (40 * 1 + 60) / 2 = 50
            assert float(merged["cpu"]["collectl"]) == pytest.approx(50.00)

    def test_shared_source_average_uses_volume_count(self) -> None:
        """Running average uses the stored volume count for a third+ volume."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")

            # Two volumes already processed, running average is 50, source_count == 2
            stored_data: InternalFormattedOutputType = {
                "randread": {"1": {"4096": {"32": {"cpu": {"top": "50.00"}, "memory": {"top": "0.00"}}}}}
            }
            result._processed_data = stored_data
            result._resource_volume_counts[test_config] = 2
            result._resource_source_counts[(test_config, "top")] = 2

            resource_data: dict[str, dict[str, str]] = {"cpu": {"top": "62.00"}, "memory": {"top": "0.00"}}
            merged = result._merge_resource_data(test_config, resource_data)

            # (50 * 2 + 62) / 3 = 162 / 3 = 54
            assert float(merged["cpu"]["top"]) == pytest.approx(54.00)

    def test_multiple_sources_merged_independently(self) -> None:
        """FIO is summed while collectl is averaged in the same merge call."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")

            stored_data: InternalFormattedOutputType = {
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
            result._processed_data = stored_data
            result._resource_volume_counts[test_config] = 1
            result._resource_source_counts[(test_config, "collectl")] = 1

            resource_data: dict[str, dict[str, str]] = {
                "cpu": {"fio": "15.00", "collectl": "60.00"},
                "memory": {"fio": "0.00", "collectl": "0.00"},
            }
            merged = result._merge_resource_data(test_config, resource_data)

            assert float(merged["cpu"]["fio"]) == pytest.approx(25.00)  # summed
            assert float(merged["cpu"]["collectl"]) == pytest.approx(50.00)  # averaged

    def test_merge_metric_dict_direct(self) -> None:
        """_merge_metric_dict merges single metric dictionary directly across sources."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")
            result._resource_source_counts[(test_config, "collectl")] = 1

            existing_metrics = {"fio": "10.00", "collectl": "40.00"}
            new_metrics = {"fio": "15.00", "collectl": "60.00"}
            merged = result._merge_metric_dict(test_config, existing_metrics, new_metrics, set())

            assert float(merged["fio"]) == pytest.approx(25.00)
            assert float(merged["collectl"]) == pytest.approx(50.00)

    def _merge_with_stored(
        self,
        result: RBDFIO,
        new_resource: dict[str, dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        """Merge new_resource into stored collectl cpu=40.00/memory=100.00 after one volume."""
        test_config = ("randread", "4096", "32", "1")
        stored_data: InternalFormattedOutputType = {
            "randread": {"1": {"4096": {"32": {"cpu": {"collectl": "40.00"}, "memory": {"collectl": "100.00"}}}}}
        }
        result._processed_data = stored_data
        result._resource_volume_counts[test_config] = 1
        result._resource_source_counts[(test_config, "collectl")] = 1
        return result._merge_resource_data(test_config, new_resource)

    def test_shared_source_zero_cpu_nonzero_memory_is_still_aggregated(self) -> None:
        """A shared source is only 'missing' if BOTH cpu and memory are zero."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            merged = self._merge_with_stored(
                result,
                {"cpu": {"collectl": "0.00"}, "memory": {"collectl": "200.00"}},
            )

            # Real (idle-cpu) volume is averaged in for both metrics
            assert float(merged["cpu"]["collectl"]) == pytest.approx(20.00)
            assert float(merged["memory"]["collectl"]) == pytest.approx(150.00)

    def test_shared_source_zero_memory_nonzero_cpu_is_still_aggregated(self) -> None:
        """A zero memory value alongside non-zero cpu must not be treated as a missing source."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            merged = self._merge_with_stored(
                result,
                {"cpu": {"collectl": "60.00"}, "memory": {"collectl": "0.00"}},
            )

            assert float(merged["cpu"]["collectl"]) == pytest.approx(50.00)
            assert float(merged["memory"]["collectl"]) == pytest.approx(50.00)

    def test_shared_source_both_zero_is_treated_as_missing(self) -> None:
        """Both cpu and memory zero means missing directory: accumulated values are unchanged."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            merged = self._merge_with_stored(
                result,
                {"cpu": {"collectl": "0.00"}, "memory": {"collectl": "0.00"}},
            )

            assert merged["cpu"]["collectl"] == "40.00"
            assert merged["memory"]["collectl"] == "100.00"

    def test_previously_seen_source_absent_from_volume_raises(self) -> None:
        """A shared source seen in earlier volumes but absent now indicates corrupted source data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._make_rbdfio(tmpdir)
            test_config = ("randread", "4096", "32", "1")
            stored_data: InternalFormattedOutputType = {
                "randread": {"1": {"4096": {"32": {"cpu": {"collectl": "40.00"}, "memory": {"collectl": "100.00"}}}}}
            }
            result._processed_data = stored_data
            result._resource_volume_counts[test_config] = 1
            result._resource_source_counts[(test_config, "collectl")] = 1

            with pytest.raises(ValueError, match="collectl"):
                result._merge_resource_data(test_config, {"cpu": {"fio": "15.00"}, "memory": {"fio": "0.00"}})


class TestResourceAggregationIntegration:
    """Integration tests confirming CPU is correctly aggregated across multiple volumes."""

    def _make_volume_data(self, sys_cpu: float = 5.0, usr_cpu: float = 10.0) -> dict[str, Any]:
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

    def test_fio_cpu_summed_across_three_volumes(self) -> None:
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
            assert isinstance(iodepth_entry, dict)
            cpu_entry = iodepth_entry["cpu"]
            assert isinstance(cpu_entry, dict)

            fio_cpu = float(cpu_entry["fio"])
            assert fio_cpu == pytest.approx(per_volume_cpu * 3, rel=0.01)

    def test_single_volume_fio_cpu_unchanged(self) -> None:
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
            assert isinstance(iodepth_entry, dict)
            cpu_entry = iodepth_entry["cpu"]
            assert isinstance(cpu_entry, dict)

            fio_cpu = float(cpu_entry["fio"])
            assert fio_cpu == pytest.approx(per_volume_cpu, rel=0.01)

    def test_volume_counter_not_stored_in_processed_data(self) -> None:
        """The internal volume counter must not appear anywhere in the output data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)

            for i in range(2):
                (path / f"json_output.{i}").write_text(json.dumps(self._make_volume_data()))

            result = RBDFIO(path, "json_output")
            data = result.get()
            iodepth_entry = data["randread"]["1"]["4096"]["32"]
            assert isinstance(iodepth_entry, dict)

            # Counter lives in _resource_volume_counts, not inside _processed_data
            assert "_resource_volume_count" not in iodepth_entry
            raw_cpu = iodepth_entry.get("cpu", {})
            if isinstance(raw_cpu, dict):
                assert "_resource_volume_count" not in raw_cpu

    def test_multi_volume_multi_resource_end_to_end(self) -> None:
        """
        Behavioural test: with 3 volume files sharing collectl and top directories,
        FIO CPU is summed, collectl CPU is averaged, and top CPU/memory are averaged.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            sys_cpu, usr_cpu = 5.5, 10.2
            cpu_count = 4
            per_volume_fio_cpu = (sys_cpu + usr_cpu) / cpu_count

            # Create 3 volume files
            for i in range(3):
                (path / f"json_output.{i}").write_text(
                    json.dumps(self._make_volume_data(sys_cpu=sys_cpu, usr_cpu=usr_cpu))
                )

            # Create collectl directory with a CPU file (50% CPU)
            collectl_dir = path / "collectl"
            collectl_dir.mkdir()
            (collectl_dir / "client1-20260619.cpu").write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;50\n")

            # Create top directory with an OSD output file
            # 2 snapshots: 25% + 35% -> average 30.0 core-units / 4 cores = 7.50%
            # memory: (0.6 + 0.8) / 2 = 0.70%
            top_dir = path / "top"
            top_dir.mkdir()
            top_content = (
                "top - 14:32:01 up 2 days\n"
                "Tasks: 1 total\n\n"
                "  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND\n"
                "12345 ceph      20   0  1.234g 234.0m  12.0m S  25.0   0.6   1:23.45 crimson-osd\n\n"
                "top - 14:32:02 up 2 days\n"
                "Tasks: 1 total\n\n"
                "  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND\n"
                "12345 ceph      20   0  1.234g 234.0m  12.0m S  35.0   0.8   1:24.50 crimson-osd\n"
            )
            (top_dir / "osd_0.out").write_text(top_content)

            with patch("os.cpu_count", return_value=cpu_count):
                result = RBDFIO(path, "json_output")
                data = result.get()

            iodepth_entry = data["randread"]["1"]["4096"]["32"]
            assert isinstance(iodepth_entry, dict)
            cpu_data = iodepth_entry["cpu"]
            memory_data = iodepth_entry["memory"]
            assert isinstance(cpu_data, dict)
            assert isinstance(memory_data, dict)

            # FIO CPU is summed across 3 volumes
            assert float(cpu_data["fio"]) == pytest.approx(per_volume_fio_cpu * 3, rel=0.01)
            # Collectl CPU is averaged across 3 volume passes (remains 50.00)
            assert float(cpu_data["collectl"]) == pytest.approx(50.00, rel=0.01)
            # Top CPU is averaged across 3 volume passes (remains 7.50)
            assert float(cpu_data["top"]) == pytest.approx(7.50, rel=0.01)
            # Top memory is averaged across 3 volume passes (remains 0.70)
            assert float(memory_data["top"]) == pytest.approx(0.70, rel=0.01)

    def test_shared_source_zero_as_missing_behaviour(self) -> None:
        """
        Behavioural test: a volume that produces 0.00 for a shared monitoring source
        does not reduce or dilute the accumulated average.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            test_config = ("randread", "4096", "32", "1")

            result = RBDFIO(path, "json_output")

            # First volume has valid collectl data (60.00)
            first_resource: dict[str, dict[str, str]] = {
                "cpu": {"fio": "5.00", "collectl": "60.00"},
                "memory": {"fio": "0.00", "collectl": "0.00"},
            }
            merged1 = result._merge_resource_data(test_config, first_resource)
            result._resource_volume_counts[test_config] = 1
            result._resource_source_counts[(test_config, "collectl")] = 1
            stored_iodepth: IodepthDataType = {
                "cpu": merged1["cpu"],
                "memory": merged1["memory"],
            }
            stored_data: InternalFormattedOutputType = {"randread": {"1": {"4096": {"32": stored_iodepth}}}}
            result._processed_data = stored_data

            # Second volume has missing/zero collectl data
            second_resource: dict[str, dict[str, str]] = {
                "cpu": {"fio": "5.00", "collectl": "0.00"},
                "memory": {"fio": "0.00", "collectl": "0.00"},
            }
            merged2 = result._merge_resource_data(test_config, second_resource)

            # FIO CPU is summed (5 + 5 = 10)
            assert float(merged2["cpu"]["fio"]) == pytest.approx(10.00)
            # Collectl CPU is NOT halved to 30.00; it remains 60.00
            assert float(merged2["cpu"]["collectl"]) == pytest.approx(60.00)


# Made with Bob
