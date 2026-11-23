"""
Tests for the RunResult base class.

This module tests the abstract base class functionality and helper methods
that are inherited by concrete implementations like RBDFIO.
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional
from unittest.mock import Mock, patch

import pytest

from post_processing.post_processing_types import InternalFormattedOutputType
from post_processing.run_results.rbdfio import RBDFIO
from post_processing.run_results.resource_result import ResourceResult

GET_ALL_RESOURCES = "post_processing.run_results.run_result.get_all_resources"


@pytest.fixture(name="results_dir")
def results_dir_fixture(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    An empty results directory.

    The directory name is fixed on purpose: ``tmp_path`` is named after the test, and a test whose name
    contains "precond" would put every file it creates under a path that ``file_is_precondition`` matches.
    """
    return tmp_path_factory.mktemp("results")


def _fio_output(
    iops: float = 4000.0, sys_cpu: float = 5.5, usr_cpu: float = 10.2, jobname: Optional[str] = None
) -> dict[str, Any]:
    """Build the contents of a single-job FIO json output file for a 4096 byte randread run at iodepth 32."""
    job: dict[str, Any] = {
        "read": {
            "io_bytes": 1000000000,
            "bw_bytes": 16666666,
            "iops": iops,
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
    if jobname is not None:
        job["jobname"] = jobname

    return {
        "global options": {"bs": "4096", "rw": "randread", "iodepth": "32", "numjobs": "1", "runtime": "60"},
        "jobs": [job],
    }


def _write_fio_file(path: Path, **kwargs: Any) -> Path:
    """Write a FIO json output file (see _fio_output for the keyword arguments), creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_fio_output(**kwargs)), encoding="utf8")
    return path


def _iodepth_entry(data: InternalFormattedOutputType) -> dict[str, Any]:
    """Return the entry for randread / 1 job / 4096 / iodepth 32, failing if it is missing or not a dict."""
    entry = data["randread"]["1"]["4096"]["32"]
    assert isinstance(entry, dict), f"Expected a dict at randread/1/4096/32, got {entry!r}"
    return entry


def _resource(source: str, cpu: str, memory: str = "0.00") -> Mock:
    """A stand-in for a ResourceResult that reports fixed cpu and memory values."""
    resource = Mock(spec=ResourceResult)
    resource.source = source
    resource.get.return_value = {"cpu": cpu, "memory": memory}
    return resource


def _process_volumes_with_resources(results_dir: Path, resources_per_volume: list[list[Mock]]) -> dict[str, Any]:
    """
    Process one FIO file per entry of resources_per_volume and return the aggregated iodepth entry.

    Only the discovery of resource parsers is replaced: the Nth call to get_all_resources returns the Nth list.
    The FIO files themselves are real and are parsed by the real code.
    """
    for index in range(len(resources_per_volume)):
        _write_fio_file(results_dir / f"json_output.{index}")

    with patch(GET_ALL_RESOURCES, side_effect=resources_per_volume):
        return _iodepth_entry(RBDFIO(results_dir, "json_output").get())


class TestRunResultInitialization:
    """Test RunResult initialization through concrete subclass."""

    def test_construction_does_not_process_files(self, results_dir: Path) -> None:
        """Constructing a result must not read any file; processing is lazy."""
        _write_fio_file(results_dir / "json_output.0")

        with patch.object(RBDFIO, "process", autospec=True) as process:
            RBDFIO(results_dir, "json_output")

        process.assert_not_called()

    def test_new_result_has_not_been_processed(self, results_dir: Path) -> None:
        """A new result reports that it has not been processed."""
        result = RBDFIO(results_dir, "json_output")

        # There is no public accessor for the processed flag; this test pins its initial value.
        # pylint: disable-next=protected-access
        assert result._has_been_processed is False

    def test_timeseries_is_off_by_default(self, results_dir: Path) -> None:
        """Time-series output is opt-in."""
        result = RBDFIO(results_dir, "json_output")

        # There is no public accessor: the flag only changes what process() writes to disk.
        # pylint: disable-next=protected-access
        assert result._include_timeseries is False

    def test_timeseries_can_be_enabled(self, results_dir: Path) -> None:
        """include_timeseries=True is remembered."""
        result = RBDFIO(results_dir, "json_output", include_timeseries=True)

        # There is no public accessor: the flag only changes what process() writes to disk.
        # pylint: disable-next=protected-access
        assert result._include_timeseries is True


class TestProcessMethod:
    """Test the process() method."""

    def test_process_with_no_files(self, results_dir: Path) -> None:
        """Test process() when no files are found."""
        result = RBDFIO(results_dir, "json_output")

        result.process()

        assert not result.get(), "A directory with no result files must produce no data"

    def test_process_with_files(self, results_dir: Path) -> None:
        """Test process() with valid files."""
        _write_fio_file(results_dir / "json_output.0")

        result = RBDFIO(results_dir, "json_output")
        result.process()

        assert float(_iodepth_entry(result.get())["iops"]) == pytest.approx(4000.0)


class TestGetMethods:
    """Test get() method."""

    def test_get_processes_lazily_and_only_once(self, results_dir: Path) -> None:
        """get() processes the files on first use, and never again."""
        _write_fio_file(results_dir / "json_output.0")
        result = RBDFIO(results_dir, "json_output")

        with patch.object(RBDFIO, "process", autospec=True, side_effect=RBDFIO.process) as process:
            first = result.get()
            second = result.get()

        assert process.call_count == 1, "process() must run exactly once, on the first get()"
        assert first == second
        assert float(_iodepth_entry(first)["iops"]) == pytest.approx(4000.0)

    def test_get_after_explicit_process_does_not_process_again(self, results_dir: Path) -> None:
        """Calling process() explicitly means get() just returns the stored data."""
        _write_fio_file(results_dir / "json_output.0")
        result = RBDFIO(results_dir, "json_output")
        result.process()

        with patch.object(RBDFIO, "process", autospec=True) as process:
            data = result.get()

        process.assert_not_called()
        assert float(_iodepth_entry(data)["iops"]) == pytest.approx(4000.0)


class TestProcessTestRunFiles:
    """Test how files are selected for processing."""

    def test_empty_file_is_skipped_with_a_warning(self, results_dir: Path, caplog: pytest.LogCaptureFixture) -> None:
        """An empty result file produces no data and a warning that names the file."""
        empty_file = results_dir / "json_output.0"
        empty_file.touch()

        with caplog.at_level(logging.WARNING, logger="cbt.formatter"):
            data = RBDFIO(results_dir, "json_output").get()

        assert not data, "An empty file must produce no data"
        assert f"Cannot process file {empty_file} as it is empty" in caplog.text

    def test_only_precondition_file_produces_no_data(self, results_dir: Path) -> None:
        """A directory that holds nothing but a precondition run has nothing to report."""
        _write_fio_file(results_dir / "precond" / "json_output.0", jobname="precondition")

        assert not RBDFIO(results_dir, "json_output").get(), "A precondition file must produce no data"

    @pytest.mark.parametrize("precondition_position", [0, 1, 2], ids=["first", "middle", "last"])
    def test_precondition_file_is_skipped_wherever_it_is_listed(
        self, results_dir: Path, precondition_position: int
    ) -> None:
        """
        The precondition file is neither processed nor allowed to hide a real file.

        Removing entries from the list that is being iterated skips the entry that follows the removed
        one, so a real volume was silently dropped when a precondition file was listed before it.
        The order of the file list comes from the file system, so it is fixed here.
        """
        precondition = _write_fio_file(results_dir / "precond" / "json_output.0", iops=9000.0, jobname="precondition")
        volumes = [
            _write_fio_file(results_dir / "iodepth-32" / "json_output.0", iops=1000.0),
            _write_fio_file(results_dir / "iodepth-32" / "json_output.1", iops=2000.0),
        ]
        listed_files = list(volumes)
        listed_files.insert(precondition_position, precondition)

        with patch.object(RBDFIO, "_find_files_for_testrun", return_value=listed_files):
            result = RBDFIO(results_dir, "json_output")
        data = result.get()

        assert float(_iodepth_entry(data)["iops"]) == pytest.approx(
            3000.0
        ), "Both real volumes, and only those, must be summed"
        # No public accessor exposes which files were kept; this pins that a skipped file is dropped from the list.
        # pylint: disable-next=protected-access
        assert sorted(result._files) == volumes, "The precondition file must be removed from the files to process"


class TestProcessingFileContents:
    """Test the conversion of the contents of result files."""

    def test_single_volume_values_are_reported_unchanged(self, results_dir: Path) -> None:
        """With one volume there is nothing to merge: the values are those in the file."""
        _write_fio_file(results_dir / "json_output.0")

        entry = _iodepth_entry(RBDFIO(results_dir, "json_output").get())

        assert float(entry["io_bytes"]) == pytest.approx(1000000000.0)
        assert float(entry["iops"]) == pytest.approx(4000.0)

    def test_two_volumes_are_summed(self, results_dir: Path) -> None:
        """Volumes with the same test parameters have their io_bytes and iops added together."""
        _write_fio_file(results_dir / "json_output.0")
        _write_fio_file(results_dir / "json_output.1")

        entry = _iodepth_entry(RBDFIO(results_dir, "json_output").get())

        assert float(entry["io_bytes"]) == pytest.approx(2000000000.0)
        assert float(entry["iops"]) == pytest.approx(8000.0)

    def test_file_that_is_not_json_raises(self, results_dir: Path) -> None:
        """A file that is not valid JSON is read as empty, which the benchmark result rejects."""
        (results_dir / "json_output.0").write_text("invalid json", encoding="utf8")

        with pytest.raises(ValueError, match="is empty"):
            RBDFIO(results_dir, "json_output").get()

    def test_full_workflow_single_volume(self, results_dir: Path) -> None:
        """Test complete workflow with single volume."""
        _write_fio_file(results_dir / "json_output.0")

        data = RBDFIO(results_dir, "json_output").get()

        assert list(data) == ["randread"]
        assert list(data["randread"]) == ["1"]
        assert list(data["randread"]["1"]) == ["4096"]
        assert list(data["randread"]["1"]["4096"]) == ["32"]

    def test_full_workflow_multiple_volumes(self, results_dir: Path) -> None:
        """Test complete workflow with multiple volumes: 3 volumes of 4000 iops add up to 12000."""
        for index in range(3):
            _write_fio_file(results_dir / f"json_output.{index}")

        entry = _iodepth_entry(RBDFIO(results_dir, "json_output").get())

        assert float(entry["iops"]) == pytest.approx(12000.0, rel=0.01)


class TestMergeResourceData:
    """Test that CPU/memory are aggregated correctly across volumes."""

    def test_first_volume_is_reported_unchanged(self, results_dir: Path) -> None:
        """First volume has no existing data, so its resource data is reported as-is."""
        entry = _process_volumes_with_resources(results_dir, [[_resource("fio", "15.70")]])

        assert entry["cpu"] == {"fio": "15.70"}
        assert entry["memory"] == {"fio": "0.00"}

    def test_fio_cpu_is_summed_across_volumes(self, results_dir: Path) -> None:
        """FIO CPU values should be summed because each volume file is independent."""
        entry = _process_volumes_with_resources(results_dir, [[_resource("fio", "10.00")], [_resource("fio", "15.00")]])

        assert float(entry["cpu"]["fio"]) == pytest.approx(25.00)

    def test_shared_source_cpu_is_averaged_across_volumes(self, results_dir: Path) -> None:
        """collectl/top CPU should be averaged because they share one directory."""
        entry = _process_volumes_with_resources(
            results_dir, [[_resource("collectl", "40.00")], [_resource("collectl", "60.00")]]
        )

        # (40 * 1 + 60) / 2 = 50
        assert float(entry["cpu"]["collectl"]) == pytest.approx(50.00)

    def test_shared_source_average_uses_volume_count(self, results_dir: Path) -> None:
        """Running average uses the number of volumes seen so far for a third+ volume."""
        entry = _process_volumes_with_resources(
            results_dir,
            [[_resource("top", "40.00")], [_resource("top", "60.00")], [_resource("top", "62.00")]],
        )

        # Running average after two volumes is 50, so (50 * 2 + 62) / 3 = 54
        assert float(entry["cpu"]["top"]) == pytest.approx(54.00)

    def test_multiple_sources_merged_independently(self, results_dir: Path) -> None:
        """FIO is summed while collectl is averaged in the same merge."""
        entry = _process_volumes_with_resources(
            results_dir,
            [
                [_resource("fio", "10.00"), _resource("collectl", "40.00")],
                [_resource("fio", "15.00"), _resource("collectl", "60.00")],
            ],
        )

        assert float(entry["cpu"]["fio"]) == pytest.approx(25.00)  # summed
        assert float(entry["cpu"]["collectl"]) == pytest.approx(50.00)  # averaged

    def test_shared_source_zero_cpu_nonzero_memory_is_still_aggregated(self, results_dir: Path) -> None:
        """A shared source is only 'missing' if BOTH cpu and memory are zero."""
        entry = _process_volumes_with_resources(
            results_dir,
            [[_resource("collectl", "40.00", "100.00")], [_resource("collectl", "0.00", "200.00")]],
        )

        # The idle-cpu volume is averaged in for both metrics
        assert float(entry["cpu"]["collectl"]) == pytest.approx(20.00)
        assert float(entry["memory"]["collectl"]) == pytest.approx(150.00)

    def test_shared_source_zero_memory_nonzero_cpu_is_still_aggregated(self, results_dir: Path) -> None:
        """A zero memory value alongside non-zero cpu must not be treated as a missing source."""
        entry = _process_volumes_with_resources(
            results_dir,
            [[_resource("collectl", "40.00", "100.00")], [_resource("collectl", "60.00", "0.00")]],
        )

        assert float(entry["cpu"]["collectl"]) == pytest.approx(50.00)
        assert float(entry["memory"]["collectl"]) == pytest.approx(50.00)

    def test_shared_source_both_zero_is_treated_as_missing(self, results_dir: Path) -> None:
        """
        Both cpu and memory zero means a missing directory: the accumulated values are unchanged,
        while the independent FIO values are still summed.
        """
        entry = _process_volumes_with_resources(
            results_dir,
            [
                [_resource("fio", "5.00"), _resource("collectl", "40.00", "100.00")],
                [_resource("fio", "5.00"), _resource("collectl", "0.00", "0.00")],
            ],
        )

        assert entry["cpu"]["collectl"] == "40.00"
        assert entry["memory"]["collectl"] == "100.00"
        assert float(entry["cpu"]["fio"]) == pytest.approx(10.00)

    def test_previously_seen_source_absent_from_volume_raises(self, results_dir: Path) -> None:
        """A shared source seen in earlier volumes but absent now indicates corrupted source data."""
        with pytest.raises(ValueError, match="collectl"):
            _process_volumes_with_resources(
                results_dir,
                [[_resource("collectl", "40.00", "100.00")], [_resource("fio", "15.00")]],
            )

    @pytest.mark.parametrize("volumes", [1, 3])
    def test_fio_cpu_from_real_files_is_normalised_then_summed(self, results_dir: Path, volumes: int) -> None:
        """FIO CPU from identical volumes should be the per-volume normalised value times the volume count."""
        sys_cpu, usr_cpu, cpu_count = 5.5, 10.2, 4
        per_volume_cpu = (sys_cpu + usr_cpu) / cpu_count

        for index in range(volumes):
            _write_fio_file(results_dir / f"json_output.{index}", sys_cpu=sys_cpu, usr_cpu=usr_cpu)

        with patch("post_processing.run_results.resources.fio_resource.os.cpu_count", return_value=cpu_count):
            entry = _iodepth_entry(RBDFIO(results_dir, "json_output").get())

        assert float(entry["cpu"]["fio"]) == pytest.approx(per_volume_cpu * volumes, rel=0.01)

    def test_multi_volume_multi_resource_end_to_end(self, results_dir: Path) -> None:
        """
        With 3 volume files sharing collectl and top directories, FIO CPU is summed,
        collectl CPU is averaged, and top CPU and memory are averaged.
        """
        sys_cpu, usr_cpu, cpu_count = 5.5, 10.2, 4
        per_volume_fio_cpu = (sys_cpu + usr_cpu) / cpu_count

        for index in range(3):
            _write_fio_file(results_dir / f"json_output.{index}", sys_cpu=sys_cpu, usr_cpu=usr_cpu)

        # A collectl directory with a CPU file (50% CPU)
        collectl_dir = results_dir / "collectl"
        collectl_dir.mkdir()
        (collectl_dir / "client1-20260619.cpu").write_text(
            "#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;50\n", encoding="utf8"
        )

        # A top directory with an OSD output file
        # 2 snapshots: 25% + 35% -> average 30.0 core-units / 4 cores = 7.50%
        # memory: (0.6 + 0.8) / 2 = 0.70%
        top_dir = results_dir / "top"
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
        (top_dir / "osd_0.out").write_text(top_content, encoding="utf8")

        with patch("os.cpu_count", return_value=cpu_count):
            entry = _iodepth_entry(RBDFIO(results_dir, "json_output").get())

        # FIO CPU is summed across 3 volumes
        assert float(entry["cpu"]["fio"]) == pytest.approx(per_volume_fio_cpu * 3, rel=0.01)
        # Collectl CPU is averaged across 3 volume passes, so it remains 50.00
        assert float(entry["cpu"]["collectl"]) == pytest.approx(50.00, rel=0.01)
        # Top CPU is averaged across 3 volume passes, so it remains 7.50
        assert float(entry["cpu"]["top"]) == pytest.approx(7.50, rel=0.01)
        # Top memory is averaged across 3 volume passes, so it remains 0.70
        assert float(entry["memory"]["top"]) == pytest.approx(0.70, rel=0.01)
