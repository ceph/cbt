"""
Unit tests for resource_result_factory module
"""

# pyright: strict, reportPrivateUsage=false

import json
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from post_processing.run_results.resource_result_factory import get_all_resources
from post_processing.run_results.resources.collectl_resource import CollectlResource
from post_processing.run_results.resources.fio_resource import FIOResource
from post_processing.run_results.resources.top_resource import TopResource


class TestGetAllResourcesFIOOnly:
    """Test get_all_resources with only FIO data available"""

    def test_fio_only_returns_single_resource(self) -> None:
        """Test that only FIO resource is returned when collectl dir missing"""
        fio_data = {
            "jobs": [
                {
                    "job_name": "test",
                    "usr_cpu": 45.5,
                    "sys_cpu": 10.2,
                    "ctx": 1000,
                    "majf": 0,
                    "minf": 100,
                }
            ]
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            resources = get_all_resources(file_path)

            assert len(resources) == 1
            assert isinstance(resources[0], FIOResource)
            assert resources[0].source == "fio"

    def test_fio_only_with_empty_directory(self) -> None:
        """Test FIO-only scenario with empty parent directory"""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 30.0, "sys_cpu": 20.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            resources = get_all_resources(file_path)

            assert len(resources) == 1
            assert resources[0].source == "fio"


class TestGetAllResourcesWithInvalidData:
    """Test get_all_resources with invalid data (both sources still created)"""

    def test_both_created_even_with_invalid_fio_json(self) -> None:
        """Test that both resources are created even if FIO JSON is invalid"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text("invalid json data")

            # Create collectl directory with CPU file
            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;50\n")

            resources = get_all_resources(file_path)

            # Both resources are created (FIO handles invalid JSON gracefully)
            assert len(resources) == 2
            sources = {r.source for r in resources}
            assert sources == {"fio", "collectl"}

    def test_both_created_with_empty_fio_file(self) -> None:
        """Test both resources created when FIO file is empty"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text("")

            # Create collectl directory
            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;45\n")

            resources = get_all_resources(file_path)

            # Both created (empty file is handled gracefully)
            assert len(resources) == 2
            sources = {r.source for r in resources}
            assert sources == {"fio", "collectl"}


class TestGetAllResourcesBothSources:
    """Test get_all_resources with both FIO and Collectl available"""

    def test_both_sources_returns_two_resources(self) -> None:
        """Test that both FIO and Collectl resources are returned"""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 30.0, "sys_cpu": 15.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            # Create collectl directory
            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;50\n")

            resources = get_all_resources(file_path)

            assert len(resources) == 2

            # Check that we have both sources
            sources = {r.source for r in resources}
            assert sources == {"fio", "collectl"}

            # Verify types
            fio_resources = [r for r in resources if isinstance(r, FIOResource)]
            collectl_resources = [r for r in resources if isinstance(r, CollectlResource)]

            assert len(fio_resources) == 1
            assert len(collectl_resources) == 1

    def test_both_sources_order(self) -> None:
        """Test that FIO is returned before Collectl (order matters for consistency)"""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 25.0, "sys_cpu": 10.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;60\n")

            resources = get_all_resources(file_path)

            # FIO should be first
            assert resources[0].source == "fio"
            assert resources[1].source == "collectl"


class TestGetAllResourcesMinimalScenarios:
    """Test get_all_resources in minimal scenarios"""

    def test_fio_always_created_if_file_exists(self) -> None:
        """Test that FIO resource is always created if file exists"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text("invalid json")

            # No collectl directory
            resources = get_all_resources(file_path)

            # FIO is always created (handles invalid data gracefully)
            assert len(resources) == 1
            assert resources[0].source == "fio"

    def test_fio_only_with_empty_collectl_dir(self) -> None:
        """Test FIO-only when collectl dir exists but has no CPU files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text("not valid json")

            # Create empty collectl directory (no .cpu files)
            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()

            resources = get_all_resources(file_path)

            # Only FIO (collectl has no CPU files)
            assert len(resources) == 1
            assert resources[0].source == "fio"


class TestGetAllResourcesErrorHandling:
    """Test error handling in get_all_resources"""

    def test_handles_collectl_exception_gracefully(self) -> None:
        """Test that Collectl exceptions don't prevent FIO from being added"""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 40.0, "sys_cpu": 20.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            # Create collectl dir but with no CPU files (will raise exception)
            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            # Don't create any .cpu files

            resources = get_all_resources(file_path)

            # Should still get FIO even though Collectl failed
            assert len(resources) == 1
            assert resources[0].source == "fio"

    def test_handles_missing_fio_file(self) -> None:
        """Test that missing FIO file is handled gracefully"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "nonexistent.json"
            # Don't create the file

            # Create valid collectl data
            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;55\n")

            resources = get_all_resources(file_path)

            # Both are created (FIO handles missing file gracefully)
            assert len(resources) == 2
            sources = {r.source for r in resources}
            assert sources == {"fio", "collectl"}


class TestGetAllResourcesCollectlDirectoryChecks:
    """Test collectl directory existence checks"""

    def test_collectl_dir_is_file_not_directory(self) -> None:
        """Test that collectl is skipped if it's a file, not a directory"""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 35.0, "sys_cpu": 15.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            # Create 'collectl' as a file, not directory
            collectl_file = base_path / "collectl"
            collectl_file.write_text("this is a file")

            resources = get_all_resources(file_path)

            # Should only get FIO (collectl is not a directory)
            assert len(resources) == 1
            assert resources[0].source == "fio"

    def test_collectl_dir_does_not_exist(self) -> None:
        """Test that missing collectl directory is handled correctly"""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 50.0, "sys_cpu": 10.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            # Don't create collectl directory
            resources = get_all_resources(file_path)

            assert len(resources) == 1
            assert resources[0].source == "fio"


class TestGetAllResourcesIntegration:
    """Integration tests for get_all_resources"""

    def test_realistic_scenario_both_sources(self) -> None:
        """Test realistic scenario with both FIO and Collectl data"""
        fio_data = {
            "jobs": [
                {
                    "job_name": "seq_read",
                    "usr_cpu": 25.5,
                    "sys_cpu": 15.3,
                    "ctx": 5000,
                    "majf": 0,
                    "minf": 250,
                }
            ]
        }

        cpu_data = """#Date;Time;[CPU:0]User%;[CPU:0]Sys%;[CPU:0]Totl%;[CPU:1]User%;[CPU:1]Sys%;[CPU:1]Totl%
20260619;17:06:30;30;20;50;32;18;50
20260619;17:06:40;28;22;50;30;20;50
20260619;17:06:50;29;21;50;31;19;50
"""

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "cephalasquad3-20260619.cpu"
            cpu_file.write_text(cpu_data)

            resources = get_all_resources(file_path)

            assert len(resources) == 2

            # Verify both resources can be used
            for resource in resources:
                result = resource.get()
                assert "cpu" in result
                assert "memory" in result
                assert "source" in result
                assert result["source"] in ["fio", "collectl"]

    def test_can_retrieve_data_from_all_resources(self) -> None:
        """Test that data can be retrieved from all returned resources"""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 20.0, "sys_cpu": 10.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            file_path = base_path / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            collectl_dir = base_path / "collectl"
            collectl_dir.mkdir()
            cpu_file = collectl_dir / "host-20260619.cpu"
            cpu_file.write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;45\n")

            resources = get_all_resources(file_path)

            # Get data from all resources
            results = [r.get() for r in resources]

            assert len(results) == 2
            assert all("cpu" in r for r in results)
            assert all("memory" in r for r in results)
            assert all("source" in r for r in results)

            # Verify sources are different
            sources = [r["source"] for r in results]
            assert set(sources) == {"fio", "collectl"}


class TestGetAllResourcesWithTop:
    """Test get_all_resources when top monitoring data is present."""

    def _make_top_dir(self, base: Path, content: str) -> None:
        top_dir = base / "top"
        top_dir.mkdir()
        (top_dir / "12345_osd_top.out").write_text(content)

    _TOP_CONTENT = (
        "top - 14:32:01 up 1 day\n%Cpu(s):  5.0 us\n\n"
        "  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND\n"
        "12345 ceph      20   0  1.0g  100m  10m  S  30.0   0.5   0:01.00 crimson-osd\n"
    )

    def test_top_only_returns_fio_and_top(self) -> None:
        """FIO and Top resources are returned when only a top dir is present."""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 20.0, "sys_cpu": 10.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.write_text(json.dumps(fio_data))
            self._make_top_dir(base, self._TOP_CONTENT)

            resources = get_all_resources(file_path)

            assert len(resources) == 2
            sources = {r.source for r in resources}
            assert sources == {"fio", "top"}

    def test_top_resource_is_top_resource_instance(self) -> None:
        """The top entry in the list is a TopResource instance."""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 20.0, "sys_cpu": 10.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.write_text(json.dumps(fio_data))
            self._make_top_dir(base, self._TOP_CONTENT)

            resources = get_all_resources(file_path)

            top_resources = [r for r in resources if isinstance(r, TopResource)]
            assert len(top_resources) == 1

    def test_all_three_sources_when_collectl_and_top_present(self) -> None:
        """FIO, Collectl, and Top are all returned when both dirs exist."""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 20.0, "sys_cpu": 10.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            # Collectl dir
            collectl_dir = base / "collectl"
            collectl_dir.mkdir()
            (collectl_dir / "host-20260619.cpu").write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;50\n")

            # Top dir
            self._make_top_dir(base, self._TOP_CONTENT)

            resources = get_all_resources(file_path)

            assert len(resources) == 3
            sources = {r.source for r in resources}
            assert sources == {"fio", "collectl", "top"}

    def test_source_order_fio_collectl_top(self) -> None:
        """Resources are returned in fio → collectl → top order."""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 20.0, "sys_cpu": 10.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.write_text(json.dumps(fio_data))

            collectl_dir = base / "collectl"
            collectl_dir.mkdir()
            (collectl_dir / "host-20260619.cpu").write_text("#Date;Time;[CPU:0]Totl%\n20260619;17:06:30;50\n")

            self._make_top_dir(base, self._TOP_CONTENT)

            resources = get_all_resources(file_path)

            assert resources[0].source == "fio"
            assert resources[1].source == "collectl"
            assert resources[2].source == "top"

    def test_top_dir_without_out_files_skipped(self) -> None:
        """An empty top dir does not add a TopResource."""
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 20.0, "sys_cpu": 10.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.write_text(json.dumps(fio_data))
            (base / "top").mkdir()  # exists but empty

            resources = get_all_resources(file_path)

            sources = {r.source for r in resources}
            assert "top" not in sources

    def test_top_resource_data_retrievable(self) -> None:
        """get() on the TopResource returns the expected dict structure.

        _TOP_CONTENT has one snapshot, one thread at 30.0 core-units.
        Normalised by cpu_count=4: 30.0 / 4 = 7.5.
        """
        fio_data = {"jobs": [{"job_name": "test", "usr_cpu": 20.0, "sys_cpu": 10.0}]}

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.write_text(json.dumps(fio_data))
            self._make_top_dir(base, self._TOP_CONTENT)

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=4):
                resources = get_all_resources(file_path)
                top_resource = next(r for r in resources if r.source == "top")
                result = top_resource.get()

            assert result["source"] == "top"
            assert "cpu" in result
            assert "memory" in result
            assert float(result["cpu"]) == pytest.approx(7.5)


# Made with Bob
