"""Unit tests for the Elbencho benchmark class.

Validates construction, config handling, factory integration,
CLI command building, run loop fan-out, and pdsh-free lifecycle.
"""

# pylint: disable=protected-access

import tempfile
from collections.abc import Generator
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest

import benchmarkfactory
import common as _common
import settings
from benchmark.elbencho import Elbencho
from remote.async_ssh import AsyncSSHExecutor

INVARIANT_YAML = "tools/invariant.yaml"

_MINIMAL_CONFIG: dict[str, Any] = {
    "iteration": 0,
    "benchmark": "elbencho",
}

_FULL_CONFIG: dict[str, Any] = {
    "iteration": 0,
    "benchmark": "elbencho",
    "cmd_path": "/opt/elbencho/bin/elbencho",
    "auth": {"config": "access_key=AKID;secret_key=<redacted>;url=http://rgw:7480;retry=9"},
    "workloads": {
        "write_small": {
            "s3_bucket": "cbt-benchmark",
            "mkdirs": True,
            "threads": [1, 4, 16],
            "iodepth": [1, 4, 16],
            "blocksize": ["4k", "128k"],
            "size": "4g",
            "num_objects": 1000,
            "mode": "write",
            "duration": 60,
        },
        "read_small": {
            "s3_bucket": "cbt-benchmark",
            "threads": [1, 4],
            "iodepth": [1, 4],
            "blocksize": ["4k"],
            "size": "4g",
            "num_objects": 1000,
            "mode": "read",
            "duration": 60,
        },
    },
}


@pytest.fixture(autouse=True)
def init_settings() -> None:
    """Populate settings globals from the invariant YAML fixture file."""
    settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]


def _mock_cluster() -> MagicMock:
    """Return a minimal mock cluster object."""
    cluster = MagicMock()
    cluster.config = {}
    return cluster


def _make(config: Optional[dict[str, Any]] = None) -> Elbencho:
    """Instantiate Elbencho directly with an optional config overlay."""
    cfg = dict(_MINIMAL_CONFIG, **(config or {}))
    return Elbencho(archive_dir="/tmp", cluster=_mock_cluster(), config=cfg)


# ---------------------------------------------------------------------------
# Default construction
# ---------------------------------------------------------------------------


class TestElbenchoDefaults:
    """Tests for Elbencho instantiation with default config."""

    def test_returns_elbencho_instance(self) -> None:
        """Factory returns an Elbencho instance."""
        assert isinstance(_make(), Elbencho)

    def test_default_cmd_path(self) -> None:
        """Default cmd_path is the system-wide elbencho binary."""
        assert _make().cmd_path == "/usr/local/bin/elbencho"

    def test_default_auth_is_empty_dict(self) -> None:
        """Auth defaults to an empty dict when not configured."""
        assert _make().auth == {}

    def test_no_workloads_registered_by_default(self) -> None:
        """No workloads are registered when none are supplied."""
        assert not _make()._workloads.exist()

    def test_base_run_dir_set(self) -> None:
        """base_run_dir is populated as a non-empty string after construction."""
        b = _make()
        assert b.base_run_dir is not None
        assert isinstance(b.base_run_dir, str)


# ---------------------------------------------------------------------------
# Explicit / full config
# ---------------------------------------------------------------------------


class TestElbenchoExplicitConfig:
    """Tests for Elbencho instantiation with a fully populated config."""

    def _make_full(self) -> Elbencho:
        return Elbencho(archive_dir="/tmp", cluster=_mock_cluster(), config=dict(_FULL_CONFIG))

    def test_custom_cmd_path(self) -> None:
        """cmd_path is taken from config when supplied."""
        assert self._make_full().cmd_path == "/opt/elbencho/bin/elbencho"

    def test_custom_auth(self) -> None:
        """Auth credentials are stored verbatim from the nested auth dict."""
        b = self._make_full()
        assert "config" in b.auth
        assert "access_key=AKID" in b.auth["config"]

    def test_workloads_registered(self) -> None:
        """Workloads are registered when the config provides them."""
        assert self._make_full()._workloads.exist()

    def test_workload_names_preserved(self) -> None:
        """Workload names from config are preserved in the workload registry."""
        names = self._make_full()._workloads.get_names()
        assert "write_small" in names
        assert "read_small" in names


# ---------------------------------------------------------------------------
# Workload validation
# ---------------------------------------------------------------------------


class TestElbenchoValidation:
    """Tests for workload config validation errors raised during construction."""

    def _make_with_workloads(self, workloads: dict[str, Any]) -> Elbencho:
        return _make({"workloads": workloads})

    def test_missing_mode_raises(self) -> None:
        """A workload without a 'mode' key raises ValueError on construction."""
        with pytest.raises(ValueError, match="missing required key 'mode'"):
            self._make_with_workloads({"no_mode": {"s3_bucket": "test-bucket"}})

    def test_missing_s3_bucket_raises(self) -> None:
        """A workload without an 's3_bucket' key raises ValueError on construction."""
        with pytest.raises(ValueError, match="missing required key 's3_bucket'"):
            self._make_with_workloads({"bad": {"mode": "write"}})

    def test_non_integer_threads_raises(self) -> None:
        """A non-integer thread count raises ValueError on construction."""
        with pytest.raises(ValueError, match="threads value 'bad_value' is not an integer"):
            self._make_with_workloads({"bad": {"mode": "write", "s3_bucket": "b", "threads": ["bad_value"]}})

    def test_non_integer_iodepth_raises(self) -> None:
        """A non-integer iodepth raises ValueError on construction."""
        with pytest.raises(ValueError, match="iodepth value 'not_a_number' is not an integer"):
            self._make_with_workloads({"bad": {"mode": "write", "s3_bucket": "b", "iodepth": "not_a_number"}})

    def test_string_integer_threads_accepted(self) -> None:
        """YAML-quoted integers such as '4' are accepted as thread counts."""
        # YAML may deserialise quoted integers as strings — "4" should be valid.
        b = self._make_with_workloads({"w": {"mode": "write", "s3_bucket": "b", "threads": ["4", 16]}})
        assert b._workloads.exist()

    def test_mixed_valid_invalid_threads_raises_on_bad_item(self) -> None:
        """A mixed list raises when any single thread count is non-integer."""
        with pytest.raises(ValueError, match="threads value 'oops' is not an integer"):
            self._make_with_workloads({"w": {"mode": "write", "s3_bucket": "b", "threads": [1, 4, "oops"]}})


# ---------------------------------------------------------------------------
# exists()
# ---------------------------------------------------------------------------


class TestElbenchoExists:
    """Tests for the Elbencho.exists() check."""

    def test_exists_false_when_archive_dir_absent(self) -> None:
        """exists() returns False when the archive directory does not exist."""
        b = _make()
        b.archive_dir = "/tmp/__cbt_elbencho_no_such_dir_xyzzy__"
        assert not b.exists()

    def test_exists_true_when_archive_dir_present(self) -> None:
        """exists() returns True when the archive directory exists."""
        b = _make()
        with tempfile.TemporaryDirectory() as tmpdir:
            b.archive_dir = tmpdir
            assert b.exists()


# ---------------------------------------------------------------------------
# benchmarkfactory integration
# ---------------------------------------------------------------------------


class TestBenchmarkFactoryIntegration:
    """Tests for benchmarkfactory.get_object / get_all integration."""

    def test_get_object_returns_elbencho(self) -> None:
        """get_object returns an Elbencho instance for the 'elbencho' key."""
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            "/tmp", _mock_cluster(), "elbencho", dict(_MINIMAL_CONFIG)
        )
        assert isinstance(b, Elbencho)

    def test_unknown_benchmark_returns_none(self) -> None:
        """get_object returns None for an unrecognised benchmark name."""
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            "/tmp", _mock_cluster(), "no_such_benchmark", dict(_MINIMAL_CONFIG)
        )
        assert b is None

    def test_get_all_yields_single_instance_despite_list_valued_workload_params(self) -> None:
        """get_all returns exactly one Elbencho object even with list-valued workload params."""
        settings.benchmarks = {
            "elbencho": {
                "cmd_path": "/usr/local/bin/elbencho",
                "auth": {},
                "workloads": {
                    "w": {
                        "s3_bucket": "b",
                        "threads": [1, 4, 16],
                        "iodepth": [1, 4, 16],
                        "blocksize": ["4k", "128k"],
                        "mode": "write",
                    }
                },
            }
        }
        objects = list(benchmarkfactory.get_all("/tmp", _mock_cluster(), 0))  # type: ignore[no-untyped-call]
        elbencho_objects = [o for o in objects if isinstance(o, Elbencho)]
        assert len(elbencho_objects) == 1


# ---------------------------------------------------------------------------
# Run-loop tests
# ---------------------------------------------------------------------------


class TestRunLoop:
    """Tests for the _run_workloads() run-matrix fan-out."""

    @pytest.fixture(autouse=True)
    def _stub_monitoring(self) -> Generator[None, None, None]:
        """Stub out monitoring start/stop so run-loop tests don't touch real monitors."""
        with (
            patch("monitoring.monitoring_factory.MonitoringFactory.start"),
            patch("monitoring.monitoring_factory.MonitoringFactory.stop"),
        ):
            yield

    def _make_with_workloads(self, workloads: dict[str, Any]) -> Elbencho:
        return _make(
            {
                "cmd_path": "/usr/local/bin/elbencho",
                "auth": {},
                "workloads": workloads,
            }
        )

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_call_count_matches_run_matrix(self, mock_exec: MagicMock, _mock_mkdir: MagicMock) -> None:
        """2 blocksizes x 2 threads x 2 iodepths = 8 run_command calls."""
        mock_exec.return_value = []
        b = self._make_with_workloads(
            {
                "w": {
                    "s3_bucket": "bkt",
                    "mode": "write",
                    "blocksize": ["4k", "128k"],
                    "threads": [1, 4],
                    "iodepth": [1, 4],
                }
            }
        )
        b._run_workloads()
        assert mock_exec.call_count == 8

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_run_dir_path_structure(self, mock_exec: MagicMock, mock_mkdir: MagicMock) -> None:
        """make_remote_dir is called with a path encoding blocksize/threads/iodepth."""
        mock_exec.return_value = []
        b = self._make_with_workloads(
            {"w": {"s3_bucket": "bkt", "mode": "write", "blocksize": ["4k"], "threads": [16], "iodepth": [4]}}
        )
        b._run_workloads()
        mkdir_calls = [c.args[1] for c in mock_mkdir.call_args_list]
        assert any("elbencho/write_4096/threads-016/iodepth-004" in d for d in mkdir_calls), (
            f"Expected path segment not found in: {mkdir_calls}"
        )

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_stat_workload_skipped(self, mock_exec: MagicMock, _mock_mkdir: MagicMock) -> None:
        """Stat-mode workloads are skipped; no run_command call is made."""
        mock_exec.return_value = []
        b = self._make_with_workloads({"w": {"s3_bucket": "bkt", "mode": "stat", "threads": [1], "iodepth": [1]}})
        b._run_workloads()
        mock_exec.assert_not_called()

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_scalar_threads_and_iodepth(self, mock_exec: MagicMock, _mock_mkdir: MagicMock) -> None:
        """Scalar (non-list) threads and iodepth values produce exactly one run_command call."""
        mock_exec.return_value = []
        b = self._make_with_workloads(
            {"w": {"s3_bucket": "bkt", "mode": "write", "blocksize": "4k", "threads": 4, "iodepth": 2}}
        )
        b._run_workloads()
        assert mock_exec.call_count == 1

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_cell_emits_matching_command_and_byte_run_dir(self, mock_exec: MagicMock, mock_mkdir: MagicMock) -> None:
        """Command uses human blocksize (128k); run-dir uses byte count (131072)."""
        # The run loop must feed elbencho the human blocksize (128k) while
        # naming the run directory with the byte count (131072). Mixing the two
        # up is a real regression risk, so pin both from a single cell.
        mock_exec.return_value = []
        b = self._make_with_workloads(
            {
                "w": {
                    "s3_bucket": "bkt",
                    "mode": "write",
                    "blocksize": ["128k"],
                    "threads": [8],
                    "iodepth": [16],
                    "size": "4g",
                }
            }
        )
        b._run_workloads()

        assert mock_exec.call_count == 1
        cmd = mock_exec.call_args.args[1]
        assert "--block 128k" in cmd
        assert "--block 131072" not in cmd  # byte count must not reach --block
        assert "--threads 8" in cmd
        assert "--iodepth 16" in cmd
        assert "--size 4g" in cmd
        assert cmd.endswith("s3://bkt")

        run_dirs = [c.args[1] for c in mock_mkdir.call_args_list]
        assert any("elbencho/write_131072/threads-008/iodepth-016" in d for d in run_dirs), (
            f"byte-based run dir not found in: {run_dirs}"
        )


# ---------------------------------------------------------------------------
# Pdsh-free lifecycle (AsyncSSH executor)
# ---------------------------------------------------------------------------


class TestElbenchoNoPdsh:
    """Tests for the pdsh-free AsyncSSH executor lifecycle methods."""

    def _make_no_pdsh(self) -> Elbencho:
        return _make({"cmd_path": "/usr/local/bin/elbencho", "auth": {}, "workloads": {}})

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "clean_remote_dir")
    def test_cleandir_uses_async_helpers(self, mock_clean: MagicMock, mock_mkdir: MagicMock) -> None:
        """cleandir() delegates to AsyncSSHExecutor.clean_remote_dir and make_remote_dir."""
        b = self._make_no_pdsh()
        b.cleandir()
        assert mock_clean.call_args.args[1] == b.run_dir
        assert mock_mkdir.call_args.args[0] == mock_clean.call_args.args[0]
        assert mock_mkdir.call_args.args[1] == b.run_dir

    @patch.object(AsyncSSHExecutor, "run_command")
    def test_dropcaches_uses_remote_executor(self, mock_exec: MagicMock) -> None:
        """dropcaches() issues sync and echo 3 > drop_caches via the remote executor."""
        mock_exec.return_value = []
        b = self._make_no_pdsh()
        b.dropcaches()
        assert mock_exec.call_count == 2
        commands = [c.args[1] for c in mock_exec.call_args_list]
        assert "sync" in commands
        assert any("drop_caches" in cmd for cmd in commands)

    @patch.object(AsyncSSHExecutor, "sync_files")
    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_run_does_not_call_pdsh(self, mock_exec: MagicMock, _mock_mkdir: MagicMock, _mock_sync: MagicMock) -> None:
        """run() never calls pdsh; all remote ops go through AsyncSSHExecutor."""
        mock_exec.return_value = []
        b = self._make_no_pdsh()
        with (
            patch.object(b, "dropcaches"),
            patch.object(b.cluster, "dump_config"),
            patch.object(b.cluster, "set_osd_param"),
            patch("monitoring.monitoring_factory.MonitoringFactory.start"),
            patch("monitoring.monitoring_factory.MonitoringFactory.stop"),
            patch.object(b, "_run_workloads"),
            patch.object(_common, "pdsh", side_effect=AssertionError("pdsh called")),
        ):
            b.run()
