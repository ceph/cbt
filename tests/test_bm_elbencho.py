"""Unit tests for the Elbencho benchmark class.

Validates construction, config handling, factory integration,
CLI command building, run loop fan-out, and pdsh-free lifecycle.
"""

import shutil
import tempfile
import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import benchmarkfactory
import common
import settings
from benchmark.elbencho import Elbencho
from cluster.ceph import Ceph
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


class TestElbenchoDefaults(unittest.TestCase):
    """Test the values an Elbencho benchmark gets when the config does not set them."""

    archive_dir = "/tmp"
    cluster: Ceph

    @classmethod
    def setUpClass(cls) -> None:
        settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
        cls.cluster = Ceph.mockinit(settings.cluster)  # type: ignore[no-untyped-call]

    def _make(self, config: Optional[dict[str, Any]] = None) -> Elbencho:
        cfg = dict(_MINIMAL_CONFIG, **(config or {}))
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            self.archive_dir, self.cluster, "elbencho", cfg
        )
        assert isinstance(b, Elbencho)
        return b

    def test_returns_elbencho_instance(self) -> None:
        """The factory builds an Elbencho from a minimal config."""
        self.assertIsInstance(self._make(), Elbencho)

    def test_default_cmd_path(self) -> None:
        """cmd_path defaults to /usr/local/bin/elbencho."""
        self.assertEqual("/usr/local/bin/elbencho", self._make().cmd_path)

    def test_default_auth_is_empty_dict(self) -> None:
        """auth defaults to an empty dict."""
        self.assertEqual({}, self._make().auth)

    def test_no_workloads_registered_by_default(self) -> None:
        """A config without workloads registers none."""
        # Elbencho has no public accessor for its Workloads; this pins that none are registered at construction.
        # pylint: disable-next=protected-access
        self.assertFalse(self._make()._workloads.exist())

    def test_base_run_dir_set(self) -> None:
        """base_run_dir is set to a string."""
        b = self._make()
        self.assertIsNotNone(b.base_run_dir)
        self.assertIsInstance(b.base_run_dir, str)


class TestElbenchoExplicitConfig(unittest.TestCase):
    """Test that values given in the config are used."""

    archive_dir = "/tmp"
    cluster: Ceph

    @classmethod
    def setUpClass(cls) -> None:
        settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
        cls.cluster = Ceph.mockinit(settings.cluster)  # type: ignore[no-untyped-call]

    def _make(self) -> Elbencho:
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            self.archive_dir, self.cluster, "elbencho", dict(_FULL_CONFIG)
        )
        assert isinstance(b, Elbencho)
        return b

    def test_custom_cmd_path(self) -> None:
        """cmd_path from the config is used."""
        self.assertEqual("/opt/elbencho/bin/elbencho", self._make().cmd_path)

    def test_custom_auth(self) -> None:
        """auth from the config is kept."""
        b = self._make()
        self.assertIn("config", b.auth)
        self.assertIn("access_key=AKID", b.auth["config"])

    def test_workloads_registered(self) -> None:
        """The workloads in the config are registered."""
        # Elbencho has no public accessor for its Workloads; this pins that construction registers them.
        # pylint: disable-next=protected-access
        self.assertTrue(self._make()._workloads.exist())

    def test_workload_names_preserved(self) -> None:
        """The workload names from the config are kept."""
        # Elbencho has no public accessor for its Workloads; this pins that the names from the config are kept.
        # pylint: disable-next=protected-access
        names = self._make()._workloads.get_names()
        self.assertIn("write_small", names)
        self.assertIn("read_small", names)


class TestElbenchoValidation(unittest.TestCase):
    """Test that an invalid workload is rejected when the benchmark is built."""

    archive_dir = "/tmp"
    cluster: Ceph

    @classmethod
    def setUpClass(cls) -> None:
        settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
        cls.cluster = Ceph.mockinit(settings.cluster)  # type: ignore[no-untyped-call]

    def _make(self, workloads: dict[str, Any]) -> Elbencho:
        cfg = dict(_MINIMAL_CONFIG, workloads=workloads)
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            self.archive_dir, self.cluster, "elbencho", cfg
        )
        assert isinstance(b, Elbencho)
        return b

    def test_missing_mode_raises(self) -> None:
        """A workload without a mode is rejected."""
        with self.assertRaises(ValueError) as ctx:
            self._make({"no_mode": {"s3_bucket": "test-bucket"}})
        self.assertIn("missing required key 'mode'", str(ctx.exception))

    def test_missing_s3_bucket_raises(self) -> None:
        """A workload without an s3_bucket is rejected."""
        with self.assertRaises(ValueError) as ctx:
            self._make({"bad": {"mode": "write"}})
        self.assertIn("missing required key 's3_bucket'", str(ctx.exception))

    def test_non_integer_threads_raises(self) -> None:
        """A threads value that is not an integer is rejected."""
        with self.assertRaises(ValueError) as ctx:
            self._make({"bad": {"mode": "write", "s3_bucket": "b", "threads": ["bad_value"]}})
        self.assertIn("threads value 'bad_value' is not an integer", str(ctx.exception))

    def test_non_integer_iodepth_raises(self) -> None:
        """An iodepth value that is not an integer is rejected."""
        with self.assertRaises(ValueError) as ctx:
            self._make({"bad": {"mode": "write", "s3_bucket": "b", "iodepth": "not_a_number"}})
        self.assertIn("iodepth value 'not_a_number' is not an integer", str(ctx.exception))

    def test_string_integer_threads_accepted(self) -> None:
        """Quoted integers in the YAML are read as strings; "4" is valid, so construction must succeed."""
        b = self._make({"w": {"mode": "write", "s3_bucket": "b", "threads": ["4", 16]}})
        # Elbencho has no public accessor for its Workloads; this pins that the workload was registered.
        # pylint: disable-next=protected-access
        self.assertTrue(b._workloads.exist())

    def test_mixed_valid_invalid_threads_raises_on_bad_item(self) -> None:
        """One bad item in a list of threads values is enough to reject the workload."""
        with self.assertRaises(ValueError) as ctx:
            self._make({"w": {"mode": "write", "s3_bucket": "b", "threads": [1, 4, "oops"]}})
        self.assertIn("threads value 'oops' is not an integer", str(ctx.exception))


class TestElbenchoExists(unittest.TestCase):
    """Test the check for results from an earlier run."""

    archive_dir = "/tmp"
    cluster: Ceph

    @classmethod
    def setUpClass(cls) -> None:
        settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
        cls.cluster = Ceph.mockinit(settings.cluster)  # type: ignore[no-untyped-call]

    def _make(self) -> Elbencho:
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            self.archive_dir, self.cluster, "elbencho", dict(_MINIMAL_CONFIG)
        )
        assert isinstance(b, Elbencho)
        return b

    def test_exists_false_when_archive_dir_absent(self) -> None:
        """No archive directory means there are no earlier results."""
        b = self._make()
        b.archive_dir = "/tmp/__cbt_elbencho_no_such_dir_xyzzy__"
        self.assertFalse(b.exists())

    def test_exists_true_when_archive_dir_present(self) -> None:
        """An existing archive directory means the results are already there."""
        b = self._make()
        with tempfile.TemporaryDirectory() as tmpdir:
            b.archive_dir = tmpdir
            self.assertTrue(b.exists())


class TestBenchmarkFactoryIntegration(unittest.TestCase):
    """Test that the benchmark factory builds Elbencho benchmarks."""

    archive_dir = "/tmp"
    cluster: Ceph

    @classmethod
    def setUpClass(cls) -> None:
        settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
        cls.cluster = Ceph.mockinit(settings.cluster)  # type: ignore[no-untyped-call]

    def test_get_object_returns_elbencho(self) -> None:
        """get_object() builds an Elbencho for the "elbencho" benchmark."""
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            self.archive_dir, self.cluster, "elbencho", dict(_MINIMAL_CONFIG)
        )
        self.assertIsInstance(b, Elbencho)

    def test_unknown_benchmark_returns_none(self) -> None:
        """get_object() returns None for a benchmark name it does not know."""
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            self.archive_dir, self.cluster, "no_such_benchmark", dict(_MINIMAL_CONFIG)
        )
        self.assertIsNone(b)

    def test_get_all_yields_single_instance_despite_list_valued_workload_params(self) -> None:
        """List valued threads, iodepth and blocksize are expanded by the run loop, not into several benchmarks."""
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
        objects = list(benchmarkfactory.get_all(self.archive_dir, self.cluster, 0))  # type: ignore[no-untyped-call]
        elbencho_objects = [o for o in objects if isinstance(o, Elbencho)]
        self.assertEqual(1, len(elbencho_objects))


# ---------------------------------------------------------------------------
# Run-loop tests
# ---------------------------------------------------------------------------


class TestRunLoop(unittest.TestCase):
    """Test the commands and run directories that Elbencho.run() produces for a workload."""

    _BOOLEAN_FLAGS = ("--deldirs", "--s3nompcheck", "--mkdirs")

    cluster: Ceph

    @classmethod
    def setUpClass(cls) -> None:
        settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
        cls.cluster = Ceph.mockinit(settings.cluster)  # type: ignore[no-untyped-call]

    def setUp(self) -> None:
        # run() writes benchmark_config.yaml into archive_dir, so give each test its own directory.
        self.archive_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.archive_dir)

        # run() wraps each command group in monitoring; stub it so
        # the run-loop tests don't touch real monitors.
        for target in (
            "monitoring.monitoring_factory.MonitoringFactory.start",
            "monitoring.monitoring_factory.MonitoringFactory.stop",
        ):
            patcher = patch(target)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _run(self, workloads: dict[str, Any]) -> tuple[MagicMock, MagicMock]:
        """
        Run the workloads through Elbencho.run().

        Only the remote boundary, the cluster and the dropping of caches are replaced.
        Returns the mocks for run_command and make_remote_dir.
        """
        config = dict(_MINIMAL_CONFIG, cmd_path="/usr/local/bin/elbencho", auth={}, workloads=workloads)
        benchmark = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            self.archive_dir, self.cluster, "elbencho", config
        )
        assert isinstance(benchmark, Elbencho)

        with (
            patch.object(AsyncSSHExecutor, "run_command", return_value=[]) as run_command,
            patch.object(AsyncSSHExecutor, "make_remote_dir") as make_remote_dir,
            patch.object(AsyncSSHExecutor, "sync_files"),
            patch.object(Elbencho, "dropcaches"),
            patch.object(Ceph, "dump_config"),
            patch.object(Ceph, "set_osd_param"),
        ):
            benchmark.run()

        return run_command, make_remote_dir

    def test_call_count_matches_run_matrix(self) -> None:
        """2 blocksizes x 2 threads x 2 iodepths = 8 run_command calls."""
        run_command, _ = self._run(
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
        self.assertEqual(8, run_command.call_count)

    def test_run_dir_path_structure(self) -> None:
        """The run directory is named after the mode, the blocksize in bytes, the threads and the iodepth."""
        _, make_remote_dir = self._run(
            {
                "w": {
                    "s3_bucket": "bkt",
                    "mode": "write",
                    "blocksize": ["4k"],
                    "threads": [16],
                    "iodepth": [4],
                }
            }
        )
        mkdir_calls = [c.args[1] for c in make_remote_dir.call_args_list]
        self.assertTrue(
            any("elbencho/write_4096/threads-016/iodepth-004" in d for d in mkdir_calls),
            f"Expected path segment not found in: {mkdir_calls}",
        )

    def test_stat_workload_skipped(self) -> None:
        """A stat workload has no supported command, so nothing is run."""
        run_command, _ = self._run({"w": {"s3_bucket": "bkt", "mode": "stat", "threads": [1], "iodepth": [1]}})
        run_command.assert_not_called()

    def test_scalar_threads_and_iodepth(self) -> None:
        """Threads and iodepth given as single values, not lists, produce one command."""
        run_command, _ = self._run(
            {
                "w": {
                    "s3_bucket": "bkt",
                    "mode": "write",
                    "blocksize": "4k",
                    "threads": 4,
                    "iodepth": 2,
                }
            }
        )
        self.assertEqual(1, run_command.call_count)

    def test_cell_emits_matching_command_and_byte_run_dir(self) -> None:
        """
        The run loop feeds elbencho the human blocksize (128k) while naming the run directory with the byte count.

        Mixing the two up is a real regression risk, so pin both from a single cell.
        """
        run_command, make_remote_dir = self._run(
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

        self.assertEqual(1, run_command.call_count)
        cmd = run_command.call_args.args[1]
        self.assertIn("--block 128k", cmd)
        self.assertNotIn("--block 131072", cmd)  # byte count must not reach --block
        self.assertIn("--threads 8", cmd)
        self.assertIn("--iodepth 16", cmd)
        self.assertIn("--size 4g", cmd)
        self.assertTrue(cmd.endswith("s3://bkt"))

        run_dirs = [c.args[1] for c in make_remote_dir.call_args_list]
        self.assertTrue(
            any("elbencho/write_131072/threads-008/iodepth-016" in d for d in run_dirs),
            f"byte-based run dir not found in: {run_dirs}",
        )

    def test_boolean_options_set_to_true_emit_their_flags(self) -> None:
        """A test plan with `deldirs: true` (and the others) reaches the command line as flags."""
        run_command, _ = self._run(
            {
                "w": {
                    "s3_bucket": "bkt",
                    "mode": "write",
                    "blocksize": ["4k"],
                    "threads": [1],
                    "iodepth": [1],
                    "deldirs": True,
                    "s3nompcheck": True,
                    "mkdirs": True,
                }
            }
        )

        self.assertEqual(1, run_command.call_count)
        cmd = run_command.call_args.args[1]
        for flag in self._BOOLEAN_FLAGS:
            self.assertIn(flag, cmd, f"{flag} must be emitted for an option set to true")

    def test_boolean_options_set_to_false_emit_no_flags(self) -> None:
        """Workloads stringifies False to "false", which must not switch the flags on."""
        run_command, _ = self._run(
            {
                "w": {
                    "s3_bucket": "bkt",
                    "mode": "write",
                    "blocksize": ["4k"],
                    "threads": [1],
                    "iodepth": [1],
                    "deldirs": False,
                    "s3nompcheck": False,
                    "mkdirs": False,
                }
            }
        )

        self.assertEqual(1, run_command.call_count)
        cmd = run_command.call_args.args[1]
        for flag in self._BOOLEAN_FLAGS:
            self.assertNotIn(flag, cmd, f"{flag} must not be emitted for an option set to false")


class TestElbenchoNoPdsh(unittest.TestCase):
    """Test that the lifecycle uses the remote executor and not pdsh."""

    archive_dir = "/tmp"
    cluster: Ceph

    @classmethod
    def setUpClass(cls) -> None:
        settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
        cls.cluster = Ceph.mockinit(settings.cluster)  # type: ignore[no-untyped-call]

    def _make(self, workloads: Optional[dict[str, Any]] = None) -> Elbencho:
        cfg = dict(
            _MINIMAL_CONFIG,
            cmd_path="/usr/local/bin/elbencho",
            auth={},
            workloads=workloads or {},
        )
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            self.archive_dir, self.cluster, "elbencho", cfg
        )
        assert isinstance(b, Elbencho)
        return b

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "clean_remote_dir")
    def test_cleandir_uses_async_helpers(self, mock_clean: Any, mock_mkdir: Any) -> None:
        """cleandir() cleans and then recreates the run directory on the clients through the remote executor."""
        b = self._make()
        b.cleandir()
        clients = mock_clean.call_args.args[0]
        self.assertEqual(mock_clean.call_args.args[1], b.run_dir)
        self.assertEqual(mock_mkdir.call_args.args[0], clients)
        self.assertEqual(mock_mkdir.call_args.args[1], b.run_dir)

    @patch.object(AsyncSSHExecutor, "run_command")
    def test_dropcaches_uses_remote_executor(self, mock_exec: Any) -> None:
        """dropcaches() runs sync and the drop_caches write through the remote executor."""
        mock_exec.return_value = []
        b = self._make()
        b.dropcaches()
        self.assertEqual(2, mock_exec.call_count)
        commands = [c.args[1] for c in mock_exec.call_args_list]
        self.assertIn("sync", commands)
        self.assertTrue(any("drop_caches" in cmd for cmd in commands))

    def test_run_does_not_call_pdsh(self) -> None:
        """run() sends a real workload to the clients through the remote executor and never through pdsh."""
        # run() writes benchmark_config.yaml into archive_dir, so use a directory of its own.
        self.archive_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.archive_dir)
        b = self._make(
            {"w": {"s3_bucket": "bkt", "mode": "write", "blocksize": ["4k"], "threads": [1], "iodepth": [1]}}
        )

        # The cluster and monitoring still use pdsh, so they are stubbed out. Everything else, including the
        # dropping of caches and the running of the workload, is Elbencho's own and goes through the executor.
        with (
            patch.object(AsyncSSHExecutor, "run_command", return_value=[]) as run_command,
            patch.object(AsyncSSHExecutor, "make_remote_dir"),
            patch.object(AsyncSSHExecutor, "sync_files"),
            patch.object(Ceph, "dump_config"),
            patch.object(Ceph, "set_osd_param"),
            patch("monitoring.monitoring_factory.MonitoringFactory.start"),
            patch("monitoring.monitoring_factory.MonitoringFactory.stop"),
            patch.object(common, "pdsh") as common_pdsh,
            patch("workloads.workloads.pdsh") as workloads_pdsh,
        ):
            b.run()

        self.assertTrue(
            any("/usr/local/bin/elbencho" in call.args[1] for call in run_command.call_args_list),
            "the workload must have been run through the remote executor",
        )
        common_pdsh.assert_not_called()
        workloads_pdsh.assert_not_called()


# ---------------------------------------------------------------------------
# estimate_duration() tests
# ---------------------------------------------------------------------------


class TestElbenchoEstimateDuration(unittest.TestCase):
    """Test the estimate of how long the workloads will take."""

    archive_dir = "/tmp"
    cluster: Ceph

    @classmethod
    def setUpClass(cls) -> None:
        settings.mock_initialize(config_file=INVARIANT_YAML)  # type: ignore[no-untyped-call]
        cls.cluster = Ceph.mockinit(settings.cluster)  # type: ignore[no-untyped-call]

    def _make(self, workloads: dict[str, Any]) -> Elbencho:
        cfg = dict(_MINIMAL_CONFIG, workloads=workloads)
        b = benchmarkfactory.get_object(  # type: ignore[no-untyped-call]
            self.archive_dir, self.cluster, "elbencho", cfg
        )
        assert isinstance(b, Elbencho)
        return b

    def test_no_workloads_returns_zero(self) -> None:
        """estimate_duration() returns 0 when no workloads are configured."""
        b = self._make({})
        self.assertEqual(0, b.estimate_duration())

    def test_single_workload_scalar_params(self) -> None:
        """1 param set * duration=30s → 30s."""
        b = self._make({"w": {"s3_bucket": "b", "mode": "write", "threads": 1, "iodepth": 1, "duration": 30}})
        self.assertEqual(30, b.estimate_duration())

    def test_single_workload_list_params(self) -> None:
        """2 threads x 2 iodepths = 4 param sets * duration=30s → 120s."""
        b = self._make({"w": {"s3_bucket": "b", "mode": "write", "threads": [1, 4], "iodepth": [1, 4], "duration": 30}})
        self.assertEqual(120, b.estimate_duration())

    def test_multiple_workloads_summed(self) -> None:
        """Durations from multiple workloads are summed.

        write_small: threads=[1,4,16] x iodepth=[1,4,16] x blocksize=['4k','128k'] = 18 sets * 60s = 1080s
        read_small:  threads=[1,4]    x iodepth=[1,4]    x blocksize=['4k']        =  4 sets * 60s =  240s
        total: 1320s
        """
        b = self._make(_FULL_CONFIG["workloads"])
        self.assertEqual(1320, b.estimate_duration())

    def test_workload_without_duration_contributes_zero(self) -> None:
        """A workload with no duration set contributes 0 to the total."""
        b = self._make({"w": {"s3_bucket": "b", "mode": "write", "threads": [1, 4], "iodepth": [1, 4]}})
        self.assertEqual(0, b.estimate_duration())

    def test_time_and_duration_together_raises(self) -> None:
        """Setting both 'time' and 'duration' on the same workload raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            b = self._make({"w": {"s3_bucket": "b", "mode": "write", "time": 60, "duration": 30}})
            b.estimate_duration()
        self.assertIn("mutually exclusive", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
