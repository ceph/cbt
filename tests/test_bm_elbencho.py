"""Unit tests for the Elbencho benchmark class.

Validates construction, config handling, factory integration,
CLI command building, run loop fan-out, and pdsh-free lifecycle.
"""

import tempfile
import unittest
from unittest.mock import patch

import benchmarkfactory
import settings
from benchmark.elbencho import Elbencho
from cluster.ceph import Ceph
from remote.async_ssh import AsyncSSHExecutor

INVARIANT_YAML = "tools/invariant.yaml"

_MINIMAL_CONFIG = {
    "iteration": 0,
    "benchmark": "elbencho",
}

_FULL_CONFIG = {
    "iteration": 0,
    "benchmark": "elbencho",
    "cmd_path": "/opt/elbencho/bin/elbencho",
    "auth": {
        "config": "access_key=AKID;secret_key=<redacted>;url=http://rgw:7480;retry=9"
    },
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

    archive_dir = "/tmp"

    @classmethod
    def setUpClass(cls):
        settings.mock_initialize(config_file=INVARIANT_YAML)
        cls.cluster = Ceph.mockinit(settings.cluster)

    def _make(self, config=None) -> Elbencho:
        cfg = dict(_MINIMAL_CONFIG, **(config or {}))
        b = benchmarkfactory.get_object(self.archive_dir, self.cluster, "elbencho", cfg)
        assert isinstance(b, Elbencho)
        return b

    def test_returns_elbencho_instance(self):
        self.assertIsInstance(self._make(), Elbencho)

    def test_default_cmd_path(self):
        self.assertEqual("/usr/local/bin/elbencho", self._make().cmd_path)

    def test_default_auth_is_empty_dict(self):
        self.assertEqual({}, self._make().auth)

    def test_no_workloads_registered_by_default(self):
        self.assertFalse(self._make()._workloads.exist())

    def test_base_run_dir_set(self):
        b = self._make()
        self.assertIsNotNone(b.base_run_dir)
        self.assertIsInstance(b.base_run_dir, str)


class TestElbenchoExplicitConfig(unittest.TestCase):

    archive_dir = "/tmp"

    @classmethod
    def setUpClass(cls):
        settings.mock_initialize(config_file=INVARIANT_YAML)
        cls.cluster = Ceph.mockinit(settings.cluster)

    def _make(self) -> Elbencho:
        b = benchmarkfactory.get_object(
            self.archive_dir, self.cluster, "elbencho", dict(_FULL_CONFIG)
        )
        assert isinstance(b, Elbencho)
        return b

    def test_custom_cmd_path(self):
        self.assertEqual("/opt/elbencho/bin/elbencho", self._make().cmd_path)

    def test_custom_auth(self):
        b = self._make()
        self.assertIn("config", b.auth)
        self.assertIn("access_key=AKID", b.auth["config"])

    def test_workloads_registered(self):
        self.assertTrue(self._make()._workloads.exist())

    def test_workload_names_preserved(self):
        names = self._make()._workloads.get_names()
        self.assertIn("write_small", names)
        self.assertIn("read_small", names)


class TestElbenchoValidation(unittest.TestCase):

    archive_dir = "/tmp"

    @classmethod
    def setUpClass(cls):
        settings.mock_initialize(config_file=INVARIANT_YAML)
        cls.cluster = Ceph.mockinit(settings.cluster)

    def _make(self, workloads) -> Elbencho:
        cfg = dict(_MINIMAL_CONFIG, workloads=workloads)
        b = benchmarkfactory.get_object(self.archive_dir, self.cluster, "elbencho", cfg)
        assert isinstance(b, Elbencho)
        return b

    def test_missing_mode_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._make({"no_mode": {"s3_bucket": "test-bucket"}})
        self.assertIn("missing required key 'mode'", str(ctx.exception))

    def test_missing_s3_bucket_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._make({"bad": {"mode": "write"}})
        self.assertIn("missing required key 's3_bucket'", str(ctx.exception))

    def test_non_integer_threads_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._make({"bad": {"mode": "write", "s3_bucket": "b", "threads": ["bad_value"]}})
        self.assertIn("threads value 'bad_value' is not an integer", str(ctx.exception))

    def test_non_integer_iodepth_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._make({"bad": {"mode": "write", "s3_bucket": "b", "iodepth": "not_a_number"}})
        self.assertIn("iodepth value 'not_a_number' is not an integer", str(ctx.exception))

    def test_string_integer_threads_accepted(self):
        # YAML may deserialise quoted integers as strings — "4" should be valid,
        # so construction must succeed without raising.
        b = self._make({"w": {"mode": "write", "s3_bucket": "b", "threads": ["4", 16]}})
        self.assertTrue(b._workloads.exist())

    def test_mixed_valid_invalid_threads_raises_on_bad_item(self):
        with self.assertRaises(ValueError) as ctx:
            self._make({"w": {"mode": "write", "s3_bucket": "b", "threads": [1, 4, "oops"]}})
        self.assertIn("threads value 'oops' is not an integer", str(ctx.exception))


class TestElbenchoExists(unittest.TestCase):

    archive_dir = "/tmp"

    @classmethod
    def setUpClass(cls):
        settings.mock_initialize(config_file=INVARIANT_YAML)
        cls.cluster = Ceph.mockinit(settings.cluster)

    def _make(self) -> Elbencho:
        b = benchmarkfactory.get_object(
            self.archive_dir, self.cluster, "elbencho", dict(_MINIMAL_CONFIG)
        )
        assert isinstance(b, Elbencho)
        return b

    def test_exists_false_when_archive_dir_absent(self):
        b = self._make()
        b.archive_dir = "/tmp/__cbt_elbencho_no_such_dir_xyzzy__"
        self.assertFalse(b.exists())

    def test_exists_true_when_archive_dir_present(self):
        b = self._make()
        with tempfile.TemporaryDirectory() as tmpdir:
            b.archive_dir = tmpdir
            self.assertTrue(b.exists())


class TestBenchmarkFactoryIntegration(unittest.TestCase):

    archive_dir = "/tmp"

    @classmethod
    def setUpClass(cls):
        settings.mock_initialize(config_file=INVARIANT_YAML)
        cls.cluster = Ceph.mockinit(settings.cluster)

    def test_get_object_returns_elbencho(self):
        b = benchmarkfactory.get_object(
            self.archive_dir, self.cluster, "elbencho", dict(_MINIMAL_CONFIG)
        )
        self.assertIsInstance(b, Elbencho)

    def test_unknown_benchmark_returns_none(self):
        b = benchmarkfactory.get_object(
            self.archive_dir, self.cluster, "no_such_benchmark", dict(_MINIMAL_CONFIG)
        )
        self.assertIsNone(b)

    def test_get_all_yields_single_instance_despite_list_valued_workload_params(self):
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
        objects = list(benchmarkfactory.get_all(self.archive_dir, self.cluster, 0))
        elbencho_objects = [o for o in objects if isinstance(o, Elbencho)]
        self.assertEqual(1, len(elbencho_objects))


# ---------------------------------------------------------------------------
# Run-loop tests
# ---------------------------------------------------------------------------

class TestRunLoop(unittest.TestCase):

    archive_dir = "/tmp"

    @classmethod
    def setUpClass(cls):
        settings.mock_initialize(config_file=INVARIANT_YAML)
        cls.cluster = Ceph.mockinit(settings.cluster)

    def setUp(self):
        # _run_workloads() wraps each command group in monitoring; stub it so
        # the run-loop tests don't touch real monitors.
        for target in ("monitoring.start", "monitoring.stop"):
            patcher = patch(target)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _make(self, workloads: dict) -> Elbencho:
        cfg = dict(
            _MINIMAL_CONFIG,
            cmd_path="/usr/local/bin/elbencho",
            auth={},
            workloads=workloads,
        )
        b = benchmarkfactory.get_object(self.archive_dir, self.cluster, "elbencho", cfg)
        assert isinstance(b, Elbencho)
        return b

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_call_count_matches_run_matrix(self, mock_exec, mock_mkdir):
        """2 blocksizes x 2 threads x 2 iodepths = 8 run_command calls."""
        mock_exec.return_value = []
        b = self._make({
            "w": {
                "s3_bucket": "bkt",
                "mode": "write",
                "blocksize": ["4k", "128k"],
                "threads": [1, 4],
                "iodepth": [1, 4],
            }
        })
        b._run_workloads()
        self.assertEqual(8, mock_exec.call_count)

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_run_dir_path_structure(self, mock_exec, mock_mkdir):
        mock_exec.return_value = []
        b = self._make({
            "w": {
                "s3_bucket": "bkt",
                "mode": "write",
                "blocksize": ["4k"],
                "threads": [16],
                "iodepth": [4],
            }
        })
        b._run_workloads()
        mkdir_calls = [c.args[1] for c in mock_mkdir.call_args_list]
        self.assertTrue(
            any("elbencho/write_4096/threads-016/iodepth-004" in d for d in mkdir_calls),
            f"Expected path segment not found in: {mkdir_calls}",
        )

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_stat_workload_skipped(self, mock_exec, mock_mkdir):
        mock_exec.return_value = []
        b = self._make({
            "w": {"s3_bucket": "bkt", "mode": "stat", "threads": [1], "iodepth": [1]}
        })
        b._run_workloads()
        mock_exec.assert_not_called()

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_scalar_threads_and_iodepth(self, mock_exec, mock_mkdir):
        mock_exec.return_value = []
        b = self._make({
            "w": {
                "s3_bucket": "bkt",
                "mode": "write",
                "blocksize": "4k",
                "threads": 4,
                "iodepth": 2,
            }
        })
        b._run_workloads()
        self.assertEqual(1, mock_exec.call_count)

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_cell_emits_matching_command_and_byte_run_dir(self, mock_exec, mock_mkdir):
        # The run loop must feed elbencho the human blocksize (128k) while
        # naming the run directory with the byte count (131072). Mixing the two
        # up is a real regression risk, so pin both from a single cell.
        mock_exec.return_value = []
        b = self._make({
            "w": {
                "s3_bucket": "bkt",
                "mode": "write",
                "blocksize": ["128k"],
                "threads": [8],
                "iodepth": [16],
                "size": "4g",
            }
        })
        b._run_workloads()

        self.assertEqual(1, mock_exec.call_count)
        cmd = mock_exec.call_args.args[1]
        self.assertIn("--block 128k", cmd)
        self.assertNotIn("--block 131072", cmd)  # byte count must not reach --block
        self.assertIn("--threads 8", cmd)
        self.assertIn("--iodepth 16", cmd)
        self.assertIn("--size 4g", cmd)
        self.assertTrue(cmd.endswith("s3://bkt"))

        run_dirs = [c.args[1] for c in mock_mkdir.call_args_list]
        self.assertTrue(
            any("elbencho/write_131072/threads-008/iodepth-016" in d for d in run_dirs),
            f"byte-based run dir not found in: {run_dirs}",
        )


class TestElbenchoNoPdsh(unittest.TestCase):

    archive_dir = "/tmp"

    @classmethod
    def setUpClass(cls):
        settings.mock_initialize(config_file=INVARIANT_YAML)
        cls.cluster = Ceph.mockinit(settings.cluster)

    def _make(self) -> Elbencho:
        cfg = dict(
            _MINIMAL_CONFIG,
            cmd_path="/usr/local/bin/elbencho",
            auth={},
            workloads={},
        )
        b = benchmarkfactory.get_object(self.archive_dir, self.cluster, "elbencho", cfg)
        assert isinstance(b, Elbencho)
        return b

    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "clean_remote_dir")
    def test_cleandir_uses_async_helpers(self, mock_clean, mock_mkdir):
        b = self._make()
        b.cleandir()
        clients = mock_clean.call_args.args[0]
        self.assertEqual(mock_clean.call_args.args[1], b.run_dir)
        self.assertEqual(mock_mkdir.call_args.args[0], clients)
        self.assertEqual(mock_mkdir.call_args.args[1], b.run_dir)

    @patch.object(AsyncSSHExecutor, "run_command")
    def test_dropcaches_uses_remote_executor(self, mock_exec):
        mock_exec.return_value = []
        b = self._make()
        b.dropcaches()
        self.assertEqual(2, mock_exec.call_count)
        commands = [c.args[1] for c in mock_exec.call_args_list]
        self.assertIn("sync", commands)
        self.assertTrue(any("drop_caches" in cmd for cmd in commands))

    @patch.object(AsyncSSHExecutor, "sync_files")
    @patch.object(AsyncSSHExecutor, "make_remote_dir")
    @patch.object(AsyncSSHExecutor, "run_command")
    def test_run_does_not_call_pdsh(self, mock_exec, mock_mkdir, mock_sync):
        import common as _common
        mock_exec.return_value = []
        b = self._make()
        with patch.object(b, "dropcaches"), \
             patch.object(b.cluster, "dump_config"), \
             patch.object(b.cluster, "set_osd_param"), \
             patch("monitoring.start"), \
             patch("monitoring.stop"), \
             patch.object(b, "_run_workloads"):
            with patch.object(_common, "pdsh", side_effect=AssertionError("pdsh called")):
                b.run()


if __name__ == "__main__":
    unittest.main()
