"""
Tests for FIO CLI command generation.

Scope:
  - RbdFioCommand.get() — workloads-path CLI for the librbd engine
  - Class-level state isolation between RbdFioCommand instances
  - Fio._apply_blocksize_option() — bssplit > bsrange > bs > op_size precedence
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from benchmark.fio import Fio
from command.rbd_fio_command import RbdFioCommand

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FQDN_CMD = "hostname -f"  # value returned by the common.get_fqdn_cmd() mock


def _make_fio(extra: Optional[dict[str, Any]] = None) -> Fio:
    """Build a Fio stub with stable defaults — enough to test _apply_blocksize_option()."""
    base_config: dict[str, Any] = {
        "mode": "write",
        "op_size": 4194304,
        "iodepth": 16,
        "numjobs": 1,
        "end_fsync": 0,
        "size": 4096,
        "procs_per_endpoint": 1,
    }
    if extra:
        base_config.update(extra)

    cluster_mock = MagicMock()
    cluster_mock.config = {}

    with patch("settings.getnodes", return_value="fake-client"), patch("common.get_fqdn_cmd", return_value=_FQDN_CMD):
        bm = Fio.__new__(Fio)
        with patch("benchmark.benchmark.Benchmark.__init__", return_value=None):
            bm.archive_dir = "/tmp/archive/"
            bm.run_dir = "/tmp/run/"
            bm.osd_ra = "0"
            bm.osd_ra_changed = False
            bm._base_archive_directory = "/tmp/archive/"
            bm._create_report = False
            bm.log_iops = True
            bm.log_bw = True
            bm.log_lat = True
            bm.cmd_path_full = ""
            bm.config = base_config
            bm.cluster = cluster_mock
            workloads_stub = MagicMock()
            workloads_stub.exist.return_value = False
            bm._workloads = workloads_stub
            Fio.__init__(bm, "/tmp/archive/", cluster_mock, base_config)
    return bm


# ---------------------------------------------------------------------------
# Fio._apply_blocksize_option() — bssplit > bsrange > bs > op_size precedence
# ---------------------------------------------------------------------------


class TestFioApplyBlocksizeOption(unittest.TestCase):
    """
    Fio._apply_blocksize_option() resolves the winning block-size key.

    Priority (highest to lowest): bssplit > bsrange > bs > op_size fallback.
    Exactly one of these four keys must appear in the output options dict;
    the others must be absent.
    """

    def _options(self, **kwargs: Any) -> dict[str, str]:
        """Return the options dict produced by _apply_blocksize_option()."""
        bm = _make_fio(dict(kwargs))
        out: dict[str, str] = {}
        bm._apply_blocksize_option(out)
        return out

    def test_op_size_used_when_no_other_key(self) -> None:
        opts = self._options(bs=None, bssplit=None, bsrange=None, op_size=4194304)
        self.assertIn("op_size", opts)
        self.assertNotIn("bs", opts)
        self.assertNotIn("bsrange", opts)
        self.assertNotIn("bssplit", opts)

    def test_bs_wins_over_op_size(self) -> None:
        opts = self._options(bs="512k", bssplit=None, bsrange=None, op_size=4194304)
        self.assertIn("bs", opts)
        self.assertEqual(opts["bs"], "512k")
        self.assertNotIn("op_size", opts)
        self.assertNotIn("bsrange", opts)
        self.assertNotIn("bssplit", opts)

    def test_bsrange_wins_over_bs(self) -> None:
        opts = self._options(bs="512k", bssplit=None, bsrange="4k-512k", op_size=4194304)
        self.assertIn("bsrange", opts)
        self.assertEqual(opts["bsrange"], "4k-512k")
        self.assertNotIn("bs", opts)
        self.assertNotIn("op_size", opts)
        self.assertNotIn("bssplit", opts)

    def test_bssplit_wins_over_bsrange_and_bs(self) -> None:
        opts = self._options(bs="512k", bssplit="4k/50:512k/50", bsrange="4k-512k", op_size=4194304)
        self.assertIn("bssplit", opts)
        self.assertEqual(opts["bssplit"], "4k/50:512k/50")
        self.assertNotIn("bs", opts)
        self.assertNotIn("bsrange", opts)
        self.assertNotIn("op_size", opts)

    def test_exactly_one_key_emitted(self) -> None:
        """Each combination of inputs produces exactly one block-size key."""
        cases: list[dict[str, Any]] = [
            {"bs": None, "bssplit": None, "bsrange": None},
            {"bs": "4k", "bssplit": None, "bsrange": None},
            {"bs": "4k", "bssplit": None, "bsrange": "4k-512k"},
            {"bs": "4k", "bssplit": "4k/50:512k/50", "bsrange": "4k-512k"},
        ]
        bs_keys = {"op_size", "bs", "bsrange", "bssplit"}
        for case in cases:
            with self.subTest(**case):
                opts = self._options(op_size=4096, **case)
                present = bs_keys & set(opts.keys())
                self.assertEqual(len(present), 1, f"Expected exactly one bs key, got {present!r} for {case!r}")


# ---------------------------------------------------------------------------
# RbdFioCommand — workloads-path CLI generation
# ---------------------------------------------------------------------------


class TestRbdFioCommandGeneration(unittest.TestCase):
    """Characterize the CLI produced by the RbdFioCommand (workloads path)."""

    def _make_cmd(self, overrides: Optional[dict[str, Any]] = None) -> str:
        base_options: dict[str, Any] = {
            "target_number": "0",
            "name": "test-workload",
            "mode": "write",
            "op_size": "4194304",
            "iodepth": "16",
            "numjobs": "1",
            "end_fsync": "0",
            "fio_out_format": "json,normal",
            "poolname": "cbt-librbdfio",
            "rbdname": "cbt-rbdfio",
            "procs_per_volume": "1",
            "no_sudo": "false",
        }
        if overrides:
            base_options.update(overrides)

        with patch("common.get_fqdn_cmd", return_value=_FQDN_CMD):
            rbd_cmd = RbdFioCommand(base_options, "/tmp/workload_output/")
        rbd_cmd.set_executable("/usr/bin/fio")
        with patch("common.get_fqdn_cmd", return_value=_FQDN_CMD):
            return rbd_cmd.get()

    def test_starts_with_sudo(self) -> None:
        cmd = self._make_cmd()
        self.assertTrue(cmd.startswith("sudo "), f"Expected 'sudo ' prefix, got: {cmd!r}")

    def test_no_sudo_option(self) -> None:
        cmd = self._make_cmd({"no_sudo": "true"})
        self.assertFalse(cmd.startswith("sudo "))

    def test_ioengine_rbd(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--ioengine=rbd", cmd)

    def test_clientname_admin(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--clientname=admin", cmd)

    def test_rw_mode(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--rw=write", cmd)

    def test_bs_option(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--bs=4194304", cmd)

    def test_pool_option(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--pool=cbt-librbdfio", cmd)

    def test_invalidate_zero(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--invalidate=0", cmd)

    def test_direct_default_1(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--direct=1", cmd)

    def test_time_based_flag(self) -> None:
        cmd = self._make_cmd({"time_based": "true"})
        self.assertIn("--time_based", cmd)

    def test_runtime_flag(self) -> None:
        cmd = self._make_cmd({"time": "60"})
        self.assertIn("--runtime=60", cmd)

    def test_ramp_time_flag(self) -> None:
        cmd = self._make_cmd({"ramp": "10"})
        self.assertIn("--ramp_time=10", cmd)

    def test_randrw_rwmix_flags(self) -> None:
        cmd = self._make_cmd({"mode": "randrw", "rwmixread": "70"})
        self.assertIn("--rwmixread=70", cmd)
        self.assertIn("--rwmixwrite=30", cmd)

    def test_norandommap_flag(self) -> None:
        cmd = self._make_cmd({"norandommap": "1"})
        self.assertIn("--norandommap", cmd)

    def test_rate_iops_flag(self) -> None:
        cmd = self._make_cmd({"rate_iops": "5000"})
        self.assertIn("--rate_iops=5000", cmd)

    def test_random_distribution_flag(self) -> None:
        cmd = self._make_cmd({"random_distribution": "zipf:1.2"})
        self.assertIn("--random_distribution=zipf:1.2", cmd)

    def test_log_avg_msec_flag(self) -> None:
        cmd = self._make_cmd({"log_avg_msec": "500"})
        self.assertIn("--log_avg_msec=500", cmd)

    def test_write_iops_log_included(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--write_iops_log=", cmd)

    def test_write_bw_log_included(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--write_bw_log=", cmd)

    def test_write_lat_log_included(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("--write_lat_log=", cmd)

    def test_output_file_redirect(self) -> None:
        cmd = self._make_cmd()
        self.assertIn("> ", cmd)
        self.assertIn("output.0", cmd)

    def test_benchmark_type_rbdfio(self) -> None:
        options: dict[str, Any] = {
            "target_number": "0",
            "name": "w",
            "mode": "write",
            "op_size": "4096",
            "iodepth": "8",
            "numjobs": "1",
            "end_fsync": "0",
            "fio_out_format": "json,normal",
            "poolname": "pool",
            "rbdname": "rbd",
            "procs_per_volume": "1",
            "no_sudo": "false",
        }
        with patch("common.get_fqdn_cmd", return_value=_FQDN_CMD):
            rbd_cmd = RbdFioCommand(options, "/tmp/out/")
        self.assertEqual(rbd_cmd.benchmark, "rbdfio")


class TestRbdFioCommandNoClassStateMutation(unittest.TestCase):
    """Regression test for the _RBD_DEFAULT_OPTIONS class-level mutation bug.

    Creating two RbdFioCommand instances in sequence must produce independent
    commands.  Previously, _parse_ioengine_specific_parameters() aliased the
    class-level dict directly (``rbd_options = self._RBD_DEFAULT_OPTIONS``),
    so setting ``rbd_options["rbdname"]`` on the first instance permanently
    mutated the shared dict.  Subsequent instances would inherit the stale
    rbdname from the first call instead of computing their own.
    """

    def _make(self, target_number: str, rbdname: str) -> RbdFioCommand:
        options: dict[str, str] = {
            "target_number": target_number,
            "name": "wl",
            "mode": "write",
            "op_size": "4194304",
            "iodepth": "16",
            "numjobs": "1",
            "end_fsync": "0",
            "fio_out_format": "json,normal",
            "poolname": "test-pool",
            "rbdname": rbdname,
            "procs_per_volume": "1",
            "no_sudo": "false",
        }
        with patch("common.get_fqdn_cmd", return_value="hostname -f"):
            cmd = RbdFioCommand(options, "/tmp/out/")
        cmd.set_executable("/usr/bin/fio")
        return cmd

    def test_second_instance_has_independent_rbdname(self) -> None:
        """The rbdname for instance 1 must not bleed into instance 0's class dict."""
        cmd0 = self._make("0", "vol-a")
        cmd1 = self._make("1", "vol-b")

        with patch("common.get_fqdn_cmd", return_value="hostname -f"):
            out0 = cmd0.get()
            out1 = cmd1.get()

        self.assertIn("vol-a", out0, f"cmd0 should reference vol-a, got: {out0!r}")
        self.assertIn("vol-b", out1, f"cmd1 should reference vol-b, got: {out1!r}")
        self.assertNotIn("vol-b", out0, f"cmd0 must not contain vol-b (class state pollution), got: {out0!r}")
        self.assertNotIn("vol-a", out1, f"cmd1 must not contain vol-a (class state pollution), got: {out1!r}")

    def test_class_dict_unchanged_after_instantiation(self) -> None:
        """_RBD_DEFAULT_OPTIONS must contain only its original two keys after use."""
        original_keys = set(RbdFioCommand._RBD_DEFAULT_OPTIONS.keys())
        self._make("0", "vol-a")
        self._make("1", "vol-b")
        self.assertEqual(
            set(RbdFioCommand._RBD_DEFAULT_OPTIONS.keys()),
            original_keys,
            "Class-level _RBD_DEFAULT_OPTIONS must not be mutated by instance creation",
        )


if __name__ == "__main__":
    unittest.main()
