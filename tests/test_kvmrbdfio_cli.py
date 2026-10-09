"""
CLI flag tests for KvmRbdFio._run_io_loop().

Representative config derived from example/example-kvmrbdfio.yaml:
block_devices=/dev/vdb, concurrent_procs=1, iodepth=2, numjobs=1,
mode=randwrite, op_size=4096, vol_size=64, time=60, ramp=20,
startdelay=10, rate_iops=2.

Known deliberate differences from the old code are labelled D1-D6
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from benchmark.kvmrbdfio import KvmRbdFio

_FAKE_CLIENT = "fake-client"
_FAKE_CLUSTER_SETTINGS: dict[str, Any] = {"tmp_dir": "/tmp/cbt-test", "osd_ra": "0"}


def _mock_cluster() -> MagicMock:
    cluster: MagicMock = MagicMock()
    cluster.config = {}
    cluster.mnt_dir = "/srv"
    return cluster


def _make(extra: Optional[dict[str, Any]] = None) -> KvmRbdFio:
    config: dict[str, Any] = {
        "iteration": 0,
        "block_devices": "/dev/vdb",
        "concurrent_procs": 1,
        "iodepth": 2,
        "numjobs": 1,
        "mode": "randwrite",
        "op_size": 4096,
        "vol_size": 64,
        "time": 60,
        "ramp": 20,
        "startdelay": 10,
        "rate_iops": 2,
    }
    if extra:
        config.update(extra)
    with (
        patch("settings.cluster", _FAKE_CLUSTER_SETTINGS),
        patch("settings.getnodes", return_value=_FAKE_CLIENT),
        patch("common.get_osd_ra", return_value="0"),
    ):
        bm = KvmRbdFio(archive_dir="/tmp/archive", cluster=_mock_cluster(), config=config)
    bm.cmd_path_full = "/usr/bin/fio"
    return bm


def _cli(extra: Optional[dict[str, Any]] = None) -> str:
    """Return the first pdsh CLI string produced by _run_io_loop()."""
    bm = _make(extra)
    pdsh_mock = MagicMock()
    pdsh_mock.return_value = MagicMock()
    with patch("common.pdsh", pdsh_mock):
        bm._run_io_loop()  # pylint: disable=protected-access
    return str(pdsh_mock.call_args_list[0].args[1])


class TestKvmRbdFioCli(unittest.TestCase):  # pylint: disable=too-many-public-methods
    """CLI flag assertions for KvmRbdFio._run_io_loop()."""

    # --- required flags -------------------------------------------------------

    def test_libaio_ioengine(self) -> None:
        """--ioengine=libaio is emitted."""
        self.assertIn("--ioengine=libaio", _cli())

    def test_rw_mode(self) -> None:
        """--rw reflects the configured mode."""
        self.assertIn("--rw=randwrite", _cli())

    def test_block_size(self) -> None:
        """--bs reflects op_size."""
        self.assertIn("--bs=4096", _cli())

    def test_iodepth(self) -> None:
        """--iodepth reflects the configured value."""
        self.assertIn("--iodepth=2", _cli())

    def test_numjobs(self) -> None:
        """--numjobs reflects the configured value."""
        self.assertIn("--numjobs=1", _cli())

    def test_direct_default_1(self) -> None:
        """--direct=1 is emitted by default."""
        self.assertIn("--direct=1", _cli())

    def test_direct_configurable(self) -> None:
        """--direct reflects the configured value."""
        self.assertIn("--direct=0", _cli({"direct": 0}))
        self.assertNotIn("--direct=1", _cli({"direct": 0}))

    def test_invalidate_always_0(self) -> None:
        """--invalidate=0 is always present; old KvmRbdFio did not emit it (D5)."""
        self.assertIn("--invalidate=0", _cli())

    def test_end_fsync_always_0(self) -> None:
        """--end_fsync=0 is always present; old KvmRbdFio did not emit it (D4)."""
        self.assertIn("--end_fsync=0", _cli())

    def test_runtime_when_configured(self) -> None:
        """--runtime is emitted when time is configured."""
        self.assertIn("--runtime=60", _cli())

    def test_ramp_time_when_configured(self) -> None:
        """--ramp_time is emitted when ramp is configured."""
        self.assertIn("--ramp_time=20", _cli())

    def test_startdelay_when_configured(self) -> None:
        """--startdelay is emitted when configured."""
        self.assertIn("--startdelay=10", _cli())

    def test_rate_iops_when_configured(self) -> None:
        """--rate_iops is emitted when configured."""
        self.assertIn("--rate_iops=2", _cli())

    def test_size_from_vol_size(self) -> None:
        """--size is derived from vol_size * 0.9 (vol_size=64 -> 58M)."""
        self.assertIn("--size=58M", _cli())

    def test_fio_img_path_in_command(self) -> None:
        """The fio image file path appears as the target."""
        self.assertIn("fio0.img", _cli())

    def test_output_redirect_present(self) -> None:
        """Output is redirected to a canonical output.N file."""
        cli = _cli()
        self.assertIn("> ", cli)
        self.assertIn("output.0", cli)

    # --- D1: no --runtime / --ramp_time when not configured ------------------

    def test_no_runtime_when_time_not_configured(self) -> None:
        """--runtime is absent entirely when time is not configured (D1).

        Old KvmRbdFio always emitted --runtime; new code omits it when None.
        """
        self.assertNotIn("--runtime=", _cli({"time": None}))

    def test_no_ramp_time_when_ramp_not_configured(self) -> None:
        """--ramp_time is absent entirely when ramp is not configured (D1)."""
        self.assertNotIn("--ramp_time=", _cli({"ramp": None}))

    # --- randrw ---------------------------------------------------------------

    def test_randrw_emits_rwmix_flags(self) -> None:
        """randrw mode emits --rwmixread and --rwmixwrite from config."""
        cli = _cli({"mode": "randrw", "rwmixread": 40})
        self.assertIn("--rw=randrw", cli)
        self.assertIn("--rwmixread=40", cli)
        self.assertIn("--rwmixwrite=60", cli)

    # --- recovery_test adds --time_based -------------------------------------

    def test_recovery_test_adds_time_based(self) -> None:
        """--time_based is added when recovery_test is in cluster config."""
        bm = _make()
        bm.cluster.config = {"recovery_test": "blocking"}
        pdsh_mock = MagicMock()
        pdsh_mock.return_value = MagicMock()
        with patch("common.pdsh", pdsh_mock):
            bm._run_io_loop()  # pylint: disable=protected-access
        cli = str(pdsh_mock.call_args_list[0].args[1])
        self.assertIn("--time_based", cli)

    # --- setup_targets() prefill command & fqdn paths ------------------------

    def test_setup_targets_command(self) -> None:
        """setup_targets uses --ioengine=sync and consistent fqdn mount paths."""
        bm = _make()
        pdsh_mock = MagicMock()
        proc_mock = MagicMock()
        pdsh_mock.return_value = proc_mock
        with (
            patch("common.pdsh", pdsh_mock),
            patch("common.get_fqdn_cmd", return_value="hostname -f"),
        ):
            bm.setup_targets()
        # calls: mkfs, mkdir, mount, pre_cmd
        calls = [str(c[0][1]) for c in pdsh_mock.call_args_list]
        pre_cmd = calls[-1]
        self.assertIn("--ioengine=sync", pre_cmd)
        self.assertNotIn(" -ioengine=", pre_cmd)
        self.assertIn("/srv/rbdfio-`hostname -f`-vdb/fio0.img", pre_cmd)
        self.assertNotIn("hostname -s", pre_cmd)
