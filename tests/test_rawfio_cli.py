"""
CLI flag tests for RawFio._run_io_loop().

Representative config: block_devices=/dev/vdb, concurrent_procs=1,
iodepth=16, numjobs=1, mode=write, ioengine=libaio, op_size=4096000,
vol_size=1024, time=60, ramp=0.

RawFio always uses fio_out_format=json (not json,normal).

Known deliberate differences from the old code are labelled D1-D6
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from benchmark.rawfio import RawFio

_FAKE_CLIENT = "fake-client"
_FAKE_CLUSTER_SETTINGS: dict[str, Any] = {"tmp_dir": "/tmp/cbt-test", "osd_ra": "0"}


def _mock_cluster() -> MagicMock:
    cluster: MagicMock = MagicMock()
    cluster.config = {}
    cluster.mnt_dir = "/srv"
    return cluster


def _make(extra: Optional[dict[str, Any]] = None) -> RawFio:
    config: dict[str, Any] = {
        "iteration": 0,
        "block_devices": "/dev/vdb",
        "concurrent_procs": 1,
        "iodepth": 16,
        "numjobs": 1,
        "mode": "write",
        "ioengine": "libaio",
        "op_size": 4096000,
        "vol_size": 1024,
        "time": 60,
        "ramp": 0,
    }
    if extra:
        config.update(extra)
    with (
        patch("settings.cluster", _FAKE_CLUSTER_SETTINGS),
        patch("settings.getnodes", return_value=_FAKE_CLIENT),
        patch("common.get_osd_ra", return_value="0"),
    ):
        bm = RawFio(archive_dir="/tmp/archive", cluster=_mock_cluster(), config=config)
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


class TestRawFioCli(unittest.TestCase):  # pylint: disable=too-many-public-methods
    """CLI flag assertions for RawFio._run_io_loop()."""

    # --- required flags -------------------------------------------------------

    def test_libaio_ioengine(self) -> None:
        """--ioengine=libaio is emitted."""
        self.assertIn("--ioengine=libaio", _cli())

    def test_rw_mode(self) -> None:
        """--rw reflects the configured mode."""
        self.assertIn("--rw=write", _cli())

    def test_block_size(self) -> None:
        """--bs reflects op_size."""
        self.assertIn("--bs=4096000", _cli())

    def test_iodepth(self) -> None:
        """--iodepth reflects the configured value."""
        self.assertIn("--iodepth=16", _cli())

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
        """--invalidate=0 is always present; old RawFio did not emit it (D5)."""
        self.assertIn("--invalidate=0", _cli())

    def test_end_fsync_always_0(self) -> None:
        """--end_fsync=0 is always present; old RawFio did not emit it (D4)."""
        self.assertIn("--end_fsync=0", _cli())

    def test_output_format_json_only(self) -> None:
        """RawFio hardcodes fio_out_format=json, not json,normal."""
        self.assertIn("--output-format=json", _cli())
        self.assertNotIn("--output-format=json,normal", _cli())

    def test_runtime_when_configured(self) -> None:
        """--runtime is emitted when time is configured."""
        self.assertIn("--runtime=60", _cli())

    def test_ramp_time_0_when_configured(self) -> None:
        """--ramp_time=0 is emitted when ramp=0 is configured."""
        self.assertIn("--ramp_time=0", _cli())

    def test_size_from_vol_size(self) -> None:
        """--size is derived from vol_size * 0.9 (vol_size=1024 -> 922M)."""
        self.assertIn("--size=922M", _cli())

    def test_block_device_path_in_command(self) -> None:
        """The block device path appears as the fio target."""
        self.assertIn("/dev/vdb", _cli())

    def test_output_redirect_present(self) -> None:
        """Output is redirected to a canonical output.N file."""
        cli = _cli()
        self.assertIn("> ", cli)
        self.assertIn("output.0", cli)

    # --- D1: no --runtime / --ramp_time when not configured ------------------

    def test_no_runtime_when_time_not_configured(self) -> None:
        """--runtime is absent entirely when time is not configured (D1).

        Old RawFio stored str(config.get('time', '300')) and always emitted it.
        New code uses Optional[int] and omits the flag when None.
        """
        self.assertNotIn("--runtime=", _cli({"time": None}))

    def test_no_ramp_time_when_ramp_not_configured(self) -> None:
        """--ramp_time is absent entirely when ramp is not configured (D1)."""
        self.assertNotIn("--ramp_time=", _cli({"ramp": None}))

    # --- startdelay -----------------------------------------------------------

    def test_startdelay_when_configured(self) -> None:
        """--startdelay is emitted when configured."""
        self.assertIn("--startdelay=5", _cli({"startdelay": 5}))

    def test_no_startdelay_by_default(self) -> None:
        """--startdelay is absent when not configured."""
        self.assertNotIn("--startdelay", _cli())

    # --- logging flags --------------------------------------------------------

    def test_write_iops_log_present_by_default(self) -> None:
        """--write_iops_log is present by default."""
        self.assertIn("--write_iops_log=", _cli())

    def test_write_bw_log_present_by_default(self) -> None:
        """--write_bw_log is present by default."""
        self.assertIn("--write_bw_log=", _cli())

    def test_write_lat_log_present_by_default(self) -> None:
        """--write_lat_log is present by default."""
        self.assertIn("--write_lat_log=", _cli())

    def test_iops_log_suppressed(self) -> None:
        """--write_iops_log is absent when log_iops=False."""
        self.assertNotIn("--write_iops_log=", _cli({"log_iops": False}))

    def test_bw_log_suppressed(self) -> None:
        """--write_bw_log is absent when log_bw=False."""
        self.assertNotIn("--write_bw_log=", _cli({"log_bw": False}))

    def test_lat_log_suppressed(self) -> None:
        """--write_lat_log is absent when log_lat=False."""
        self.assertNotIn("--write_lat_log=", _cli({"log_lat": False}))

    # --- D3: log_avg_msec before > -------------------------------------------

    def test_log_avg_msec_before_redirect(self) -> None:
        """--log_avg_msec appears before the > redirect (D3).

        Old code appended it after the shell redirect, silently ignoring it.
        """
        cli = _cli({"log_avg_msec": 1000})
        self.assertIn("--log_avg_msec=1000", cli)
        self.assertLess(cli.index("--log_avg_msec=1000"), cli.index("> "))

    # --- setup_targets() prefill command --------------------------------------

    def test_setup_targets_command(self) -> None:
        """setup_targets issues prefill command with single sudo and --ioengine."""
        bm = _make({"op_size": 4194304, "vol_size": 1024, "ioengine": "libaio"})
        pdsh_mock = MagicMock()
        proc_mock = MagicMock()
        pdsh_mock.return_value = proc_mock
        with patch("common.pdsh", pdsh_mock):
            bm.setup_targets()
        cmd = str(pdsh_mock.call_args[0][1])
        self.assertTrue(cmd.startswith("sudo /usr/bin/fio "), f"Expected 'sudo /usr/bin/fio ', got: {cmd!r}")
        self.assertFalse(cmd.startswith("sudo sudo"), f"Got double sudo in: {cmd!r}")
        self.assertIn("--ioengine=libaio", cmd)
        self.assertNotIn(" -ioengine=", cmd)
        self.assertIn("--name=/dev/vdb", cmd)
        self.assertIn("--output-format=json", cmd)
