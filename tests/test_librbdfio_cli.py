"""
CLI flag tests for LibrbdFio._run_io_loop().

Representative config: volumes_per_client=1, iodepth=16, numjobs=1,
mode=write, op_size=4194304, vol_size=65536, time=60, ramp=5.

Known deliberate differences from the old code are labelled D1-D6
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from benchmark.librbdfio import LibrbdFio

_FQDN_CMD = "hostname -f"
_FAKE_CLIENT = "fake-client"
_FAKE_CLUSTER_SETTINGS: dict[str, Any] = {"tmp_dir": "/tmp/cbt-test", "osd_ra": "0"}


def _mock_cluster() -> MagicMock:
    cluster: MagicMock = MagicMock()
    cluster.config = {}
    cluster.mnt_dir = "/srv"
    return cluster


def _make(extra: Optional[dict[str, Any]] = None) -> LibrbdFio:
    config: dict[str, Any] = {
        "iteration": 0,
        "volumes_per_client": 1,
        "iodepth": 16,
        "numjobs": 1,
        "mode": "write",
        "op_size": 4194304,
        "vol_size": 65536,
        "time": 60,
        "ramp": 5,
    }
    if extra:
        config.update(extra)
    with (
        patch("settings.cluster", _FAKE_CLUSTER_SETTINGS),
        patch("settings.getnodes", return_value=_FAKE_CLIENT),
        patch("common.get_osd_ra", return_value="0"),
    ):
        bm = LibrbdFio(archive_dir="/tmp/archive", cluster=_mock_cluster(), config=config)
    bm.cmd_path_full = "/usr/bin/fio"
    return bm


def _cli(extra: Optional[dict[str, Any]] = None) -> str:
    """Return the single pdsh CLI string produced by _run_io_loop()."""
    bm = _make(extra)
    pdsh_mock = MagicMock()
    pdsh_mock.return_value = MagicMock()
    with (
        patch("common.pdsh", pdsh_mock),
        patch("common.get_fqdn_cmd", return_value=_FQDN_CMD),
    ):
        bm._run_io_loop()  # pylint: disable=protected-access
    return str(pdsh_mock.call_args_list[0].args[1])


class TestLibrbdFioCli(unittest.TestCase):  # pylint: disable=too-many-public-methods
    """CLI flag assertions for LibrbdFio._run_io_loop()."""

    # --- required flags -------------------------------------------------------

    def test_sudo_prefix_by_default(self) -> None:
        """Command is prefixed with sudo when no_sudo is not set."""
        self.assertTrue(_cli().startswith("sudo "))

    def test_rbd_ioengine(self) -> None:
        """--ioengine=rbd is emitted."""
        self.assertIn("--ioengine=rbd", _cli())

    def test_rw_mode(self) -> None:
        """--rw reflects the configured mode."""
        self.assertIn("--rw=write", _cli())

    def test_block_size(self) -> None:
        """--bs reflects op_size."""
        self.assertIn("--bs=4194304", _cli())

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
        """--invalidate=0 is always present (D5)."""
        self.assertIn("--invalidate=0", _cli())

    def test_end_fsync_always_0(self) -> None:
        """--end_fsync=0 is always present (D4)."""
        self.assertIn("--end_fsync=0", _cli())

    def test_output_format_json_normal(self) -> None:
        """--output-format=json,normal is the default."""
        self.assertIn("--output-format=json,normal", _cli())

    def test_runtime_when_configured(self) -> None:
        """--runtime is emitted when time is configured."""
        self.assertIn("--runtime=60", _cli())

    def test_ramp_time_when_configured(self) -> None:
        """--ramp_time is emitted when ramp is configured."""
        self.assertIn("--ramp_time=5", _cli())

    def test_pool_name(self) -> None:
        """--pool reflects the configured pool name."""
        self.assertIn("--pool=cbt-librbdfio", _cli())

    def test_write_iops_log_present(self) -> None:
        """--write_iops_log is present by default."""
        self.assertIn("--write_iops_log=", _cli())

    def test_write_bw_log_present(self) -> None:
        """--write_bw_log is present by default."""
        self.assertIn("--write_bw_log=", _cli())

    def test_write_lat_log_present(self) -> None:
        """--write_lat_log is present by default."""
        self.assertIn("--write_lat_log=", _cli())

    def test_output_redirect_present(self) -> None:
        """Output is redirected to a canonical output.N file."""
        cli = _cli()
        self.assertIn("> ", cli)
        self.assertIn("output.0", cli)

    # --- flags absent when not configured ------------------------------------

    def test_no_norandommap_by_default(self) -> None:
        """--norandommap is absent when not configured."""
        self.assertNotIn("--norandommap", _cli())

    def test_no_random_distribution_by_default(self) -> None:
        """--random_distribution is absent when not configured."""
        self.assertNotIn("--random_distribution", _cli())

    def test_no_rate_iops_by_default(self) -> None:
        """--rate_iops is absent when not configured."""
        self.assertNotIn("--rate_iops", _cli())

    def test_no_log_avg_msec_by_default(self) -> None:
        """--log_avg_msec is absent when not configured."""
        self.assertNotIn("--log_avg_msec", _cli())

    def test_no_time_based_by_default(self) -> None:
        """--time_based is absent when recovery_test is not configured."""
        self.assertNotIn("--time_based", _cli())

    # --- D2: --runtime=None / --ramp_time=None never emitted -----------------

    def test_runtime_none_never_emitted(self) -> None:
        """--runtime=None and --ramp_time=None are never emitted (D2).

        Old RbdFio stored str(config.get('time', None)) -> literal "None".
        New code uses Optional[int] and omits the flag entirely.
        """
        cli = _cli({"time": None, "ramp": None})
        self.assertNotIn("--runtime=None", cli)
        self.assertNotIn("--ramp_time=None", cli)

    def test_no_runtime_when_time_not_configured(self) -> None:
        """--runtime is absent entirely when time is not configured (D1)."""
        self.assertNotIn("--runtime=", _cli({"time": None}))

    def test_no_ramp_time_when_ramp_not_configured(self) -> None:
        """--ramp_time is absent entirely when ramp is not configured (D1)."""
        self.assertNotIn("--ramp_time=", _cli({"ramp": None}))

    # --- D3: --log_avg_msec before the > redirect ----------------------------

    def test_log_avg_msec_before_redirect(self) -> None:
        """--log_avg_msec appears before the > redirect (D3).

        Old code appended it after the shell redirect, silently ignoring it.
        """
        cli = _cli({"log_avg_msec": 500})
        self.assertIn("--log_avg_msec=500", cli)
        self.assertLess(cli.index("--log_avg_msec=500"), cli.index("> "))

    # --- randrw ---------------------------------------------------------------

    def test_randrw_emits_rwmix_flags(self) -> None:
        """randrw mode emits --rwmixread and --rwmixwrite from config."""
        cli = _cli({"mode": "randrw", "rwmixread": 70})
        self.assertIn("--rw=randrw", cli)
        self.assertIn("--rwmixread=70", cli)
        self.assertIn("--rwmixwrite=30", cli)

    # --- no_sudo drops sudo prefix -------------------------------------------

    def test_no_sudo_drops_sudo_prefix(self) -> None:
        """sudo prefix is absent when no_sudo=True."""
        self.assertFalse(_cli({"no_sudo": True}).startswith("sudo "))

    # --- recovery_test adds --time_based -------------------------------------

    def test_recovery_test_adds_time_based(self) -> None:
        """--time_based is added when recovery_test is in cluster config."""
        bm = _make()
        bm.cluster.config = {"recovery_test": "blocking"}
        pdsh_mock = MagicMock()
        pdsh_mock.return_value = MagicMock()
        with (
            patch("common.pdsh", pdsh_mock),
            patch("common.get_fqdn_cmd", return_value=_FQDN_CMD),
        ):
            bm._run_io_loop()  # pylint: disable=protected-access
        cli = str(pdsh_mock.call_args_list[0].args[1])
        self.assertIn("--time_based", cli)
