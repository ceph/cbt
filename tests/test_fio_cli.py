"""
CLI flag tests for Fio._run_io_loop().

Representative config: endpoint_type=directory, endpoints_per_client=1,
iodepth=16, numjobs=1, mode=write, ioengine=libaio, op_size=4194304,
size=4096, logging=True, time=60, ramp=5.

Known deliberate differences from the old code are labelled D1-D6
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from benchmark.fio import Fio

_FAKE_CLIENT = "fake-client"
_FAKE_CLUSTER_SETTINGS: dict[str, Any] = {"tmp_dir": "/tmp/cbt-test", "osd_ra": "0"}


def _mock_cluster() -> MagicMock:
    cluster: MagicMock = MagicMock()
    cluster.config = {}
    cluster.mnt_dir = "/srv"
    return cluster


def _make(extra: Optional[dict[str, Any]] = None) -> Fio:
    config: dict[str, Any] = {
        "iteration": 0,
        "size": 4096,
        "iodepth": 16,
        "numjobs": 1,
        "mode": "write",
        "ioengine": "libaio",
        "op_size": 4194304,
        "logging": True,
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
        bm = Fio(archive_dir="/tmp/archive", cluster=_mock_cluster(), config=config)
    bm.cmd_path_full = "/usr/bin/fio"
    # Stub endpoint attributes normally set by initialize_endpoints()
    bm.endpoint_type = "directory"
    bm.endpoints_per_client = 1
    bm.endpoints = ["/mnt/ep0"]
    return bm


def _cli(extra: Optional[dict[str, Any]] = None) -> str:
    """Return the single pdsh CLI string produced by _run_io_loop()."""
    bm = _make(extra)
    pdsh_mock = MagicMock()
    pdsh_mock.return_value = MagicMock()
    with patch("common.pdsh", pdsh_mock):
        bm._run_io_loop()  # pylint: disable=protected-access
    return str(pdsh_mock.call_args_list[0].args[1])


class TestFioCli(unittest.TestCase):  # pylint: disable=too-many-public-methods
    """CLI flag assertions for Fio._run_io_loop()."""

    # --- required flags -------------------------------------------------------

    def test_libaio_ioengine(self) -> None:
        """--ioengine=libaio is emitted."""
        self.assertIn("--ioengine=libaio", _cli())

    def test_rw_mode(self) -> None:
        """--rw reflects the configured mode."""
        self.assertIn("--rw=write", _cli())

    def test_block_size_from_op_size(self) -> None:
        """--bs reflects op_size when no bssplit/bsrange/bs override is set."""
        self.assertIn("--bs=4194304", _cli())

    def test_size_flag(self) -> None:
        """--size=NM is derived from the size config value."""
        self.assertIn("--size=4096M", _cli())

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

    def test_runtime_when_configured(self) -> None:
        """--runtime is emitted when time is configured."""
        self.assertIn("--runtime=60", _cli())

    def test_ramp_time_when_configured(self) -> None:
        """--ramp_time is emitted when ramp is configured."""
        self.assertIn("--ramp_time=5", _cli())

    def test_write_iops_log_when_logging_true(self) -> None:
        """--write_iops_log is present when logging=True."""
        self.assertIn("--write_iops_log=", _cli())

    def test_write_bw_log_when_logging_true(self) -> None:
        """--write_bw_log is present when logging=True."""
        self.assertIn("--write_bw_log=", _cli())

    def test_write_lat_log_when_logging_true(self) -> None:
        """--write_lat_log is present when logging=True."""
        self.assertIn("--write_lat_log=", _cli())

    def test_output_redirect_present(self) -> None:
        """Output is redirected to a canonical output.N file."""
        cli = _cli()
        self.assertIn("> ", cli)
        self.assertIn("output.0", cli)

    def test_direct_1_with_other_config_changes(self) -> None:
        """--direct=1 default is still present when only op_size changes."""
        self.assertIn("--direct=1", _cli({"op_size": 65536}))

    # --- logging=False suppresses all write_*_log flags ----------------------

    def test_logging_false_suppresses_iops_log(self) -> None:
        """--write_iops_log is absent when logging=False."""
        self.assertNotIn("--write_iops_log=", _cli({"logging": False}))

    def test_logging_false_suppresses_bw_log(self) -> None:
        """--write_bw_log is absent when logging=False."""
        self.assertNotIn("--write_bw_log=", _cli({"logging": False}))

    def test_logging_false_suppresses_lat_log(self) -> None:
        """--write_lat_log is absent when logging=False."""
        self.assertNotIn("--write_lat_log=", _cli({"logging": False}))

    def test_log_avg_msec_only_when_logging_true(self) -> None:
        """--log_avg_msec is emitted when logging=True and suppressed when False."""
        self.assertIn("--log_avg_msec=250", _cli({"log_avg_msec": 250}))
        self.assertNotIn("--log_avg_msec=250", _cli({"logging": False, "log_avg_msec": 250}))

    # --- block-size precedence: bssplit > bsrange > bs > op_size -------------

    def test_bssplit_wins_over_op_size(self) -> None:
        """--bssplit is emitted and --bs is suppressed when bssplit is set."""
        cli = _cli({"bssplit": "4k/50:8k/50"})
        self.assertIn("--bssplit=4k/50:8k/50", cli)
        self.assertNotIn("--bs=4194304", cli)

    def test_bsrange_wins_over_op_size(self) -> None:
        """--bsrange is emitted and --bs is suppressed when bsrange is set."""
        cli = _cli({"bsrange": "4k-8k"})
        self.assertIn("--bsrange=4k-8k", cli)
        self.assertNotIn("--bs=4194304", cli)

    def test_bssplit_wins_over_bsrange(self) -> None:
        """bssplit takes precedence over bsrange when both are set."""
        cli = _cli({"bssplit": "4k/100", "bsrange": "4k-8k"})
        self.assertIn("--bssplit=4k/100", cli)
        self.assertNotIn("--bsrange=", cli)

    def test_bs_wins_over_op_size(self) -> None:
        """--bs from config takes precedence over op_size."""
        cli = _cli({"bs": "8k"})
        self.assertIn("--bs=8k", cli)
        self.assertNotIn("--bs=4194304", cli)

    def test_bssplit_wins_over_bs(self) -> None:
        """bssplit takes precedence over bs when both are set."""
        cli = _cli({"bssplit": "4k/100", "bs": "8k"})
        self.assertIn("--bssplit=4k/100", cli)
        self.assertNotIn("--bs=8k", cli)

    # --- sync flag -----------------------------------------------------------

    def test_sync_flag_when_configured(self) -> None:
        """--sync is emitted when configured."""
        self.assertIn("--sync=1", _cli({"sync": 1}))

    def test_no_sync_by_default(self) -> None:
        """--sync is absent when not configured."""
        self.assertNotIn("--sync=", _cli())

    # --- randrw ---------------------------------------------------------------

    def test_randrw_emits_rwmix_flags(self) -> None:
        """randrw mode emits --rwmixread and --rwmixwrite from config."""
        cli = _cli({"mode": "randrw", "rwmixread": 60})
        self.assertIn("--rw=randrw", cli)
        self.assertIn("--rwmixread=60", cli)
        self.assertIn("--rwmixwrite=40", cli)

    # --- norandommap ---------------------------------------------------------

    def test_norandommap_when_configured(self) -> None:
        """--norandommap is emitted when configured."""
        self.assertIn("--norandommap", _cli({"norandommap": True}))

    def test_no_norandommap_by_default(self) -> None:
        """--norandommap is absent when not configured."""
        self.assertNotIn("--norandommap", _cli())

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
