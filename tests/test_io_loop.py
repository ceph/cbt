"""
Tests for the non-workloads I/O loop in each FIO benchmark subclass.

Each subclass implements _run_io_loop() which:
  - Instantiates the correct Command subclass for its storage backend
  - Builds an options dict and calls cmd.get() for the CLI string
  - Issues one pdsh call per target (volume / device / endpoint)
  - Output files are written under the canonical workloads naming scheme:
      {run_dir}/{benchmark}/numjobs-NNN/[total_iodepth-T/]iodepth-NNNNNN/output.N

These tests mock common.pdsh so no real cluster connections are made.
They inspect the captured pdsh calls to verify:
  - The correct Command class was used (via flag patterns in the CLI)
  - The right number of pdsh calls were made (one per target)
  - The output file redirect contains the canonical path segments
  - Per-target iodepth values are passed correctly, including total_iodepth
    distribution across multiple volumes/devices

G2 (RbdFio) nuance: _run_io_loop() passes self.names — a pre-assembled
multi-token string of ``--name=<path>`` flags — as the ``target`` option.
LibaioFioCommand emits this verbatim into the CLI.  The test verifies that
all expected --name= tokens appear in the issued command, not just that
some command was dispatched.
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from benchmark.fio import Fio
from benchmark.kvmrbdfio import KvmRbdFio
from benchmark.librbdfio import LibrbdFio
from benchmark.rawfio import RawFio
from benchmark.rbdfio import RbdFio

_FQDN_CMD = "hostname -f"
_FAKE_CLIENT = "fake-client"

# Minimal settings.cluster dict: only what Benchmark.__init__ reads directly.
_FAKE_CLUSTER_SETTINGS: dict[str, Any] = {"tmp_dir": "/tmp/cbt-test", "osd_ra": "0"}


# ---------------------------------------------------------------------------
# Cluster mock + benchmark factories
# ---------------------------------------------------------------------------


def _mock_cluster() -> MagicMock:
    cluster: MagicMock = MagicMock()
    cluster.config = {}
    cluster.mnt_dir = "/srv"
    return cluster


def _make_librbdfio(extra: Optional[dict[str, Any]] = None) -> LibrbdFio:
    config: dict[str, Any] = {"iteration": 0}
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


def _make_rbdfio(extra: Optional[dict[str, Any]] = None) -> RbdFio:
    config: dict[str, Any] = {"iteration": 0}
    if extra:
        config.update(extra)
    with (
        patch("settings.cluster", _FAKE_CLUSTER_SETTINGS),
        patch("settings.getnodes", return_value=_FAKE_CLIENT),
        patch("common.get_osd_ra", return_value="0"),
    ):
        bm = RbdFio(archive_dir="/tmp/archive", cluster=_mock_cluster(), config=config)
    bm.cmd_path_full = "/usr/bin/fio"
    return bm


def _make_kvmrbdfio(extra: Optional[dict[str, Any]] = None) -> KvmRbdFio:
    config: dict[str, Any] = {"iteration": 0}
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


def _make_rawfio(extra: Optional[dict[str, Any]] = None) -> RawFio:
    config: dict[str, Any] = {"iteration": 0}
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


def _make_fio(extra: Optional[dict[str, Any]] = None) -> Fio:
    config: dict[str, Any] = {"iteration": 0, "size": 4096}
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
    bm.endpoints_per_client = 2
    bm.endpoints = ["/mnt/ep0", "/mnt/ep1"]
    return bm


# ---------------------------------------------------------------------------
# Helper: run _run_io_loop() and collect pdsh CLI strings
# ---------------------------------------------------------------------------


def _run_io_loop_calls(bm: Any) -> list[str]:
    """
    Run _run_io_loop() with pdsh mocked and return the list of CLI strings
    that were passed to pdsh, in call order.
    """
    pdsh_mock = MagicMock()
    pdsh_mock.return_value = MagicMock()
    with (
        patch("common.pdsh", pdsh_mock),
        patch("common.get_fqdn_cmd", return_value=_FQDN_CMD),
    ):
        bm._run_io_loop()  # pylint: disable=protected-access

    # Each call is pdsh(nodes, cli_string) — extract the second positional arg
    return [c.args[1] for c in pdsh_mock.call_args_list]


def _canonical_output_path(bm: Any, benchmark_name: str, iodepth: int, target_number: int) -> str:
    """
    Return the expected canonical output file path for a single fio target.

    Path structure:
      {run_dir}/{benchmark}/numjobs-NNN/iodepth-NNNNNN/output.N
      {run_dir}/{benchmark}/numjobs-NNN/total_iodepth-T/iodepth-NNNNNN/output.N  (when total_iodepth set)
    """
    numjobs: int = getattr(bm, "numjobs", 1)
    total_iodepth: Optional[int] = getattr(bm, "total_iodepth", None)
    path = f"{bm.run_dir}/{benchmark_name}/numjobs-{numjobs:03d}/"
    if total_iodepth is not None:
        path += f"total_iodepth-{total_iodepth}/"
    path += f"iodepth-{iodepth:06d}/output.{target_number}"
    return path


# ===========================================================================
# LibrbdFio._run_io_loop()  (G1)
# ===========================================================================


class TestLibrbdFioRunIoLoop(unittest.TestCase):
    """LibrbdFio._run_io_loop() uses RbdFioCommand and issues one pdsh per volume."""

    def test_one_pdsh_call_per_volume(self) -> None:
        """One pdsh call is issued per volume."""
        bm = _make_librbdfio({"volumes_per_client": 3})
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 3)

    def test_uses_rbd_ioengine(self) -> None:
        """Every command uses the rbd ioengine."""
        bm = _make_librbdfio({"volumes_per_client": 2})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            self.assertIn("--ioengine=rbd", cmd)

    def test_output_redirect_uses_canonical_path(self) -> None:
        """Output file redirect points to the canonical numjobs-N/iodepth-N path."""
        bm = _make_librbdfio({"iodepth": 16, "volumes_per_client": 2})
        cmds = _run_io_loop_calls(bm)
        for i, cmd in enumerate(cmds):
            expected = _canonical_output_path(bm, "rbdfio", 16, i)
            self.assertIn(f"> {expected}", cmd)

    def test_output_redirect_contains_numjobs_segment(self) -> None:
        """The canonical path always contains a numjobs-NNN directory segment."""
        bm = _make_librbdfio({"volumes_per_client": 2})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            redirect_part = cmd.split(">")[1]
            self.assertIn("numjobs-", redirect_part)

    def test_output_redirect_contains_iodepth_segment(self) -> None:
        """The canonical path always contains an iodepth-NNNNNN directory segment."""
        bm = _make_librbdfio({"volumes_per_client": 2})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            redirect_part = cmd.split(">")[1]
            self.assertIn("iodepth-", redirect_part)

    def test_per_volume_iodepth_flat_when_iodepth_key(self) -> None:
        """With plain iodepth=8 and 2 volumes, both commands get --iodepth=8."""
        bm = _make_librbdfio({"iodepth": 8, "volumes_per_client": 2})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            self.assertIn("--iodepth=8", cmd)

    def test_total_iodepth_distributed_across_volumes(self) -> None:
        """total_iodepth=10, 3 volumes → 4, 3, 3."""
        bm = _make_librbdfio({"total_iodepth": 10, "volumes_per_client": 3})
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 3)
        self.assertIn("--iodepth=4", cmds[0])
        self.assertIn("--iodepth=3", cmds[1])
        self.assertIn("--iodepth=3", cmds[2])

    def test_total_iodepth_in_output_path(self) -> None:
        """When total_iodepth is set the output path contains a total_iodepth-T segment."""
        bm = _make_librbdfio({"total_iodepth": 8, "volumes_per_client": 2})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            redirect_part = cmd.split(">")[1]
            self.assertIn("total_iodepth-", redirect_part)

    def test_default_rbdname_contains_volnum(self) -> None:
        """Default rbd name includes the volume index."""
        bm = _make_librbdfio({"volumes_per_client": 2})
        cmds = _run_io_loop_calls(bm)
        self.assertIn("-0", cmds[0])
        self.assertIn("-1", cmds[1])

    def test_use_existing_volumes_passes_custom_rbdname(self) -> None:
        """When use_existing_volumes is set, the configured rbdname is used."""
        bm = _make_librbdfio({"use_existing_volumes": True, "rbdname": "my-vol"})
        cmds = _run_io_loop_calls(bm)
        self.assertIn("my-vol", cmds[0])

    def test_pool_name_in_command(self) -> None:
        """Pool name appears in the rbd command."""
        bm = _make_librbdfio()
        cmds = _run_io_loop_calls(bm)
        self.assertIn("--pool=cbt-librbdfio", cmds[0])


# ===========================================================================
# RbdFio._run_io_loop()  (G2)
# ===========================================================================


class TestRbdFioRunIoLoop(unittest.TestCase):
    """
    RbdFio._run_io_loop() uses LibaioFioCommand with the pre-assembled
    self.names string (multi --name= flags) passed as the ``target`` option.

    G2 nuance: self.names is built in __init__ as a single string of space-
    separated ``--name=<path>`` tokens, one per concurrent_proc.  LibaioFioCommand
    receives this as ``target`` and emits it verbatim.  The test verifies that
    all expected --name= tokens appear in the issued command, not just that a
    command was dispatched.
    """

    def test_exactly_one_pdsh_call(self) -> None:
        """RbdFio issues a single pdsh call containing all --name= flags."""
        bm = _make_rbdfio({"concurrent_procs": 2})
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 1)

    def test_names_string_present_in_command(self) -> None:
        """All --name=<path> tokens from self.names appear in the command."""
        bm = _make_rbdfio({"concurrent_procs": 2})
        cmds = _run_io_loop_calls(bm)
        cmd = cmds[0]
        # self.names contains one --name= token per concurrent_proc; each
        # references cbt-kernelrbdfio-`hostname -s`/cbt-kernelrbdfio-N
        self.assertIn("cbt-kernelrbdfio-", cmd)
        # Both proc indices (0 and 1) should appear
        self.assertIn("cbt-kernelrbdfio-0", cmd)
        self.assertIn("cbt-kernelrbdfio-1", cmd)

    def test_libaio_ioengine(self) -> None:
        """RbdFio uses the libaio engine."""
        bm = _make_rbdfio()
        cmds = _run_io_loop_calls(bm)
        self.assertIn("--ioengine=libaio", cmds[0])

    def test_output_redirect_uses_canonical_path(self) -> None:
        """Output file redirect points to the canonical numjobs-N/iodepth-N path."""
        bm = _make_rbdfio({"iodepth": 16})
        cmds = _run_io_loop_calls(bm)
        expected = _canonical_output_path(bm, "libaio_fio", 16, 0)
        self.assertIn(f"> {expected}", cmds[0])

    def test_output_redirect_contains_numjobs_and_iodepth_segments(self) -> None:
        """Output path always contains numjobs- and iodepth- segments."""
        bm = _make_rbdfio()
        cmds = _run_io_loop_calls(bm)
        redirect_part = cmds[0].split(">")[1]
        self.assertIn("numjobs-", redirect_part)
        self.assertIn("iodepth-", redirect_part)

    def test_total_iodepth_distributed_across_procs(self) -> None:
        """total_iodepth=12, concurrent_procs=3 → per-job --iodepth=4."""
        bm = _make_rbdfio({"total_iodepth": 12, "concurrent_procs": 3})
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 1)
        self.assertIn("--iodepth=4", cmds[0])


# ===========================================================================
# KvmRbdFio._run_io_loop()  (G3)
# ===========================================================================


class TestKvmRbdFioRunIoLoop(unittest.TestCase):
    """KvmRbdFio._run_io_loop() issues one pdsh per concurrent_proc, each with the correct fiopath."""

    def test_one_pdsh_call_per_concurrent_proc(self) -> None:
        """One pdsh call is issued per concurrent process."""
        bm = _make_kvmrbdfio({"concurrent_procs": 3, "block_devices": "/dev/vdb,/dev/vdc,/dev/vdd"})
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 3)

    def test_fiopath_contains_device_basename(self) -> None:
        """Each command references a fio image file under the device mount point."""
        bm = _make_kvmrbdfio({"concurrent_procs": 2, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        # Device names cycle: proc 0 → vdb, proc 1 → vdc
        self.assertIn("vdb", cmds[0])
        self.assertIn("vdc", cmds[1])

    def test_fiopath_contains_fio_img(self) -> None:
        """Each command targets a .img fio file."""
        bm = _make_kvmrbdfio({"concurrent_procs": 2, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            self.assertIn("fio", cmd)
            self.assertIn(".img", cmd)

    def test_output_redirect_uses_canonical_path(self) -> None:
        """Output file redirect points to the canonical numjobs-N/iodepth-N path."""
        bm = _make_kvmrbdfio({"concurrent_procs": 2, "iodepth": 16, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        for i, cmd in enumerate(cmds):
            expected = _canonical_output_path(bm, "libaio_fio", 16, i)
            self.assertIn(f"> {expected}", cmd)

    def test_output_redirect_contains_numjobs_and_iodepth_segments(self) -> None:
        """Output path always contains numjobs- and iodepth- segments."""
        bm = _make_kvmrbdfio({"concurrent_procs": 2, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            redirect_part = cmd.split(">")[1]
            self.assertIn("numjobs-", redirect_part)
            self.assertIn("iodepth-", redirect_part)

    def test_startdelay_flag_when_set(self) -> None:
        """startdelay option appears in the command when configured."""
        bm = _make_kvmrbdfio({"startdelay": 5, "concurrent_procs": 2, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            self.assertIn("--startdelay=5", cmd)

    def test_no_startdelay_by_default(self) -> None:
        """No startdelay flag appears when not configured."""
        bm = _make_kvmrbdfio({"concurrent_procs": 2, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            self.assertNotIn("--startdelay", cmd)

    def test_output_file_index_increments(self) -> None:
        """Each pdsh call writes to a distinct output.N file."""
        bm = _make_kvmrbdfio({"concurrent_procs": 3, "block_devices": "/dev/vdb,/dev/vdc,/dev/vdd"})
        cmds = _run_io_loop_calls(bm)
        for i, cmd in enumerate(cmds):
            self.assertIn(f"output.{i}", cmd)

    def test_total_iodepth_distributed_across_targets(self) -> None:
        """total_iodepth=10, 3 targets → 4, 3, 3."""
        bm = _make_kvmrbdfio(
            {
                "total_iodepth": 10,
                "concurrent_procs": 3,
                "block_devices": "/dev/vdb,/dev/vdc,/dev/vdd",
            }
        )
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 3)
        self.assertIn("--iodepth=4", cmds[0])
        self.assertIn("--iodepth=3", cmds[1])
        self.assertIn("--iodepth=3", cmds[2])


# ===========================================================================
# RawFio._run_io_loop()  (G4)
# ===========================================================================


class TestRawFioRunIoLoop(unittest.TestCase):
    """RawFio._run_io_loop() issues one pdsh per concurrent_proc against the raw block device."""

    def test_one_pdsh_call_per_concurrent_proc(self) -> None:
        """One pdsh call is issued per concurrent process."""
        bm = _make_rawfio({"concurrent_procs": 3, "block_devices": "/dev/vdb,/dev/vdc,/dev/vdd"})
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 3)

    def test_block_device_path_in_command(self) -> None:
        """Each command targets the raw block device path directly."""
        bm = _make_rawfio({"concurrent_procs": 2, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        # Devices cycle: proc 0 → /dev/vdb, proc 1 → /dev/vdc
        self.assertIn("/dev/vdb", cmds[0])
        self.assertIn("/dev/vdc", cmds[1])

    def test_output_format_json_only(self) -> None:
        """RawFio always uses json (not json,normal)."""
        bm = _make_rawfio({"concurrent_procs": 2, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            self.assertIn("--output-format=json", cmd)
            self.assertNotIn("--output-format=json,normal", cmd)

    def test_output_redirect_uses_canonical_path(self) -> None:
        """Output file redirect points to the canonical numjobs-N/iodepth-N path."""
        bm = _make_rawfio({"concurrent_procs": 2, "iodepth": 16, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        for i, cmd in enumerate(cmds):
            expected = _canonical_output_path(bm, "libaio_fio", 16, i)
            self.assertIn(f"> {expected}", cmd)

    def test_output_redirect_contains_numjobs_and_iodepth_segments(self) -> None:
        """Output path always contains numjobs- and iodepth- segments."""
        bm = _make_rawfio({"concurrent_procs": 2, "block_devices": "/dev/vdb,/dev/vdc"})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            redirect_part = cmd.split(">")[1]
            self.assertIn("numjobs-", redirect_part)
            self.assertIn("iodepth-", redirect_part)

    def test_output_file_index_increments(self) -> None:
        """Each pdsh call writes to a distinct output.N file."""
        bm = _make_rawfio({"concurrent_procs": 3, "block_devices": "/dev/vdb,/dev/vdc,/dev/vdd"})
        cmds = _run_io_loop_calls(bm)
        for i, cmd in enumerate(cmds):
            self.assertIn(f"output.{i}", cmd)

    def test_total_iodepth_distributed_across_targets(self) -> None:
        """total_iodepth=10, 3 targets → 4, 3, 3."""
        bm = _make_rawfio(
            {
                "total_iodepth": 10,
                "concurrent_procs": 3,
                "block_devices": "/dev/vdb,/dev/vdc,/dev/vdd",
            }
        )
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 3)
        self.assertIn("--iodepth=4", cmds[0])
        self.assertIn("--iodepth=3", cmds[1])
        self.assertIn("--iodepth=3", cmds[2])


# ===========================================================================
# Fio._run_io_loop()  (G5)
# ===========================================================================


class TestFioRunIoLoop(unittest.TestCase):
    """Fio._run_io_loop() uses EndpointFioCommand and issues one pdsh per endpoint."""

    def test_one_pdsh_call_per_endpoint(self) -> None:
        """One pdsh call is issued per endpoint."""
        bm = _make_fio()
        bm.endpoints_per_client = 3
        bm.endpoints = ["/mnt/ep0", "/mnt/ep1", "/mnt/ep2"]
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 3)

    def test_output_redirect_uses_canonical_path(self) -> None:
        """Output file redirect points to the canonical numjobs-N/iodepth-N path."""
        bm = _make_fio({"iodepth": 16})
        cmds = _run_io_loop_calls(bm)
        for i, cmd in enumerate(cmds):
            expected = _canonical_output_path(bm, "endpointfio", 16, i)
            self.assertIn(f"> {expected}", cmd)

    def test_output_redirect_contains_numjobs_and_iodepth_segments(self) -> None:
        """Output path always contains numjobs- and iodepth- segments."""
        bm = _make_fio()
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            redirect_part = cmd.split(">")[1]
            self.assertIn("numjobs-", redirect_part)
            self.assertIn("iodepth-", redirect_part)

    def test_output_file_index_increments(self) -> None:
        """Each pdsh call writes to a distinct output.N file."""
        bm = _make_fio()
        cmds = _run_io_loop_calls(bm)
        self.assertIn("output.0", cmds[0])
        self.assertIn("output.1", cmds[1])

    def test_rw_mode_in_command(self) -> None:
        """The configured rw mode appears in the command."""
        bm = _make_fio({"mode": "randread"})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            self.assertIn("--rw=randread", cmd)

    def test_iodepth_in_command(self) -> None:
        """The configured iodepth appears in the command."""
        bm = _make_fio({"iodepth": 32})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            self.assertIn("--iodepth=32", cmd)

    def test_bssplit_wins_over_op_size(self) -> None:
        """bssplit fed through _apply_blocksize_option() → EndpointFioCommand."""
        bm = _make_fio({"bssplit": "4k/50:512k/50", "bs": None, "bsrange": None})
        cmds = _run_io_loop_calls(bm)
        for cmd in cmds:
            self.assertIn("--bssplit=4k/50:512k/50", cmd)
            self.assertNotIn("--bs=", cmd)

    def test_total_iodepth_distributed_across_endpoints(self) -> None:
        """total_iodepth=10, 3 endpoints → 4, 3, 3."""
        bm = _make_fio({"total_iodepth": 10})
        bm.endpoints_per_client = 3
        bm.endpoints = ["/mnt/ep0", "/mnt/ep1", "/mnt/ep2"]
        cmds = _run_io_loop_calls(bm)
        self.assertEqual(len(cmds), 3)
        self.assertIn("--iodepth=4", cmds[0])
        self.assertIn("--iodepth=3", cmds[1])
        self.assertIn("--iodepth=3", cmds[2])


if __name__ == "__main__":
    unittest.main()
