"""
Unit tests for EndpointFioCommand.

EndpointFioCommand generates FIO CLI strings for benchmarks that use the
client_endpoints_factory abstraction (the ``Fio`` benchmark class).

Two endpoint types are exercised:

  directory  -  files inside a mounted directory; names are generated as
                ``<endpoint_path>/`<fqdn_cmd>`-<ep_num>-<proc_num>``.

  rbd        -  RBD images via the librbd engine; engine-specific options
                (clientname, pool, rbdname) and per-process names
                ``<pool>/<rbdname>-<proc_num>`` are emitted.
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from typing import Optional

from command.command import Command
from command.endpoint_fio_command import EndpointFioCommand
from command.libaio_fio_command import LibaioFioCommand
from workloads.workload import Workload

# The directory-endpoint name format uses `hostname` (no -f) so that the
# space-based split in FioCommand._generate_full_command() works correctly.
_HOSTNAME_TOKEN = "`hostname`"


# ---------------------------------------------------------------------------
# Shared factory helpers
# ---------------------------------------------------------------------------


def _make_dir_cmd(overrides: Optional[dict[str, str]] = None) -> str:
    """Build an EndpointFioCommand for a directory endpoint."""
    base_options: dict[str, str] = {
        "target_number": "0",
        "name": "test-workload",
        "mode": "write",
        "op_size": "4194304",
        "iodepth": "16",
        "numjobs": "1",
        "end_fsync": "0",
        "fio_out_format": "json,normal",
        "endpoint_type": "directory",
        "endpoint_path": "/mnt/cbt-endpoint",
        "procs_per_endpoint": "1",
        "procs_per_volume": "1",
        "no_sudo": "false",
    }
    if overrides:
        base_options.update(overrides)

    cmd_obj = EndpointFioCommand(base_options, "/tmp/workload_output/")
    cmd_obj.set_executable("/usr/bin/fio")
    return cmd_obj.get()


def _make_rbd_cmd(overrides: Optional[dict[str, str]] = None) -> str:
    """Build an EndpointFioCommand for an rbd endpoint."""
    base_options: dict[str, str] = {
        "target_number": "0",
        "name": "test-workload",
        "mode": "write",
        "op_size": "4194304",
        "iodepth": "16",
        "numjobs": "1",
        "end_fsync": "0",
        "fio_out_format": "json,normal",
        "endpoint_type": "rbd",
        "endpoint_path": "cbt-pool/cbt-image",
        "procs_per_endpoint": "1",
        "procs_per_volume": "1",
        "no_sudo": "false",
    }
    if overrides:
        base_options.update(overrides)

    cmd_obj = EndpointFioCommand(base_options, "/tmp/workload_output/")
    cmd_obj.set_executable("/usr/bin/fio")
    return cmd_obj.get()


# ===========================================================================
# Directory endpoint - defaults
# ===========================================================================


class TestEndpointFioCommandDirectoryDefaults(unittest.TestCase):
    """Verify the default CLI for a directory-type endpoint."""

    def setUp(self) -> None:
        self.cmd = _make_dir_cmd()

    def test_starts_with_sudo(self) -> None:
        self.assertTrue(self.cmd.startswith("sudo "), f"Expected 'sudo ' prefix, got: {self.cmd!r}")

    def test_ioengine_is_libaio(self) -> None:
        self.assertIn("--ioengine=libaio", self.cmd)

    def test_rw_mode_write(self) -> None:
        self.assertIn("--rw=write", self.cmd)

    def test_bs_op_size(self) -> None:
        self.assertIn("--bs=4194304", self.cmd)

    def test_iodepth_default(self) -> None:
        self.assertIn("--iodepth=16", self.cmd)

    def test_numjobs_default(self) -> None:
        self.assertIn("--numjobs=1", self.cmd)

    def test_direct_default_1(self) -> None:
        self.assertIn("--direct=1", self.cmd)

    def test_end_fsync_zero(self) -> None:
        self.assertIn("--end_fsync=0", self.cmd)

    def test_output_format_json_normal(self) -> None:
        self.assertIn("--output-format=json,normal", self.cmd)

    def test_directory_name_includes_endpoint_path(self) -> None:
        self.assertIn("/mnt/cbt-endpoint/", self.cmd)

    def test_directory_name_includes_ep_and_proc_nums(self) -> None:
        # format: <path>/`hostname`-<ep_num>-<proc_num>
        self.assertIn("`hostname`-0-0", self.cmd)

    def test_redirects_to_output_file(self) -> None:
        self.assertIn("> ", self.cmd)
        self.assertIn("output.0", self.cmd)

    def test_no_rwmix_flags_for_plain_write(self) -> None:
        self.assertNotIn("--rwmixread", self.cmd)
        self.assertNotIn("--rwmixwrite", self.cmd)

    def test_no_clientname_for_directory(self) -> None:
        self.assertNotIn("--clientname", self.cmd)

    def test_no_pool_for_directory(self) -> None:
        self.assertNotIn("--pool=", self.cmd)


# ===========================================================================
# Directory endpoint - multiple procs_per_endpoint
# ===========================================================================


class TestEndpointFioCommandDirectoryMultiProcs(unittest.TestCase):
    def setUp(self) -> None:
        self.cmd = _make_dir_cmd({"procs_per_endpoint": "3"})

    def test_three_name_flags_for_three_procs(self) -> None:
        # Each proc contributes one --name= token
        self.assertEqual(self.cmd.count("--name="), 3)

    def test_names_include_proc_nums_0_1_2(self) -> None:
        for proc in range(3):
            self.assertIn(f"-0-{proc}", self.cmd)


# ===========================================================================
# RBD endpoint - defaults
# ===========================================================================


class TestEndpointFioCommandRbdDefaults(unittest.TestCase):
    """Verify the default CLI for an rbd-type endpoint."""

    def setUp(self) -> None:
        self.cmd = _make_rbd_cmd()

    def test_starts_with_sudo(self) -> None:
        self.assertTrue(self.cmd.startswith("sudo "), f"Expected 'sudo ' prefix, got: {self.cmd!r}")

    def test_ioengine_is_rbd(self) -> None:
        self.assertIn("--ioengine=rbd", self.cmd)

    def test_clientname_admin(self) -> None:
        self.assertIn("--clientname=admin", self.cmd)

    def test_pool_name(self) -> None:
        self.assertIn("--pool=cbt-pool", self.cmd)

    def test_rbdname(self) -> None:
        self.assertIn("--rbdname=cbt-image", self.cmd)

    def test_name_includes_image_and_proc_num(self) -> None:
        self.assertIn("cbt-pool/cbt-image-0", self.cmd)

    def test_no_directory_style_name(self) -> None:
        # rbd endpoints use pool/image names, not directory paths
        self.assertNotIn("/mnt/", self.cmd)


# ===========================================================================
# RBD endpoint - multiple procs_per_endpoint
# ===========================================================================


class TestEndpointFioCommandRbdMultiProcs(unittest.TestCase):
    def setUp(self) -> None:
        self.cmd = _make_rbd_cmd({"procs_per_endpoint": "2"})

    def test_two_name_flags(self) -> None:
        self.assertEqual(self.cmd.count("--name="), 2)

    def test_names_include_proc_nums_0_and_1(self) -> None:
        self.assertIn("cbt-pool/cbt-image-0", self.cmd)
        self.assertIn("cbt-pool/cbt-image-1", self.cmd)


# ===========================================================================
# sudo / no_sudo
# ===========================================================================


class TestEndpointFioCommandSudo(unittest.TestCase):
    def test_no_sudo_directory(self) -> None:
        cmd = _make_dir_cmd({"no_sudo": "true"})
        self.assertFalse(cmd.startswith("sudo "))

    def test_no_sudo_rbd(self) -> None:
        cmd = _make_rbd_cmd({"no_sudo": "true"})
        self.assertFalse(cmd.startswith("sudo "))


# ===========================================================================
# Timing options
# ===========================================================================


class TestEndpointFioCommandTimingOptions(unittest.TestCase):
    def test_runtime_flag(self) -> None:
        cmd = _make_dir_cmd({"time": "120"})
        self.assertIn("--runtime=120", cmd)

    def test_time_based_flag(self) -> None:
        cmd = _make_dir_cmd({"time_based": "true"})
        self.assertIn("--time_based", cmd)

    def test_ramp_time_flag(self) -> None:
        cmd = _make_dir_cmd({"ramp": "15"})
        self.assertIn("--ramp_time=15", cmd)

    def test_no_runtime_by_default(self) -> None:
        cmd = _make_dir_cmd()
        self.assertNotIn("--runtime", cmd)

    def test_no_time_based_by_default(self) -> None:
        cmd = _make_dir_cmd()
        self.assertNotIn("--time_based", cmd)


# ===========================================================================
# Mixed read/write modes
# ===========================================================================


class TestEndpointFioCommandMixedRw(unittest.TestCase):
    def test_randrw_has_rwmix_flags(self) -> None:
        cmd = _make_dir_cmd({"mode": "randrw", "rwmixread": "70"})
        self.assertIn("--rwmixread=70", cmd)
        self.assertIn("--rwmixwrite=30", cmd)

    def test_readwrite_has_rwmix_flags(self) -> None:
        cmd = _make_dir_cmd({"mode": "readwrite", "rwmixread": "40"})
        self.assertIn("--rwmixread=40", cmd)
        self.assertIn("--rwmixwrite=60", cmd)

    def test_randread_no_rwmix(self) -> None:
        cmd = _make_dir_cmd({"mode": "randread"})
        self.assertNotIn("--rwmixread", cmd)
        self.assertNotIn("--rwmixwrite", cmd)


# ===========================================================================
# Optional flags
# ===========================================================================


class TestEndpointFioCommandOptionalFlags(unittest.TestCase):
    def test_norandommap_flag(self) -> None:
        cmd = _make_dir_cmd({"norandommap": "1"})
        self.assertIn("--norandommap", cmd)

    def test_rate_iops_flag(self) -> None:
        cmd = _make_dir_cmd({"rate_iops": "2000"})
        self.assertIn("--rate_iops=2000", cmd)

    def test_random_distribution_flag(self) -> None:
        cmd = _make_dir_cmd({"random_distribution": "zipf:0.9"})
        self.assertIn("--random_distribution=zipf:0.9", cmd)

    def test_log_avg_msec_flag(self) -> None:
        cmd = _make_dir_cmd({"log_avg_msec": "1000"})
        self.assertIn("--log_avg_msec=1000", cmd)

    def test_size_flag_when_provided(self) -> None:
        cmd = _make_dir_cmd({"size": "4096"})
        self.assertIn("--size=4096M", cmd)

    def test_sync_flag_when_provided(self) -> None:
        cmd = _make_dir_cmd({"sync": "1"})
        self.assertIn("--sync=1", cmd)

    def test_no_size_by_default(self) -> None:
        cmd = _make_dir_cmd()
        self.assertNotIn("--size=", cmd)

    def test_no_sync_by_default(self) -> None:
        cmd = _make_dir_cmd()
        self.assertNotIn("--sync=", cmd)


# ===========================================================================
# Logging flags
# ===========================================================================


class TestEndpointFioCommandLoggingFlags(unittest.TestCase):
    def test_write_iops_log(self) -> None:
        cmd = _make_dir_cmd()
        self.assertIn("--write_iops_log=", cmd)

    def test_write_bw_log(self) -> None:
        cmd = _make_dir_cmd()
        self.assertIn("--write_bw_log=", cmd)

    def test_write_lat_log(self) -> None:
        cmd = _make_dir_cmd()
        self.assertIn("--write_lat_log=", cmd)

    def test_no_logging_when_disabled(self) -> None:
        cmd = _make_dir_cmd({"log_iops": "false", "log_bw": "false", "log_lat": "false"})
        self.assertNotIn("--write_iops_log", cmd)
        self.assertNotIn("--write_bw_log", cmd)
        self.assertNotIn("--write_lat_log", cmd)


# ===========================================================================
# Recovery test
# ===========================================================================


class TestEndpointFioCommandRecovery(unittest.TestCase):
    def test_time_based_added_for_recovery(self) -> None:
        cmd = _make_dir_cmd({"recovery_test": "blocking"})
        self.assertIn("--time_based", cmd)


# ===========================================================================
# Output directory path
# ===========================================================================


class TestEndpointFioCommandOutputPath(unittest.TestCase):
    def test_output_file_target_number(self) -> None:
        cmd = _make_dir_cmd({"target_number": "2"})
        self.assertIn("output.2", cmd)

    def test_output_path_includes_numjobs(self) -> None:
        cmd = _make_dir_cmd({"numjobs": "8"})
        self.assertIn("numjobs-008", cmd)

    def test_total_iodepth_in_path(self) -> None:
        cmd = _make_dir_cmd({"total_iodepth": "64", "iodepth": "16"})
        self.assertIn("total_iodepth-64", cmd)

    def test_no_total_iodepth_in_path_when_absent(self) -> None:
        cmd = _make_dir_cmd()
        self.assertNotIn("total_iodepth-", cmd)


# ===========================================================================
# Benchmark property
# ===========================================================================


class TestEndpointFioCommandBenchmarkProperty(unittest.TestCase):
    def test_benchmark_type_endpointfio(self) -> None:
        options: dict[str, str] = {
            "target_number": "0",
            "name": "w",
            "mode": "write",
            "op_size": "4096",
            "iodepth": "8",
            "numjobs": "1",
            "end_fsync": "0",
            "fio_out_format": "json,normal",
            "endpoint_type": "directory",
            "endpoint_path": "/mnt/ep",
            "procs_per_endpoint": "1",
            "procs_per_volume": "1",
            "no_sudo": "false",
        }
        cmd_obj = EndpointFioCommand(options, "/tmp/out/")
        self.assertEqual(cmd_obj.benchmark, "endpointfio")


# ===========================================================================
# Workload registration smoke test
# ===========================================================================


class TestWorkloadCreatesEndpointCommand(unittest.TestCase):
    """Smoke-test that Workload._create_command_class dispatches to EndpointFioCommand."""

    def test_fio_benchmark_type_creates_endpoint_command(self) -> None:
        workload = Workload("test-wl", {}, "/tmp/runs/")
        workload.set_benchmark_type("fio")

        options: dict[str, str] = {
            "mode": "write",
            "iodepth": "8",
            "numjobs": "1",
            "target_number": "0",
            "name": "test-wl",
            "endpoint_type": "directory",
            "endpoint_path": "/mnt/ep",
            "procs_per_endpoint": "1",
            "procs_per_volume": "1",
        }
        command = workload._create_command_class(options)
        self.assertIsNotNone(command)
        self.assertIsInstance(command, Command)
        self.assertIsInstance(command, EndpointFioCommand)


class TestWorkloadCreatesLibaioCommand(unittest.TestCase):
    """Smoke-test that Workload._create_command_class dispatches to LibaioFioCommand."""

    def _check_type(self, bm_type: str) -> None:
        workload = Workload("test-wl", {}, "/tmp/runs/")
        workload.set_benchmark_type(bm_type)

        options: dict[str, str] = {
            "mode": "write",
            "iodepth": "8",
            "numjobs": "1",
            "target_number": "0",
            "name": "test-wl",
            "target": "/dev/vdb",
            "procs_per_volume": "1",
        }
        command = workload._create_command_class(options)
        self.assertIsNotNone(command)
        self.assertIsInstance(command, Command)
        self.assertIsInstance(command, LibaioFioCommand)

    def test_kvmrbdfio_creates_libaio_command(self) -> None:
        self._check_type("kvmrbdfio")

    def test_rawfio_creates_libaio_command(self) -> None:
        self._check_type("rawfio")

    def test_rbdfiokrbd_creates_libaio_command(self) -> None:
        self._check_type("rbdfiokrbd")


if __name__ == "__main__":
    unittest.main()
