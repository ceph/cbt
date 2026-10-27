"""
Unit tests for LibaioFioCommand.

LibaioFioCommand generates FIO CLI strings for libaio / block-device /
file-based I/O engines (used by KvmRbdFio, RawFio, RbdFio, and plain Fio
against directory endpoints without the client_endpoints abstraction).

Test matrix mirrors the existing TestRbdFioCommandGeneration pattern so that
any future merge of the two classes immediately shows divergences.
"""

# pyright: strict, reportPrivateUsage=false

import unittest
from typing import Optional

from command.libaio_fio_command import LibaioFioCommand

# ---------------------------------------------------------------------------
# Shared factory helper
# ---------------------------------------------------------------------------


def _make_cmd(overrides: Optional[dict[str, str]] = None) -> str:
    """Build a LibaioFioCommand with default options and return the CLI string."""
    base_options: dict[str, str] = {
        "target_number": "0",
        "name": "test-workload",
        "mode": "write",
        "op_size": "4194304",
        "iodepth": "16",
        "numjobs": "1",
        "end_fsync": "0",
        "fio_out_format": "json,normal",
        "target": "/dev/vdb",
        "procs_per_volume": "1",
        "no_sudo": "false",
    }
    if overrides:
        base_options.update(overrides)

    cmd_obj = LibaioFioCommand(base_options, "/tmp/workload_output/")
    cmd_obj.set_executable("/usr/bin/fio")
    return cmd_obj.get()


# ===========================================================================
# Default behaviour
# ===========================================================================


class TestLibaioFioCommandDefaults(unittest.TestCase):
    """Verify the default CLI produced by LibaioFioCommand."""

    def setUp(self) -> None:
        self.cmd = _make_cmd()

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

    def test_invalidate_zero(self) -> None:
        self.assertIn("--invalidate=0", self.cmd)

    def test_end_fsync_zero(self) -> None:
        self.assertIn("--end_fsync=0", self.cmd)

    def test_output_format_json_normal(self) -> None:
        self.assertIn("--output-format=json,normal", self.cmd)

    def test_target_name_in_command(self) -> None:
        self.assertIn("--name=/dev/vdb", self.cmd)

    def test_redirects_to_output_file(self) -> None:
        self.assertIn("> ", self.cmd)
        self.assertIn("output.0", self.cmd)

    def test_no_rwmix_flags_for_plain_write(self) -> None:
        self.assertNotIn("--rwmixread", self.cmd)
        self.assertNotIn("--rwmixwrite", self.cmd)

    def test_no_runtime_when_time_not_set(self) -> None:
        self.assertNotIn("--runtime", self.cmd)

    def test_no_time_based_by_default(self) -> None:
        self.assertNotIn("--time_based", self.cmd)

    def test_no_ramp_time_by_default(self) -> None:
        self.assertNotIn("--ramp_time", self.cmd)

    def test_no_norandommap_by_default(self) -> None:
        self.assertNotIn("--norandommap", self.cmd)

    def test_no_random_distribution_by_default(self) -> None:
        self.assertNotIn("--random_distribution", self.cmd)

    def test_no_rate_iops_by_default(self) -> None:
        self.assertNotIn("--rate_iops", self.cmd)

    def test_no_log_avg_msec_by_default(self) -> None:
        self.assertNotIn("--log_avg_msec", self.cmd)

    def test_no_size_when_not_set(self) -> None:
        # size is optional; should not appear unless explicitly provided
        self.assertNotIn("--size=", self.cmd)

    def test_no_startdelay_by_default(self) -> None:
        self.assertNotIn("--startdelay", self.cmd)


# ===========================================================================
# sudo / no_sudo
# ===========================================================================


class TestLibaioFioCommandSudo(unittest.TestCase):
    def test_no_sudo_prefix(self) -> None:
        cmd = _make_cmd({"no_sudo": "true"})
        self.assertFalse(cmd.startswith("sudo "))

    def test_sudo_present_when_no_sudo_false(self) -> None:
        cmd = _make_cmd({"no_sudo": "false"})
        self.assertTrue(cmd.startswith("sudo "))


# ===========================================================================
# Timing options
# ===========================================================================


class TestLibaioFioCommandTimingOptions(unittest.TestCase):
    def setUp(self) -> None:
        self.cmd = _make_cmd({"time": "60", "time_based": "true", "ramp": "10"})

    def test_runtime_flag(self) -> None:
        self.assertIn("--runtime=60", self.cmd)

    def test_time_based_flag(self) -> None:
        self.assertIn("--time_based", self.cmd)

    def test_ramp_time_flag(self) -> None:
        self.assertIn("--ramp_time=10", self.cmd)


# ===========================================================================
# Mixed read/write modes
# ===========================================================================


class TestLibaioFioCommandMixedRw(unittest.TestCase):
    def test_randrw_has_rwmix_flags(self) -> None:
        cmd = _make_cmd({"mode": "randrw", "rwmixread": "70"})
        self.assertIn("--rwmixread=70", cmd)
        self.assertIn("--rwmixwrite=30", cmd)

    def test_readwrite_has_rwmix_flags(self) -> None:
        cmd = _make_cmd({"mode": "readwrite", "rwmixread": "40"})
        self.assertIn("--rwmixread=40", cmd)
        self.assertIn("--rwmixwrite=60", cmd)

    def test_randread_has_no_rwmix_flags(self) -> None:
        cmd = _make_cmd({"mode": "randread"})
        self.assertNotIn("--rwmixread", cmd)
        self.assertNotIn("--rwmixwrite", cmd)


# ===========================================================================
# Optional flags
# ===========================================================================


class TestLibaioFioCommandOptionalFlags(unittest.TestCase):
    def test_norandommap_flag(self) -> None:
        cmd = _make_cmd({"norandommap": "1"})
        self.assertIn("--norandommap", cmd)

    def test_rate_iops_flag(self) -> None:
        cmd = _make_cmd({"rate_iops": "5000"})
        self.assertIn("--rate_iops=5000", cmd)

    def test_random_distribution_flag(self) -> None:
        cmd = _make_cmd({"random_distribution": "zipf:1.2"})
        self.assertIn("--random_distribution=zipf:1.2", cmd)

    def test_log_avg_msec_flag(self) -> None:
        cmd = _make_cmd({"log_avg_msec": "500"})
        self.assertIn("--log_avg_msec=500", cmd)

    def test_size_flag_when_provided(self) -> None:
        cmd = _make_cmd({"size": "58982M"})
        self.assertIn("--size=58982M", cmd)

    def test_startdelay_flag(self) -> None:
        cmd = _make_cmd({"startdelay": "5"})
        self.assertIn("--startdelay=5", cmd)


# ===========================================================================
# Configurable ioengine
# ===========================================================================


class TestLibaioFioCommandIoengine(unittest.TestCase):
    def test_custom_ioengine(self) -> None:
        cmd = _make_cmd({"ioengine": "sync"})
        self.assertIn("--ioengine=sync", cmd)
        self.assertNotIn("--ioengine=libaio", cmd)

    def test_default_ioengine_is_libaio(self) -> None:
        cmd = _make_cmd()
        self.assertIn("--ioengine=libaio", cmd)

    def test_psync_ioengine(self) -> None:
        cmd = _make_cmd({"ioengine": "psync"})
        self.assertIn("--ioengine=psync", cmd)


# ===========================================================================
# Logging flags
# ===========================================================================


class TestLibaioFioCommandLoggingFlags(unittest.TestCase):
    def test_write_iops_log(self) -> None:
        cmd = _make_cmd()
        self.assertIn("--write_iops_log=", cmd)

    def test_write_bw_log(self) -> None:
        cmd = _make_cmd()
        self.assertIn("--write_bw_log=", cmd)

    def test_write_lat_log(self) -> None:
        cmd = _make_cmd()
        self.assertIn("--write_lat_log=", cmd)

    def test_no_logging_flags_when_disabled(self) -> None:
        cmd = _make_cmd({"log_iops": "false", "log_bw": "false", "log_lat": "false"})
        self.assertNotIn("--write_iops_log", cmd)
        self.assertNotIn("--write_bw_log", cmd)
        self.assertNotIn("--write_lat_log", cmd)


# ===========================================================================
# Recovery test
# ===========================================================================


class TestLibaioFioCommandRecovery(unittest.TestCase):
    def test_time_based_added_for_recovery(self) -> None:
        cmd = _make_cmd({"recovery_test": "blocking"})
        self.assertIn("--time_based", cmd)


# ===========================================================================
# Output directory path
# ===========================================================================


class TestLibaioFioCommandOutputPath(unittest.TestCase):
    def test_output_file_target_number(self) -> None:
        """output file is output.<target_number>"""
        cmd = _make_cmd({"target_number": "3"})
        self.assertIn("output.3", cmd)

    def test_output_path_includes_numjobs(self) -> None:
        cmd = _make_cmd({"numjobs": "4"})
        self.assertIn("numjobs-004", cmd)

    def test_total_iodepth_in_path(self) -> None:
        cmd = _make_cmd({"total_iodepth": "32", "iodepth": "16"})
        self.assertIn("total_iodepth-32", cmd)

    def test_no_total_iodepth_in_path_when_absent(self) -> None:
        cmd = _make_cmd()
        self.assertNotIn("total_iodepth-", cmd)


# ===========================================================================
# Benchmark property
# ===========================================================================


class TestLibaioFioCommandBenchmarkProperty(unittest.TestCase):
    def test_benchmark_type_libaio_fio(self) -> None:
        options: dict[str, str] = {
            "target_number": "0",
            "name": "w",
            "mode": "write",
            "op_size": "4096",
            "iodepth": "8",
            "numjobs": "1",
            "end_fsync": "0",
            "fio_out_format": "json,normal",
            "target": "/dev/vdb",
            "procs_per_volume": "1",
            "no_sudo": "false",
        }
        cmd_obj = LibaioFioCommand(options, "/tmp/out/")
        self.assertEqual(cmd_obj.benchmark, "libaio_fio")


# ===========================================================================
# Target path variants
# ===========================================================================


class TestLibaioFioCommandTargetVariants(unittest.TestCase):
    def test_image_file_target(self) -> None:
        cmd = _make_cmd({"target": "/srv/rbdfio-host-vdb/fio0.img"})
        self.assertIn("--name=/srv/rbdfio-host-vdb/fio0.img", cmd)

    def test_raw_block_device_target(self) -> None:
        cmd = _make_cmd({"target": "/dev/sdb"})
        self.assertIn("--name=/dev/sdb", cmd)


if __name__ == "__main__":
    unittest.main()
