"""Unit tests for ElbenchoCommand (command/elbencho_command.py).

These tests pin the exact elbencho command line produced for a run cell, the
exact S3 auth-flag translation, and blocksize parsing — including the negative
cases (optional flags omitted when unset) and error/edge conditions.
"""

import unittest

from command.elbencho_command import ElbenchoCommand

_AUTH = {"config": "access_key=AK;secret_key=SK;url=http://rgw:7480"}


def _cmd(workload, bs="4k", threads=1, iodepth=1, run_dir="/tmp/run",
         auth=_AUTH, cmd_path="/usr/local/bin/elbencho", set_executable=True):
    # Auth reaches the command as flat string options (as it does through the
    # Workloads pipeline), not a separate constructor argument.
    options = {
        **workload,
        "blocksize": bs,
        "threads": threads,
        "iodepth": iodepth,
        "s3_auth_config": auth.get("config", ""),
        "s3_session_token": auth.get("s3_session_token", ""),
    }
    command = ElbenchoCommand(options, run_dir)
    if set_executable:
        command.set_executable(cmd_path)
    return command.get()


class TestParseBlocksizeToBytes(unittest.TestCase):

    def test_valid_conversions(self):
        cases = {
            "4k": 4096,
            "128k": 131072,
            "1m": 1048576,
            "1g": 1073741824,
            "512": 512,          # no suffix -> raw bytes
            "4K": 4096,          # case-insensitive
            "2M": 2097152,
            "1.5k": 1536,        # fractional multiplier
            " 4k ": 4096,        # surrounding whitespace tolerated
        }
        for text, expected in cases.items():
            with self.subTest(blocksize=text):
                self.assertEqual(expected, ElbenchoCommand.parse_blocksize_to_bytes(text))

    def test_invalid_forms_raise(self):
        for text in ["", "abc", "4kb", "1.2.3k", "k", "-4k", "4 k", "4tb"]:
            with self.subTest(blocksize=text):
                with self.assertRaises(ValueError):
                    ElbenchoCommand.parse_blocksize_to_bytes(text)


class TestBuildAuthFlags(unittest.TestCase):

    def test_full_config_exact_order(self):
        # retry=9 is an unknown key and must be dropped; the emitted flags must
        # be endpoints -> key -> secret in that order.
        flags = ElbenchoCommand.build_auth_flags(
            {"config": "access_key=AK;secret_key=SK;url=http://rgw:7480;retry=9"}
        )
        self.assertEqual(
            ["--s3endpoints", "http://rgw:7480", "--s3key", "AK", "--s3secret", "SK"],
            flags,
        )

    def test_config_without_url_omits_endpoints(self):
        self.assertEqual(
            ["--s3key", "AK", "--s3secret", "SK"],
            ElbenchoCommand.build_auth_flags({"config": "access_key=AK;secret_key=SK"}),
        )

    def test_config_url_only(self):
        self.assertEqual(
            ["--s3endpoints", "http://rgw:7480"],
            ElbenchoCommand.build_auth_flags({"config": "url=http://rgw:7480"}),
        )

    def test_malformed_entries_are_skipped(self):
        # "garbage" has no '=' and must be ignored rather than crash.
        self.assertEqual(
            ["--s3key", "AK", "--s3secret", "SK"],
            ElbenchoCommand.build_auth_flags({"config": "access_key=AK;garbage;secret_key=SK"}),
        )

    def test_session_token_appended_after_config(self):
        self.assertEqual(
            ["--s3endpoints", "http://rgw:7480", "--s3key", "AK", "--s3secret", "SK",
             "--s3authtoken", "tok"],
            ElbenchoCommand.build_auth_flags(
                {"config": "access_key=AK;secret_key=SK;url=http://rgw:7480", "s3_session_token": "tok"}
            ),
        )

    def test_token_only(self):
        self.assertEqual(["--s3authtoken", "tok"],
                         ElbenchoCommand.build_auth_flags({"s3_session_token": "tok"}))

    def test_empty_auth_and_empty_config_emit_nothing(self):
        self.assertEqual([], ElbenchoCommand.build_auth_flags({}))
        self.assertEqual([], ElbenchoCommand.build_auth_flags({"config": ""}))


class TestGenerateFullCommand(unittest.TestCase):

    def test_minimal_write_command_exact(self):
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write"})
        self.assertEqual(
            "/usr/local/bin/elbencho --write --threads 1 --block 4k --iodepth 1 "
            "--s3endpoints http://rgw:7480 --s3key AK --s3secret SK "
            "--s3region default "
            "--resfile /tmp/run/elbencho/write_4096/threads-001/iodepth-001/result.csv "
            "s3://bkt/$(hostname -s)",
            cmd,
        )

    def test_all_options_command_exact(self):
        # Every optional field set, plus a session token, at a custom exe/run dir.
        # Pins flag ordering end-to-end.
        workload = {
            "s3_bucket": "bkt", "mode": "write", "size": "4g", "num_objects": 100,
            "num_dirs": 4, "duration": 30, "deldirs": True, "s3nompcheck": True,
            "hosts": "c1,c2", "s3_region": "us-east-1", "mkdirs": True,
        }
        cmd = _cmd(
            workload, bs="128k", threads=8, iodepth=16, run_dir="/tmp/myrun",
            cmd_path="/opt/elbencho",
            auth={"config": "access_key=AK;secret_key=SK;url=http://rgw:7480", "s3_session_token": "tok"},
        )
        self.assertEqual(
            "/opt/elbencho --write --threads 8 --block 128k --iodepth 16 "
            "--size 4g --files 100 --dirs 4 --timelimit 30 --deldirs --s3nompcheck "
            "--hosts c1,c2 --s3endpoints http://rgw:7480 --s3key AK --s3secret SK "
            "--s3authtoken tok --s3region us-east-1 "
            "--resfile /tmp/myrun/elbencho/write_131072/threads-008/iodepth-016/result.csv "
            "--mkdirs s3://bkt/$(hostname -s)",
            cmd,
        )

    def test_optional_flags_absent_when_unset(self):
        # A minimal workload must not leak flags for options the user didn't set
        # (e.g. "--files None" or an always-on "--mkdirs").
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write"})
        for flag in ["--size", "--files", "--dirs", "--timelimit", "--deldirs",
                     "--s3nompcheck", "--hosts", "--mkdirs", "--s3authtoken"]:
            with self.subTest(flag=flag):
                self.assertNotIn(flag, cmd)

    def test_num_objects_zero_still_emits_files_flag(self):
        # 0 is a meaningful value: the builder keys off "is not None", so
        # num_objects=0 must produce "--files 0", not be dropped as falsy.
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write", "num_objects": 0})
        self.assertIn("--files 0", cmd)

    def test_block_uses_raw_blocksize_not_bytes(self):
        # The command must pass elbencho the human blocksize (128k), never the
        # byte count used elsewhere for directory naming.
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write"}, bs="128k")
        self.assertIn("--block 128k", cmd)
        self.assertNotIn("--block 131072", cmd)

    def test_write_mode_is_exclusive(self):
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write"})
        self.assertIn("--write", cmd)
        self.assertNotIn("--read", cmd)

    def test_read_mode_is_exclusive(self):
        cmd = _cmd({"s3_bucket": "bkt", "mode": "read"})
        self.assertIn("--read", cmd)
        self.assertNotIn("--write", cmd)

    def test_readwrite_emits_both_flags_in_order(self):
        cmd = _cmd({"s3_bucket": "bkt", "mode": "readwrite"})
        self.assertIn("--write --read", cmd)

    def test_unknown_mode_raises_naming_the_mode(self):
        with self.assertRaises(ValueError) as ctx:
            _cmd({"s3_bucket": "bkt", "mode": "bogus"})
        self.assertIn("bogus", str(ctx.exception))

    def test_unsupported_modes_return_empty_string(self):
        # stat/list are recognised by elbencho but not yet formatter-supported,
        # so the builder yields "" and the run loop skips them.
        for mode in ["stat", "list"]:
            with self.subTest(mode=mode):
                self.assertEqual("", _cmd({"s3_bucket": "bkt", "mode": mode}))

    def test_no_executable_returns_empty_string(self):
        # Without set_executable() there is nothing to run; get() must refuse
        # rather than emit a command starting with "None".
        self.assertEqual("", _cmd({"s3_bucket": "bkt", "mode": "write"}, set_executable=False))


class TestTotalIodepthAndMultiBucket(unittest.TestCase):
    """total_iodepth directory segment and per-bucket naming in the command line."""

    def test_total_iodepth_inserts_directory_segment(self):
        # The total_iodepth segment sits between threads- and iodepth- so the
        # post-processor can recover the aggregate depth from the path.
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write", "total_iodepth": 16}, threads=8, iodepth=4)
        self.assertIn(
            "--resfile /tmp/run/elbencho/write_4096/threads-008/total_iodepth-16/iodepth-004/result.csv",
            cmd,
        )

    def test_no_total_iodepth_segment_when_unset(self):
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write"}, iodepth=4)
        self.assertNotIn("total_iodepth-", cmd)

    def test_multi_bucket_suffixes_target_index(self):
        # With total_iodepth and >1 bucket, each process targets its own bucket.
        cmd = _cmd(
            {"s3_bucket": "cbt", "mode": "write", "total_iodepth": 16, "num_buckets": 4, "target_number": 2},
            iodepth=4,
        )
        self.assertTrue(cmd.endswith("s3://cbt-2/$(hostname -s)"), f"expected bucket suffix -2 in: {cmd}")

    def test_single_bucket_uses_base_name_even_with_total_iodepth(self):
        cmd = _cmd(
            {"s3_bucket": "cbt", "mode": "write", "total_iodepth": 16, "num_buckets": 1, "target_number": 0},
            iodepth=16,
        )
        self.assertTrue(cmd.endswith("s3://cbt/$(hostname -s)"), f"expected unsuffixed bucket in: {cmd}")

    def test_num_buckets_without_total_iodepth_is_ignored(self):
        # num_buckets is only meaningful alongside total_iodepth; on its own it
        # must not rename the bucket (contract in yaml-config-reference.md).
        cmd = _cmd(
            {"s3_bucket": "cbt", "mode": "write", "num_buckets": 4, "target_number": 3},
            iodepth=4,
        )
        self.assertTrue(cmd.endswith("s3://cbt/$(hostname -s)"), f"expected unsuffixed bucket in: {cmd}")
        self.assertNotIn("total_iodepth-", cmd)

    def test_mkdirs_precedes_suffixed_bucket(self):
        cmd = _cmd(
            {"s3_bucket": "cbt", "mode": "write", "total_iodepth": 8,
             "num_buckets": 2, "target_number": 1, "mkdirs": True},
            iodepth=4,
        )
        self.assertIn("--mkdirs s3://cbt-1/$(hostname -s)", cmd)

    def test_non_integer_num_buckets_raises_naming_key(self):
        with self.assertRaises(ValueError) as ctx:
            _cmd({"s3_bucket": "cbt", "mode": "write", "total_iodepth": 8, "num_buckets": "many"}, iodepth=4)
        self.assertIn("num_buckets", str(ctx.exception))

    def test_multi_bucket_resfiles_are_target_specific(self):
        # Concurrent per-bucket processes in one run cell must write distinct
        # result files; a shared result.csv would clobber all but one bucket.
        cmd0 = _cmd(
            {"s3_bucket": "cbt", "mode": "write", "total_iodepth": 16, "num_buckets": 4, "target_number": 0},
            iodepth=4,
        )
        cmd1 = _cmd(
            {"s3_bucket": "cbt", "mode": "write", "total_iodepth": 16, "num_buckets": 4, "target_number": 1},
            iodepth=4,
        )
        self.assertIn("/total_iodepth-16/iodepth-004/result-0.csv", cmd0)
        self.assertIn("/total_iodepth-16/iodepth-004/result-1.csv", cmd1)

    def test_single_bucket_resfile_is_plain_result_csv(self):
        # Single-bucket runs keep the established result.csv name (no suffix).
        cmd = _cmd(
            {"s3_bucket": "cbt", "mode": "write", "total_iodepth": 16, "num_buckets": 1, "target_number": 0},
            iodepth=16,
        )
        self.assertIn("/iodepth-016/result.csv", cmd)
        self.assertNotIn("result-0.csv", cmd)


if __name__ == "__main__":
    unittest.main()
