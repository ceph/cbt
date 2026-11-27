"""Unit tests for ElbenchoCommand (command/elbencho_command.py).

These tests pin the exact elbencho command line produced for a run cell, the
exact S3 auth-flag translation, and blocksize parsing — including the negative
cases (optional flags omitted when unset) and error/edge conditions.
"""

import unittest
from typing import Any, Optional

from command.elbencho_command import ElbenchoCommand

_AUTH = {"config": "access_key=AK;secret_key=SK;url=http://rgw:7480"}

# Boolean workload options and the elbencho flag each one switches on.
_BOOLEAN_FLAGS = {"deldirs": "--deldirs", "s3nompcheck": "--s3nompcheck", "mkdirs": "--mkdirs"}


def _cmd(
    workload: dict[str, Any],
    run_dir: str = "/tmp/run",
    auth: Optional[dict[str, str]] = None,
    cmd_path: str = "/usr/local/bin/elbencho",
    set_executable: bool = True,
) -> str:
    """
    Build the elbencho command line for one run cell.

    blocksize, threads and iodepth default to 4k, 1 and 1; put them in the workload to change them.
    Auth reaches the command as flat string options (as it does through the Workloads pipeline),
    not a separate constructor argument. It defaults to _AUTH.
    """
    credentials = _AUTH if auth is None else auth
    options = {
        "blocksize": "4k",
        "threads": 1,
        "iodepth": 1,
        **workload,
        "s3_auth_config": credentials.get("config", ""),
        "s3_session_token": credentials.get("s3_session_token", ""),
    }
    command = ElbenchoCommand(options, run_dir)
    if set_executable:
        command.set_executable(cmd_path)
    return command.get()


class TestParseBlocksizeToBytes(unittest.TestCase):
    """Test the conversion of a human readable blocksize into bytes."""

    def test_valid_conversions(self) -> None:
        """Sizes with and without a suffix convert to the right number of bytes."""
        cases = {
            "4k": 4096,
            "128k": 131072,
            "1m": 1048576,
            "1g": 1073741824,
            "512": 512,  # no suffix -> raw bytes
            "4K": 4096,  # case-insensitive
            "2M": 2097152,
            "1.5k": 1536,  # fractional multiplier
            " 4k ": 4096,  # surrounding whitespace tolerated
        }
        for text, expected in cases.items():
            with self.subTest(blocksize=text):
                self.assertEqual(expected, ElbenchoCommand.parse_blocksize_to_bytes(text))

    def test_invalid_forms_raise(self) -> None:
        """Text that is not a number with an optional k/m/g suffix is rejected."""
        for text in ["", "abc", "4kb", "1.2.3k", "k", "-4k", "4 k", "4tb"]:
            with self.subTest(blocksize=text):
                with self.assertRaises(ValueError):
                    ElbenchoCommand.parse_blocksize_to_bytes(text)


class TestBuildAuthFlags(unittest.TestCase):
    """Test the translation of the auth settings into elbencho S3 flags."""

    def test_full_config_exact_order(self) -> None:
        """Unknown keys are dropped and the flags come out as endpoints, key, secret in that order."""
        flags = ElbenchoCommand.build_auth_flags({"config": "access_key=AK;secret_key=SK;url=http://rgw:7480;retry=9"})
        self.assertEqual(
            ["--s3endpoints", "http://rgw:7480", "--s3key", "AK", "--s3secret", "SK"],
            flags,
        )

    def test_config_without_url_omits_endpoints(self) -> None:
        """No url in the config means no --s3endpoints flag."""
        self.assertEqual(
            ["--s3key", "AK", "--s3secret", "SK"],
            ElbenchoCommand.build_auth_flags({"config": "access_key=AK;secret_key=SK"}),
        )

    def test_config_url_only(self) -> None:
        """A config with only a url produces only --s3endpoints."""
        self.assertEqual(
            ["--s3endpoints", "http://rgw:7480"],
            ElbenchoCommand.build_auth_flags({"config": "url=http://rgw:7480"}),
        )

    def test_malformed_entries_are_skipped(self) -> None:
        """An entry with no '=' ("garbage") is ignored rather than crashing."""
        self.assertEqual(
            ["--s3key", "AK", "--s3secret", "SK"],
            ElbenchoCommand.build_auth_flags({"config": "access_key=AK;garbage;secret_key=SK"}),
        )

    def test_session_token_appended_after_config(self) -> None:
        """The session token flag comes after the flags from the config."""
        self.assertEqual(
            ["--s3endpoints", "http://rgw:7480", "--s3key", "AK", "--s3secret", "SK", "--s3authtoken", "tok"],
            ElbenchoCommand.build_auth_flags(
                {"config": "access_key=AK;secret_key=SK;url=http://rgw:7480", "s3_session_token": "tok"}
            ),
        )

    def test_token_only(self) -> None:
        """A session token without a config produces only --s3authtoken."""
        self.assertEqual(["--s3authtoken", "tok"], ElbenchoCommand.build_auth_flags({"s3_session_token": "tok"}))

    def test_empty_auth_and_empty_config_emit_nothing(self) -> None:
        """No credentials means no flags."""
        self.assertEqual([], ElbenchoCommand.build_auth_flags({}))
        self.assertEqual([], ElbenchoCommand.build_auth_flags({"config": ""}))


class TestGenerateFullCommand(unittest.TestCase):
    """Test the complete command line generated for a run cell."""

    def test_minimal_write_command_exact(self) -> None:
        """A minimal write workload produces exactly this command line."""
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write"})
        self.assertEqual(
            "/usr/local/bin/elbencho --write --threads 1 --block 4k --iodepth 1 "
            "--s3endpoints http://rgw:7480 --s3key AK --s3secret SK "
            "--s3region default "
            "--resfile /tmp/run/elbencho/write_4096/threads-001/iodepth-001/result.csv s3://bkt",
            cmd,
        )

    def test_all_options_command_exact(self) -> None:
        """
        Every optional field set, plus a session token, at a custom exe/run dir pins flag ordering end-to-end.

        Booleans are the "true" strings the Workloads layer produces.
        """
        workload = {
            "s3_bucket": "bkt",
            "mode": "write",
            "blocksize": "128k",
            "threads": 8,
            "iodepth": 16,
            "size": "4g",
            "num_objects": 100,
            "num_dirs": 4,
            "duration": 30,
            "deldirs": "true",
            "s3nompcheck": "true",
            "hosts": "c1,c2",
            "s3_region": "us-east-1",
            "mkdirs": "true",
        }
        cmd = _cmd(
            workload,
            run_dir="/tmp/myrun",
            cmd_path="/opt/elbencho",
            auth={"config": "access_key=AK;secret_key=SK;url=http://rgw:7480", "s3_session_token": "tok"},
        )
        self.assertEqual(
            "/opt/elbencho --write --threads 8 --block 128k --iodepth 16 "
            "--size 4g --files 100 --dirs 4 --timelimit 30 --deldirs --s3nompcheck "
            "--hosts c1,c2 --s3endpoints http://rgw:7480 --s3key AK --s3secret SK "
            "--s3authtoken tok --s3region us-east-1 "
            "--resfile /tmp/myrun/elbencho/write_131072/threads-008/iodepth-016/result.csv "
            "--mkdirs s3://bkt",
            cmd,
        )

    def test_optional_flags_absent_when_unset(self) -> None:
        """A minimal workload must not leak flags for options the user did not set, e.g. "--files None"."""
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write"})
        for flag in [
            "--size",
            "--files",
            "--dirs",
            "--timelimit",
            "--deldirs",
            "--s3nompcheck",
            "--hosts",
            "--mkdirs",
            "--s3authtoken",
        ]:
            with self.subTest(flag=flag):
                self.assertNotIn(flag, cmd)

    def test_boolean_flags_are_emitted_when_true(self) -> None:
        """The Workloads layer hands boolean options to the command as "true"/"false" strings."""
        for option, flag in _BOOLEAN_FLAGS.items():
            with self.subTest(option=option):
                cmd = _cmd({"s3_bucket": "bkt", "mode": "write", option: "true"})
                self.assertIn(flag, cmd)

    def test_boolean_flags_are_omitted_when_false(self) -> None:
        """The string "false" is non-empty and so truthy: it must still not switch the flag on."""
        for option, flag in _BOOLEAN_FLAGS.items():
            for value in ("false", False):
                with self.subTest(option=option, value=value):
                    cmd = _cmd({"s3_bucket": "bkt", "mode": "write", option: value})
                    self.assertNotIn(flag, cmd)

    def test_num_objects_zero_still_emits_files_flag(self) -> None:
        """
        0 is a meaningful value for num_objects.

        The builder keys off "is not None", so num_objects=0 must produce "--files 0", not be dropped as falsy.
        """
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write", "num_objects": 0})
        self.assertIn("--files 0", cmd)

    def test_block_uses_raw_blocksize_not_bytes(self) -> None:
        """The command passes elbencho the human blocksize (128k), never the byte count used for directory naming."""
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write", "blocksize": "128k"})
        self.assertIn("--block 128k", cmd)
        self.assertNotIn("--block 131072", cmd)

    def test_write_mode_is_exclusive(self) -> None:
        """Write mode emits --write and not --read."""
        cmd = _cmd({"s3_bucket": "bkt", "mode": "write"})
        self.assertIn("--write", cmd)
        self.assertNotIn("--read", cmd)

    def test_read_mode_is_exclusive(self) -> None:
        """Read mode emits --read and not --write."""
        cmd = _cmd({"s3_bucket": "bkt", "mode": "read"})
        self.assertIn("--read", cmd)
        self.assertNotIn("--write", cmd)

    def test_readwrite_emits_both_flags_in_order(self) -> None:
        """Readwrite mode emits --write then --read."""
        cmd = _cmd({"s3_bucket": "bkt", "mode": "readwrite"})
        self.assertIn("--write --read", cmd)

    def test_unknown_mode_raises_naming_the_mode(self) -> None:
        """An unknown mode raises a ValueError that names the mode."""
        with self.assertRaises(ValueError) as ctx:
            _cmd({"s3_bucket": "bkt", "mode": "bogus"})
        self.assertIn("bogus", str(ctx.exception))

    def test_unsupported_modes_return_empty_string(self) -> None:
        """
        stat and list are recognised by elbencho but not yet supported by the formatter.

        The builder yields "" and the run loop skips them.
        """
        for mode in ["stat", "list"]:
            with self.subTest(mode=mode):
                self.assertEqual("", _cmd({"s3_bucket": "bkt", "mode": mode}))

    def test_no_executable_returns_empty_string(self) -> None:
        """Without set_executable() there is nothing to run, so get() returns "" and not a command starting "None"."""
        self.assertEqual("", _cmd({"s3_bucket": "bkt", "mode": "write"}, set_executable=False))


if __name__ == "__main__":
    unittest.main()
