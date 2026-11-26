"""Unit tests for the options handling in FioCommand (command/fio_command.py).

FioCommand is abstract, so the tests go through RbdFioCommand, which needs no I/O engine setup
beyond its own option parsing.
"""

import unittest
from typing import Optional

from command.rbd_fio_command import RbdFioCommand


def _fio_command_line(**extra_options: str) -> str:
    """Build the fio command line for one target, with extra_options added to the minimum required options."""
    options = {
        "name": "workload",
        "target_number": "0",
        "numjobs": "1",
        "iodepth": "4",
        "mode": "randwrite",
        **extra_options,
    }
    command = RbdFioCommand(options, "/tmp/run")
    command.set_executable("/usr/bin/fio")
    return command.get()


class TestNorandommap(unittest.TestCase):
    """The --norandommap flag follows the string value that the workloads layer gives the option."""

    def test_flag_is_emitted_when_set_to_true(self) -> None:
        """norandommap: true switches the flag on."""
        self.assertIn(" --norandommap ", _fio_command_line(norandommap="true"))

    def test_flag_is_omitted_when_set_to_false(self) -> None:
        """norandommap: false reaches the command as the string "false", which must not switch the flag on."""
        self.assertNotIn("--norandommap", _fio_command_line(norandommap="false"))

    def test_flag_is_omitted_when_not_set(self) -> None:
        """No norandommap option means no flag."""
        self.assertNotIn("--norandommap", _fio_command_line())


class TestBooleanOptionsFollowTheSameRule(unittest.TestCase):
    """Boolean options already handled correctly, pinned so norandommap stays consistent with them."""

    def _assert_flag(self, option: str, flag: str, value: Optional[str], expected: bool) -> None:
        options = {} if value is None else {option: value}
        command_line = _fio_command_line(**options)
        self.assertEqual(flag in command_line, expected, f"{option}={value}: {command_line}")

    def test_time_based(self) -> None:
        """time_based is only switched on by the string "true"."""
        self._assert_flag("time_based", "--time_based", "true", True)
        self._assert_flag("time_based", "--time_based", "false", False)
        self._assert_flag("time_based", "--time_based", None, False)


if __name__ == "__main__":
    unittest.main()
