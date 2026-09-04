"""Unit tests for the async parallel SSH module (remote/async_ssh.py).

These tests verify that the SSH/scp argv is constructed as expected and that
error conditions (non-zero exit, launch failure, mixed multi-node results) are
handled correctly. The actual ``asyncio.create_subprocess_exec`` call is
mocked — we do not exercise real asyncio/ssh here.
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from remote.async_ssh import AsyncSSHExecutor, _build_scp_args

_SSH_BASE = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no"]


def _make_proc(returncode=0, stdout=b"out", stderr=b"err"):
    """Build a fake asyncio subprocess with an awaitable communicate()."""
    proc = MagicMock()
    proc.communicate = AsyncMock(return_value=(stdout, stderr))
    proc.returncode = returncode
    return proc


class TestSshArgvConstruction(unittest.TestCase):

    @patch.dict("settings.cluster", {}, clear=True)
    def test_ssh_args_no_user(self):
        self.assertEqual(_SSH_BASE, AsyncSSHExecutor()._build_ssh_args())

    @patch.dict("settings.cluster", {"user": "bob"}, clear=True)
    def test_ssh_args_with_user(self):
        self.assertEqual(_SSH_BASE + ["-l", "bob"], AsyncSSHExecutor()._build_ssh_args())

    def test_scp_args(self):
        self.assertEqual(
            ["scp", "-r", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no"],
            _build_scp_args(),
        )

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_run_command_passes_expected_argv(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=0)
        AsyncSSHExecutor().run_command("h1", "ls -l")
        self.assertEqual(list(mock_exec.call_args.args), _SSH_BASE + ["h1", "ls -l"])

    @patch.dict("settings.cluster", {"user": "bob"}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_run_command_includes_user_in_argv(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=0)
        AsyncSSHExecutor().run_command("h1", "whoami")
        self.assertEqual(list(mock_exec.call_args.args), _SSH_BASE + ["-l", "bob", "h1", "whoami"])


class TestRunCommandResults(unittest.TestCase):

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_success_returns_result_tuple(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=0, stdout=b"hi", stderr=b"")
        results = AsyncSSHExecutor().run_command("h1", "ls")
        self.assertEqual([("h1", "hi", "", 0)], results)

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_fans_out_to_all_nodes(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=0)
        results = AsyncSSHExecutor().run_command("h1,h2,h3", "ls")
        self.assertEqual(3, mock_exec.call_count)
        self.assertEqual(3, len(results))


class TestRunCommandErrorHandling(unittest.TestCase):

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_nonzero_exit_raises_when_continue_false(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=1, stdout=b"", stderr=b"boom")
        with self.assertRaises(RuntimeError) as ctx:
            AsyncSSHExecutor().run_command("h1", "false", continue_if_error=False)
        self.assertIn("h1", str(ctx.exception))
        self.assertIn("exited 1", str(ctx.exception))

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_run_command_with_error_checking_raises(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=2, stderr=b"nope")
        with self.assertRaises(RuntimeError):
            AsyncSSHExecutor().run_command_with_error_checking("h1", "false")

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_nonzero_exit_continues_when_true(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=1, stderr=b"warn")
        with self.assertLogs("cbt", level="WARNING"):
            results = AsyncSSHExecutor().run_command("h1", "false", continue_if_error=True)
        self.assertEqual([("h1", "out", "warn", 1)], results)

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_launch_failure_is_captured_and_raised(self, mock_exec):
        # A failure to even launch the process is always surfaced as a
        # RuntimeError, even with continue_if_error=True.
        mock_exec.side_effect = OSError("cannot spawn ssh")
        with self.assertLogs("cbt", level="WARNING"):
            with self.assertRaises(RuntimeError):
                AsyncSSHExecutor().run_command("h1", "ls", continue_if_error=True)

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_multinode_one_failure_names_failing_host(self, mock_exec):
        # gather preserves task-creation order: h1 then h2.
        mock_exec.side_effect = [
            _make_proc(returncode=0),
            _make_proc(returncode=1, stderr=b"bad"),
        ]
        with self.assertRaises(RuntimeError) as ctx:
            AsyncSSHExecutor().run_command("h1,h2", "cmd", continue_if_error=False)
        self.assertIn("h2", str(ctx.exception))
        self.assertNotIn("[h1]", str(ctx.exception))

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_error_detail_prefers_stderr_over_stdout(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=1, stdout=b"stdout detail", stderr=b"stderr detail")
        with self.assertRaises(RuntimeError) as ctx:
            AsyncSSHExecutor().run_command("h1", "cmd", continue_if_error=False)
        self.assertIn("stderr detail", str(ctx.exception))
        self.assertNotIn("stdout detail", str(ctx.exception))

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_error_detail_uses_stderr_when_no_stdout(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=1, stdout=b"", stderr=b"stderr detail")
        with self.assertRaises(RuntimeError) as ctx:
            AsyncSSHExecutor().run_command("h1", "cmd", continue_if_error=False)
        self.assertIn("stderr detail", str(ctx.exception))

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_error_detail_falls_back_to_stdout_when_stderr_empty(self, mock_exec):
        mock_exec.return_value = _make_proc(returncode=1, stdout=b"stdout detail", stderr=b"")
        with self.assertRaises(RuntimeError) as ctx:
            AsyncSSHExecutor().run_command("h1", "cmd", continue_if_error=False)
        self.assertIn("stdout detail", str(ctx.exception))

    @patch.dict("settings.cluster", {}, clear=True)
    @patch("remote.async_ssh.asyncio.create_subprocess_exec", new_callable=AsyncMock)
    def test_zero_exit_with_stderr_is_not_a_failure(self, mock_exec):
        # A command that writes to stderr but exits 0 (e.g. warnings) succeeds:
        # only the exit status decides failure, never the presence of stderr.
        mock_exec.return_value = _make_proc(returncode=0, stdout=b"", stderr=b"just a warning")
        results = AsyncSSHExecutor().run_command("h1", "cmd", continue_if_error=False)
        self.assertEqual([("h1", "", "just a warning", 0)], results)


if __name__ == "__main__":
    unittest.main()
