"""
Tests for the check_regression.py CLI entry point.

run_comparison and setup_loggers are mocked: these tests cover argument parsing,
threshold validation and exit codes only.
"""

import contextlib
import io
import unittest
from unittest.mock import MagicMock, patch

from post_processing.post_processing_types import Thresholds
from tools.check_regression import main


class TestCheckRegressionExitCodes(unittest.TestCase):
    """Usage and input errors must exit 3 (ERROR), never 2 (FAIL)."""

    def setUp(self) -> None:
        run_patcher = patch("tools.check_regression.run_comparison", return_value=0)
        self.mock_run: MagicMock = run_patcher.start()
        self.addCleanup(run_patcher.stop)

        logger_patcher = patch("tools.check_regression.setup_loggers")
        logger_patcher.start()
        self.addCleanup(logger_patcher.stop)

    @staticmethod
    def _run_main(*argv: str) -> tuple[int, str]:
        """Run main() with the given argv; return (exit code, captured stderr)."""
        stderr = io.StringIO()
        with patch("sys.argv", ["check_regression.py", *argv]), contextlib.redirect_stderr(stderr):
            try:
                code = main()
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else -1
        return code, stderr.getvalue()

    def test_missing_candidate_exits_3(self) -> None:
        """A missing required argument is an input error (3), not a regression (2)."""
        code, stderr = self._run_main("--baseline", "base")
        self.assertEqual(code, 3, "Missing --candidate must exit 3")
        self.assertIn("--candidate", stderr, "The error message must name the missing argument")
        self.mock_run.assert_not_called()

    def test_non_numeric_threshold_exits_3(self) -> None:
        """A non-numeric --weak is an input error (3)."""
        code, _ = self._run_main("--baseline", "b", "--candidate", "c", "--weak", "abc")
        self.assertEqual(code, 3, "Non-numeric --weak must exit 3")
        self.mock_run.assert_not_called()

    def test_weak_not_below_strong_exits_3(self) -> None:
        """--weak >= --strong is rejected with exit 3."""
        code, _ = self._run_main("--baseline", "b", "--candidate", "c", "--weak", "0.5", "--strong", "0.1")
        self.assertEqual(code, 3, "--weak >= --strong must exit 3")
        self.mock_run.assert_not_called()

    def test_infinite_strong_exits_3(self) -> None:
        """--strong inf would make STRONG unreachable, so it is rejected with exit 3."""
        code, _ = self._run_main("--baseline", "b", "--candidate", "c", "--strong", "inf")
        self.assertEqual(code, 3, "--strong inf must exit 3")
        self.mock_run.assert_not_called()

    def test_valid_arguments_pass_thresholds_and_return_run_code(self) -> None:
        """Valid arguments reach run_comparison and its exit code is returned unchanged."""
        self.mock_run.return_value = 2
        code, _ = self._run_main("--baseline", "b", "--candidate", "c", "--weak", "0.1", "--strong", "0.2")
        self.assertEqual(code, 2, "run_comparison's exit code must be returned unchanged")
        passed: Thresholds = self.mock_run.call_args.args[2]
        self.assertEqual(passed, Thresholds(weak=0.1, strong=0.2), "Custom thresholds must be passed through")
