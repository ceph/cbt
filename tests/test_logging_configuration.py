"""
Unit tests for logging_configuration.py
"""

import logging
import unittest
from unittest.mock import MagicMock, patch

from logging_configuration import (
    FILE_DATEFMT,
    FILE_FORMAT,
    SCREEN_FORMAT,
    ColoredFormatter,
    setup_loggers,
)


class TestLogSupportConstants(unittest.TestCase):
    """Tests for the format string constants."""

    def test_screen_format_contains_levelname(self) -> None:
        self.assertIn("%(levelname)", SCREEN_FORMAT)

    def test_screen_format_contains_name(self) -> None:
        self.assertIn("%(name)s", SCREEN_FORMAT)

    def test_screen_format_contains_message(self) -> None:
        self.assertIn("%(message)s", SCREEN_FORMAT)

    def test_file_format_contains_asctime(self) -> None:
        self.assertIn("%(asctime)s", FILE_FORMAT)

    def test_file_format_contains_levelname(self) -> None:
        self.assertIn("%(levelname)", FILE_FORMAT)

    def test_file_format_contains_name(self) -> None:
        self.assertIn("%(name)", FILE_FORMAT)

    def test_file_format_contains_module_and_lineno(self) -> None:
        self.assertIn("%(module)s", FILE_FORMAT)
        self.assertIn("%(lineno)d", FILE_FORMAT)

    def test_file_datefmt_is_iso_like(self) -> None:
        self.assertIn("%Y", FILE_DATEFMT)
        self.assertIn("%H", FILE_DATEFMT)
        self.assertIn("%S", FILE_DATEFMT)


class TestColoredFormatter(unittest.TestCase):
    """Tests for ColoredFormatter."""

    def test_format_pads_levelname_to_eight_chars(self) -> None:
        formatter = ColoredFormatter(SCREEN_FORMAT)
        record = logging.LogRecord(
            name="cbt",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello",
            args=(),
            exc_info=None,
        )
        # patch has_a_tty to False so no colour codes are injected
        with patch("logging_configuration.has_a_tty", False):
            result = formatter.format(record)
        self.assertIn("INFO    ", result)

    def test_format_restores_record_after_formatting(self) -> None:
        formatter = ColoredFormatter(SCREEN_FORMAT)
        record = logging.LogRecord(
            name="cbt",
            level=logging.DEBUG,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        original_levelname = record.levelname
        with patch("logging_configuration.has_a_tty", False):
            formatter.format(record)
        self.assertEqual(record.levelname, original_levelname)


class TestSetupLoggers(unittest.TestCase):
    """Tests for setup_loggers()."""

    def setUp(self) -> None:
        # Start each test with a clean 'cbt' logger
        logger = logging.getLogger("cbt")
        logger.handlers.clear()

    def test_setup_loggers_adds_stream_handler(self) -> None:
        setup_loggers()
        logger = logging.getLogger("cbt")
        stream_handlers = [
            h
            for h in logger.handlers
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        ]
        self.assertEqual(len(stream_handlers), 1)

    def test_stream_handler_level_is_info(self) -> None:
        setup_loggers()
        logger = logging.getLogger("cbt")
        sh = next(
            h
            for h in logger.handlers
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        )
        self.assertEqual(sh.level, logging.INFO)

    def test_logger_level_is_debug(self) -> None:
        setup_loggers()
        self.assertEqual(logging.getLogger("cbt").level, logging.DEBUG)

    def test_calling_twice_does_not_duplicate_stream_handler(self) -> None:
        setup_loggers()
        setup_loggers()
        logger = logging.getLogger("cbt")
        stream_handlers = [
            h
            for h in logger.handlers
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        ]
        self.assertEqual(len(stream_handlers), 1)

    @patch("logging_configuration.os.makedirs")
    def test_setup_loggers_with_log_fname_adds_file_handler(self, mock_makedirs: MagicMock) -> None:
        with patch("logging_configuration.FileHandler", spec=logging.FileHandler) as mock_fh_cls:
            mock_fh_cls.return_value = MagicMock(spec=logging.FileHandler)
            setup_loggers(logfile_name="/tmp/test_cbt/cbt.log")
        mock_makedirs.assert_called_once_with("/tmp/test_cbt", exist_ok=True)

    def test_post_processing_loggers_propagate_to_cbt(self) -> None:
        """cbt.formatter / cbt.plotter / cbt.reports must reach the cbt root logger."""
        for child in ("cbt.formatter", "cbt.plotter", "cbt.reports", "cbt.parser"):
            child_logger = logging.getLogger(child)
            self.assertTrue(child_logger.propagate, f"{child} should propagate to parent")
            # Walk up the hierarchy and confirm 'cbt' is an ancestor
            parent = child_logger.parent
            found = False
            while parent:
                if parent.name == "cbt":
                    found = True
                    break
                parent = getattr(parent, "parent", None)
            self.assertTrue(found, f"'cbt' not found as ancestor of {child}")


if __name__ == "__main__":
    unittest.main()
