"""
Logging configuration for CBT: coloured console output and optional file logging.
"""

import os
from enum import IntEnum
from logging import DEBUG, INFO, FileHandler, Formatter, Logger, LogRecord, StreamHandler, getLogger
from typing import Any, Optional, TextIO

has_a_tty = os.isatty(1)  # test stdout

# Unified log formats used by both cbt and post_processing
SCREEN_FORMAT = "%(levelname)-8s - %(name)s: %(message)s"
FILE_FORMAT = "%(asctime)s - %(levelname)-8s - %(name)-12s - %(module)s.%(funcName)s:%(lineno)d - %(message)s"
FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"


class Colour(IntEnum):
    """
    ANSI colour codes for terminal escape sequences.
    """

    BLACK = 30
    RED = 31
    GREEN = 32
    YELLOW = 33
    BLUE = 34
    MAGENTA = 35
    CYAN = 36
    WHITE = 37


_RESET = "\033[0m"
_LEVEL_COLOURS: dict[str, str] = {
    "DEBUG": f"\033[{Colour.BLUE}m",
    "INFO": f"\033[{Colour.GREEN}m",
    "WARNING": f"\033[{Colour.YELLOW}m",
    "ERROR": f"\033[{Colour.RED}m",
    "CRITICAL": f"\033[{Colour.RED}m",
}


class ColoredFormatter(Formatter):
    """
    A logging formatter that applies ANSI colour codes to level names on TTY outputs.
    """

    def __init__(self, msg: str, use_color: bool = True, datefmt: Optional[str] = None) -> None:
        Formatter.__init__(self, fmt=msg, datefmt=datefmt)
        self.use_color: bool = use_color

    def format(self, record: LogRecord) -> str:
        original_record: dict[str, Any] = record.__dict__
        record.__dict__ = record.__dict__.copy()
        levelname: str = record.levelname

        aligned_level_name: str = f"{levelname:<8}"

        if has_a_tty and levelname in _LEVEL_COLOURS:
            record.levelname = _LEVEL_COLOURS[levelname] + aligned_level_name + _RESET
        else:
            record.levelname = aligned_level_name

        res: str = super().format(record)

        # restore record, as it will be used by other formatters
        record.__dict__ = original_record
        return res


def setup_loggers(logfile_name: Optional[str] = None, log_file_mode: str = "w") -> None:
    """Configure the 'cbt' logger.

    Sets up a screen handler (INFO+, coloured on TTY) and, when log_fname is
    provided, a file handler (DEBUG+) writing to that path.

    Call once at startup without log_fname for early console output, then call
    again with log_fname=<archive_dir>/cbt.log once the archive directory is
    known.  The second call adds the file handler without duplicating the
    screen handler.
    """
    logger: Logger = getLogger("cbt")
    logger.setLevel(DEBUG)

    # Only add a stream handler if one is not already present
    if not any(isinstance(h, StreamHandler) and not isinstance(h, FileHandler) for h in logger.handlers):
        tty_output: StreamHandler[TextIO] = StreamHandler()
        tty_output.setLevel(INFO)
        tty_output.setFormatter(ColoredFormatter(SCREEN_FORMAT))
        logger.addHandler(tty_output)

    if logfile_name is not None:
        os.makedirs(os.path.dirname(logfile_name), exist_ok=True)
        log_file_output: FileHandler = FileHandler(logfile_name, mode=log_file_mode)
        log_file_output.setLevel(DEBUG)
        log_file_output.setFormatter(Formatter(FILE_FORMAT, datefmt=FILE_DATEFMT))
        logger.addHandler(log_file_output)
