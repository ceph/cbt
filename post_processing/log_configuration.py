"""
The log configuration for CBT post processing
"""

import logging.config
import os
from logging import Logger, getLogger
from typing import Any

from post_processing.types import HandlerType

LOGFILE_LOCATION: str = os.getenv("CBT_PP_LOGFILE_LOCATION", "/tmp")
LOGFILE_NAME_BASE: str = f"{LOGFILE_LOCATION}/cbt/post_processing"
LOGFILE_EXTENSION: str = ".log"

LOGGERS: list[str] = ["formatter", "plotter", "reports"]


def setup_logging() -> None:
    """
    Set up the logging for the post processing. This should be called before
    trying to post-process any results
    """
    os.makedirs(f"{LOGFILE_LOCATION}/cbt/", exist_ok=True)
    logging.config.dictConfig(_get_configuration())
    log: Logger = getLogger("formatter")

    log.info("=== Starting Post Processing of CBT results ===")


def _get_handlers_configuration() -> HandlerType:
    handlers: HandlerType = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "console",
            "level": "INFO",
            "stream": "ext://sys.stdout",
        },
        "log_file": {
            "class": "logging.FileHandler",
            "filename": f"{LOGFILE_NAME_BASE}{LOGFILE_EXTENSION}",
            "formatter": "logfile",
            "level": "DEBUG",
        },
    }

    return handlers


def _get_handler_names() -> list[str]:
    """
    return the names of the handlers
    """
    return list(_get_handlers_configuration().keys())


def _get_configuration() -> dict[str, Any]:
    """
    Builds the configuration dictionary for logging. It includes 2 handlers,
    one for a log file and one to log to the console.
    """

    logging_config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(levelname)s - %(module)s %(funcName)s : %(message)s",
                "datefmt": "%Y-%m-%d %H:%M",
            },
            "console": {
                "format": "%(levelname)s - %(module)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M",
            },
            "logfile": {
                "format": "%(asctime)s - %(levelname)s - %(module)s %(funcName)s:%(lineno)d : %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "loggers": {},
    }

    # For each logger we want to use, we need to do some setup
    for logger_name in LOGGERS:
        new_logger: dict[str, Any] = {
            "handlers": _get_handler_names(),
            "level": "DEBUG",
            "propagate": False,
            "filename": f"{LOGFILE_NAME_BASE}{LOGFILE_EXTENSION}",
        }
        logging_config["loggers"][logger_name] = new_logger

    handler_entry: HandlerType = _get_handlers_configuration()
    logging_config["handlers"] = handler_entry

    return logging_config
