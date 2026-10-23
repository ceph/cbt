"""
A class to encapsulate a set of configuration options that can be used to
construct the CLI to use to run a benchmark
"""

from collections import UserDict
from logging import Logger, getLogger
from typing import Optional

log: Logger = getLogger("cbt")


class CliOptions(UserDict[str, Optional[str]]):
    """
    This class encapsulates a set of CLI options that can be passed to a
    command line invocation. It is based on a python dictionary, but with
    behaviour modified so that duplicate keys do not update the original.
    """

    def __setitem__(self, key: str, value: Optional[str]) -> None:
        """
        Set an entry in the configuration.
        If the entry already exists, do not overwrite it.
        """
        if key not in self.data:
            self.data[key] = value
        else:
            log.debug("Not Updating %s:%s in configuration. Value already exists", key, value)

    def __getitem__(self, key: str) -> Optional[str]:
        """
        Get the value for key in the configuration.
        Return None and log a warning if the key does not exist
        """
        if key in self.data:
            return self.data[key]
        log.debug("Key %s does not exist in configuration", key)
        return None

    def clear(self) -> None:
        """
        Clear the configuration
        """
        self.data = {}
