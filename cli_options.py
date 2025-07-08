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
    Thic class encapsulates a set of CLI options that can be passed to a
    command line invocation. It is based on a python dictionary, but with
    behaviour modified so that duplicate keys do not update the original.
    """

    def __setitem__(self, key: str, value: Optional[str]) -> None:
        """
        Add an entry to the configuration.
        Will report an error if key already exists
        """
        if key not in self.data.keys():
            self.data[key] = value
        else:
            log.debug("Not adding %s:%s to configuration. A value is already set", key, value)

    def __update__(self, key_value_pair: tuple[str, str]) -> None:
        """
        Update an existing entry in the configuration.
        If the entry exists then don't update it
        """
        key, value = key_value_pair
        if key not in self.data.keys():
            self.data[key] = value
        else:
            log.debug("Not Updating %s:%s in configuration. Value already exists", key, value)

    def __getitem__(self, key: str) -> Optional[str]:
        """
        Get the value for key in the configuration.
        Return None and log a warning if the key does not exist
        """
        if key in self.data.keys():
            return self.data[key]
        else:
            log.debug("Key %s does not exist in configuration", key)
            return None

    def clear(self) -> None:
        """
        Clear the configuration
        """
        self.data = {}
