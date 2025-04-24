"""
A class to deal with a command that will run a single instance of
a benchmark executable

It will return the full executable string that can be used to run a
cli command using whatever method the Benchmark chooses
"""

from abc import ABCMeta, abstractmethod
from logging import Logger, getLogger
from typing import Optional

from cli_options import CliOptions

log: Logger = getLogger("cbt")


class Command(metaclass=ABCMeta):
    """
    A class that encapsulates a single CLI command that can be run on a
    system
    """

    def __init__(self, options: dict[str, str]) -> None:
        self._executable: Optional[str] = None
        self._output_directory: str = ""
        self._options: CliOptions = self._parse_options(options)

    @abstractmethod
    def _parse_options(self, options: dict[str, str]) -> CliOptions:
        """
        Take the options passed in from the configuration yaml file and
        convert them to a list of key/value pairs that match the parameters
        to pass to the benchmark executable
        """

    @abstractmethod
    def _generate_full_command(self) -> str:
        """
        generate the full cli command that will be sent to the client
        to run the benchmark
        """

    @abstractmethod
    def _parse_global_options(self, options: dict[str, str]) -> CliOptions:
        """
        Parse the set of global options into the correct format for the command type
        """

    @abstractmethod
    def _generate_output_directory_path(self) -> str:
        """
        Generate the part of the output directory that is relevant to this
        specific command.

        The format is dependent on the specific Command implementation
        """

    def get(self) -> str:
        """
        get the full cli string that can be sent to a system.

        This string contains all the options for a single run of the
        benchmark executable
        """
        if self._executable is None:
            log.error("Executable has not yet been set for this command.")
            return ""

        return self._generate_full_command()

    def get_output_directory(self) -> str:
        """
        Return the output directory that will be used for this command
        """
        return self._generate_output_directory_path()

    def set_executable(self, executable_path: str) -> None:
        """
        set the executable to be used for this command
        """
        self._executable = executable_path

    def set_global_options(self, global_options: dict[str, str]) -> None:
        """
        Update the global options
        """
        self._options.update(self._parse_global_options(global_options))

    def update_options(self, new_options: dict[str, str]) -> None:
        """
        Update the command with the new_options dictionary
        """
        self._options.update(new_options)
        for key, value in new_options.items():
            if key not in self._options.keys():
                self._options[key] = value
            else:
                log.debug("key %s already exists. Not overwriting", key)
