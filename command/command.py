"""
A class to deal with a command that will run a single instance of
a benchmark executable

It will return the full executable string that can be used to run a
cli command using whatever method the Benchmark chooses
"""

from abc import ABC, abstractmethod
from logging import Logger, getLogger
from typing import Optional

from cli_options import CliOptions

log: Logger = getLogger("cbt")


def require_int(key: str, value: object) -> int:
    """
    Convert an option value to int, naming the offending key on failure.

    Option values arrive as strings from the yaml, so a typo surfaces as a
    conversion error a long way from the key that caused it. Use this instead
    of a bare int() wherever the value came from a configuration file.
    """
    try:
        return int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} value {value!r} is not an integer") from exc


class Command(ABC):
    """
    A class that encapsulates a single CLI command that can be run on a
    system
    """

    def __init__(self, options: dict[str, str], workload_output_directory: str) -> None:
        self._executable: Optional[str] = None
        self._output_directory: str = ""
        # Set before _parse_options(), which subclasses may use it from.
        self._workload_output_directory: str = workload_output_directory
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

    @property
    @abstractmethod
    def benchmark(self) -> str:
        """
        Return the benchmark type for this command type
        """

    @staticmethod
    def target_count(options: dict[str, str]) -> int:
        """
        How many parallel targets a single run cell fans out to.

        The Workload builds one Command per target and divides the cell's
        iodepth between them, so this is what decides the fan-out. What a
        target *is* belongs to the benchmark: fio drives one per volume, so
        that is the default. Override in a subclass that fans out over
        something else rather than teaching Workload about it.
        """
        return require_int("volumes_per_client", options.get("volumes_per_client", 1))

    @staticmethod
    def workload_output_directory(base_run_directory: str, workload_name: str) -> str:
        """
        Root directory beneath which this command writes its results.

        Defaults to a per-workload subtree so that two workloads in one test
        plan cannot collide. Override where the benchmark's own path layout
        already distinguishes its runs.
        """
        return f"{base_run_directory}{workload_name}"

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

    @property
    def output_directory(self) -> str:
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

    @property
    def ramp_time(self) -> Optional[str]:
        """
        Get the ramp time for this command
        """
        return self._options.get("ramp_time", None)
