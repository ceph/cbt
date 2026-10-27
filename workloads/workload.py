"""
The workload class that encapsulates a single workload that can be
run by any benchmark
"""

from collections.abc import Generator
from logging import Logger, getLogger
from typing import Optional

from command.command import Command
from command.elbencho_command import ElbenchoCommand
from command.endpoint_fio_command import EndpointFioCommand
from command.libaio_fio_command import LibaioFioCommand
from command.rbd_fio_command import RbdFioCommand
from common import all_configs  # pyright: ignore[reportUnknownVariableType]
from iodepth import calculate_iodepth_per_target, get_iodepth_key
from workloads.workload_types import WorkloadType

log: Logger = getLogger("cbt")


class Workload:
    """
    A single workload that is expected to be run against a client system.
    This workload can contain one or more Command objects, representing a
    single invocation of an I/O exerciser. Typically there will be a Command
    object for each volume_per_client, as specified in the configuration
    yaml
    """

    def __init__(self, name: str, options: WorkloadType, base_run_directory: str) -> None:
        self._name: str = name
        self._base_run_directory: str = base_run_directory
        self._commands: dict[int, list[Command]] = {}
        self._parent_benchmark_type: Optional[str] = None
        self._all_options: WorkloadType = options.copy()
        self._executable_path: str
        self._script: str = f"{options.get('pre_workload_script', '')}"

    def get_commands_list(self) -> Generator[tuple[str, list[str]], None, None]:
        """
        Return all I/O exerciser commands that need to be run to fully execute
        this workload
        """
        if not self._commands:
            self._create_commands_from_options()

        if not self._commands:
            log.warning("There are no commands for workload %s", self._name)
            return

        for command_list in self._commands.values():
            commands_list: list[str] = []
            output_directory: str = command_list[0].output_directory
            for command in command_list:
                command.set_executable(self._executable_path)
                commands_list.append(command.get())

            yield (output_directory, commands_list)
        return

    def get_output_directories(self) -> Generator[str, None, None]:
        """
        For each individual run of the I/O exerciser get the output directory
        for the results.

        Eventually the idea is to change this to 'create_output_directories()'
        and have the workload be able to create what is needed, but that will
        require more re-factoring in the CBT code that is outwith the scope of
        this change
        """
        if not self._commands:
            self._create_commands_from_options()

        unique_output_directories: set[str] = set()

        for command_list in self._commands.values():
            for command in command_list:
                unique_output_directories.add(command.output_directory)

        yield from unique_output_directories

    def phase_duration_secs(self) -> Optional[int]:
        """Return the estimated duration in seconds for a single parameter-set run.

        Uses the workload's own resolved options (workload-level values take
        precedence over global benchmark values via :meth:`add_global_options`),
        so this matches the ``--runtime`` and ``--ramp_time`` values that will
        actually be passed to the I/O exerciser.

        Benchmarks that express run length as ``duration`` (e.g. elbencho) are
        supported.  Setting both ``time`` and ``duration`` in the same workload
        is a configuration error and raises :exc:`ValueError`.

        Returns ``None`` when neither ``time`` nor ``duration`` is configured.
        """
        has_time = self._all_options.get("time") is not None
        has_duration = self._all_options.get("duration") is not None
        if has_time and has_duration:
            raise ValueError(f"Workload '{self._name}': 'time' and 'duration' are mutually exclusive — set only one.")
        if has_time:
            run_secs = int(str(self._all_options["time"]))
            ramp_secs = int(str(self._all_options.get("ramp", 0))) or 0
            return run_secs + ramp_secs
        if has_duration:
            return int(str(self._all_options["duration"]))
        return None

    def param_set_count(self) -> int:
        """Return the number of parameter-set iterations this workload will run.

        This is the product of the lengths of all list-valued options — the
        same expansion that ``all_configs`` performs — without constructing
        full config objects.  Used by :meth:`Workloads.estimate_duration`.
        """
        count = 1
        for value in self._all_options.values():
            if isinstance(value, list):
                count *= len(value)
        return max(count, 1)

    def get_name(self) -> str:
        """
        Return the name of this workload
        """
        return self._name

    def has_script(self) -> bool:
        """
        The test plan .yaml can specify a pre_workload_script that is to be run
        before every workload (see https://github.com/ceph/cbt/pull/329).

        We need to know if the user has specified a script to run before this
        particular workload.
        return True of there is a script, otherwise False
        """
        return self._script != ""

    def get_script_command(self) -> Optional[str]:
        """
        If the test plan yaml specifies a script to be run before this workload
        then return the command line invocation, otherwise return None
        """
        return self._script or None

    def set_executable(self, executable_path: str) -> None:
        """
        Set the executable to be used for the workload
        """
        self._executable_path = executable_path

    def set_benchmark_type(self, parent_benchmark_type: str) -> None:
        """
        Set the type of the parent benchmark for this workload. This
        determines which Command sub class we need to instantiate for this
        particular workload
        """
        self._parent_benchmark_type = parent_benchmark_type

    def add_global_options(self, global_options: WorkloadType) -> None:
        """
        Set any options for the workload that are not included in the
        'workloads' section of the configuration yaml

        if a value exists already in the configuration then ignore it
        """
        for key, value in global_options.items():
            if key not in self._all_options.keys():
                self._all_options[key] = value

    def _create_command_class(self, options: dict[str, Optional[str]]) -> Command:
        """
        Create the concrete command classes for each command for this workload
        """
        if self._parent_benchmark_type == "rbdfio":
            return RbdFioCommand(options, f"{self._base_run_directory}{self._name}")

        if self._parent_benchmark_type == "elbencho":
            # Elbencho builds its own output path (mode/blocksize-bytes/threads/
            # iodepth) from the base run directory, with no workload-name segment.
            return ElbenchoCommand(options, self._base_run_directory)

        if self._parent_benchmark_type in ("kvmrbdfio", "rawfio", "rbdfiokrbd"):
            return LibaioFioCommand(options, f"{self._base_run_directory}{self._name}")

        if self._parent_benchmark_type == "fio":
            return EndpointFioCommand(options, f"{self._base_run_directory}{self._name}")

        log.error("Benchmark Class %s is not supported by workloads yet", self._parent_benchmark_type)
        raise NotImplementedError

    def _create_commands_from_options(self) -> None:
        unique_options: dict[str, Optional[str]]

        set_number: int = 0
        for unique_options in all_configs(self._all_options):  # type: ignore[no-untyped-call]
            iodepth_key: str = get_iodepth_key(list(unique_options.keys()))
            unique_options["iodepth_key"] = iodepth_key
            iodepth: int = int(unique_options.get(iodepth_key) or 16)
            number_of_volumes: int = int(unique_options.get("volumes_per_client") or 1)
            iodepth_per_target: dict[int, int] = calculate_iodepth_per_target(number_of_volumes, iodepth, iodepth_key)
            unique_options["name"] = self._name

            command_list: list[Command] = []
            for target_number, iodepth in iodepth_per_target.items():
                unique_options["iodepth"] = f"{iodepth}"

                unique_options["target_number"] = f"{target_number}"
                command_list.append(self._create_command_class(unique_options))

            self._commands[set_number] = command_list
            set_number += 1

            # The above will overwrite the iodepth to be used for the command,
            # while still retaining a total_iodepth value if one is passed. We can then
            # use the total_iodepth value to add into the output_dir so we can read it
            # in post-processing.

    def __str__(self) -> str:
        return f"Name: {self._name}."
