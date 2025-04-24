"""
The workload class that encapsulates a single workload that can be
run by any benchmark
"""

from logging import Logger, getLogger
from typing import Generator, Optional, Union

from benchmarkfactory import all_configs  # pyright: ignore[reportUnknownVariableType]
from command.command import Command
from command.fio_command import FioCommand

WORKLOAD_TYPE = dict[str, Union[str, list[str]]]  # pylint: disable=["invalid-name"]
WORKLOAD_YAML_TYPE = dict[str, WORKLOAD_TYPE]  # pylint: disable=["invalid-name"]

log: Logger = getLogger("cbt")


class Workload:
    """
    A single workload that is expected to be run against a client system.
    This workload can contain one or more Command objects, representing a
    single invocation of an I/O exerciser. Typically there will be a Command
    object for each volume_per_client, as specified in the configuration
    yaml
    """

    def __init__(self, name: str, options: WORKLOAD_TYPE, base_run_directory: str) -> None:
        self._name: str = name
        self._base_run_directory: str = base_run_directory
        self._commands: list[Command] = []
        self._parent_benchmark_type: Optional[str] = None
        self._all_options: WORKLOAD_TYPE = options.copy()
        self._executable_path: str
        self._script: str = f"{options.get('pre_workload_script', '')}"

    def get_commands(self) -> Generator[str, None, None]:
        """
        Return all I/O exerciser commands that need to be run to fully execute
        this workload
        """
        if not self._commands:
            self._create_commands_from_options()

        if not self._commands:
            log.warning("There are no commands for workload %s", self._name)
            return

        for command in self._commands:
            command.set_executable(self._executable_path)
            yield command.get()
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

        for command in self._commands:
            yield command.get_output_directory()

    def get_name(self) -> str:
        """
        Return the name of this workload
        """
        return self._name

    def has_script(self) -> bool:
        """
        True if there is a script for this workload, otherwise false
        """
        return self._script != ""

    def get_script_command(self) -> Optional[str]:
        """
        If the yaml specifies a script to be run before this workload then
        return the command line invocation, otherwise return None
        """
        if self._script == "":
            return None

        return self._script

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

    def add_global_options(self, global_options: WORKLOAD_TYPE) -> None:
        """
        Set any options for the workload that are not included in the
        'workloads' section of the configuration yaml

        if a value exists already in the configuration then ignore it
        """
        for key, value in global_options.items():
            if key not in self._all_options.keys():
                self._all_options[key] = value

    def _create_command_class(self, options: dict[str, str]) -> Command:
        """
        Create the concrete command classes for each command for this workload
        """
        if self._parent_benchmark_type == "fio":
            return FioCommand(options, f"{self._base_run_directory}/{self._name}")

        log.error("Benchmark Class %s is not supported by workloads yet", self._parent_benchmark_type)
        raise NotImplementedError

    def _create_commands_from_options(self) -> None:
        unique_options: dict[str, str]
        for unique_options in all_configs(self._all_options):  # type: ignore[no-untyped-call]
            iodepth_key: str = self._get_iodepth_key(list(unique_options.keys()))
            unique_options["iodepth_key"] = iodepth_key
            iodepth: int = int(unique_options.get(iodepth_key, 16))
            number_of_volumes: int = int(unique_options.get("volumes_per_client", 1))
            iodepth_per_volume: dict[int, int] = self._calculate_iodepth_per_volume(
                number_of_volumes, iodepth, iodepth_key
            )
            unique_options["name"] = self._name

            for volume_number, iodepth in iodepth_per_volume.items():
                unique_options["iodepth"] = f"{iodepth}"
                unique_options["volume_number"] = f"{volume_number}"
                self._commands.append(self._create_command_class(unique_options))

            # The above will overwrite the iodepth to be used for the command,
            # while still retaining a total_iodepth value if one is passed. We can then
            # use the total_iodepth value to add into the output_dir so we can read it
            # in post-processing.

    def _get_iodepth_key(self, configuration_keys: list[str]) -> str:
        """
        Get the range of iodepth values to use for this test. This will either
        be the list of total_iodepth values if the total_iodepth key exists,
        or the iodepth value if it does not
        """
        iodepth_key: str = "iodepth"
        if "total_iodepth" in configuration_keys:
            iodepth_key = "total_iodepth"

        return iodepth_key

    def _calculate_iodepth_per_volume(self, number_of_volumes: int, iodepth: int, iodepth_key: str) -> dict[int, int]:
        """
        Calculate the desired iodepth per volume for a single benchmark run.
        If total_iodepth is to be used calculate what the iodepth per volume
        should be and return that, otherwise return the iodepth value for each
        volume
        """
        if iodepth_key == "total_iodepth":
            return self._calculate_iodepth_per_volume_from_total_iodepth(number_of_volumes, iodepth)
        else:
            return self._set_iodepth_for_every_volume(number_of_volumes, iodepth)

    def _calculate_iodepth_per_volume_from_total_iodepth(
        self, number_of_volumes: int, total_desired_iodepth: int
    ) -> dict[int, int]:
        """
        Given the total desired iodepth and the number of volumes from the
        configuration yaml file, calculate the iodepth for each volume

        If the iodepth specified in total_iodepth is too small to allow
        an iodepth of 1 per volume, then reduce the number of volumes
        used to allow an iodepth of 1 per volume.
        """
        queue_depths: dict[int, int] = {}

        if number_of_volumes > total_desired_iodepth:
            log.warning(
                "The total iodepth requested: %s is less than 1 per volume (%s)",
                total_desired_iodepth,
                number_of_volumes,
            )
            log.warning(
                "Number of volumes per client will be reduced from %s to %s", number_of_volumes, total_desired_iodepth
            )
            number_of_volumes = total_desired_iodepth

        iodepth_per_volume: int = total_desired_iodepth // number_of_volumes
        remainder: int = total_desired_iodepth % number_of_volumes

        for volume_id in range(number_of_volumes):
            iodepth: int = iodepth_per_volume

            if remainder > 0:
                iodepth += 1
                remainder -= 1
            queue_depths[volume_id] = iodepth

        return queue_depths

    def _set_iodepth_for_every_volume(self, number_of_volumes: int, iodepth: int) -> dict[int, int]:
        """
        Given an iodepth value and the number of volumes return a dictionary
        that contains the desired iodepth value for each volume
        """
        queue_depths: dict[int, int] = {}
        for volume_id in range(number_of_volumes):
            queue_depths[volume_id] = iodepth

        return queue_depths

    def __str__(self) -> str:
        return f"Name: {self._name}."
