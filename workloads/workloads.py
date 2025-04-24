"""
The workloads class that contains all the Workloads for a given Benchmark run
"""

from logging import Logger, getLogger
from typing import Generator, Union

from workloads.workload import WORKLOAD_TYPE, WORKLOAD_YAML_TYPE, Workload

BENCHMARK_CONFIGURATION_TYPE = dict[
    str, dict[str, Union[str, list[str], dict[str, dict[str, Union[str, list[int]]]], dict[str, str]]]
]

log: Logger = getLogger("cbt")


class Workloads:
    def __init__(self, benchmark_configuration: BENCHMARK_CONFIGURATION_TYPE, base_run_directory: str) -> None:
        self._benchmark_configuration: BENCHMARK_CONFIGURATION_TYPE = benchmark_configuration
        self._base_run_directory: str = base_run_directory

        self._global_options: WORKLOAD_TYPE = self._get_global_options_from_configuration(benchmark_configuration)

        self._benchmark_type: str = ""
        self._executable: str = ""
        self._workloads: list[Workload] = []

        workloads_configuration: WORKLOAD_YAML_TYPE = benchmark_configuration.get("workloads", {})  # type: ignore[assignment]
        self._create_configurations(workloads_configuration)

    def exist(self) -> bool:
        """
        Return True if there is a workload configuration, otherwise False

        Can be used to check if we want to run a workload-style test run
        or a normal style test run
        """
        return self._workloads != []

    def get(self) -> Generator[Workload, None, None]:
        """
        Return all the workloads, one at a time
        """
        if self._workloads == []:
            return

        if not self._benchmark_type:
            log.error("Benchmark type has not been set. Run set_benchmark_type() to set it")

        if not self._executable:
            log.error("Executable path has not been set Run set_executable() to set it")

        for workload in self._workloads:
            workload.set_benchmark_type(self._benchmark_type)
            workload.set_executable(self._executable)
            # workload.create_output_directory()
            yield workload
        return

    def get_all_commands(self) -> Generator[str, None, None]:
        """
        Yield the string for each of the commands required to run all
        the workloads we know about
        TODO: Do we want this?????
        """
        for workload in self._workloads:
            for command in workload.get_commands():
                yield command

    def set_benchmark_type(self, benchmark_type: str) -> None:
        """
        set the benchmark type that will be used to run the workload

        This must be done by the benchmark before it attempts to run
        any commands
        """
        self._benchmark_type = benchmark_type

    def set_executable(self, executable_path: str) -> None:
        """
        Set the executable to be used for the
        """
        self._executable = executable_path

    def _create_configurations(self, workload_json: WORKLOAD_YAML_TYPE) -> None:
        """
        Get the options needed to construct the benchmark command to run the test
        """
        for workload_name, workload_options in workload_json.items():
            workload = Workload(workload_name, workload_options, self._base_run_directory)
            workload.add_global_options(self._global_options)
            # workload.set_benchmark_type(self._benchmark_type)

            self._workloads.append(workload)

    def _get_global_options_from_configuration(self, configuration: BENCHMARK_CONFIGURATION_TYPE) -> WORKLOAD_TYPE:
        """
        Get any configuration options from the yaml that are not workload
        specific
        """
        global_options: WORKLOAD_TYPE = {}

        for option_name, value in configuration.items():
            if option_name == "workloads" or option_name == "prefill":
                # prefill is not an option for workloads as it is used in the Benchmark prefill()
                # method.
                # Workloads we also want to ignore
                pass
            elif isinstance(value, list):
                global_options[option_name] = value
            else:
                global_options[option_name] = f"{value}"

        return global_options
