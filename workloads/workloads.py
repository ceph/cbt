"""
The workloads class that contains all the Workloads for a given Benchmark run
"""

from logging import Logger, getLogger
from typing import Union

from common import CheckedPopen, CheckedPopenLocal, make_remote_dir, pdsh  # pyright: ignore[reportUnknownVariableType]
from settings import getnodes  # pyright: ignore[reportUnknownVariableType]
from workloads.workload import WORKLOAD_TYPE, WORKLOAD_YAML_TYPE, Workload

BENCHMARK_CONFIGURATION_TYPE = dict[  # pylint: disable = ["invalid-name"]
    str, dict[str, Union[str, list[str], dict[str, dict[str, Union[str, list[int]]]], dict[str, str]]]
]

log: Logger = getLogger("cbt")


class Workloads:
    """
    A class that holds a collection of workloads that are used for a particular
    benchmark type.

    It parses the benchmark type configuration for a workloads section, and
    for each workload named therein it will create a workload object.

    set_executable() and set_benchmark_type() must be called on the Workloads
    before attemting to use the run() method
    """

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
        return bool(self._workloads)

    def run(self) -> None:
        """
        Run all the I/O exerciser commands for each workload in turn, including
        any scripts that should be run between workloads
        """
        if not self._workloads:
            log.error("No workloads to run %s", self._workloads)
            return

        if not self._benchmark_type:
            log.error("Benchmark type has not been set. Run set_benchmark_type() to set it")
            return

        if not self._executable:
            log.error("Executable path has not been set Run set_executable() to set it")
            return

        processes: list[Union[CheckedPopen, CheckedPopenLocal]] = []
        for workload in self._workloads:
            workload.set_benchmark_type(self._benchmark_type)
            workload.set_executable(self._executable)

            script_command = workload.get_script_command()
            if workload.has_script() and script_command is not None:
                log.debug("Scheduling script %s to run before workload %s", script_command, workload.get_name())
                pdsh(getnodes("clients"), script_command).wait()  # type: ignore[no-untyped-call]
            for output_directory in workload.get_output_directories():
                make_remote_dir(output_directory)  # type: ignore[no-untyped-call]
            for fio_command in workload.get_commands():
                processes.append(pdsh(getnodes("clients"), fio_command))  # type: ignore[no-untyped-call]
            for process in processes:
                process.wait()  # type: ignore[no-untyped-call]

        log.info("== Workloads completed ==")

    def get_names(self) -> str:
        """
        Get the names for all the workloads
        """
        names: str = ""
        for workload in self._workloads:
            names += f"{workload.get_name()} "
        return names

    def set_benchmark_type(self, benchmark_type: str) -> None:
        """
        set the benchmark type that will be used to run the workloads

        This must be done by the benchmark before it attempts to run
        any commands
        """
        self._benchmark_type = benchmark_type

    def set_executable(self, executable_path: str) -> None:
        """
        Set the executable to be used for this set of workloads.

        This must be set by the parent benchmark before calling the run() method
        """
        self._executable = executable_path

    def get_base_run_directory(self) -> str:
        """
        Return the base un directory for this test
        """
        return self._base_run_directory

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
        Get any configuration options from the test plan .yaml that are not workload
        specific
        """
        global_options: WORKLOAD_TYPE = {}

        for option_name, value in configuration.items():
            if option_name == "workloads" or option_name == "prefill":
                # prefill is not an option for workloads as it is used in the Benchmark prefill()
                # method.
                # workloads we also want to ignore as these will be dealt with at a later date
                pass
            elif isinstance(value, list):
                global_options[option_name] = value
            else:
                global_options[option_name] = f"{value}"

        return global_options
