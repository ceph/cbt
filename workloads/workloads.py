"""
The workloads class that contains all the Workloads for a given Benchmark run
"""

from collections.abc import Generator
from logging import Logger, getLogger
from time import sleep
from typing import Optional, Union

import progress
from common import CheckedPopen, CheckedPopenLocal, make_remote_dir, pdsh  # pyright: ignore[reportUnknownVariableType]
from monitoring.monitoring_factory import MonitoringFactory
from settings import getnodes  # pyright: ignore[reportUnknownVariableType]
from workloads.workload import Workload
from workloads.workload_types import BenchmarkConfigurationType, WorkloadType, WorkloadYamlType

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

    def __init__(self, benchmark_configuration: BenchmarkConfigurationType, base_run_directory: str) -> None:
        self._benchmark_configuration: BenchmarkConfigurationType = benchmark_configuration
        self._base_run_directory: str = base_run_directory

        self._global_options: WorkloadType = self._get_global_options_from_configuration(benchmark_configuration)

        self._benchmark_type: str = ""
        self._executable: str = ""
        self._workloads: list[Workload] = []

        workloads_configuration: WorkloadYamlType = self._benchmark_configuration.get("workloads", {})
        self._create_configurations(workloads_configuration)

    def exist(self) -> bool:
        """
        Return True if there is a workload configuration, otherwise False

        Can be used to check if we want to run a workload-style test run
        or a normal style test run
        """
        return bool(self._workloads)

    def estimate_duration(self) -> int:
        """Estimate total wall-clock seconds for all workloads.

        For each workload, delegates to :meth:`~workloads.workload.Workload.phase_duration_secs`
        and multiplies by the number of parameter-set combinations.  Returns 0
        when no workloads are configured or when no duration is set on any workload.
        """
        total = 0
        for workload in self._workloads:
            per_set_secs = workload.phase_duration_secs()
            if per_set_secs:
                total += workload.param_set_count() * per_set_secs
        return total

    def run(self) -> None:  # pylint: disable=too-many-locals
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

        ramp_time: str = f"{self._benchmark_configuration.get('ramp', '')}"

        total_workloads = len(self._workloads)
        for workload_index, workload in enumerate(self._workloads, start=1):
            workload_name = workload.get_name()
            log.info("Starting workload '%s' (%d/%d)...", workload_name, workload_index, total_workloads)
            workload.set_benchmark_type(self._benchmark_type)
            workload.set_executable(self._executable)

            script_command: Optional[str] = None
            if workload.has_script():
                script_command = workload.get_script_command()
                log.debug("Scheduling script %s to run before workload %s", script_command, workload_name)

            for output_directory in workload.get_output_directories():
                make_remote_dir(output_directory)  # type: ignore[no-untyped-call]

            param_sets = list(workload.get_commands_list())
            total_param_sets = len(param_sets)
            for param_index, (output_directory, fio_command_list) in enumerate(param_sets, start=1):
                log.info(
                    "Workload '%s': running parameter set %d/%d -> %s",
                    workload_name,
                    param_index,
                    total_param_sets,
                    output_directory,
                )
                phase_desc = f"Workload '{workload_name}' {param_index}/{total_param_sets}"
                # Use the workload's own resolved time+ramp so the bar matches
                # the --runtime/--ramp_time values actually passed to the exerciser.
                phase_secs: Optional[int] = workload.phase_duration_secs()

                with progress.phase_bar(phase_desc, phase_secs, overall=progress.get_overall_bar()):
                    if script_command:
                        pdsh(getnodes("clients"), script_command).wait()  # type: ignore[no-untyped-call]

                    processes: list[Union[CheckedPopen, CheckedPopenLocal]] = [
                        pdsh(getnodes("clients"), fio_command)  # type: ignore[no-untyped-call]
                        for fio_command in fio_command_list
                    ]

                    # Sleep for the ramp time and then collect stats
                    if ramp_time:
                        log.info("Ramp time: waiting %ss before collecting stats...", ramp_time)
                        sleep(int(ramp_time))

                    MonitoringFactory.start(output_directory)

                    for process in processes:
                        process.wait()  # type: ignore[no-untyped-call]

                    MonitoringFactory.stop()
                log.info("Workload '%s': parameter set %d/%d complete.", workload_name, param_index, total_param_sets)

            log.info("Workload '%s' complete (%d/%d).", workload_name, workload_index, total_workloads)

        log.info("== Workloads completed ==")

    def command_groups(self) -> Generator[tuple[str, list[str]], None, None]:
        """Yield (output_directory, [command, ...]) for every run cell without executing.

        This factors the (pdsh-free) command-generation bookkeeping out of
        run() so a caller can choose how to fan the commands out — e.g. via a
        remote.RemoteExecutor for benchmarks that have moved off pdsh.

        set_benchmark_type() and set_executable() must be called first.

        TODO: pre_workload_script and ramp_time are not yet yielded here.
        TODO: collapse with run() once the rbdfio path is also executor-driven,
        so we have a single place where we set up and execute command lists.
        """
        if not self._benchmark_type:
            log.error("Benchmark type has not been set, set_benchmark_type() must be called first.")
            return

        if not self._executable:
            log.error("Executable path has not been set, set_executable() must be called first.")
            return

        for workload in self._workloads:
            workload.set_benchmark_type(self._benchmark_type)
            workload.set_executable(self._executable)
            yield from workload.get_commands_list()

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

    def _create_configurations(self, workload_json: WorkloadYamlType) -> None:
        """
        Get the options needed to construct the benchmark command to run the test
        """
        for workload_name, workload_options in workload_json.items():
            normalised_options = self._normalise_options(workload_options)
            workload = Workload(workload_name, normalised_options, self._base_run_directory)
            workload.add_global_options(self._global_options)
            # workload.set_benchmark_type(self._benchmark_type)

            self._workloads.append(workload)

    @staticmethod
    def _to_str(value: object) -> str:
        """Convert a scalar option value to its string representation.

        Booleans are lowercased (``True`` → ``"true"``, ``False`` → ``"false"``)
        so that downstream CLI tools receive the conventional form.
        The bool check must precede any int check because ``bool`` is a subclass
        of ``int`` in Python.
        """
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)

    @staticmethod
    def _normalise_options(options: WorkloadType) -> WorkloadType:
        """
        Convert every option value (and every element within list values)
        to a string so that dict[str, str | list[str]] is consistently
        typed before being handed to Workload / FioCommand (which expect
        dict[str, str]).
        Booleans are lowercased (``True`` → ``"true"``, ``False`` → ``"false"``).
        """
        normalised: WorkloadType = {}
        for key, value in options.items():
            if isinstance(value, list):
                normalised[key] = [Workloads._to_str(item) for item in value]
            else:
                normalised[key] = Workloads._to_str(value)
        return normalised

    def _get_global_options_from_configuration(self, configuration: BenchmarkConfigurationType) -> WorkloadType:
        """
        Get any configuration options from the test plan .yaml that are not workload
        specific
        """
        global_options: WorkloadType = {}

        for option_name, value in configuration.items():
            if option_name in ("workloads", "prefill"):
                # prefill is not an option for workloads as it is used in the Benchmark prefill()
                # method.
                # workloads we also want to ignore as these will be dealt with at a later date
                pass
            elif isinstance(value, list):
                global_options[option_name] = [Workloads._to_str(item) for item in value]
            else:
                global_options[option_name] = Workloads._to_str(value)

        return global_options
