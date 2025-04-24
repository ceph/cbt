"""
A class to deal with a command that will run a single instance of the
fio I/O exerciser

It will return the full executable string that can be used to run a
cli command using whatever method the Benchmark chooses
"""

from logging import Logger, getLogger
from typing import Optional

from cli_options import CliOptions
from command.command import Command
from common import get_fqdn_cmd

log: Logger = getLogger("cbt")


class FioCommand(Command):
    """
    The fio command class. This class represents a single fio command
    line that can be run on a local or remote client system.
    """

    REQUIRED_OPTIONS = {"ioengine": "rbd", "clientname": "admin", "invalidate": "0", "direct": "1"}
    DIRECT_TRANSLATIONS: list[str] = ["numjobs", "iodepth"]

    def __init__(self, options: dict[str, str], workload_output_directory: str) -> None:
        self._volume_number: int = int(options["volume_number"])
        self._total_iodepth: Optional[str] = options.get("total_iodepth", None)
        self._workload_output_directory: str = workload_output_directory
        super().__init__(options)

    def _parse_global_options(self, options: dict[str, str]) -> CliOptions:
        global_options: CliOptions = CliOptions(options)

        return global_options

    def _parse_options(self, options: dict[str, str]) -> CliOptions:
        fio_cli_options: CliOptions = CliOptions()

        fio_cli_options.update(self.REQUIRED_OPTIONS)
        for option in self.DIRECT_TRANSLATIONS:
            # Below only needed if testing?
            fio_cli_options[option] = options[option] if option in options.keys() else ""

        fio_cli_options["rw"] = options.get("mode", "write")
        fio_cli_options["output-format"] = options.get("fio_out_format", "json,normal")
        fio_cli_options["pool"] = options.get("poolname", "cbt-librbdfio")
        fio_cli_options["numjobs"] = options.get("numjobs", "1")
        fio_cli_options["bs"] = options.get("op_size", "4194304")
        fio_cli_options["end_fsync"] = f"{options.get('end_fsync', 0)}"

        if options.get("random_distribution", None) is not None:
            fio_cli_options["random_distribution"] = options.get("random_distribution", None)

        if options.get("log_avg_msec", None) is not None:
            fio_cli_options["log_avg_msec"] = options.get("log_avg_msec", None)

        if options.get("time", None) is not None:
            fio_cli_options["runtime"] = options.get("time", None)

        if options.get("ramp", None) is not None:
            fio_cli_options["ramp_time"] = options.get("ramp", None)

        if options.get("rate_iops", None) is not None:
            fio_cli_options["rate_iops"] = options.get("rate_iops", None)

        if bool(options.get("time_based", False)) is True:
            fio_cli_options["time_based"] = ""

        if bool(options.get("no_sudo", False)) is False:
            fio_cli_options["sudo"] = ""

        if options.get("norandommap", None) is not None:
            fio_cli_options["norandommap"] = ""

        if "recovery_test" in options.keys():
            fio_cli_options["time_based"] = ""

        # Secondary options
        if fio_cli_options["rw"] == "readwrite" or fio_cli_options["rw"] == "randrw":
            read_percent: str = options.get("rwmixread", "50")
            write_percent: str = f"{100 - int(read_percent)}"
            fio_cli_options["rwmixread"] = read_percent
            fio_cli_options["rwmixwrite"] = write_percent

        rbd_name: str = options.get("rbdname", "")
        if rbd_name == "":
            rbd_name = f"cbt-fio-`{get_fqdn_cmd()}`-{self._volume_number:d}"  # type: ignore[no-untyped-call]
        fio_cli_options["rbdname"] = rbd_name

        if bool(options.get("log_iops", True)):
            fio_cli_options["log_iops"] = ""

        if bool(options.get("log_bw", True)):
            fio_cli_options["log_bw"] = ""

        if bool(options.get("log_lat", True)):
            fio_cli_options["log_lat"] = ""

        processes_per_volume: int = int(options.get("procs_per_volume", 1))

        fio_cli_options["name"] = self._get_job_name(options["name"], processes_per_volume)

        return fio_cli_options

    def _generate_full_command(self) -> str:
        command: str = ""

        output_file: str = f"{self._generate_output_directory_path()}/output.{self._volume_number:d}"
        self._setup_logging(output_file)

        if "sudo" in self._options.keys():
            command += "sudo "
            del self._options["sudo"]

        command += f"{self._executable} "

        for name, value in self._options.items():
            if name == "name" and value is not None:
                for jobname in value.strip().split(" "):
                    command += f"--{name}={jobname} "
            elif value != "":
                command += f"--{name}={value} "
            else:
                command += f"--{name} "

        command += f"> {output_file}"

        return command

    def _generate_output_directory_path(self) -> str:
        """
        For an fio command the output format is:
        numjobs-<numjobs>/total_iodepth-<total_iodepth>/iodepth-<iodepth>
        if total_iodepth was used in the options, otherwise:
        numjobs-<numjobs>/iodepth-<iodepth>
        """
        output_path: str = f"{self._workload_output_directory}/numjobs-{int(str(self._options['numjobs'])):03d}/"

        if self._total_iodepth is not None:
            output_path += f"total_iodepth-{self._total_iodepth}/"

        output_path += f"iodepth-{int(str(self._options['iodepth'])):03d}"

        return output_path

    def _get_job_name(self, parent_workload_name: str, processes_per_volume: int) -> str:
        """
        Get the name for this job to give to fio
        This is of the format:

        cbt-<workload_name>-<hostname>-
        """

        job_name: str = ""

        for process_number in range(processes_per_volume):
            job_name += f"cbt-fio-{parent_workload_name}-`hostname`-file-{process_number} "

        return job_name

    def _setup_logging(self, output_file_name: str) -> None:
        """
        Set up the log paths if required
        """
        if "log_iops" in self._options.keys():
            self._options.pop("log_iops")
            self._options["write_iops_log"] = output_file_name

        if "log_bw" in self._options.keys():
            self._options.pop("log_bw")
            self._options["write_bw_log"] = output_file_name

        if "log_lat" in self._options.keys():
            self._options.pop("log_lat")
            self._options["write_lat_log"] = output_file_name
