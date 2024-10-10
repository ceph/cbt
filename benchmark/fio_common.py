"""
The methods common to all fio subclasses
"""

import os
from abc import ABC, abstractmethod
from logging import Logger, getLogger
from pathlib import PurePath
from typing import Any, Dict, List, Optional

import common
import settings
from benchmark.benchmark import Benchmark
from cluster.cluster import Cluster

log: Logger = getLogger("cbt")


class FioCommon(Benchmark, ABC):
    """
    A base class that defines the form of all of the fio sub classes
    """

    VALID_FLAGS: List[str] = ["norandommap", "time_based"]

    def __init__(self, archive_dir: str, cluster: Cluster, configuration: Dict[str, Any]) -> None:
        super().__init__(archive_dir, cluster, configuration)  # type: ignore [no-untyped-call]

        self._cluster: Cluster = cluster
        self._output_directory: str = archive_dir

        self._defaults: Dict[str, str]

        self._configuration_with_defaults: Dict[str, Any] = {
            "output-format": "json,normal",
            "cmd_path": "/usr/bin/fio",
            "iodepth": "16",
            "end_fsync": "0",
        }
        self._create_configuration_with_defaults(configuration)

        self._cli_options: Dict[str, Optional[str]] = {}
        self._cli_flags: List[str] = []
        self._set_cli_options_from_configuration()
        self._set_cli_flags_from_configuration()

        self.cmd_path = f"{self._configuration_with_defaults.get('cmd_path')}"

        self._client_endpoints: Optional[str] = self._configuration_with_defaults.get("client_endpoints", None)

    @abstractmethod
    def initialize(self) -> None:
        """
        Set up any pre-confitions required for the test
        """

    @abstractmethod
    def initialize_endpoints(self) -> None:
        """
        Initialise the endpoints for this test. If they are not passed
        then get them from the cluster details
        """

    @abstractmethod
    def fio_command_extra(self, endpoint_number: int) -> str:
        """
        Extra parameters that are required for running with endpoints
        """

    @abstractmethod
    def _build_prefill_command(self, endpoint_number: int) -> str:
        """
        Build the CLI to be used to prefill any volumes used for this test
        """

    def exists(self) -> bool:
        """
        Make sure we do not overwrite results from a previous run of the tool
        """
        if os.path.exists(self._output_directory):
            log.info("Skipping existing test in %s.", self._output_directory)
            return True
        return False

    def cleanup(self) -> None:
        """
        Make sure that all the processes for this test have completed or been
        stopped
        """
        cmd_name = PurePath(self.cmd_path).name
        common.pdsh(settings.getnodes("clients"), f"sudo killall -2 {cmd_name}").communicate()  # type: ignore [no-untyped-call]

    def _get_full_command_path(self) -> str:
        """
        Work out the full path to the fio command, including sudo if
        required
        """
        cmd_path: str = f"{self.cmd_path_full} "

        if cmd_path[:4] != "sudo":
            cmd_path = f"sudo {cmd_path}"

        return cmd_path

    def _create_configuration_with_defaults(self, configuration: Dict[str, Any]) -> None:
        """
        Create the full configuration for this test including
        any default values
        """
        self._configuration_with_defaults.update(self._defaults)
        self._configuration_with_defaults.update(configuration)

    def _set_cli_options_from_configuration(self) -> None:
        """
        Convert the configuration passed in into our own internal structure
        that we can use to build the CLI to send to fio
        """
        ############################### Test options #########################
        self._cli_options["numjobs"] = self._configuration_with_defaults.get("numjobs", "1")
        self._cli_options["runtime"] = self._configuration_with_defaults.get("time", None)
        self._cli_options["ramp_time"] = self._configuration_with_defaults.get("ramp", None)
        self._cli_options["end_fsync"] = self._configuration_with_defaults.get("end_fsync", None)
        self._cli_options["random_distribution"] = self._configuration_with_defaults.get("random_distribution", None)
        self._cli_options["output-format"] = self._configuration_with_defaults.get("output-format")

        self._cli_options["rw"] = self._configuration_with_defaults.get("mode", "write")
        if self._cli_options["rw"] == "readwrite" or self._cli_options["rw"] == "randrw":
            rwmixread: int = self._configuration_with_defaults.get("rwmixread", 50)
            rwmixwrite: int = 100 - rwmixread
            self._cli_options["rwmixread"] = f"{rwmixread}"
            self._cli_options["rwmixwrite"] = f"{rwmixwrite}"

        ############################### I/O Options ##########################
        self._cli_options["bs"] = self._configuration_with_defaults.get("bs", None)
        if self._cli_options["bs"] is None:
            log.warning(
                "bs option is not set in configuration yaml file. Checking for the deprecated op_size option instead"
            )
            self._cli_options["bs"] = self._configuration_with_defaults.get("op_size", "4194304")

        self._cli_options["ioengine"] = self._configuration_with_defaults.get("ioengine", "libaio")
        self._cli_options["direct"] = self._configuration_with_defaults.get("direct", "1")
        self._cli_options["bssplit"] = self._configuration_with_defaults.get("bssplit", None)
        self._cli_options["bsrange"] = self._configuration_with_defaults.get("bsrange", None)
        self._cli_options["iodepth"] = self._configuration_with_defaults.get("iodepth")
        self._cli_options["rate_iops"] = self._configuration_with_defaults.get("rate_iops", None)
        self._cli_options["sync"] = self._configuration_with_defaults.get("sync", None)
        self._cli_options["end_fsync"] = self._configuration_with_defaults.get("end_fsync")
        self._cli_options["size"] = self._configuration_with_defaults.get("size", "4096")
        # This assumes M is the unit - do we want to add an option in future to set the
        # units?
        self._cli_options["size"] = f"{self._cli_options['size']}M"

        ############################# Logging Options ########################
        self._cli_options["log_avg_msec"] = self._configuration_with_defaults.get("log_avg_msec", None)

    def _generate_logging_options(self, output_file: str) -> None:
        """
        Set the logging options for the fio cli
        """
        if self._configuration_with_defaults.get("logging", True) is True:
            self._cli_options["write_iops_log"] = output_file
            self._cli_options["write_bw_log"] = output_file
            self._cli_options["write_lat_log"] = output_file

    def _set_cli_flags_from_configuration(self) -> None:
        """
        Convert any flags in the configuration to our internal structure
        """

        for flag in self.VALID_FLAGS:
            if self._configuration_with_defaults.get(flag, False):
                self._cli_flags.append(flag)

    def _generate_command_line(self, endpoint_number: int) -> str:
        """
        Actually create the commnad line that will be used to run fio I/O for
        this test
        """
        out_file: str = f"{self.run_dir}/output.{endpoint_number}"

        self._generate_logging_options(out_file)

        full_command: str = self._get_full_command_path()

        for option, value in self._cli_options.items():
            if value is not None:
                full_command += f"--{option}={value} "

        for flag in self._cli_flags:
            full_command += f"--{flag} "

        full_command += self.fio_command_extra(endpoint_number)

        # Make sure we log the output to out_file
        full_command += f" > {out_file}"

        return full_command

    def __str__(self) -> str:
        return f"Run directory: {self.run_dir}\nOutput: {self._output_directory}\nConfiguration: {self._configuration_with_defaults}"
