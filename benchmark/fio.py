"""
A benchark run using the fio I/O exercisor
"""

from logging import Logger, getLogger
from time import sleep
from typing import Any, Dict, List, Union

import client_endpoints_factory
import common
import monitoring
import settings
from benchmark.fio_common import FioCommon
from cluster.cluster import Cluster

log: Logger = getLogger("cbt")


class Fio(FioCommon):
    def __init__(self, archive_dir: str, cluster: Cluster, configuration: Dict[str, Any]):
        self._defaults = {"prefill_iodepth": "16"}
        super().__init__(archive_dir, cluster, configuration)

        self._procs_per_endpoint: int = self._configuration_with_defaults.get("procs_per_endpoint", 1)
        self._recovery_test_type: str = self._configuration_with_defaults.get("recov_test_type", "blocking")
        self._endpoints_per_client: int
        self._endpoint_type: str = ""
        self._endpoints: List[str] = []

    def initialize(self) -> None:
        common.clean_remote_dir(self.run_dir)  # type: ignore [no-untyped-call]
        common.make_remote_dir(self.run_dir)  # type: ignore [no-untyped-call]

    def initialize_endpoints(self) -> None:
        # Get the client_endpoints and set them up
        if self._client_endpoints is None:
            raise ValueError("No client_endpoints defined!")
        self.client_endpoints_object = client_endpoints_factory.get(self._cluster, self._client_endpoints)  # type: ignore [no-untyped-call]

        # Create the recovery image based on test type requested
        if "recovery_test" in self._cluster.config and self._recovery_test_type == "background":
            self.client_endpoints_object.create_recovery_image()  # type: ignore [no-untyped-call]
        self.create_endpoints()

    def create_endpoints(self) -> None:
        if not self.client_endpoints_object.get_initialized():  # type: ignore [no-untyped-call]
            self.client_endpoints_object.initialize()  # type: ignore [no-untyped-call]

        self._endpoint_type = self.client_endpoints_object.get_endpoint_type()  # type: ignore [no-untyped-call]
        self._endpoints_per_client = self.client_endpoints_object.get_endpoints_per_client()  # type: ignore [no-untyped-call]
        self._endpoints = self.client_endpoints_object.get_endpoints()  # type: ignore [no-untyped-call]

        # Error out if the aggregate fio size is going to be larger than the endpoint size
        assert self._cli_options["size"] is not None and self._cli_options["numjobs"] is not None
        aggregate_size = (
            int(self._cli_options["numjobs"]) * self._procs_per_endpoint * int(self._cli_options["size"][:1])
        )
        endpoint_size = self.client_endpoints_object.get_endpoint_size()  # type: ignore [no-untyped-call]
        if aggregate_size > endpoint_size:
            raise ValueError(
                "Aggregate fio data size (%dKB) exceeds end_point size (%dKB)! Please check numjobs, procs_per_endpoint, and size settings."
                % (aggregate_size, endpoint_size)
            )

        if self._endpoint_type == "rbd" and self._cli_options["ioengine"] != "rbd":
            log.warning("rbd endpoints must use the librbd fio engine! Setting ioengine=rbd")
            self.ioengine = "rbd"
        if self._endpoint_type == "rbd" and self._cli_options["direct"] != "1":
            log.warning("rbd endpoints must use O_DIRECT. Setting direct=1")
            self.direct = "1"

    def fio_command_extra(self, endpoint_number: int) -> str:
        command: str = ""

        # typical directory endpoints
        if self._endpoint_type == "directory":
            for proc_num in range(self._procs_per_endpoint):
                command = (
                    f"--name={self._endpoints[endpoint_number]}/`{common.get_fqdn_cmd()}"  # type: ignore [no-untyped-call]
                    + "`-{endpoint_number}-{proc_num} "
                )

        # handle rbd endpoints with the librbbd engine.
        elif self._endpoint_type == "rbd":
            pool_name, rbd_name = self._endpoints[endpoint_number].split("/")
            command += " --clientname=admin --invalidate=0"
            command += f" --pool={pool_name} --rbdname={rbd_name}"
            for proc_num in range(self._procs_per_endpoint):
                rbd_name = f"{self._endpoints[endpoint_number]}-{proc_num}"
                command += f" --name={rbd_name}"
        return command

    def _build_prefill_command(self, endpoint_number: int) -> str:
        command = f"sudo {self.cmd_path} --rw=write --bs=4M --iodepth={self._configuration_with_defaults.get('prefill_iodepth')}"

        for option in ["ioengine", "numjobs", "size", "output-format"]:
            command += f"--{option}={self._cli_options[option]}"

        command += self.fio_command_extra(endpoint_number)
        return command

    def prefill(self) -> None:
        if not self._configuration_with_defaults.get("prefill", True):
            return
        # pre-populate the fio files
        processes: List[Union[common.CheckedPopen, common.CheckedPopenLocal]] = []
        log.info("Attempting to prefill fio files")
        for endpoint_number in range(self._endpoints_per_client):
            process = common.pdsh(settings.getnodes("clients"), self._build_prefill_command(endpoint_number))  # type: ignore [no-untyped-call]
            processes.append(process)
        for process in processes:
            process.wait()

    def run(self) -> None:
        super().run()  # type: ignore [no-untyped-call]

        # We'll always drop caches for rados bench
        self.dropcaches()  # type: ignore [no-untyped-call]

        # Create the run directory
        common.make_remote_dir(self.run_dir)  # type: ignore [no-untyped-call]

        # dump the cluster config
        self._cluster.dump_config(self.run_dir)  # type: ignore [no-untyped-call]

        sleep(5)

        # Run the backfill testing thread if requested
        if "recovery_test" in self._cluster.config:
            if self._recovery_test_type == "blocking":
                recovery_callback = self.recovery_callback_blocking
            elif self._recovery_test_type == "background":
                recovery_callback = self.recovery_callback_background
            self._cluster.create_recovery_test(self.run_dir, recovery_callback, self._recovery_test_type)  # type: ignore [no-untyped-call]

        if "recovery_test" in self._cluster.config and self._recovery_test_type == "background":
            # Wait for signal to start client IO
            self._cluster.wait_start_io()  # type: ignore [no-untyped-call]

        monitoring.start(self.run_dir)  # type: ignore [no-untyped-call]

        log.info("Running fio %s test.", self._cli_options["rw"])
        processes: List[Union[common.CheckedPopen, common.CheckedPopenLocal]] = []
        for i in range(self._endpoints_per_client):
            process = common.pdsh(settings.getnodes("clients"), self._generate_command_line(i))  # type: ignore [no-untyped-call]
            processes.append(process)
        for process in processes:
            process.wait()
        # If we were doing recovery, wait until it's done.
        if "recovery_test" in self._cluster.config:
            self._cluster.wait_recovery_done()  # type: ignore [no-untyped-call]

        monitoring.stop(self.run_dir)  # type: ignore [no-untyped-call]

        # Finally, get the historic ops
        self._cluster.dump_historic_ops(self.run_dir)  # type: ignore [no-untyped-call]
        common.sync_files("%s/*" % self.run_dir, self._output_directory)  # type: ignore [no-untyped-call]
        self.analyze(self._output_directory)

    def recovery_callback_blocking(self) -> None:
        self.cleanup()

    def recovery_callback_background(self) -> None:
        log.info("Recovery thread completed!")

    def analyze(self, output_directory: str) -> None:
        """
        Convert the results from the run to a json format
        """
        log.info("Converting results to json format.")
        for client in settings.getnodes("clients").split(","):  # type: ignore [no-untyped-call]
            host = settings.host_info(client)["host"]  # type: ignore [no-untyped-call]
            for i in range(self._endpoints_per_client):
                found = 1
                out_file = "%s/output.%d.%s" % (output_directory, i, host)
                json_out_file = "%s/json_output.%d.%s" % (output_directory, i, host)
                with open(out_file) as fd:
                    with open(json_out_file, "w") as json_fd:
                        for line in fd.readlines():
                            if len(line.strip()) == 0:
                                found = 0
                                break
                            if found == 1:
                                json_fd.write(line)
