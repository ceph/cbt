"""fio.py -- Endpoint-based FIO benchmark (libaio / librbd engines)."""

import logging
import pathlib
from typing import Any, Optional

import client_endpoints_factory
import common
import settings
from benchmark.base_fio import FioBenchmark
from command.endpoint_fio_command import EndpointFioCommand

logger = logging.getLogger("cbt")


class Fio(FioBenchmark):  # pylint: disable=too-many-instance-attributes
    """FIO benchmark using client endpoint abstractions (directory, rbd, etc.)."""

    def __init__(self, archive_dir: str, cluster: Any, config: dict[str, Any]) -> None:
        super().__init__(archive_dir, cluster, config)

        # Fio-specific parameters not covered by FioBenchmark
        self.prefill_iodepth: int = config.get("prefill_iodepth", 16)
        self.sync = config.get("sync", None)
        # Map the Fio-specific 'logging' key onto the base-class per-channel booleans
        # so that _populate_common_io_options() handles log suppression uniformly.
        _logging: bool = config.get("logging", True)
        self.log_iops = _logging
        self.log_bw = _logging
        self.log_lat = _logging
        self.ioengine: str = config.get("ioengine", "libaio")
        self.bssplit = config.get("bssplit", None)
        self.bsrange = config.get("bsrange", None)
        self.bs = config.get("bs", None)
        # op_size is set by FioBenchmark; bs is the preferred key going forward
        self.size: int = config.get("size", 4096)
        self.procs_per_endpoint: int = config.get("procs_per_endpoint", 1)
        self.prefill_flag: bool = config.get("prefill", True)
        self.out_dir: str = self.archive_dir
        self.client_endpoints = config.get("client_endpoints", None)
        # Populated by create_endpoints(); declared here to satisfy pylint
        self.client_endpoints_object: Any = None
        self.endpoint_type: str = ""
        self.endpoints_per_client: int = 0
        self.endpoints: list[Any] = []

    # ---------------------------------------------------------------------- #
    # FioBenchmark abstract interface
    # ---------------------------------------------------------------------- #

    @property
    def num_targets(self) -> int:
        """Number of I/O endpoints per client."""
        return getattr(self, "endpoints_per_client", 1)

    @property
    def benchmark_type(self) -> str:
        """Benchmark name for the workloads path."""
        return "fio"

    def setup_targets(self) -> None:
        """Endpoint setup is handled by initialize_endpoints()."""

    def prefill_targets(self) -> None:
        """Prefill is driven by prefill()."""

    def cleanup_targets(self) -> None:
        """Endpoint cleanup is handled by cleanup()."""

    def _apply_blocksize_option(self, options: dict[str, Optional[str]]) -> None:
        """Resolve bssplit > bsrange > bs > op_size and set the winning key in options."""
        if self.bssplit is not None:
            options["bssplit"] = self.bssplit
        elif self.bsrange is not None:
            options["bsrange"] = self.bsrange
        elif self.bs is not None:
            options["bs"] = self.bs
        else:
            options["op_size"] = str(self.op_size)

    def _build_io_options(self, ep_num: int, iodepth: int) -> dict[str, Optional[str]]:
        """Build the fio options dict for a single endpoint."""
        options: dict[str, Optional[str]] = {
            "target_number": str(ep_num),
            "endpoint_type": self.endpoint_type,
            "endpoint_path": self.endpoints[ep_num],
            "ioengine": self.ioengine,
            "mode": self.mode,
            "iodepth": str(iodepth),
            "numjobs": str(self.numjobs),
            "end_fsync": str(self.end_fsync),
            "fio_out_format": self.fio_out_format,
            "procs_per_endpoint": str(self.procs_per_endpoint),
            "procs_per_volume": str(self.procs_per_endpoint),
            "name": f"ep{ep_num}",
        }
        if self.size:
            options["size"] = str(self.size)
        if self.sync is not None:
            options["sync"] = str(self.sync)
        self._apply_blocksize_option(options)
        # Delegate all common options (direct, rwmixread, time, ramp, iodepth
        # distribution, logging flags, recovery_test, etc.) to the base method.
        self._populate_common_io_options(options)
        return options

    def _run_io_loop(self) -> None:
        """Non-workloads I/O loop: run fio against each endpoint."""
        logger.info("Running fio %s test.", self.mode)
        iodepth_per_endpoint = self.calculate_iodepth_per_target(self.endpoints_per_client, self._iodepth_key)
        ps = []
        for i, iodepth in iodepth_per_endpoint.items():
            cmd = EndpointFioCommand(self._build_io_options(i, iodepth), self.run_dir)
            cmd.set_executable(self.cmd_path_full)
            ps.append(common.pdsh(settings.getnodes("clients"), cmd.get()))
        for p in ps:
            p.wait()
        logger.info("fio %s test complete.", self.mode)

    # ---------------------------------------------------------------------- #
    # Lifecycle
    # ---------------------------------------------------------------------- #

    def initialize(self) -> None:
        super().initialize()

        # Clean and Create the run directory
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

    def initialize_endpoints(self) -> None:
        super().initialize_endpoints()

        # Get the client_endpoints and set them up
        if self.client_endpoints is None:
            raise ValueError("No client_endpoints defined!")
        self.client_endpoints_object = client_endpoints_factory.get(self.cluster, self.client_endpoints)

        # Create the recovery image based on test type requested
        if "recovery_test" in self.cluster.config and self.recov_test_type == "background":
            self.client_endpoints_object.create_recovery_image()
        self.create_endpoints()

    def create_endpoints(self) -> None:  # pylint: disable=attribute-defined-outside-init
        """Initialise endpoint objects and populate endpoint metadata attributes."""
        if not self.client_endpoints_object.get_initialized():
            self.client_endpoints_object.initialize()

        self.endpoint_type = self.client_endpoints_object.get_endpoint_type()
        self.endpoints_per_client = self.client_endpoints_object.get_endpoints_per_client()
        self.endpoints = self.client_endpoints_object.get_endpoints()

        # Error out if the aggregate fio size is going to be larger than the endpoint size
        aggregate_size = self.numjobs * self.procs_per_endpoint * self.size
        endpoint_size = self.client_endpoints_object.get_endpoint_size()
        if aggregate_size > endpoint_size:
            raise ValueError(
                f"Aggregate fio data size ({aggregate_size}KB) exceeds end_point size ({endpoint_size}KB)!"
                " Please check numjobs, procs_per_endpoint, and size settings."
            )

        if self.endpoint_type == "rbd" and self.ioengine != "rbd":
            logger.warning("rbd endpoints must use the librbd fio engine! Setting ioengine=rbd")
            self.ioengine = "rbd"

    def prefill(self) -> None:
        super().prefill()
        if not self.prefill_flag:
            return
        # populate the fio files
        ps = []
        logger.info("Attempting to prefill fio files...")
        for ep_num in range(self.endpoints_per_client):
            logger.info("Prefilling endpoint %d/%d...", ep_num + 1, self.endpoints_per_client)
            options: dict[str, Optional[str]] = {
                "target_number": str(ep_num),
                "endpoint_type": self.endpoint_type,
                "endpoint_path": self.endpoints[ep_num],
                "ioengine": self.ioengine,
                "mode": "write",
                "op_size": str(4 * 1024 * 1024),  # 4M prefill block size
                "iodepth": str(self.prefill_iodepth),
                "numjobs": str(self.numjobs),
                "end_fsync": "0",
                "fio_out_format": self.fio_out_format,
                "procs_per_endpoint": str(self.procs_per_endpoint),
                "procs_per_volume": str(self.procs_per_endpoint),
                "name": f"prefill-ep{ep_num}",
                # Suppress all logging for prefill
                "log_iops": "false",
                "log_bw": "false",
                "log_lat": "false",
            }
            if self.size:
                options["size"] = str(self.size)
            cmd = EndpointFioCommand(options, self.run_dir)
            cmd.set_executable(self.cmd_path)
            ps.append(common.pdsh(settings.getnodes("clients"), cmd.get()))
        for p in ps:
            p.wait()
        logger.info("Prefill complete.")

    def cleanup(self) -> None:
        """Send SIGINT to fio on all clients."""
        cmd_name = pathlib.PurePath(self.cmd_path).name
        common.pdsh(settings.getnodes("clients"), f"sudo killall -2 {cmd_name}").communicate()
