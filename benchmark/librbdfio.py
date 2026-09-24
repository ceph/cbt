"""
lirbdfio.py -- module to support the FIO benchmark exercising RBD.
"""

import logging
import time
from typing import Any, Optional, Union

import common
import settings
from benchmark.base_fio import FioBenchmark
from command.rbd_fio_command import RbdFioCommand
from logging_configuration import setup_loggers
from monitoring.monitoring_factory import MonitoringFactory
from post_processing.post_processing_types import ReportOptions, ReportType
from post_processing.report import Report

logger = logging.getLogger("cbt")


class LibrbdFio(FioBenchmark):  # pylint: disable=too-many-instance-attributes
    """
    FIO benchmark using the librbd I/O engine (``--ioengine=rbd``).

    Creates and manages RBD pools and images on the Ceph cluster, optionally
    runs recovery tests, and supports the workloads execution path via the
    inherited ``FioBenchmark`` dispatch.
    """

    def __init__(self, archive_dir: str, cluster: Any, config: dict[str, Any]) -> None:
        super().__init__(archive_dir, cluster, config)

        # LibrbdFio-specific parameters
        self.pool_profile: str = config.get("pool_profile", "default")
        self.recov_pool_profile: str = config.get("recov_pool_profile", "default")
        self.data_pool_profile = config.get("data_pool_profile", None)

        self.pgs: int = config.get("pgs", 2048)
        self.vol_size: int = config.get("vol_size", 65536)
        self.vol_object_size: int = config.get("vol_object_size", 22)
        self.volumes_per_client: int = int(config.get("volumes_per_client", 1))
        self.procs_per_volume: int = config.get("procs_per_volume", 1)
        self.data_pool: Optional[str] = None

        self.use_existing_volumes: bool = bool(config.get("use_existing_volumes", False))
        self.no_sudo: bool = bool(config.get("no_sudo", False))
        self.idle_monitor_sleep: int = config.get("idle_monitor_sleep", 60)
        self.pool_name: str = config.get("poolname", "cbt-librbdfio")
        self.recov_pool_name: str = config.get("recov_pool_name", "cbt-rbdfio-recov")
        self.rbdname: str = config.get("rbdname", "")
        self.prefill_vols: dict[str, str] = config.get("prefill", {"blocksize": "4M", "numjobs": "1"})
        self.total_procs: int = (
            self.procs_per_volume * self.volumes_per_client * len(settings.getnodes("clients").split(","))
        )

        self.out_dir: str = self.archive_dir

        self.wait_pgautoscaler_timeout: int = config.get("wait_pgautoscaler_timeout", -1)
        # Make the file names string (repeated across volumes)
        self.names: str = ""
        for proc_num in range(self.procs_per_volume):
            rbd_name = f"cbt-rbdfio-`{common.get_fqdn_cmd()}`-file-{proc_num:d}"
            self.names += f"--name={rbd_name} "

    # ---------------------------------------------------------------------- #
    # FioBenchmark abstract interface
    # ---------------------------------------------------------------------- #

    @property
    def num_targets(self) -> int:
        """Number of RBD volumes per client."""
        return self.volumes_per_client

    @property
    def benchmark_type(self) -> str:
        """Benchmark name for the workloads path."""
        return "rbdfio"

    def setup_targets(self) -> None:
        """Create RBD pool and images (delegates to mkimages)."""
        self.mkimages()

    def prefill_targets(self) -> None:
        """Prefill RBD images with data (delegates to prefill)."""
        self.prefill()

    def cleanup_targets(self) -> None:
        """No teardown required for librbd images — pools are cleaned on next initialize."""

    def _build_io_options(self, volnum: int, iodepth: int, rbdname: str) -> dict[str, Optional[str]]:
        """Build the fio options dict for a single RBD volume."""
        options: dict[str, Optional[str]] = {
            "target_number": str(volnum),
            "rbdname": rbdname,
            "poolname": self.pool_name,
            "mode": self.mode,
            "op_size": str(self.op_size),
            "iodepth": str(iodepth),
            "numjobs": str(self.numjobs),
            "end_fsync": str(self.end_fsync),
            "fio_out_format": self.fio_out_format,
            "procs_per_volume": str(self.procs_per_volume),
            "name": f"cbt-rbdfio-`{common.get_fqdn_cmd()}`-file",
        }
        if self.no_sudo:
            options["no_sudo"] = "true"
        self._populate_common_io_options(options)
        return options

    def _run_io_loop(self) -> None:
        """Non-workloads I/O loop: dispatch one fio process per RBD volume."""
        logger.info("Running rbd fio %s test.", self.mode)
        ps = []
        for volnum, iodepth in self.calculate_iodepth_per_target(self.volumes_per_client, self._iodepth_key).items():
            rbdname = (
                self.rbdname
                if self.use_existing_volumes and len(self.rbdname)
                else f"cbt-rbdfio-`{common.get_fqdn_cmd()}`-{volnum:d}"
            )
            cmd = RbdFioCommand(self._build_io_options(volnum, iodepth, rbdname), self.run_dir)
            cmd.set_executable(self.cmd_path_full)
            ps.append(common.pdsh(settings.getnodes("clients"), cmd.get()))
        for p in ps:
            p.wait()
        logger.info("rbd fio %s test complete.", self.mode)

    # ---------------------------------------------------------------------- #
    # Override hooks
    # ---------------------------------------------------------------------- #

    def _pre_run_hook(self) -> None:
        """Check PG autoscaler before starting I/O to avoid skewed results."""
        ret = self.cluster.check_pg_autoscaler(self.wait_pgautoscaler_timeout, f"{self.run_dir}pgautoscaler.log")
        if ret == 1:
            logger.warning("PG autoscaler taking longer to complete. Continuing anyway...results may be skewed.")

    def _source_directory(self) -> str:
        """Return the workloads base run directory when workloads are active."""
        if self._workloads.exist():
            return f"{self._workloads.get_base_run_directory()}/*"
        return f"{self.run_dir}/*"

    def _generate_report(self) -> None:
        """Generate a post-run HTML/PDF report when ``create_report`` is set."""
        if not self._create_report:
            return
        report_config: dict[str, Union[str, bool]] = settings.report
        output_directory: str = str(report_config.get("output_directory", f"{self.out_dir}/report"))
        # Append post-processing output to the main CBT log in the archive
        setup_loggers(logfile_name=f"{self.archive_dir}/cbt.log", log_file_mode="a")
        report_options: ReportOptions = ReportOptions(
            archives=[f"{self.archive_dir}"],
            output_directory=output_directory,
            results_file_root="json_output",
            create_pdf=bool(report_config.get("create_pdf", False)),
            force_refresh=bool(report_config.get("force_refresh", False)),
            no_error_bars=bool(report_config.get("no_error_bars", False)),
            report_type=ReportType.SIMPLE,
            plot_resources=bool(report_config.get("plot_resources", False)),
        )
        report: Report = Report(report_options)
        report.generate()

    # ---------------------------------------------------------------------- #
    # Lifecycle
    # ---------------------------------------------------------------------- #

    def estimate_duration(self) -> int:
        """Estimate run-phase seconds.

        When workloads are configured, delegates to
        :meth:`~workloads.workloads.Workloads.estimate_duration` which accounts
        for the number of parameter-set combinations across all workloads.
        Falls back to plain ``time + ramp`` via the base-class implementation.
        """
        if self._workloads.exist():
            return self._workloads.estimate_duration()
        return super().estimate_duration()

    def initialize(self) -> None:
        super().initialize()
        # Clean and Create the run directory
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)
        logger.info("Pausing for %ds for idle monitoring.", self.idle_monitor_sleep)
        MonitoringFactory.start(f"{self.run_dir}idle_monitoring")
        time.sleep(self.idle_monitor_sleep)
        MonitoringFactory.stop()
        logger.info("Idle monitoring complete.")
        common.sync_files(f"{self.run_dir}/", self.out_dir)
        # Create the recovery image based on test type requested
        if "recovery_test" in self.cluster.config and self.recov_test_type == "background":
            self.mkrecovimage()
        if self._workloads.exist():
            logger.info("Workloads:\n    %s", self._workloads.get_names().replace(" ", "\n"))
        logger.info("Creating fio images...")
        self.mkimages()
        logger.info("Attempting to prefill fio images...")
        self.prefill()

    # ---------------------------------------------------------------------- #
    # Setup helpers
    # ---------------------------------------------------------------------- #

    def mkrecovimage(self) -> None:
        """Create a recovery image."""
        logger.info("Creating recovery image...")
        MonitoringFactory.start(f"{self.run_dir}/recovery_pool_monitoring")
        if self.use_existing_volumes is False:
            self.cluster.rmpool(self.recov_pool_name, self.recov_pool_profile)
            self.cluster.mkpool(self.recov_pool_name, self.recov_pool_profile, "rbd")
            for node in common.get_fqdn_list("clients"):
                for volnum in range(self.volumes_per_client):
                    node = node.rpartition("@")[2]
                    self.cluster.mkimage(
                        f"cbt-rbdfio-recov-{node}-{volnum:d}",
                        self.vol_size,
                        self.recov_pool_name,
                        self.data_pool,
                        self.vol_object_size,
                    )
        MonitoringFactory.stop()

    def mkimages(self) -> None:
        """Create an RBD pool and a number of volumes per client."""
        MonitoringFactory.start(f"{self.run_dir}/pool_monitoring")
        if self.use_existing_volumes is False:
            self.cluster.rmpool(self.pool_name, self.pool_profile)
            self.cluster.mkpool(self.pool_name, self.pool_profile, "rbd")
            if self.data_pool_profile:
                self.data_pool = self.pool_name + "-data"
                self.cluster.rmpool(self.data_pool, self.data_pool_profile)
                self.cluster.mkpool(self.data_pool, self.data_pool_profile, "rbd")
        client_list = common.get_fqdn_list("clients")
        total_images = len(client_list) * self.volumes_per_client
        image_num = 0
        for node in client_list:
            for volnum in range(self.volumes_per_client):
                node = node.rpartition("@")[2]
                image_num += 1
                image_name = f"cbt-rbdfio-{node}-{volnum:d}"
                logger.info("Creating fio image %d/%d: %s", image_num, total_images, image_name)
                self.cluster.mkimage(image_name, self.vol_size, self.pool_name, self.data_pool, self.vol_object_size)
        MonitoringFactory.stop()

    def prefill(self) -> None:
        """Execute a FIO cmd to prefill the volumes."""
        ps = []
        if not self.use_existing_volumes:
            rbd_base_name: str = self.config.get("rbdname", "cbt-rbdfio")
            for volnum in range(self.volumes_per_client):
                rbd_name = f"{rbd_base_name}-`{common.get_fqdn_cmd()}`-{volnum:d}"
                logger.info("Prefilling volume %d/%d: %s", volnum + 1, self.volumes_per_client, rbd_name)
                pre_cmd = ""
                if not self.no_sudo:
                    pre_cmd += "sudo "
                numjobs = self.prefill_vols["numjobs"]
                bs = self.prefill_vols["blocksize"]
                pre_cmd += (
                    f"{self.cmd_path} --ioengine=rbd --clientname=admin"
                    f" --pool={self.pool_name}"
                    f" --rbdname={rbd_name} --invalidate=0  --rw=write"
                    f" --numjobs={numjobs}"
                    f" --bs={bs}"
                    f" --size {self.vol_size:d}M {self.names}"
                    f" --output-format={self.fio_out_format} > /dev/null"
                )
                p = common.pdsh(settings.getnodes("clients"), pre_cmd)
                ps.append(p)
            for p in ps:
                p.wait()
            logger.info("Prefill complete.")
