"""rbdfio.py -- Kernel RBD FIO benchmark (maps RBD block devices via krbd)."""

import logging
import time
from typing import Any, Optional

import common
import settings
from benchmark.base_fio import FioBenchmark
from command.libaio_fio_command import LibaioFioCommand
from monitoring.monitoring_factory import MonitoringFactory

logger = logging.getLogger("cbt")


class RbdFio(FioBenchmark):  # pylint: disable=too-many-instance-attributes
    """FIO benchmark using the kernel RBD driver (krbd): maps images, creates XFS, mounts."""

    def __init__(self, archive_dir: str, cluster: Any, config: dict[str, Any]) -> None:
        super().__init__(archive_dir, cluster, config)

        # RbdFio-specific parameters
        self.pool_profile: str = config.get("pool_profile", "default")
        self.concurrent_procs: int = config.get("concurrent_procs", 1)
        self.total_procs: int = self.concurrent_procs * len(settings.getnodes("clients").split(","))
        self.ioengine: str = config.get("ioengine", "libaio")
        self.vol_size: int = config.get("vol_size", 65536)
        self.vol_object_size: str = config.get("vol_object_size", "4M")
        self.rbdadd_mons = config.get("rbdadd_mons")
        self.rbdadd_options: str = config.get("rbdadd_options", "share")
        self.client_ra: int = config.get("client_ra", 128)
        self.poolname: str = "cbt-kernelrbdfio"
        # Make the file names string
        self.names: str = ""
        for i in range(self.concurrent_procs):
            self.names += f"--name={self.cluster.mnt_dir}/cbt-kernelrbdfio-`hostname -s`/cbt-kernelrbdfio-{i} "

    # ---------------------------------------------------------------------- #
    # FioBenchmark abstract interface
    # ---------------------------------------------------------------------- #

    @property
    def num_targets(self) -> int:
        """Number of I/O targets — concurrent processes."""
        return self.concurrent_procs

    @property
    def benchmark_type(self) -> str:
        """Benchmark name for the workloads path."""
        return "rbdfiokrbd"

    def setup_targets(self) -> None:
        """Create RBD pool and map kernel RBD device (delegates to mkimages)."""
        self.mkimages()

    def prefill_targets(self) -> None:
        """Populate fio files on the mounted filesystem."""
        size = self.vol_size * 0.9 / self.concurrent_procs
        pre_cmd = (
            f"sudo {self.cmd_path} --ioengine={self.ioengine} --rw=write"
            f" --numjobs={self.numjobs} --bs=4M --size {size:.0f}M {self.names} > /dev/null"
        )
        common.pdsh(settings.getnodes("clients"), pre_cmd).communicate()

    def cleanup_targets(self) -> None:
        """Cleanup is handled by cleanup()."""

    def _run_io_loop(self) -> None:
        """Non-workloads I/O loop: run fio against the single mounted krbd filesystem."""
        logger.info("Running rbd fio %s test.", self.mode)
        options: dict[str, Optional[str]] = {
            "target_number": "0",
            "target": self.names.strip(),
            "ioengine": self.ioengine,
            "mode": self.mode,
            "op_size": str(self.op_size),
            "iodepth": str(self.calculate_iodepth_per_target(self.concurrent_procs, self._iodepth_key)[0]),
            "numjobs": str(self.numjobs),
            "end_fsync": str(self.end_fsync),
            "fio_out_format": self.fio_out_format,
            "procs_per_volume": "1",
            "name": "krbd",
        }
        if self.vol_size:
            options["size"] = f"{int(self.vol_size) * 0.9:.0f}M"
        if self.startdelay is not None:
            options["startdelay"] = str(self.startdelay)
        self._populate_common_io_options(options)

        cmd = LibaioFioCommand(options, self.run_dir)
        cmd.set_executable(self.cmd_path_full)
        common.pdsh(settings.getnodes("clients"), cmd.get()).communicate()

    # ---------------------------------------------------------------------- #
    # Override hooks
    # ---------------------------------------------------------------------- #

    def _pre_run_hook(self) -> None:
        """Set client read-ahead on all RBD block devices before I/O."""
        self.set_client_param("read_ahead_kb", self.client_ra)

    # ---------------------------------------------------------------------- #
    # Lifecycle
    # ---------------------------------------------------------------------- #

    def initialize(self) -> None:
        super().initialize()

        logger.info("Pausing for 60s for idle monitoring.")
        MonitoringFactory.start(f"{self.run_dir}/idle_monitoring")
        time.sleep(60)
        MonitoringFactory.stop()

        common.sync_files(f"{self.run_dir}/*", self.out_dir)

        self.mkimages()

        # Create the run directory
        common.make_remote_dir(self.run_dir)

        # populate the fio files
        logger.info("Attempting to populating fio files...")
        self.prefill_targets()

    def set_client_param(self, param: str, value: object) -> None:
        """Set a sysfs queue parameter on all RBD block devices."""
        common.pdsh(
            settings.getnodes("clients"),
            f'find /sys/block/rbd* -exec sudo sh -c "echo {value} > {{{{}}}}//queue/{param}" \\;',
        ).communicate()

    def mkimages(self) -> None:
        """Create and map the kernel RBD image, mkfs XFS, and mount it."""
        MonitoringFactory.start(f"{self.run_dir}/pool_monitoring")
        self.cluster.rmpool(self.poolname, self.pool_profile)
        self.cluster.mkpool(self.poolname, self.pool_profile, "rbd")
        common.pdsh(
            settings.getnodes("clients"),
            f"/usr/bin/rbd create cbt-kernelrbdfio-`hostname -s` --size {self.vol_size} --pool {self.poolname}",
        ).communicate()
        common.pdsh(
            settings.getnodes("clients"),
            f"sudo rbd map cbt-kernelrbdfio-`hostname -s` --pool {self.poolname} --id admin",
        ).communicate()
        common.pdsh(
            settings.getnodes("clients"),
            "sudo mkfs.xfs /dev/rbd/cbt-kernelrbdfio/cbt-kernelrbdfio-`hostname -s`",
        ).communicate()
        common.pdsh(
            settings.getnodes("clients"),
            f"sudo mkdir -p -m0755 -- {self.cluster.mnt_dir}/cbt-kernelrbdfio-`hostname -s`",
        ).communicate()
        common.pdsh(
            settings.getnodes("clients"),
            f"sudo mount -t xfs -o noatime,inode64"
            f" /dev/rbd/cbt-kernelrbdfio/cbt-kernelrbdfio-`hostname -s`"
            f" {self.cluster.mnt_dir}/cbt-kernelrbdfio-`hostname -s`",
        ).communicate()
        MonitoringFactory.stop()
