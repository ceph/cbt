"""kvmrbdfio.py -- FIO benchmark against guest VM block devices (``/dev/vd*``)."""

import logging
import os
import time
from typing import Any, Optional

import common
import settings
from benchmark.base_fio import FioBenchmark
from command.libaio_fio_command import LibaioFioCommand

logger = logging.getLogger("cbt")


class KvmRbdFio(FioBenchmark):  # pylint: disable=too-many-instance-attributes
    """FIO benchmark that formats /dev/vd* with ext4, mounts, and runs fio image files."""

    def __init__(self, archive_dir: str, cluster: Any, config: dict[str, Any]) -> None:
        super().__init__(archive_dir, cluster, config)

        # KvmRbdFio-specific parameters
        self.block_device_list: str = config.get("block_devices", "/dev/vdb")
        self.block_devices: list[str] = [d.strip() for d in self.block_device_list.split(",")]
        self.concurrent_procs: int = config.get("concurrent_procs", len(self.block_devices))
        self.total_procs: int = self.concurrent_procs * len(settings.getnodes("clients").split(","))

        self.ioengine: str = config.get("ioengine", "libaio")
        self.pgs: int = config.get("pgs", 2048)
        self.vol_size: float = config.get("vol_size", 65536) * 0.9
        self.rep_size: int = config.get("rep_size", 1)
        self.rbdadd_mons = config.get("rbdadd_mons")
        self.rbdadd_options = config.get("rbdadd_options")
        self.client_ra: str = config.get("client_ra", "128")
        self.fio_cmd: str = config.get("fio_cmd", "/usr/bin/fio")

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
        return "kvmrbdfio"

    def setup_targets(self) -> None:
        """Format and mount block devices, then initialize fio image files."""
        clnts = settings.getnodes("clients")
        logger.info("creating mountpoints...")
        for b in self.block_devices:
            bnm = os.path.basename(b)
            mtpt = f"/srv/rbdfio-`{common.get_fqdn_cmd()}`-{bnm}"
            common.pdsh(clnts, f"sudo mkfs.ext4 {b}", continue_if_error=False).communicate()
            common.pdsh(clnts, f"sudo mkdir -p {mtpt}", continue_if_error=False).communicate()
            common.pdsh(clnts, f"sudo mount -t ext4 -o noatime {b} {mtpt}", continue_if_error=False).communicate()
        logger.info("Attempting to initialize fio files...")
        initializer_list = []
        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            bnm = os.path.basename(b)
            mtpt = f"/srv/rbdfio-`{common.get_fqdn_cmd()}`-{bnm}"
            fiopath = os.path.join(mtpt, f"fio{i}.img")
            pre_cmd = f"sudo {self.fio_cmd} --rw=write --ioengine=sync --bs=4M "
            pre_cmd = f"{pre_cmd} --size {self.vol_size:.0f}M --name={fiopath} > /dev/null"
            initializer_list.append(common.pdsh(clnts, pre_cmd, continue_if_error=False))
        for p in initializer_list:
            p.communicate()

    def prefill_targets(self) -> None:
        """No additional prefill needed beyond setup_targets for KvmRbdFio."""

    def cleanup_targets(self) -> None:
        """Cleanup is handled by cleanup()."""

    def _run_io_loop(self) -> None:
        """Non-workloads I/O loop: run fio against each KVM block device image file."""
        clnts = settings.getnodes("clients")
        logger.info("Starting rbd fio %s test.", self.mode)
        fio_process_list = []
        for i, iodepth in self.calculate_iodepth_per_target(self.concurrent_procs, self._iodepth_key).items():
            b = self.block_devices[i % len(self.block_devices)]
            bnm = os.path.basename(b)
            mtpt = f"/srv/rbdfio-`{common.get_fqdn_cmd()}`-{bnm}"
            fiopath = os.path.join(mtpt, f"fio{i}.img")
            options: dict[str, Optional[str]] = {
                "target_number": str(i),
                "target": fiopath,
                "ioengine": self.ioengine,
                "mode": self.mode,
                "op_size": str(self.op_size),
                "iodepth": str(iodepth),
                "numjobs": str(self.numjobs),
                "end_fsync": str(self.end_fsync),
                "fio_out_format": self.fio_out_format,
                "size": f"{self.vol_size:.0f}M",
                "procs_per_volume": "1",
                "name": "kvmrbd",
            }
            if self.startdelay is not None:
                options["startdelay"] = str(self.startdelay)
            self._populate_common_io_options(options)

            cmd = LibaioFioCommand(options, self.run_dir)
            cmd.set_executable(self.cmd_path_full)
            fio_process_list.append(common.pdsh(clnts, cmd.get(), continue_if_error=False))
        for p in fio_process_list:
            p.communicate()
        logger.info("Finished rbd fio test")

    # ---------------------------------------------------------------------- #
    # Override hooks
    # ---------------------------------------------------------------------- #

    def _pre_run_hook(self) -> None:
        """Set client read-ahead on all vd* block devices before I/O."""
        self.set_client_param("read_ahead_kb", self.client_ra)

    # ---------------------------------------------------------------------- #
    # Lifecycle
    # ---------------------------------------------------------------------- #

    def initialize(self) -> None:
        super().initialize()
        # First clean: give setup_targets() a guaranteed-empty run_dir to write
        # its fio initialiser output into (mount points, .img files, etc.).
        common.pdsh(
            settings.getnodes("clients", "osds", "mons", "rgws"),
            f"sudo rm -rf {self.run_dir}",
            continue_if_error=False,
        ).communicate()
        common.make_remote_dir(self.run_dir)

        self.setup_targets()

        # Second clean: discard the initialiser output so the benchmark run
        # starts with an empty directory for its own output files.
        common.pdsh(settings.getnodes("clients"), f"rm -rf {self.run_dir}", continue_if_error=False).communicate()
        common.make_remote_dir(self.run_dir)

    def cleanup(self) -> None:
        super().cleanup()
        clnts = settings.getnodes("clients")
        common.pdsh(clnts, "killall fio").communicate()
        time.sleep(3)
        common.pdsh(clnts, "killall -9 fio").communicate()
        time.sleep(3)
        common.pdsh(clnts, "rm -rf /srv/*/*", continue_if_error=False).communicate()
        common.pdsh(clnts, "sudo umount /srv/* || echo -n").communicate()

    def set_client_param(self, param: str, value: object) -> None:
        """Set a sysfs queue parameter on all vd* block devices."""
        cmd = f'find /sys/block/vd* ! -iname vda -exec sudo sh -c "echo {value} > {{}}/queue/{param}" \\;'
        common.pdsh(settings.getnodes("clients"), cmd).communicate()
