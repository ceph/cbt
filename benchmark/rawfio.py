"""rawfio.py -- FIO benchmark against raw block devices (no filesystem)."""

import logging
import time
from typing import Any, Optional

import common
import settings
from benchmark.base_fio import FioBenchmark
from command.libaio_fio_command import LibaioFioCommand

logger = logging.getLogger("cbt")


class RawFio(FioBenchmark):  # pylint: disable=too-many-instance-attributes
    """FIO benchmark that runs directly against raw block devices (``/dev/vd*``)."""

    def __init__(self, archive_dir: str, cluster: Any, config: dict[str, Any]) -> None:
        super().__init__(archive_dir, cluster, config)

        # RawFio-specific parameters
        self.block_device_list: str = config.get("block_devices", "/dev/vdb")
        self.block_devices: list[str] = [d.strip() for d in self.block_device_list.split(",")]
        self.concurrent_procs: int = config.get("concurrent_procs", len(self.block_devices))
        self.total_procs: int = self.concurrent_procs * len(settings.getnodes("clients").split(","))
        # RawFio always uses plain JSON output format
        self.fio_out_format: str = "json"
        self.ioengine: str = config.get("ioengine", "libaio")
        self.vol_size: float = config.get("vol_size", 65536) * 0.9
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
        return "rawfio"

    def setup_targets(self) -> None:
        """Initialize fio on raw block devices."""
        clnts = settings.getnodes("clients")
        logger.info("Attempting to initialize fio files...")
        initializer_list = []
        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            fiopath = b
            pre_cmd = (
                f"sudo {self.fio_cmd} --rw=write --ioengine={self.ioengine} --bs={self.op_size} "
                f"--size {self.vol_size:.0f}M --name={fiopath} --output-format={self.fio_out_format} > /dev/null"
            )
            initializer_list.append(common.pdsh(clnts, pre_cmd, continue_if_error=False))
        for p in initializer_list:
            p.communicate()

    def prefill_targets(self) -> None:
        """No additional prefill needed beyond setup_targets for RawFio."""

    def cleanup_targets(self) -> None:
        """Cleanup is handled by cleanup()."""

    def _run_io_loop(self) -> None:
        """Non-workloads I/O loop: run fio against each raw block device."""
        clnts = settings.getnodes("clients")
        logger.info("Starting raw fio %s test.", self.mode)
        fio_process_list = []
        for i, iodepth in self.calculate_iodepth_per_target(self.concurrent_procs, self._iodepth_key).items():
            fiopath = self.block_devices[i % len(self.block_devices)]
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
                "name": "rawfio",
            }
            if self.startdelay is not None:
                options["startdelay"] = str(self.startdelay)
            self._populate_common_io_options(options)

            cmd = LibaioFioCommand(options, self.run_dir)
            cmd.set_executable(self.cmd_path_full)
            logger.debug("FIO CMD: %s", cmd.get())
            fio_process_list.append(common.pdsh(clnts, cmd.get(), continue_if_error=False))
        for p in fio_process_list:
            p.communicate()
        logger.info("Finished raw fio test")

    # ---------------------------------------------------------------------- #
    # Lifecycle
    # ---------------------------------------------------------------------- #

    def initialize(self) -> None:
        super().initialize()
        # First clean: give setup_targets() a guaranteed-empty run_dir to write
        # its fio initialiser output into (mount points, raw-device prep, etc.).
        common.pdsh(
            settings.getnodes("clients"),
            f"sudo rm -rf {self.run_dir}",
            continue_if_error=False,
        ).communicate()
        common.make_remote_dir(self.run_dir)
        clnts = settings.getnodes("clients")
        logger.info("creating mountpoints...")

        self.setup_targets()

        # Second clean: discard the initialiser output so the benchmark run
        # starts with an empty directory for its own output files.
        common.pdsh(clnts, f"rm -rf {self.run_dir}", continue_if_error=False).communicate()
        common.make_remote_dir(self.run_dir)

    def cleanup(self) -> None:
        super().cleanup()
        clnts = settings.getnodes("clients")

        logger.debug("Kill fio: %s", clnts)
        common.pdsh(clnts, "killall fio").communicate()
        time.sleep(3)
        common.pdsh(clnts, "killall -9 fio").communicate()
        common.pdsh(clnts, "killall -9 fio").communicate()

    def set_client_param(self, param: str, value: object) -> None:
        """Set a sysfs queue parameter on all vd* block devices."""
        cmd = f'find /sys/block/vd* ! -iname vda -exec sudo sh -c "echo {value} > {{{{}}}}//queue/{param}" \\;'
        common.pdsh(settings.getnodes("clients"), cmd).communicate()
