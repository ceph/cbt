"""
An FioCommand subclass for I/O engines that target local files or block devices
(libaio, sync, psync, etc.) as opposed to the rbd or rados I/O engines.

Concrete use-cases served by this class:
  - KvmRbdFio  - image files inside XFS/ext4 mounts on /dev/vd* devices
  - RawFio     - raw block devices (/dev/vd* etc.)
  - RbdFio     - files inside kernel-RBD XFS mounts
  - Fio        - generic libaio workloads against endpoint directories

Engine-specific options parsed from the `options` dict:
  ioengine      I/O engine name (default: "libaio")
  target        Path to the file or block device used as the FIO target; maps
                to FIO's --name flag (required).
  size          Target size passed verbatim to --size (optional).
  startdelay    Per-job start delay in seconds (optional).
"""

from typing import Optional

from command.fio_command import FioCommand


class LibaioFioCommand(FioCommand):
    """
    An FioCommand type for libaio / block-device / file-based I/O engines.

    The only engine-specific parameters are the I/O engine name, the target
    path (--name), an optional --size, and an optional --startdelay.  All
    common FIO flags (rw, bs, iodepth, numjobs, logging, …) are handled by
    the base FioCommand class.
    """

    _DEFAULT_IOENGINE: str = "libaio"

    def __init__(self, options: dict[str, Optional[str]], workload_output_directory: str) -> None:
        self._target_path: str = options.get("target") or ""
        super().__init__(options, workload_output_directory)

    @property
    def benchmark(self) -> str:
        return "libaio_fio"

    def _parse_ioengine_specific_parameters(self, options: dict[str, Optional[str]]) -> dict[str, Optional[str]]:
        libaio_options: dict[str, Optional[str]] = {}

        libaio_options["ioengine"] = options.get("ioengine", self._DEFAULT_IOENGINE)

        if options.get("size", None) is not None:
            libaio_options["size"] = options["size"]

        if options.get("startdelay", None) is not None:
            libaio_options["startdelay"] = options["startdelay"]

        return libaio_options

    def _get_job_name(self, parent_workload_name: str, processes_per_volume: int) -> str:
        """Return the target path as the FIO job name for file/block-device targets."""
        return self._target_path
