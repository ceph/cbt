"""Perf monitoring backend."""

import logging
import os
import re
from typing import Any, ClassVar, Optional, cast

import common
from monitoring.monitoring import Monitoring

logger = logging.getLogger("cbt")


class PerfMonitoring(Monitoring):
    """Monitoring backend that captures perf output.

    Runs ``perf`` with a caller-supplied ``args`` template.  For OSD-specific
    PID discovery use :class:`OsdPerfMonitoring` instead.
    """

    DEFAULT_NODES: ClassVar[list[str]] = ["osds"]

    def __init__(self, mconfig: dict[str, Any]) -> None:
        """Initialize perf monitoring configuration."""
        super().__init__(mconfig)
        if "args" not in mconfig:
            raise ValueError("PerfMonitoring requires 'args' in mconfig")
        self._perf_cmd = mconfig.get("perf_cmd", "sudo perf")
        self._args_template: str = mconfig["args"]
        self._perf_runners: list[Any] = []
        self._perf_dir: Optional[str] = None

    def start(self, directory: str) -> None:
        """Create the perf output directory and start perf collection."""
        self._check_tool(self._perf_cmd.split()[-1])
        perf_dir = f"{directory}/perf"
        self._perf_dir = perf_dir
        self._make_remote_dir(perf_dir)

        perf_template = f"{self._perf_cmd} {self._args_template}"
        perf_cmd = perf_template.format(perf_dir=perf_dir)
        local_node = common.get_localnode(self._nodes)  # type: ignore[no-untyped-call]
        if local_node:
            runner = common.sh(local_node, perf_cmd)  # type: ignore[no-untyped-call]
            self._perf_runners.append(runner)
        else:
            common.pdsh(self._nodes, perf_cmd).communicate()  # type: ignore[no-untyped-call]

    def stop(self, directory: Optional[str]) -> None:
        """Stop perf collection and adjust file ownership when needed."""
        if self._perf_runners:
            for runner in self._perf_runners:
                runner.kill()
        else:
            common.pdsh(self._nodes, "sudo pkill -SIGINT -f 'perf '").communicate()  # type: ignore[no-untyped-call]
        if directory:
            common.pdsh(  # type: ignore[no-untyped-call]
                self._nodes, f"sudo chown {self._user}:{self._user} {directory}/perf/perf.data"
            ).communicate()
            common.pdsh(  # type: ignore[no-untyped-call]
                self._nodes, f"sudo chown {self._user}:{self._user} {directory}/perf/perf_stat.*"
            ).communicate()

    def get_cpu_cycles(self, out_dir: str) -> Optional[int]:
        """Return total CPU cycles from perf stat output, if available."""
        perf_dir_name = os.path.join(out_dir, "perf")
        if not os.path.isdir(perf_dir_name):
            logger.warning("get_cpu_cycles: perf directory not found: %s", perf_dir_name)
            return None
        perf_stat_fnames = os.listdir(perf_dir_name)
        if not perf_stat_fnames:
            logger.warning("get_cpu_cycles: no perf stat files found in %s", perf_dir_name)
            return None
        total_cpu_cycles = 0
        for perf_out_fname in perf_stat_fnames:
            with open(f"{perf_dir_name}/{perf_out_fname}", encoding="utf-8") as perf_output_file:
                match = re.search(r"(.*) cycles(.*?) .*", perf_output_file.read(), re.M | re.I)
            if match:
                cpu_cycles = match.group(1).strip()
            else:
                logger.warning(
                    "get_cpu_cycles: no cycles line found in %s — returning None",
                    perf_out_fname,
                )
                return None
            total_cpu_cycles = total_cpu_cycles + int(cpu_cycles.replace(",", ""))
        return cast(Optional[int], total_cpu_cycles)
