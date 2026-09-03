"""Top monitoring backend."""

import glob
import logging
import os
import re
from typing import Any, ClassVar, Optional, cast

import common
import settings
from monitoring.monitoring import Monitoring

logger = logging.getLogger("cbt")


def _estimate_top_duration(args: str) -> Optional[float]:
    """Estimate how long ``top -b`` will run based on ``-n`` and ``-d`` flags.

    Returns the estimated duration in seconds, or ``None`` if ``-n`` is not
    present (meaning top will run indefinitely and must be killed to stop).
    """
    n_match = re.search(r"-n\s+(\d+)", args)
    d_match = re.search(r"-d\s+([\d.]+)", args)
    if not n_match:
        return None
    iterations = int(n_match.group(1))
    delay = float(d_match.group(1)) if d_match else 3.0  # top default delay is 3s
    return iterations * delay


class TopMonitoring(Monitoring):
    """Monitoring backend that runs top against an explicit PID list or system-wide.

    When ``args`` contains ``{pid}``, the caller is responsible for supplying
    the PID list via ``start()``.  For OSD-specific PID discovery use
    :class:`OsdTopMonitoring` instead.
    """

    DEFAULT_NODES: ClassVar[list[str]] = ["osds"]

    def __init__(self, mconfig: dict[str, Any]) -> None:
        """Initialize top monitoring configuration."""
        super().__init__(mconfig)
        self._user = cast(str, settings.cluster.get("user"))
        self._top_cmd = mconfig.get("top_cmd", "top")
        # NOTE: top's %CPU column behaviour depends on the Irix/Solaris mode
        # toggle ('I' key / Mode_irixps in ~/.toprc).  The procps-ng build
        # default is Irix mode ON (%CPU is per-core).  TopResource assumes
        # this.  There is no CLI flag to enforce it; if a user has toggled it
        # off in their ~/.toprc the resulting CPU figures will be incorrect.
        self._args = mconfig.get("args", "-b -H -1 -n 30 > {top_dir}/top.out")
        self._top_runners: list[Any] = []

    def start(self, directory: str) -> None:
        """Create the top output directory and start top collection."""
        top_dir = f"{directory}/top"
        common.pdsh(self._nodes, f"mkdir -p -m0755 -- {top_dir}").communicate()  # type: ignore[no-untyped-call]

        top_cmd = f"{self._top_cmd} {self._args}".format(top_dir=top_dir)
        local_node = common.get_localnode(self._nodes)  # type: ignore[no-untyped-call]
        if local_node:
            runner = common.sh(local_node, top_cmd)  # type: ignore[no-untyped-call]
            self._top_runners.append(runner)
        else:
            duration = _estimate_top_duration(self._args)
            if duration is not None:
                logger.info(
                    "Top monitoring collecting %s samples (estimated ~%.0fs)...",
                    self._args.split("-n")[1].split()[0].strip(),
                    duration,
                )
            else:
                logger.info("Top monitoring running (will be killed on stop)...")
            common.pdsh(self._nodes, top_cmd).communicate()  # type: ignore[no-untyped-call]
            logger.info("Top monitoring collection complete.")

    def stop(self, directory: Optional[str]) -> None:
        """Stop top collection and adjust file ownership when needed."""
        if self._top_runners:
            for runner in self._top_runners:
                runner.kill()
        else:
            pkill_cmd = f"sudo pkill -SIGINT -f '{self._top_cmd} {self._args}'"
            common.pdsh(self._nodes, pkill_cmd).communicate()  # type: ignore[no-untyped-call]
        if directory:
            common.pdsh(  # type: ignore[no-untyped-call]
                self._nodes,
                f"sudo chown {self._user}.{self._user} {directory}/top/*top.out",
            )


class OsdTopMonitoring(TopMonitoring):
    """TopMonitoring specialised for Ceph OSD processes.

    Discovers the target PIDs by scanning PID files matching ``pid_glob``
    inside ``pid_dir`` (read from ``settings.cluster``), then launches a
    separate ``top`` invocation per OSD PID.
    """

    def __init__(self, mconfig: dict[str, Any]) -> None:
        """Initialize OSD top monitoring configuration."""
        super().__init__(mconfig)
        self._pid_dir = cast(str, settings.cluster.get("pid_dir"))
        self._pid_glob = mconfig.get("pid_glob", "osd.*.pid")
        # Override default args to include per-pid placeholders.
        self._args = mconfig.get("args", "-b -H -1 -p {pid} -n 30 > {top_dir}/{pid}_osd_top.out")
        # NOTE: see TopMonitoring.__init__ comment regarding Irix/Solaris mode.

    def start(self, directory: str) -> None:
        """Create the top output directory and start a top instance per OSD PID."""
        top_dir = f"{directory}/top"
        common.pdsh(self._nodes, f"mkdir -p -m0755 -- {top_dir}").communicate()  # type: ignore[no-untyped-call]

        top_template = f"{self._top_cmd} {self._args}"
        local_node = common.get_localnode(self._nodes)  # type: ignore[no-untyped-call]
        if local_node:
            logger.debug("OsdTopMonitoring: local_node pid_dir=%s", self._pid_dir)
            pid_paths = glob.glob(os.path.join(self._pid_dir, self._pid_glob))
            if not pid_paths:
                logger.warning(
                    "OsdTopMonitoring: no PID files matched %s in %s — no top processes started",
                    self._pid_glob,
                    self._pid_dir,
                )
            for pid_path in pid_paths:
                with open(pid_path, encoding="utf-8") as pidfile:
                    pid = pidfile.read().strip()
                    top_cmd = top_template.format(top_dir=top_dir, pid=pid)
                    runner = common.sh(local_node, top_cmd)  # type: ignore[no-untyped-call]
                    self._top_runners.append(runner)
        else:
            logger.debug("OsdTopMonitoring: remote_node")
            pid_glob_path = f"{self._pid_dir}/{self._pid_glob}"
            ls_runner = common.pdsh(self._nodes, f"ls {pid_glob_path} 2>/dev/null")  # type: ignore[no-untyped-call]
            stdout, _ = ls_runner.communicate()
            if not stdout.strip():
                logger.warning(
                    "OsdTopMonitoring: no PID files matched %s on remote nodes — no top processes started",
                    pid_glob_path,
                )
            duration = _estimate_top_duration(self._args)
            if duration is not None:
                logger.info(
                    "OSD top monitoring collecting %s samples per OSD (estimated ~%.0fs)...",
                    self._args.split("-n")[1].split()[0].strip(),
                    duration,
                )
            else:
                logger.info("OSD top monitoring running per OSD (will be killed on stop)...")
            top_cmd = top_template.format(top_dir=top_dir, pid="${pid}")
            common.pdsh(  # type: ignore[no-untyped-call]
                self._nodes,
                [f"for pid in `cat {pid_glob_path}`;", "do", top_cmd, ";", "done"],
            ).communicate()
            logger.info("OSD top monitoring collection complete.")
