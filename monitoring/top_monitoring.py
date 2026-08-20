"""Top monitoring backend."""

import logging
import re
from typing import Any, ClassVar, Optional

import common
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
        self._top_cmd = mconfig.get("top_cmd", "top")
        # NOTE: top's %CPU column behaviour depends on the Irix/Solaris mode
        # toggle ('I' key / Mode_irixps in ~/.toprc).  The procps-ng build
        # default is Irix mode ON (%CPU is per-core).  TopResource assumes
        # this.  There is no CLI flag to enforce it; if a user has toggled it
        # off in their ~/.toprc the resulting CPU figures will be incorrect.
        self._args = mconfig.get("args", "-b -H -1 -n 30 > {top_dir}/top.out")
        self._top_runners: list[Any] = []
        self._running_cmd: Optional[str] = None

    def start(self, directory: str) -> None:
        """Create the top output directory and start top collection."""
        self._check_tool(self._top_cmd)
        top_dir = f"{directory}/top"
        self._make_remote_dir(top_dir)

        top_template = f"{self._top_cmd} {self._args}"
        top_cmd = top_template.format(top_dir=top_dir)
        self._running_cmd = top_cmd  # stored for use in stop()
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
            pkill_cmd = f"sudo pkill -SIGINT -f '{self._running_cmd}'"
            common.pdsh(self._nodes, pkill_cmd).communicate()  # type: ignore[no-untyped-call]
        if directory:
            common.pdsh(  # type: ignore[no-untyped-call]
                self._nodes,
                f"sudo find {directory}/top -maxdepth 1 -name '*top.out' -exec chown {self._user}:{self._user} {{}} +",
            )
