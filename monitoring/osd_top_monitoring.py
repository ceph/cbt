"""OsdTopMonitoring: TopMonitoring specialised for Ceph OSD processes."""

from typing import Any

from monitoring.osd_pid_monitoring import OsdPidMonitoring
from monitoring.top_monitoring import TopMonitoring


class OsdTopMonitoring(OsdPidMonitoring, TopMonitoring):
    """TopMonitoring specialised for Ceph OSD processes.

    Discovers the target PIDs by scanning PID files matching ``pid_glob``
    inside ``pid_dir`` (read from ``settings.cluster``), then launches a
    separate ``top`` invocation per OSD PID.
    """

    def __init__(self, mconfig: dict[str, Any]) -> None:
        """Initialize OSD top monitoring configuration."""
        super().__init__(mconfig)
        # Override default args to include per-pid placeholders.
        # NOTE: see TopMonitoring.__init__ comment regarding Irix/Solaris mode.
        self._args = mconfig.get("args", "-b -H -1 -p {pid} -n 30 > {output_dir}/{pid}_osd_top.out")

    def start(self, directory: str) -> None:
        """Create the top output directory and start a top instance per OSD PID."""
        self._check_tool(self._top_cmd)
        top_dir = f"{directory}/top"
        self._make_remote_dir(top_dir)
        cmd_template = f"{self._top_cmd} {self._args}"
        self._start_per_pid(top_dir, "top", cmd_template, self._top_runners)
