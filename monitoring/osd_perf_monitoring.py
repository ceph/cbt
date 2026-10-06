"""OsdPerfMonitoring: PerfMonitoring specialised for Ceph OSD processes."""

from monitoring.osd_pid_monitoring import OsdPidMonitoring
from monitoring.perf_monitoring import PerfMonitoring


class OsdPerfMonitoring(OsdPidMonitoring, PerfMonitoring):
    """PerfMonitoring specialised for Ceph OSD processes.

    Discovers the target PIDs by scanning PID files matching ``pid_glob``
    inside ``pid_dir`` (read from ``settings.cluster``), then launches a
    separate ``perf`` invocation per OSD PID.
    """

    def start(self, directory: str) -> None:
        """Create the perf output directory and start a perf instance per OSD PID."""
        self._check_tool(self._perf_cmd.split()[-1])
        perf_dir = f"{directory}/perf"
        self._perf_dir = perf_dir
        self._make_remote_dir(perf_dir)
        cmd_template = f"{self._perf_cmd} {self._args_template}"
        self._start_per_pid(perf_dir, "perf", cmd_template, self._perf_runners)
