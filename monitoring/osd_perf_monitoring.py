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
        perf_template = f"{self._perf_cmd} {self._args_template}"
        try:
            cmd_template = perf_template.format(perf_dir=perf_dir, output_dir="{output_dir}", pid="{pid}")
        except KeyError as exc:
            raise ValueError(
                f"OsdPerfMonitoring args template contains an unknown placeholder {{{exc.args[0]}}}. "
                f"The supported placeholders are {{perf_dir}}, {{output_dir}}, and {{pid}}."
            ) from exc
        self._start_per_pid(perf_dir, "perf", cmd_template, self._perf_runners)
