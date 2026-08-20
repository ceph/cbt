"""Blktrace monitoring backend."""

import logging
from typing import Any, ClassVar, Optional, cast

import common
import settings
from monitoring.monitoring import Monitoring

logger = logging.getLogger("cbt")


class BlktraceMonitoring(Monitoring):
    """Monitoring backend that captures blktrace output and optionally renders seekwatcher movies."""

    DEFAULT_NODES: ClassVar[list[str]] = ["osds"]

    def __init__(self, mconfig: dict[str, Any]) -> None:
        """Initialize blktrace monitoring configuration."""
        super().__init__(mconfig)
        # Remove these casts once settings.cluster.get() is fully typed.
        self._osds_per_node = cast(int, settings.cluster.get("osds_per_node"))
        self._use_existing = cast(bool, settings.cluster.get("use_existing", True))

    def start(self, directory: str) -> None:
        """Create the blktrace output directory and start tracing on each OSD device."""
        self._check_tool("blktrace")
        blktrace_dir = f"{directory}/blktrace"
        self._make_remote_dir(blktrace_dir)
        for device in range(self._osds_per_node):
            cmd = (
                f"cd {blktrace_dir} && sudo blktrace"
                f" -o device{device} -d /dev/disk/by-partlabel/osd-device-{device}-data"
            )
            common.pdsh(self._nodes, cmd)  # type: ignore[no-untyped-call]

    def stop(self, directory: Optional[str]) -> None:
        """Stop blktrace and optionally generate seekwatcher movies."""
        common.pdsh(self._nodes, "sudo pkill -SIGINT -f blktrace").communicate()  # type: ignore[no-untyped-call]
        if directory and not self._use_existing:
            self._make_movies(directory)

    def _make_movies(self, directory: str) -> None:
        """Generate an mpg movie for each OSD device using seekwatcher."""
        seekwatcher = f"/home/{self._user}/bin/seekwatcher"
        blktrace_dir = f"{directory}/blktrace"
        for device in range(self._osds_per_node):
            common.pdsh(  # type: ignore[no-untyped-call]
                self._nodes,
                f"cd {blktrace_dir} && {seekwatcher} -t device{device} -o device{device}.mpg --movie",
            ).communicate()
