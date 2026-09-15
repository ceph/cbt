"""Collectl monitoring backend."""

from typing import Any, ClassVar, Optional

import common
from monitoring.monitoring import Monitoring


class CollectlMonitoring(Monitoring):
    """Monitoring backend that captures collectl output."""

    DEFAULT_NODES: ClassVar[list[str]] = ["clients", "osds", "mons", "rgws"]
    DEFAULT_ARGS: ClassVar[str] = (
        "-s+mYZ -i 1:10 -F0 -f {collectl_dir} "
        r"--rawdskfilt \"+cciss/c\d+d\d+ |hd[ab] | sd[a-z]+ |dm-\d+ |"
        r"xvd[a-z]+ |fio[a-z]+ | vd[a-z]+ |emcpower[a-z]+ |psv\d+ |"
        r"nvme[0-9]n[0-9]+ |nvme[0-9]n[0-9]+p[0-9]+ \""
    )

    def __init__(self, mconfig: dict[str, Any]) -> None:
        """Initialize collectl monitoring configuration."""
        super().__init__(mconfig)
        args = mconfig.get("args", self.DEFAULT_ARGS)
        self._args = args if isinstance(args, str) else self.DEFAULT_ARGS

    def start(self, directory: str) -> None:
        """Create the output directory and start collectl."""
        if not self._check_tool("collectl", fatal=False):
            return
        collectl_dir = f"{directory}/collectl"
        self._make_remote_dir(collectl_dir)
        # collectl runs as a background daemon; do not call .communicate() here.
        cmd = f"collectl {self._args.format(collectl_dir=collectl_dir)}"
        common.pdsh(self._nodes, cmd)  # type: ignore[no-untyped-call]

    def stop(self, directory: Optional[str]) -> None:
        """Stop running collectl processes."""
        del directory
        common.pdsh(self._nodes, "pkill -SIGINT -f collectl").communicate()  # type: ignore[no-untyped-call]
