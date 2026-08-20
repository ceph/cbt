"""OsdPidMonitoring base class for per-OSD-PID monitoring backends."""

import glob as _glob
import logging
import os
from abc import ABC
from typing import Any, cast

import common
import settings
from monitoring.monitoring import Monitoring

logger = logging.getLogger("cbt")


class OsdPidMonitoring(Monitoring, ABC):
    """Mixin base for monitoring backends that launch one process per OSD PID.

    Subclasses must supply :attr:`_pid_dir`, :attr:`_pid_glob`, and a
    *runners* list attribute, then call :meth:`_start_per_pid` from their
    ``start()`` implementation after creating the output directory.
    """

    def __init__(self, mconfig: dict[str, Any]) -> None:
        """Read OSD PID discovery settings from *mconfig* and ``settings.cluster``."""
        super().__init__(mconfig)
        self._pid_dir = cast(str, settings.cluster.get("pid_dir"))
        self._pid_glob = mconfig.get("pid_glob", "osd.*.pid")

    def _start_per_pid(self, output_dir: str, tool_name: str, cmd_template: str, runners: list[Any]) -> None:
        """Launch *cmd_template* once per OSD PID, populating *runners*.

        *cmd_template* is a :meth:`str.format` template that accepts
        ``{pid}`` and ``{output_dir}`` keyword arguments.

        On a local node each PID file under :attr:`_pid_dir` is read and the
        command is spawned via :func:`common.sh`.  On remote nodes a single
        ``pdsh`` shell loop iterates over the PID files instead.
        """
        pid_glob_path = f"{self._pid_dir}/{self._pid_glob}"
        local_node = common.get_localnode(self._nodes)  # type: ignore[no-untyped-call]
        if local_node:
            logger.debug("%s: local_node pid_dir=%s", type(self).__name__, self._pid_dir)
            pid_paths = _glob.glob(os.path.join(self._pid_dir, self._pid_glob))
            if not pid_paths:
                logger.warning(
                    "%s: no PID files matched %s in %s — no %s processes started",
                    type(self).__name__,
                    self._pid_glob,
                    self._pid_dir,
                    tool_name,
                )
            for pid_path in pid_paths:
                with open(pid_path, encoding="utf-8") as pidfile:
                    pid = pidfile.read().strip()
                    cmd = cmd_template.format(output_dir=output_dir, pid=pid)
                    runner = common.sh(local_node, cmd)  # type: ignore[no-untyped-call]
                    runners.append(runner)
        else:
            logger.debug("%s: remote_node", type(self).__name__)
            ls_runner = common.pdsh(self._nodes, f"ls {pid_glob_path} 2>/dev/null")  # type: ignore[no-untyped-call]
            stdout, _ = ls_runner.communicate()
            if not stdout.strip():
                logger.warning(
                    "%s: no PID files matched %s on remote nodes — no %s processes started",
                    type(self).__name__,
                    pid_glob_path,
                    tool_name,
                )
            cmd = cmd_template.format(output_dir=output_dir, pid='"$pid"')
            common.pdsh(  # type: ignore[no-untyped-call]
                self._nodes,
                f'for f in {pid_glob_path}; do pid=$(cat "$f"); {cmd}; done',
            ).communicate()
