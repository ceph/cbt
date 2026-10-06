"""Base abstractions for monitoring backends."""

import logging
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional, cast

import common
import settings

logger = logging.getLogger("cbt")


class Monitoring(ABC):
    """Abstract base class for monitoring backends."""

    DEFAULT_NODES: ClassVar[list[str]]

    def __init__(self, mconfig: dict[str, Any]) -> None:
        """Resolve monitoring nodes and common cluster settings from configuration."""
        nodes_list = mconfig.get("nodes", self.DEFAULT_NODES)
        # Remove these casts once settings is fully typed.
        self._nodes = cast(str, settings.getnodes(*nodes_list))  # type: ignore[no-untyped-call]
        self._user = cast(str, settings.cluster.get("user"))

    def _make_remote_dir(self, path: str) -> None:
        """Create *path* on all monitored nodes, waiting for completion."""
        common.pdsh(self._nodes, f"mkdir -p -m0755 -- {path}").communicate()  # type: ignore[no-untyped-call]

    def _check_tool(self, tool_name: str, *, fatal: bool = True) -> bool:
        """Verify *tool_name* is present on all monitored nodes.

        Runs ``command -v <tool>`` on every node via pdsh.  The behaviour on
        failure depends on *fatal*:

        * ``fatal=True`` (default) — raises :exc:`RuntimeError`, aborting the
          run.  Use this for tools that are explicitly configured and required.
        * ``fatal=False`` — logs a warning and returns ``False`` so the caller
          can skip further work gracefully.  Use this for tools with a built-in
          default that may simply not be installed.

        Returns:
            ``True`` if the tool was found on all nodes, ``False`` otherwise
            (only reachable when *fatal* is ``False``).

        Raises:
            RuntimeError: If the tool is not found and *fatal* is ``True``.
        """
        try:
            common.pdsh(self._nodes, f"command -v {tool_name}", continue_if_error=False).communicate()  # type: ignore[no-untyped-call]
            return True
        except Exception as exc:
            msg = (
                f"Monitoring tool '{tool_name}' not found on one or more nodes "
                f"({self._nodes}). Install it before running CBT with this monitoring backend."
            )
            if fatal:
                raise RuntimeError(msg) from exc
            logger.warning("%s — skipping %s monitoring.", msg, tool_name)
            return False

    @abstractmethod
    def start(self, directory: str) -> None:
        """Start monitoring and write output beneath the given directory."""

    @abstractmethod
    def stop(self, directory: Optional[str]) -> None:
        """Stop monitoring and finalize any output files."""
