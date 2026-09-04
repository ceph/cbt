"""
Abstract interface for fanning a command out to cluster nodes.

pdsh is being retired (tracker.ceph.com/issues/80193); this interface lets
each benchmark migrate to a new transport without touching the others.

TODO: add a pdsh-backed RemoteExecutor and move the fio path onto it. Not
done yet — deferred so the fio route stays untouched until it can be tested.
"""

import os
from abc import ABC, abstractmethod


class RemoteExecutor(ABC):
    """A way of running a single command on one or more cluster nodes."""

    @abstractmethod
    def run_command(
        self,
        nodes: str,
        command: str,
        continue_if_error: bool = True,
    ) -> list[tuple[str, str, str, int]]:
        """Run command on all nodes in parallel.

        Return a list of ``(host, stdout, stderr, exit_status)`` tuples, one
        per node.  If continue_if_error is False and any node exits non-zero
        (or fails to launch), raise ``RuntimeError``.
        """

    @abstractmethod
    def sync_files(self, nodes: str, remote_dir: str, local_dir: str) -> None:
        """Pull remote_dir from nodes into local_dir."""

    # ------------------------------------------------------------------
    # Concrete helpers built on run_command(); transport-agnostic, so
    # every subclass inherits the same implementation.
    # ------------------------------------------------------------------

    def run_command_with_error_checking(self, nodes: str, command: str) -> None:
        """Run command on all nodes; raise ``RuntimeError`` if any fail."""
        self.run_command(nodes, command, continue_if_error=False)

    def make_remote_dir(self, nodes: str, remote_dir: str) -> None:
        """Create remote_dir on nodes in parallel."""
        self.run_command_with_error_checking(
            nodes, f'mkdir -p -m0755 -- {remote_dir}'
        )

    def clean_remote_dir(self, nodes: str, remote_dir: str) -> None:
        """Remove remote_dir from nodes in parallel."""
        if remote_dir == "/" or not os.path.isabs(remote_dir):
            raise SystemExit("Cleaning the remote dir doesn't seem safe, bailing.")
        self.run_command_with_error_checking(
            nodes,
            f'if [ -d "{remote_dir}" ]; then rm -rf {remote_dir}; fi',
        )
