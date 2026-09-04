"""
Abstract interface for running a shell command across a set of cluster nodes.

CBT historically fanned commands out to remote nodes with ``pdsh`` (see
``common.py``).  pdsh is being retired (tracked in
https://tracker.ceph.com/issues/80193), so the fan-out mechanism is being
moved behind this interface.  The first concrete implementation is
``remote.async_ssh.AsyncSSHExecutor`` (stdlib asyncio + the system ``ssh``
binary); a pdsh-backed implementation can follow without changing callers.
"""

from abc import ABC, abstractmethod


class RemoteExecutor(ABC):
    """A way of running a single command on one or more cluster nodes."""

    @abstractmethod
    def run_command(self, nodes, command, continue_if_error=True) -> list:
        """Run *command* on all *nodes* in parallel.

        Return a list of ``(host, stdout, stderr, exit_status)`` tuples, one
        per node.  If *continue_if_error* is False and any node exits non-zero
        (or fails to launch), raise ``RuntimeError``.
        """

    @abstractmethod
    def run_command_with_error_checking(self, nodes, command) -> None:
        """Run *command* on all *nodes*; raise ``RuntimeError`` if any fail."""

    @abstractmethod
    def make_remote_dir(self, remote_dir) -> None:
        """Create *remote_dir* on every cluster node."""

    @abstractmethod
    def clean_remote_dir(self, remote_dir) -> None:
        """Remove *remote_dir* from every cluster node."""

    @abstractmethod
    def sync_files(self, remote_dir, local_dir) -> None:
        """Pull *remote_dir* from every cluster node into *local_dir*."""
