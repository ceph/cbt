"""
Async parallel SSH via the system OpenSSH client.

This module implements :class:`~remote.remote_executor.RemoteExecutor` using
Python's stdlib ``asyncio`` and the system ``/usr/bin/ssh`` binary — no
third-party packages are required.  The directory-setup and result-collection
helpers (``make_remote_dir``, ``clean_remote_dir``, ``sync_files``) are methods
on the executor, so swapping the fan-out mechanism swaps them too.

The naming here should not be confused with the external ``asyncssh`` PyPI
package; only built-in Python packages are used.
"""

import asyncio
import logging
import os

import settings
from common import expanded_node_list
from remote.remote_executor import RemoteExecutor

logger = logging.getLogger("cbt")


async def _ssh_exec_one(host, command, ssh_args):
    proc = await asyncio.create_subprocess_exec(
        *ssh_args, host, command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_bytes, stderr_bytes = await proc.communicate()
    return (
        host,
        stdout_bytes.decode(errors="replace"),
        stderr_bytes.decode(errors="replace"),
        proc.returncode,
    )


async def _ssh_exec_all(node_list, command, ssh_args):
    tasks = [_ssh_exec_one(h, command, ssh_args) for h in node_list]
    return await asyncio.gather(*tasks, return_exceptions=True)


class AsyncSSHExecutor(RemoteExecutor):
    """Run commands on cluster nodes concurrently via the system ``ssh`` binary."""

    def _build_ssh_args(self):
        """Build the base ``ssh`` argv shared by every node invocation.

        ``-o BatchMode=yes`` is always passed so that a host requiring
        interactive authentication fails immediately rather than hanging.
        The SSH user, if any, is taken from ``settings.cluster['user']``.
        """
        ssh_args = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no"]
        user = settings.cluster.get("user")
        if user:
            ssh_args += ["-l", user]
        return ssh_args

    def run_command(self, nodes, command, continue_if_error=True):
        node_list = expanded_node_list(nodes)
        ssh_args = self._build_ssh_args()

        raw = asyncio.run(_ssh_exec_all(node_list, command, ssh_args))

        results = []
        errors = []
        for item in raw:
            if isinstance(item, BaseException):
                errors.append(str(item))
                logger.warning("ssh: failed to launch process: %s", item)
                continue
            host, stdout, stderr, exit_status = item
            logger.debug("ssh [%s] exit=%d", host, exit_status)
            if stdout:
                logger.debug("ssh [%s] stdout: %s", host, stdout.rstrip())
            if stderr:
                logger.debug("ssh [%s] stderr: %s", host, stderr.rstrip())
            if exit_status != 0:
                detail = (stdout or stderr).rstrip()
                msg = f"ssh [{host}] exited {exit_status}: {detail}"
                if not continue_if_error:
                    errors.append(msg)
                else:
                    logger.warning(msg)
            results.append((host, stdout, stderr, exit_status))

        if errors:
            raise RuntimeError(
                "asyncssh_exec failed on one or more hosts:\n" + "\n".join(errors)
            )

        return results

    def run_command_with_error_checking(self, nodes, command):
        self.run_command(nodes, command, continue_if_error=False)

    # ------------------------------------------------------------------
    # pdsh-free cluster helpers (RemoteExecutor interface)
    # ------------------------------------------------------------------

    def make_remote_dir(self, remote_dir):
        """Create *remote_dir* on all cluster nodes via parallel SSH."""
        self.run_command_with_error_checking(
            all_cluster_nodes(), f'mkdir -p -m0755 -- {remote_dir}'
        )

    def clean_remote_dir(self, remote_dir):
        """Remove *remote_dir* from all cluster nodes via parallel SSH."""
        if remote_dir == "/" or not os.path.isabs(remote_dir):
            raise SystemExit("Cleaning the remote dir doesn't seem safe, bailing.")
        self.run_command_with_error_checking(
            all_cluster_nodes(),
            f'if [ -d "{remote_dir}" ]; then rm -rf {remote_dir}; fi',
        )

    def sync_files(self, remote_dir, local_dir):
        """Pull *remote_dir* from all cluster nodes into *local_dir* via parallel ``scp -r``."""
        nodes_str = all_cluster_nodes()
        node_list = expanded_node_list(nodes_str)

        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        if 'user' in settings.cluster:
            self.run_command_with_error_checking(
                nodes_str,
                'sudo chown -R {0}.{0} {1}'.format(settings.cluster['user'], remote_dir),
            )

        user = settings.cluster.get("user")
        scp_base_args = _build_scp_args()

        def _bare_host(h):
            return h.split("@", 1)[-1]

        async def _pull_all():
            tasks = [
                _scp_pull_one(_bare_host(h), remote_dir, local_dir, scp_base_args, user=user)
                for h in node_list
            ]
            return await asyncio.gather(*tasks, return_exceptions=True)

        raw = asyncio.run(_pull_all())

        errors = []
        for item in raw:
            if isinstance(item, BaseException):
                errors.append(str(item))
                logger.warning("scp: failed to launch process: %s", item)
                continue
            host, stdout, stderr, exit_status = item
            if exit_status != 0:
                detail = (stderr or stdout).rstrip()
                errors.append(f"scp [{host}] exited {exit_status}: {detail}")

        if errors:
            raise RuntimeError(
                "async_sync_files failed on one or more hosts:\n" + "\n".join(errors)
            )


# ---------------------------------------------------------------------------
# Module-level helpers used by AsyncSSHExecutor
# ---------------------------------------------------------------------------

def all_cluster_nodes():
    """Return every node in the cluster (clients, osds, mons, rgws, mds).

    Single source of truth for the "all nodes" set used by the cluster-wide
    helpers above, so the node groups are declared in exactly one place.
    """
    return settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')


def _build_scp_args():
    """Build the base ``scp`` argv shared by every pull invocation."""
    return ["scp", "-r", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no"]


async def _scp_pull_one(host, remote_path, local_dir, scp_base_args, user=None):
    dest = os.path.join(local_dir, host)
    os.makedirs(dest, exist_ok=True)
    src_host = f"{user}@{host}" if user else host
    proc = await asyncio.create_subprocess_exec(
        *scp_base_args,
        f"{src_host}:{remote_path}",
        dest,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_bytes, stderr_bytes = await proc.communicate()
    return (
        host,
        stdout_bytes.decode(errors="replace"),
        stderr_bytes.decode(errors="replace"),
        proc.returncode,
    )
