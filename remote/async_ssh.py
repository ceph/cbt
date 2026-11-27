"""
Parallel SSH fan-out using stdlib asyncio and the system ssh binary.
"""

import asyncio
import logging
import os
from typing import Optional, Union

import settings
from common import expanded_node_list
from remote.remote_executor import RemoteExecutor

logger = logging.getLogger("cbt")


async def _ssh_exec_one(
    host: str,
    command: str,
    ssh_args: list[str],
) -> tuple[str, str, str, int]:
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
        proc.returncode if proc.returncode is not None else -1,
    )


async def _ssh_exec_all(
    node_list: list[str],
    commands: list[str],
    ssh_args: list[str],
) -> list[Union[tuple[str, str, str, int], BaseException]]:
    tasks = [
        _ssh_exec_one(h, command, ssh_args)
        for command in commands
        for h in node_list
    ]
    return await asyncio.gather(*tasks, return_exceptions=True)


class AsyncSSHExecutor(RemoteExecutor):
    """Run commands on cluster nodes concurrently via the system ``ssh`` binary."""

    def _build_ssh_args(self) -> list[str]:
        # BatchMode=yes so a host that needs interactive auth fails fast.
        ssh_args = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no"]
        user = settings.cluster.get("user")
        if user:
            ssh_args += ["-l", str(user)]
        return ssh_args

    def run_commands(
        self,
        nodes: str,
        commands: list[str],
        continue_if_error: bool = True,
    ) -> list[tuple[str, str, str, int]]:
        node_list: list[str] = expanded_node_list(nodes)
        raw = asyncio.run(_ssh_exec_all(node_list, commands, self._build_ssh_args()))
        return self._collect_results(raw, continue_if_error)

    @staticmethod
    def _collect_results(
        raw: list[Union[tuple[str, str, str, int], BaseException]],
        continue_if_error: bool,
        transport: str = "ssh",
    ) -> list[tuple[str, str, str, int]]:
        """Turn gather() output into result tuples, raising on any failure.

        A process that never launched comes back as an exception rather than a
        tuple and is always an error; a non-zero exit is only an error when the
        caller asked for one. transport names the tool in the messages, since
        ssh and scp failures read very differently to whoever is debugging.
        """
        results: list[tuple[str, str, str, int]] = []
        errors: list[str] = []
        for item in raw:
            if isinstance(item, BaseException):
                errors.append(str(item))
                logger.warning("%s: failed to launch process: %s", transport, item)
                continue
            host, stdout, stderr, exit_status = item
            logger.debug("%s [%s] exit=%d", transport, host, exit_status)
            if stdout:
                logger.debug("%s [%s] stdout: %s", transport, host, stdout.rstrip())
            if stderr:
                logger.debug("%s [%s] stderr: %s", transport, host, stderr.rstrip())
            if exit_status != 0:
                detail = (stderr or stdout).rstrip()
                msg = f"{transport} [{host}] exited {exit_status}: {detail}"
                if not continue_if_error:
                    errors.append(msg)
                else:
                    logger.warning(msg)
            results.append((host, stdout, stderr, exit_status))

        if errors:
            raise RuntimeError(
                f"async {transport} failed on one or more hosts:\n" + "\n".join(errors)
            )

        return results

    def sync_files(self, nodes: str, remote_dir: str, local_dir: str) -> None:
        """Pull *remote_dir* from *nodes* into *local_dir* via parallel ``scp -r``."""
        node_list: list[str] = expanded_node_list(nodes)

        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        if "user" in settings.cluster:
            self.run_command_with_error_checking(
                nodes,
                f"sudo chown -R {settings.cluster['user']}.{settings.cluster['user']} {remote_dir}",
            )

        raw = asyncio.run(
            _scp_pull_all(node_list, remote_dir, local_dir, _build_scp_args(), settings.cluster.get("user"))
        )

        # A partial copy is a silently incomplete result set, so never tolerate
        # a failing host here.
        self._collect_results(raw, continue_if_error=False, transport="scp")


# ---------------------------------------------------------------------------
# Module-level helpers used by AsyncSSHExecutor
# ---------------------------------------------------------------------------

def _bare_host(h: str) -> str:
    """Strip any ``user@`` prefix, returning just the hostname."""
    return h.split("@", 1)[-1]


async def _scp_pull_all(
    node_list: list[str],
    remote_dir: str,
    local_dir: str,
    scp_base_args: list[str],
    user: Optional[str],
) -> list[Union[tuple[str, str, str, int], BaseException]]:
    tasks = [
        _scp_pull_one(
            _bare_host(h),
            remote_dir,
            local_dir,
            scp_base_args,
            user=str(user) if user else None,
        )
        for h in node_list
    ]
    return await asyncio.gather(*tasks, return_exceptions=True)


def _build_scp_args() -> list[str]:
    return ["scp", "-r", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no"]


async def _scp_pull_one(
    host: str,
    remote_path: str,
    local_dir: str,
    scp_base_args: list[str],
    user: Optional[str] = None,
) -> tuple[str, str, str, int]:
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
        proc.returncode if proc.returncode is not None else -1,
    )
