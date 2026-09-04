"""Elbencho S3 benchmark, driven by the shared Workloads pipeline.

The generated commands are fanned out to the client nodes through the pdsh-free
``RemoteExecutor``."""

import logging
import os

import yaml

import monitoring
import settings
from remote.async_ssh import AsyncSSHExecutor
from remote.remote_executor import RemoteExecutor

from .benchmark import Benchmark

logger = logging.getLogger("cbt")


class Elbencho(Benchmark):

    def __init__(self, archive_dir: str, cluster, config: dict) -> None:
        # auth comes in from the YAML as a nested dict; flatten it to 
        # strings now so the Workloads pipeline (which stringifies everything) 
        # doesn't mangle it.
        self.auth = config.get("auth", {})
        config["s3_auth_config"] = self.auth.get("config", "")
        config["s3_session_token"] = self.auth.get("s3_session_token", "")
        config.pop("auth", None)

        super().__init__(archive_dir, cluster, config)

        self.cmd_path = config.get("cmd_path", "/usr/local/bin/elbencho")

        # RemoteExecutor is the ABC which allows us to easily
        # swap out AsyncIO as the fan-out tool later if needed.
        self._remote: RemoteExecutor = AsyncSSHExecutor()

        self.base_run_dir = self.run_dir

        workloads = config.get("workloads", {})
        if not isinstance(workloads, dict):
            raise ValueError(f"workloads must be a dict, got {type(workloads).__name__}")
        self._validate_workloads(workloads)

        for wl_name, wl_params in workloads.items():
            logger.info("Elbencho workload '%s': %s", wl_name, wl_params)

    # ------------------------------------------------------------------
    # Lifecycle overrides (pdsh-free)
    # ------------------------------------------------------------------

    def exists(self) -> bool:
        if os.path.exists(self.archive_dir):
            logger.info("Skipping existing Elbencho results in %s.", self.archive_dir)
            return True
        return False

    def initialize(self) -> None:
        super().initialize()

        logger.info("Verifying elbencho binary is executable on all client nodes: %s", self.cmd_path)
        self._remote.run_command_with_error_checking(settings.getnodes('clients'), f"test -x {self.cmd_path}")

        self.cleandir()

        if not os.path.exists(self.archive_dir):
            os.makedirs(self.archive_dir)

    def cleandir(self) -> None:
        clients = settings.getnodes('clients')
        self._remote.clean_remote_dir(clients, self.run_dir)
        self._remote.make_remote_dir(clients, self.run_dir)

    def dropcaches(self) -> None:
        nodes = settings.getnodes('clients', 'osds')
        self._remote.run_command(nodes, 'sync', continue_if_error=False)
        self._remote.run_command(
            nodes,
            'echo 3 | sudo tee /proc/sys/vm/drop_caches',
            continue_if_error=False,
        )

    def run(self) -> None:
        if self.osd_ra and self.osd_ra_changed:
            logger.info('Setting OSD Read Ahead to: %s', self.osd_ra)
            self.cluster.set_osd_param('read_ahead_kb', self.osd_ra)

        config_file = os.path.join(self.archive_dir, 'benchmark_config.yaml')
        if not os.path.exists(self.archive_dir):
            os.makedirs(self.archive_dir)
        if not os.path.exists(config_file):
            config_dict = dict(cluster=self.config)
            with open(config_file, 'w') as fd:
                yaml.dump(config_dict, fd, default_flow_style=False)

        if not self._workloads.exist():
            logger.warning("Elbencho: no workloads defined — nothing to run.")
            return

        self.dropcaches()
        # TODO: call super().run() once Benchmark.run() is executor-driven and
        # is no longer using AsyncIO.
        self._remote.make_remote_dir(settings.getnodes('clients'), self.run_dir)
        self.cluster.dump_config(self.run_dir)

        self._run_workloads()

        self._remote.sync_files(settings.getnodes('clients'), self.run_dir, self.archive_dir)

    def cleanup(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_workloads(self, workloads: dict) -> None:
        """Fail early with a precise error rather than mid-run inside the pipeline."""
        for name, params in workloads.items():
            if not isinstance(params, dict):
                raise ValueError(f"workload '{name}' must be a dict")
            for field in ("mode", "s3_bucket"):
                if field not in params:
                    raise ValueError(f"workload '{name}' missing required key '{field}'")
            for field in ("threads", "iodepth"):
                values = params.get(field)
                if values is None:
                    continue
                for value in (values if isinstance(values, list) else [values]):
                    try:
                        int(value)
                    except (TypeError, ValueError):
                        raise ValueError(
                            f"workload '{name}': {field} value {value!r} is not an integer"
                        )

    # ------------------------------------------------------------------
    # Run loop
    # ------------------------------------------------------------------

    def _run_workloads(self) -> None:
        clients = settings.getnodes("clients")

        self._workloads.set_benchmark_type("elbencho")
        self._workloads.set_executable(self.cmd_path)

        for output_directory, commands in self._workloads.command_groups():
            live_commands = [cmd for cmd in commands if cmd]
            if not live_commands:
                continue

            self._remote.make_remote_dir(settings.getnodes('clients'), output_directory)
            logger.info("Elbencho: running %d command(s) → %s", len(live_commands), output_directory)
            monitoring.start(output_directory)
            for cmd in live_commands:
                logger.debug("Elbencho cmd: %s", cmd)
                self._remote.run_command(clients, cmd, continue_if_error=False)
            monitoring.stop()

        logger.info("Elbencho: all workloads complete.")
