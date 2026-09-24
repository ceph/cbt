"""
FioBenchmark — shared base class for all FIO benchmark implementations.

Responsibilities
----------------
1. Common option extraction: parse the FIO parameters that are duplicated
   across Fio, LibrbdFio, RbdFio, KvmRbdFio, and RawFio into one place.
2. Centralised iodepth calculation: ``calculate_iodepth_per_target()``
   replaces the copy of this logic that lived in LibrbdFio.
3. Unified JSON extraction: ``parse_results()`` replaces the duplicated
   output-scraping logic in LibrbdFio.parse() / Fio.analyze().
4. Standardised recovery callbacks: ``recovery_callback_blocking()`` and
   ``recovery_callback_background()`` with a single killall helper.
5. Unified lifecycle: ``run()`` implements the full execution sequence so
   subclasses only provide target-specific hooks.
"""

import json
import logging
import os
import re
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

import common
import settings
from benchmark.benchmark import Benchmark
from iodepth import calculate_iodepth_per_target, get_iodepth_key
from monitoring.monitoring_factory import MonitoringFactory

logger = logging.getLogger("cbt")


class FioBenchmark(Benchmark, ABC):  # pylint: disable=too-many-instance-attributes
    """
    Abstract base class shared by all FIO benchmark variants.

    Inherits from both ``Benchmark`` (the CBT base class) and ``ABC`` so that
    ``ABCMeta`` is active and any subclass that omits a required hook raises
    ``TypeError`` at instantiation time rather than silently running.

    Subclasses must implement:
      - ``num_targets`` property      — number of I/O targets per client
      - ``benchmark_type`` property   — name string for the workloads path
      - ``setup_targets()``           — create pools/images/filesystems
      - ``prefill_targets()``         — prefill data on targets
      - ``cleanup_targets()``         — unmount/unmap/delete targets
      - ``_run_io_loop()``            — non-workloads I/O execution loop

    Subclasses may override:
      - ``_pre_run_hook()``           — called after sleep(5), before recovery setup
      - ``_source_directory()``       — directory glob for sync_files
      - ``_generate_report()``        — post-run report generation
    """

    def __init__(self, archive_dir: str, cluster: Any, config: dict[str, Any]) -> None:
        super().__init__(archive_dir, cluster, config)

        # ------------------------------------------------------------------ #
        # Common FIO parameters (duplicated across all 5 benchmark classes)
        # ------------------------------------------------------------------ #
        self.cmd_path: str = config.get("cmd_path", "/usr/bin/fio")
        self.mode: str = config.get("mode", "write")
        self.numjobs: int = int(config.get("numjobs", 1))
        self.op_size: int = int(config.get("op_size", 4194304))
        self.end_fsync: int = int(config.get("end_fsync", 0))
        self.rwmixread: int = int(config.get("rwmixread", 50))
        self.rwmixwrite: int = 100 - self.rwmixread
        self.norandommap: bool = bool(config.get("norandommap", False))
        self.random_distribution: Optional[str] = config.get("random_distribution", None)
        self.rate_iops: Optional[int] = config.get("rate_iops", None)
        self.log_avg_msec: Optional[int] = config.get("log_avg_msec", None)
        self.fio_out_format: str = config.get("fio_out_format", "json,normal")
        self.direct: int = int(config.get("direct", 1))
        self.recov_test_type: str = config.get("recov_test_type", "blocking")
        self.startdelay: Optional[int] = config.get("startdelay", None)

        # time / ramp stored as Optional[int] — fixes the str(None) bug in
        # RbdFio where these were stored as the literal string "None".
        _time = config.get("time", None)
        self.time: Optional[int] = int(_time) if _time is not None else None
        _ramp = config.get("ramp", None)
        self.ramp: Optional[int] = int(_ramp) if _ramp is not None else None
        self.time_based: bool = bool(config.get("time_based", False))

        # ------------------------------------------------------------------ #
        # total_iodepth / iodepth precedence
        # ------------------------------------------------------------------ #
        self._iodepth_key: str = get_iodepth_key(list(config.keys()))
        self.iodepth: int = int(config.get(self._iodepth_key, 16))
        # When total_iodepth was configured, store it so _run_io_loop()
        # implementations can pass it to Command constructors, which will then
        # include the total_iodepth-T path segment in the output directory.
        self.total_iodepth: Optional[int] = int(config["total_iodepth"]) if "total_iodepth" in config else None

        # Default output directory — subclasses override with their own paths.
        self.out_dir: str = self.archive_dir

    # ---------------------------------------------------------------------- #
    # Abstract interface for subclasses
    # ---------------------------------------------------------------------- #

    @property
    @abstractmethod
    def num_targets(self) -> int:
        """Number of I/O targets per client (volumes, block devices, etc.)."""

    @property
    @abstractmethod
    def benchmark_type(self) -> str:
        """Benchmark name string passed to ``_workloads.set_benchmark_type()``."""

    @abstractmethod
    def setup_targets(self) -> None:
        """Create pools, images, filesystems, or mount points."""

    @abstractmethod
    def prefill_targets(self) -> None:
        """Prefill data on the targets before the benchmark run."""

    @abstractmethod
    def cleanup_targets(self) -> None:
        """Unmount, unmap, or delete temporary resources."""

    @abstractmethod
    def _run_io_loop(self) -> None:
        """Execute the non-workloads I/O loop for this benchmark.

        Builds commands and calls pdsh for each target.  Called by ``run()``
        when ``self._workloads.exist()`` is ``False``.
        """

    # ---------------------------------------------------------------------- #
    # Override hooks — concrete defaults; subclasses override as needed
    # ---------------------------------------------------------------------- #

    def _pre_run_hook(self) -> None:
        """Called after ``time.sleep(5)`` and before recovery-test setup.

        Override in subclasses that need benchmark-specific pre-run steps
        (e.g. PG autoscaler wait, client read-ahead setting).
        """

    def _source_directory(self) -> str:
        """Return the glob pattern passed to ``common.sync_files``.

        Defaults to ``f"{self.run_dir}/*"``.  ``LibrbdFio`` overrides this
        to use the workloads base run directory when workloads are active.
        """
        return f"{self.run_dir}/*"

    def _generate_report(self) -> None:
        """Generate a post-run report.  No-op by default; ``LibrbdFio`` overrides."""

    # ---------------------------------------------------------------------- #
    # Unified lifecycle
    # ---------------------------------------------------------------------- #

    def run(self) -> None:
        """Execute the full FIO benchmark lifecycle.

        Steps
        -----
        1.  ``super().run()`` — OSD RA, valgrind, cmd_path_full, config snapshot
        2.  ``self.dropcaches()``
        3.  ``common.make_remote_dir(self.run_dir)``
        4.  ``self.cluster.dump_config(self.run_dir)``
        5.  ``time.sleep(5)``
        6.  ``self._pre_run_hook()``   — subclass-specific pre-run work
        7.  Recovery test setup (if configured)
        8.  ``self.cluster.wait_start_io()`` (background recovery only)
        9.  Workloads branch OR ``MonitoringFactory.start`` + ``_run_io_loop()``
        10. ``self.cluster.wait_recovery_done()`` (if recovery configured)
        11. ``MonitoringFactory.stop(self.run_dir)``
        12. ``self.cluster.dump_historic_ops(self.run_dir)``
        13. ``common.sync_files(self._source_directory(), self.out_dir)``
        14. ``self.analyze(self.out_dir)``
        15. ``self._generate_report()`` (no-op unless overridden)
        """
        super().run()

        # Step 2
        self.dropcaches()

        # Step 3
        common.make_remote_dir(self.run_dir)

        # Step 4
        self.cluster.dump_config(self.run_dir)

        # Step 5
        logger.debug("Waiting 5s before starting test...")
        time.sleep(5)

        # Step 6 — subclass-specific pre-run work (PG autoscaler, client RA, …)
        self._pre_run_hook()

        # Step 7 — recovery test setup
        if "recovery_test" in self.cluster.config:
            recovery_callback = (
                self.recovery_callback_blocking
                if self.recov_test_type == "blocking"
                else self.recovery_callback_background
            )
            self.cluster.create_recovery_test(self.run_dir, recovery_callback, self.recov_test_type)

        # Step 8 — wait for signal to start I/O (background recovery only)
        if "recovery_test" in self.cluster.config and self.recov_test_type == "background":
            self.cluster.wait_start_io()

        # Step 9 — workloads path or non-workloads I/O loop.
        # The workloads path manages its own monitoring lifecycle internally
        # (MonitoringFactory.start/stop per phase inside Workloads.run()), so
        # monitoring must only be started/stopped here for the non-workloads path.
        _using_workloads = self._workloads.exist()
        if _using_workloads:
            self._workloads.set_benchmark_type(self.benchmark_type)
            self._workloads.set_executable(self.cmd_path)
            self._workloads.run()
        else:
            MonitoringFactory.start(self.run_dir)
            self._run_io_loop()

        # Step 10 — wait for recovery to complete.
        # Recovery tests are managed at this level for both paths; Workloads.run()
        # does not handle recovery.
        if "recovery_test" in self.cluster.config:
            self.cluster.wait_recovery_done()

        # Step 11 — stop monitoring for the non-workloads path only.
        # The workloads path already called MonitoringFactory.stop() per phase
        # inside Workloads.run(); calling it again here would send a spurious
        # pkill to monitoring processes that are already stopped.
        if not _using_workloads:
            MonitoringFactory.stop(self.run_dir)

        # Step 12
        self.cluster.dump_historic_ops(self.run_dir)

        # Step 13
        common.sync_files(self._source_directory(), self.out_dir)

        # Step 14
        self.analyze(self.out_dir)

        # Step 15
        self._generate_report()

    # ---------------------------------------------------------------------- #
    # Centralised iodepth calculation
    # ---------------------------------------------------------------------- #

    def calculate_iodepth_per_target(self, num_targets: int, iodepth_key: str) -> dict[int, int]:
        """
        Return a mapping of {target_index: iodepth} for a single run.

        When ``iodepth_key`` is ``"total_iodepth"``, the total queue depth is
        distributed across ``num_targets`` with any remainder spread to the
        first R targets.  When ``iodepth_key`` is ``"iodepth"``, every target
        gets ``self.iodepth``.

        Delegates to the module-level :func:`benchmark.iodepth.calculate_iodepth_per_target`.
        """
        return calculate_iodepth_per_target(num_targets, self.iodepth, iodepth_key)

    def _populate_common_io_options(self, options: dict[str, Optional[str]]) -> None:
        """Populate common FIO parameters into an options dict."""
        options["direct"] = str(self.direct)
        options["rwmixread"] = str(self.rwmixread)
        if self.total_iodepth is not None:
            options["total_iodepth"] = str(self.total_iodepth)
        if self.time is not None:
            options["time"] = str(self.time)
        if self.time_based:
            options["time_based"] = "true"
        if self.ramp is not None:
            options["ramp"] = str(self.ramp)
        if self.norandommap:
            options["norandommap"] = "true"
        if self.random_distribution is not None:
            options["random_distribution"] = self.random_distribution
        # log_avg_msec only makes sense when at least one log channel is active.
        if self.log_avg_msec is not None and (self.log_iops or self.log_bw or self.log_lat):
            options["log_avg_msec"] = str(self.log_avg_msec)
        if self.rate_iops is not None:
            options["rate_iops"] = str(self.rate_iops)
        if not self.log_iops:
            options["log_iops"] = "false"
        if not self.log_bw:
            options["log_bw"] = "false"
        if not self.log_lat:
            options["log_lat"] = "false"
        if "recovery_test" in self.cluster.config:
            options["recovery_test"] = "true"

    # ---------------------------------------------------------------------- #
    # Unified JSON extraction
    # ---------------------------------------------------------------------- #

    def parse_results(self) -> None:
        """
        Scan ``output.*`` files under ``archive_dir`` and write the first
        JSON block (``{`` … ``}``) found in each into a sibling
        ``json_output.*`` file.

        This replaces the duplicated extraction logic in:
          - ``LibrbdFio.parse()``  (used ``{`` / ``}`` delimiters)
          - ``Fio.analyze()``      (used ``"Starting"`` sentinel line)

        The ``{`` / ``}`` strategy is the canonical one going forward because
        it is format-agnostic and works for both ``json`` and ``json,normal``
        output formats.
        """
        archive_path = Path(self.archive_dir)
        files_to_process = [f for f in archive_path.glob("**/output.*") if re.match(r"^output\.\d+$", f.name)]
        decoder = json.JSONDecoder()
        for file in files_to_process:
            output_file_name = f"{file.parent}/json_output.{file.name.split('.', 1)[1]}"
            output_path = Path(output_file_name)
            content = file.read_text(encoding="utf-8", errors="replace")
            json_str: Optional[str] = None
            pos = 0
            while True:
                idx = content.find("{", pos)
                if idx == -1:
                    break
                try:
                    _, end_idx = decoder.raw_decode(content[idx:])
                    json_str = content[idx : idx + end_idx]
                    break
                except json.JSONDecodeError:
                    pos = idx + 1

            if json_str is not None:
                output_path.write_text(json_str, encoding="utf-8")
            else:
                logger.warning("parse_results: no JSON block found in %s — skipping json_output write.", file)

    def analyze(self, out_dir: str) -> None:  # pylint: disable=unused-argument
        """Convert results to JSON format (delegates to parse_results)."""
        logger.info("Converting results to JSON format.")
        self.parse_results()

    # ---------------------------------------------------------------------- #
    # Common overrides — identical across all FIO subclasses
    # ---------------------------------------------------------------------- #

    def estimate_duration(self) -> int:
        """Estimate run-phase seconds: runtime + ramp time."""
        total = int(self.time) if self.time is not None else 0
        total += int(self.ramp) if self.ramp is not None else 0
        return total

    def exists(self) -> bool:
        """Return True (and log) if the output directory for this run already exists."""
        if os.path.exists(self.out_dir):
            logger.info("Skipping existing test in %s.", self.out_dir)
            return True
        return False

    def __str__(self) -> str:
        return f"{self.run_dir}\n{self.out_dir}\n{super().__str__()}"

    # ---------------------------------------------------------------------- #
    # Standardised recovery callbacks
    # ---------------------------------------------------------------------- #

    def _killall_fio(self, signal: str = "-2") -> None:
        """Send ``signal`` to all fio processes on client nodes.

        Uses the basename of ``self.cmd_path`` so that a non-default binary
        name (e.g. ``/usr/local/bin/fio-3.35``) is killed correctly.
        """
        cmd_name = Path(self.cmd_path).name
        common.pdsh(settings.getnodes("clients"), f"sudo killall {signal} {cmd_name}").communicate()

    def recovery_callback_blocking(self) -> None:
        """Blocking recovery: interrupt fio cleanly then wait."""
        self._killall_fio("-2")

    def recovery_callback_background(self) -> None:
        """Background recovery: no-op — the recovery thread signals completion."""
        logger.info("Recovery thread completed!")
