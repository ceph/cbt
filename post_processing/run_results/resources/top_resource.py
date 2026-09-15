"""
Process CPU and memory statistics from Linux top batch-mode output files.

CBT runs ``top -b -H -1 -p {pid} -n {count}`` for each monitored PID and
writes output to a ``top/`` subdirectory of the benchmark run directory
(e.g. ``results/.../iodepth-000128/top/``).

The output filename is controlled by the ``args`` template in
``TopMonitoring`` and can be anything the user configures.  ``TopResource``
therefore reads **every file** in the ``top/`` directory rather than
filtering by a fixed glob, keeping the two components fully decoupled.

The -H flag shows individual threads, so each snapshot may contain many lines.

CPU aggregation (Irix mode assumption)
--------------------------------------
The ``%CPU`` column in top's task area is controlled by the Irix/Solaris mode
toggle (``I`` key, or ``Mode_irixps`` in ``~/.toprc``).

* **Irix mode ON** (procps-ng build default, ``Mode_irixps=1``):
  ``%CPU`` is per-core — 100 % means one core fully busy.
  A 32-thread OSD that saturates 8 cores will show eight rows near 100 %.

* **Solaris mode** (``Mode_irixps=0``, user must have explicitly toggled and
  saved their ``~/.toprc``):
  ``%CPU`` is already divided by the total CPU count — 100 % means all cores
  fully busy.  Applying the normalisation below would double-correct.

There is no procps-ng command-line flag that forces Irix mode when combined
with batch mode (``-A`` requires being the sole argument).  CBT therefore
assumes Irix mode is active — the factory default on all mainstream
distributions.

Aggregation steps (Irix mode):

1. For each snapshot in a PID file: **sum** ``%CPU`` across all thread rows
   to get the total core-utilisation at that instant.
2. **Average** the per-snapshot totals across all snapshots in that file to
   get a representative core-utilisation for that PID.
3. **Sum** the per-file averages across all PID files (each file is one OSD
   process) to get the total OSD-fleet core-utilisation.
4. **Divide by** ``os.cpu_count()`` to normalise to the range 0-100 %
   (100 % = all cores on the host fully busy).

Memory: ``%MEM`` is a system-relative percentage regardless of Irix/Solaris
mode, so it is simply averaged across all threads, snapshots, and files.
"""

import os
from logging import Logger, getLogger
from pathlib import Path
from typing import Any

from post_processing.run_results.resource_result import ResourceResult

log: Logger = getLogger("cbt.formatter")


class TopResource(ResourceResult):
    """
    Processes resource usage from Linux top batch-mode output files.

    CBT stores one file per monitored PID in a ``top/`` subdirectory alongside
    the benchmark output file.  Each file contains multiple top snapshots
    (because top is run with ``-n <count>``).

    All files present in the ``top/`` directory are parsed — the output
    filename is determined by the ``args`` template in ``TopMonitoring`` and
    may be anything the user configures.

    CPU is normalised to 0-100 % of total system capacity.  See the module
    docstring for the full aggregation algorithm and the Irix mode assumption.

    Memory is averaged across all threads, snapshots, and files.

    Args:
        file_path: Path to the benchmark output file (e.g., ``json_output.0``)
    """

    @property
    def source(self) -> str:
        """Return the source identifier for this resource parser."""
        return "top"

    def _get_resource_output_file_from_file_path(self, file_path: Path) -> Path:
        """
        Locate the top output directory from the benchmark result file path.

        The ``top/`` directory must exist next to the benchmark output file
        and contain at least one file.  All files present are parsed — the
        naming convention is not enforced here.

        Args:
            file_path: Path to the benchmark output file (e.g., ``json_output.0``)

        Returns:
            Path to the top output directory

        Raises:
            FileNotFoundError: If the top directory does not exist or is empty
        """
        top_dir = file_path.parent / "top"

        if not top_dir.exists():
            raise FileNotFoundError(f"top directory not found: {top_dir}")

        if not any(top_dir.iterdir()):
            raise FileNotFoundError(f"top directory is empty: {top_dir}")

        # Return the directory so _parse() can iterate all files inside it
        return top_dir

    def _read_results_from_file(self) -> dict[str, Any]:
        """
        Override parent: top output is plain text, not JSON.

        Returns:
            Empty dict; parsing happens directly in _parse_top_directory()
        """
        return {}

    def _parse(self, data: dict[str, Any]) -> None:
        """
        Parse all *_osd_top.out files and average CPU and memory across all
        threads, snapshots, and PID files.

        Args:
            data: Not used (top output is plain text, not JSON)
        """
        try:
            cpu_usage, memory_usage = self._parse_top_directory()
            self._cpu = f"{cpu_usage:.2f}"
            self._memory = f"{memory_usage:.2f}"
            self._has_been_parsed = True
        except Exception as e:  # pylint: disable=broad-except
            log.error("Failed to parse top data from %s: %s", self._resource_file_path, e)
            self._cpu = "0.00"
            self._memory = "0.00"
            self._has_been_parsed = True

    def _parse_top_directory(self) -> tuple[float, float]:
        """
        Aggregate CPU and memory from all files in the top directory.

        CPU aggregation (assumes Irix mode — see module docstring):
          - Per file: sum threads per snapshot, then average across snapshots.
          - Across files: sum the per-file averages (one file = one OSD process).
          - Final: divide by os.cpu_count() to normalise to system capacity.

        Memory: averaged across all thread rows, all snapshots, all files.

        Returns:
            Tuple of (normalised_cpu_percent, average_memory_percent)
        """
        top_files = [f for f in self._resource_file_path.iterdir() if f.is_file()]

        all_mem: list[float] = []
        total_cpu: float = 0.0
        file_count: int = 0

        for top_file in top_files:
            snapshot_cpu_totals, mem_samples = self._parse_top_file(top_file)
            if snapshot_cpu_totals:
                total_cpu += sum(snapshot_cpu_totals) / len(snapshot_cpu_totals)
                file_count += 1
            all_mem.extend(mem_samples)

        if not file_count:
            log.warning("No valid CPU samples found in %s", self._resource_file_path)
            return 0.0, 0.0

        cpu_count: int = os.cpu_count() or 1
        normalised_cpu = total_cpu / cpu_count
        avg_mem = sum(all_mem) / len(all_mem) if all_mem else 0.0
        log.debug("Parsed %d PID files from top, normalised CPU: %.2f%%", file_count, normalised_cpu)

        return normalised_cpu, avg_mem

    def _parse_top_file(self, file_path: Path) -> tuple[list[float], list[float]]:
        """
        Parse a single top batch-mode output file.

        top -b produces multiple snapshots concatenated in one file.  Each
        snapshot has a column-header line starting with "PID", followed by one
        data row per thread.  We locate the %CPU and %MEM columns from the
        header and extract values from every subsequent data row until the next
        snapshot boundary (a line beginning with 'top').

        Returns a list of **per-snapshot CPU totals** (sum of all thread %CPU
        values within that snapshot) and a flat list of all %MEM values seen.

        Args:
            file_path: Path to a single top output file

        Returns:
            Tuple of (per_snapshot_cpu_total_list, flat_mem_sample_list)
        """
        snapshot_cpu_totals: list[float] = []
        all_mem: list[float] = []

        current_snapshot_cpu: float = 0.0
        in_process_block: bool = False
        has_rows_in_snapshot: bool = False
        cpu_col: int = -1
        mem_col: int = -1

        with open(file_path, encoding="utf-8") as f:
            lines = f.readlines()

        for line in lines:
            stripped = line.strip()

            # Column-header line anchors the column positions for this snapshot.
            # Commit any in-progress snapshot total first.
            if stripped.startswith("PID"):
                if has_rows_in_snapshot:
                    snapshot_cpu_totals.append(current_snapshot_cpu)
                    current_snapshot_cpu = 0.0
                    has_rows_in_snapshot = False
                headers = stripped.split()
                try:
                    cpu_col = headers.index("%CPU")
                    mem_col = headers.index("%MEM")
                except ValueError:
                    log.warning("Could not find %%CPU/%%MEM columns in header: %s", stripped)
                    in_process_block = False
                    continue
                in_process_block = True
                continue

            if not in_process_block:
                continue

            # Non-process lines mark a snapshot boundary or summary section
            if any(stripped.startswith(kw) for kw in ("top", "%Cpu", "Tasks", "MiB", "KiB", "Cpu")):
                in_process_block = False
                continue

            if not stripped:
                continue

            parts = stripped.split()
            if len(parts) <= max(cpu_col, mem_col):
                continue

            try:
                current_snapshot_cpu += float(parts[cpu_col])
                all_mem.append(float(parts[mem_col]))
                has_rows_in_snapshot = True
            except ValueError:
                log.debug("Skipping unparseable line in %s: %s", file_path.name, stripped)

        # Commit the final snapshot if the file didn't end with a boundary line
        if has_rows_in_snapshot:
            snapshot_cpu_totals.append(current_snapshot_cpu)

        return snapshot_cpu_totals, all_mem
