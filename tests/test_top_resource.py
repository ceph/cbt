"""
Unit tests for TopResource class.
"""

# pyright: strict, reportPrivateUsage=false

import tempfile
from pathlib import Path
from unittest import mock

import pytest

from post_processing.run_results.resources.top_resource import TopResource

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

# A minimal two-snapshot top -b output for a single PID.
# Snapshot 1: one thread at 25.0 % CPU, 0.6 % MEM  → snapshot total = 25.0
# Snapshot 2: one thread at 35.0 % CPU, 0.8 % MEM  → snapshot total = 35.0
# Per-file average = (25.0 + 35.0) / 2 = 30.0 core-units
# With cpu_count=4: normalised = 30.0 / 4 = 7.50
# Mem: avg(0.6, 0.8) = 0.70
SINGLE_PID_TWO_SNAPSHOTS = """\
top - 14:32:01 up 2 days,  3:10,  0 users,  load average: 6.06, 7.09, 8.13
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s): 12.3 us,  4.1 sy,  0.0 ni, 83.2 id,  0.3 wa
MiB Mem : 376023.2 total, 333174.8 free,  38078.1 used

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
12345 ceph      20   0  1.234g 234.0m  12.0m S  25.0   0.6   1:23.45 crimson-osd

top - 14:32:02 up 2 days,  3:10,  0 users,  load average: 6.06, 7.09, 8.13
Tasks:   1 total,   0 running,   1 sleeping,   0 stopped,   0 zombie
%Cpu(s): 13.1 us,  3.9 sy,  0.0 ni, 82.6 id,  0.4 wa
MiB Mem : 376023.2 total, 333174.8 free,  38078.1 used

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
12345 ceph      20   0  1.234g 234.0m  12.0m S  35.0   0.8   1:24.50 crimson-osd
"""

# Two threads per snapshot (as produced by top -H).
# Snapshot 1: thread A 20.0 + thread B 10.0 → snapshot total = 30.0
# Snapshot 2: thread A 30.0 + thread B 10.0 → snapshot total = 40.0
# Per-file average = (30.0 + 40.0) / 2 = 35.0 core-units
# With cpu_count=4: normalised = 35.0 / 4 = 8.75
# Mem: avg(0.4, 0.2, 0.5, 0.2) = 0.325 → "0.33"
MULTI_THREAD_TWO_SNAPSHOTS = """\
top - 14:32:01 up 1 day
Tasks:   2 total
%Cpu(s):  5.0 us
MiB Mem : 376023.2 total

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 1001 ceph      20   0  1.0g  100m  10m  S  20.0   0.4   0:10.00 reactor-0
 1002 ceph      20   0  1.0g  100m  10m  S  10.0   0.2   0:05.00 reactor-1

top - 14:32:02 up 1 day
Tasks:   2 total
%Cpu(s):  5.0 us
MiB Mem : 376023.2 total

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 1001 ceph      20   0  1.0g  100m  10m  S  30.0   0.5   0:11.00 reactor-0
 1002 ceph      20   0  1.0g  100m  10m  S  10.0   0.2   0:06.00 reactor-1
"""

_CPU_COUNT = 4


def _make_top_dir(base: Path, files: dict[str, str]) -> None:
    """Create a 'top' subdirectory containing the given {filename: content} files."""
    top_dir = base / "top"
    top_dir.mkdir()
    for name, content in files.items():
        (top_dir / name).write_text(content)


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


class TestTopResourceInitialization:
    """Test TopResource initialisation."""

    def test_valid_setup(self) -> None:
        """Initialisation succeeds when top dir exists with an output file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": SINGLE_PID_TWO_SNAPSHOTS})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)

            assert resource.source == "top"

    def test_missing_top_directory_raises(self) -> None:
        """FileNotFoundError raised when the top directory does not exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "json_output.0"
            file_path.touch()

            with pytest.raises(FileNotFoundError, match="top directory not found"):
                TopResource(file_path)

    def test_empty_top_directory_raises(self) -> None:
        """FileNotFoundError raised when the top directory is empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            (base / "top").mkdir()

            with pytest.raises(FileNotFoundError, match="top directory is empty"):
                TopResource(file_path)


# ---------------------------------------------------------------------------
# Parsing — single file
# ---------------------------------------------------------------------------


class TestTopResourceParsingSingleFile:
    """Test CPU/memory extraction from a single top output file."""

    def test_single_snapshot_single_thread(self) -> None:
        """Parse one snapshot with one process line.

        Snapshot total = 40.0 core-units.
        Avg across snapshots = 40.0.
        Normalised (÷4) = 10.00.
        """
        content = """\
top - 14:32:01 up 1 day
Tasks:   1 total
%Cpu(s):  5.0 us
MiB Mem : 376023.2 total

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
12345 ceph      20   0  1.0g  100m  10m  S  40.0   1.2   0:10.00 crimson-osd
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": content})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                assert resource.cpu == "10.00"
                assert resource.memory == "1.20"

    def test_two_snapshots_single_thread(self) -> None:
        """CPU is summed per snapshot then averaged, then normalised.

        Snapshot 1 total = 25.0, snapshot 2 total = 35.0.
        Per-file avg = 30.0 core-units.
        Normalised (÷4) = 7.50.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": SINGLE_PID_TWO_SNAPSHOTS})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                assert resource.cpu == "7.50"
                assert resource.memory == "0.70"

    def test_multi_thread_two_snapshots(self) -> None:
        """Threads are summed per snapshot before averaging (not averaged individually).

        Snapshot 1: 20.0 + 10.0 = 30.0; snapshot 2: 30.0 + 10.0 = 40.0.
        Per-file avg = 35.0 core-units.
        Normalised (÷4) = 8.75.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"1001_osd_top.out": MULTI_THREAD_TWO_SNAPSHOTS})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                assert resource.cpu == "8.75"
                assert resource.memory == "0.33"

    def test_zero_cpu_values(self) -> None:
        """Handles zero CPU/memory without errors."""
        content = """\
top - 14:32:01 up 1 day
%Cpu(s):  0.0 us
MiB Mem : 100.0 total

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
99999 root      20   0  100m   10m   1m  S   0.0   0.0   0:00.00 idle
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"99999_osd_top.out": content})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                assert resource.cpu == "0.00"
                assert resource.memory == "0.00"

    def test_header_only_no_data_rows(self) -> None:
        """A file with a PID header but no data rows returns 0."""
        content = """\
top - 14:32:01 up 1 day
%Cpu(s):  5.0 us

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": content})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                assert resource.cpu == "0.00"
                assert resource.memory == "0.00"

    def test_empty_file_returns_zero(self) -> None:
        """An empty .out file does not crash and returns 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": ""})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                assert resource.cpu == "0.00"
                assert resource.memory == "0.00"


# ---------------------------------------------------------------------------
# Parsing — multiple files
# ---------------------------------------------------------------------------


class TestTopResourceParsingMultipleFiles:
    """Test aggregation across multiple PID output files."""

    def test_two_pid_files_summed(self) -> None:
        """CPU per-file averages are summed across PID files (not averaged).

        File A: one snapshot, one thread at 20.0 → per-file avg = 20.0 core-units.
        File B: one snapshot, one thread at 60.0 → per-file avg = 60.0 core-units.
        Total = 80.0. Normalised (÷4) = 20.00.
        """
        content_a = """\
top - 14:32:01 up 1 day
%Cpu(s):  5.0 us

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 1001 ceph      20   0  1.0g  100m  10m  S  20.0   0.4   0:10.00 reactor-0
"""
        content_b = """\
top - 14:32:01 up 1 day
%Cpu(s): 20.0 us

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 2001 ceph      20   0  1.0g  100m  10m  S  60.0   0.8   0:10.00 reactor-1
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"1001_osd_top.out": content_a, "2001_osd_top.out": content_b})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                # total = 20.0 + 60.0 = 80.0; normalised = 80.0 / 4 = 20.00
                assert resource.cpu == "20.00"

    def test_three_pid_files(self) -> None:
        """Aggregation works for three files.

        Files at 10, 20, 30 → total = 60.0 core-units → normalised (÷4) = 15.00.
        """

        def _make_content(cpu_val: float, mem_val: float) -> str:
            return (
                "top - 14:32:01\n%Cpu(s):  5.0 us\n\n"
                "  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND\n"
                f" 1000 ceph      20   0  1.0g  100m  10m  S  {cpu_val}   {mem_val}   0:01.00 osd\n"
            )

        files = {
            "1000_osd_top.out": _make_content(10.0, 0.2),
            "2000_osd_top.out": _make_content(20.0, 0.4),
            "3000_osd_top.out": _make_content(30.0, 0.6),
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, files)

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                # total = 10 + 20 + 30 = 60; normalised = 60 / 4 = 15.0
                assert resource.cpu == "15.00"
                # mem avg = (0.2 + 0.4 + 0.6) / 3 = 0.4 (unchanged by new algorithm)
                assert resource.memory == "0.40"


# ---------------------------------------------------------------------------
# get() method
# ---------------------------------------------------------------------------


class TestTopResourceGet:
    """Test the get() method output contract."""

    def test_get_returns_correct_keys(self) -> None:
        """get() returns a dict with source, cpu, and memory keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": SINGLE_PID_TWO_SNAPSHOTS})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                result = TopResource(file_path).get()

            # avg snapshots = 30.0; normalised = 30.0 / 4 = 7.50
            assert result == {"source": "top", "cpu": "7.50", "memory": "0.70"}

    def test_get_triggers_parsing_once(self) -> None:
        """Calling get() twice does not re-parse."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": SINGLE_PID_TWO_SNAPSHOTS})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                first = resource.get()
                second = resource.get()

            assert first == second
            assert resource._has_been_parsed is True  # pylint: disable=protected-access

    def test_source_property(self) -> None:
        """source property returns 'top' before any parsing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": SINGLE_PID_TWO_SNAPSHOTS})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
            assert resource.source == "top"


# ---------------------------------------------------------------------------
# Edge cases — error paths not covered by the happy-path tests
# ---------------------------------------------------------------------------


class TestTopResourceEdgeCases:
    """Test uncommon but reachable branches."""

    def test_pid_header_missing_cpu_column_is_skipped(self) -> None:
        """A PID header without a %%CPU column logs a warning and is skipped."""
        content = """\
top - 14:32:01 up 1 day
%Cpu(s):  5.0 us

  PID USER      PR  NI    VIRT    RES    SHR S  NOCPU  NOMEM     TIME+ COMMAND
12345 ceph      20   0  1.0g  100m  10m  S  40.0   1.2   0:10.00 crimson-osd
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": content})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                assert resource.cpu == "0.00"
                assert resource.memory == "0.00"

    def test_data_row_too_short_is_skipped(self) -> None:
        """A process row that has fewer columns than the %CPU/%MEM indices is skipped."""
        content = """\
top - 14:32:01 up 1 day
%Cpu(s):  5.0 us

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
12345 ceph too_short
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": content})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                assert resource.cpu == "0.00"
                assert resource.memory == "0.00"

    def test_unparseable_cpu_value_is_skipped(self) -> None:
        """A data row with a non-numeric CPU value is skipped without crashing."""
        content = """\
top - 14:32:01 up 1 day
%Cpu(s):  5.0 us

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
12345 ceph      20   0  1.0g  100m  10m  S  N/A   N/A   0:10.00 crimson-osd
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": content})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                assert resource.cpu == "0.00"
                assert resource.memory == "0.00"

    def test_parse_exception_returns_zero(self) -> None:
        """If _parse_top_directory raises unexpectedly, cpu/memory fall back to 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"12345_osd_top.out": SINGLE_PID_TWO_SNAPSHOTS})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
            with mock.patch.object(resource, "_parse_top_directory", side_effect=RuntimeError("boom")):
                resource._parse({})  # pylint: disable=protected-access

            assert resource.cpu == "0.00"
            assert resource.memory == "0.00"
            assert resource._has_been_parsed is True  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# All-files behaviour
# ---------------------------------------------------------------------------


class TestTopResourceAllFiles:
    """TopResource reads every file in the top/ directory regardless of name."""

    def test_arbitrarily_named_file_is_parsed(self) -> None:
        """A file with any name in top/ is parsed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(base, {"my_custom_output.txt": SINGLE_PID_TWO_SNAPSHOTS})

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                # avg snapshots = 30.0; normalised = 30.0 / 4 = 7.50
                assert resource.cpu == "7.50"
                assert resource.memory == "0.70"

    def test_mixed_naming_all_files_summed(self) -> None:
        """Files with different naming conventions are all included and summed.

        Two identical files, each with per-file avg = 30.0 core-units.
        Total = 60.0. Normalised (÷4) = 15.00.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            file_path = base / "json_output.0"
            file_path.touch()
            _make_top_dir(
                base,
                {
                    "12345_osd_top.out": SINGLE_PID_TWO_SNAPSHOTS,
                    "99999_mon_top.out": SINGLE_PID_TWO_SNAPSHOTS,
                },
            )

            with mock.patch("post_processing.run_results.resources.top_resource.os.cpu_count", return_value=_CPU_COUNT):
                resource = TopResource(file_path)
                # Two files, each contributing 30.0 core-units → total 60.0 → /4 = 15.00
                assert resource.cpu == "15.00"
