"""
Unit tests for post_processing/regression/loader.py.

Uses real JSON files written to a temp directory.  No mocking of internal CBT
code — per the coding guidelines.
"""

import io
import json
import logging
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any

from post_processing.regression.loader import (
    ArchiveLoadError,
    WorkloadPair,
    load_archive_pairs,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Minimal valid intermediate JSON.  All values are strings; iodepth keys are
# numeric string keys whose values are dicts (headline fields are plain str).
_IODEPTH_ENTRY: dict[str, Any] = {
    "bandwidth_bytes": "1000000",
    "blocksize": "4096",
    "iops": "250",
    "latency": "4000000",
    "number_of_jobs": "1",
    "runtime_seconds": "60",
}

_HEADLINE_FIELDS: dict[str, Any] = {
    "maximum_iops": "250",
    "maximum_bandwidth": "1000000",
    "maximum_cpu_usage": "0.50",
    "maximum_memory_usage": "512",
    "benchmark": "librbdfio",
}


def _make_json(iodepths: list[str], runtime: str = "60") -> dict[str, Any]:
    """Return a minimal intermediate-format dict with the given iodepth keys."""
    data: dict[str, Any] = {}
    for depth in iodepths:
        entry = dict(_IODEPTH_ENTRY)
        entry["runtime_seconds"] = runtime
        data[depth] = entry
    data.update(_HEADLINE_FIELDS)
    return data


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Write `data` as JSON to `path`, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf8")


def _make_archive(
    base: Path,
    stems_and_iodepths: dict[str, list[str]],
    vis_subpath: str = "visualisation",
    runtime: str = "60",
) -> Path:
    """
    Create an archive directory with JSON files at base/<vis_subpath>/<stem>.json.
    Returns the archive root (base).
    """
    vis_dir = base / vis_subpath
    vis_dir.mkdir(parents=True, exist_ok=True)
    for stem, iodepths in stems_and_iodepths.items():
        _write_json(vis_dir / f"{stem}.json", _make_json(iodepths, runtime=runtime))
    return base


class _ArchiveTestCase(unittest.TestCase):
    """Base class giving each test a temp directory with baseline and candidate archive roots."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.baseline = self.tmp / "baseline"
        self.candidate = self.tmp / "candidate"

    def _write_raw_baseline(self, content: bytes, stem: str = "4096_1_randread") -> Path:
        """Write `content` verbatim as the baseline's only intermediate file and return its path."""
        vis = self.baseline / "visualisation"
        vis.mkdir(parents=True, exist_ok=True)
        path = vis / f"{stem}.json"
        path.write_bytes(content)
        return path


# ---------------------------------------------------------------------------
# Happy path: basic pairing
# ---------------------------------------------------------------------------


class TestLoadArchivePairs(_ArchiveTestCase):
    """Tests for load_archive_pairs()."""

    def test_pairs_by_stem_ignoring_id_directory(self) -> None:
        """Workloads pair by filename stem regardless of id-xxx directory names."""
        # Different id-xxx wrapper paths, same stems
        _make_archive(self.baseline / "results" / "00000000" / "id-aaa" / "randread", {"4096_1_randread": ["1", "2"]})
        _make_archive(self.candidate / "results" / "00000000" / "id-bbb" / "randread", {"4096_1_randread": ["1", "2"]})

        pairs, missing, extra = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 1, "Should pair the one matching stem")
        self.assertEqual(pairs[0].stem, "4096_1_randread")
        self.assertEqual(missing, [], "No missing workloads")
        self.assertEqual(extra, [], "No extra workloads")

    def test_pairs_multiple_stems(self) -> None:
        """All matching stems are paired; result includes all of them."""
        stems = {
            "4096_1_randread": ["1", "2"],
            "262144_1_randread": ["1", "2"],
            "4194304_1_randwrite": ["1", "2"],
        }
        _make_archive(self.baseline, stems)
        _make_archive(self.candidate, stems)

        pairs, _missing, _extra = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 3, "All three stems should be paired")
        self.assertEqual({p.stem for p in pairs}, set(stems.keys()))

    def test_different_stems_show_as_missing_and_extra(self) -> None:
        """Stems only in baseline → missing; only in candidate → extra."""
        _make_archive(self.baseline, {"4096_1_randread": ["1"], "256k_1_randwrite": ["1"]})
        _make_archive(self.candidate, {"4096_1_randread": ["1"], "new_workload_1_randrw": ["1"]})

        pairs, missing, extra = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 1, "Only one stem matches")
        self.assertIn("256k_1_randwrite", missing, "Baseline-only stem should be missing")
        self.assertIn("new_workload_1_randrw", extra, "Candidate-only stem should be extra")

    def test_timeseries_files_are_excluded(self) -> None:
        """Files ending in _timeseries are not treated as workloads."""
        for archive in (self.baseline, self.candidate):
            _make_archive(archive, {"4096_1_randread": ["1", "2"], "4096_1_randread_timeseries": ["1", "2"]})

        pairs, _missing, _extra = load_archive_pairs(self.baseline, self.candidate)

        stems = {p.stem for p in pairs}
        self.assertIn("4096_1_randread", stems, "Normal file should be paired")
        self.assertNotIn("4096_1_randread_timeseries", stems, "Timeseries file must be excluded")
        self.assertEqual(len(pairs), 1, "Exactly one workload pair")

    def test_runtime_seconds_mismatch_logs_warning(self) -> None:
        """A differing runtime_seconds in any iodepth entry triggers a warning."""
        _make_archive(self.baseline, {"4096_1_randread": ["1", "2"]}, runtime="60")
        _make_archive(self.candidate, {"4096_1_randread": ["1", "2"]}, runtime="30")

        with self.assertLogs(level=logging.WARNING) as log_ctx:
            pairs, _, _ = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 1, "Pair should still be returned")
        self.assertIn("runtime_seconds", " ".join(log_ctx.output), "Warning must mention runtime_seconds")

    def test_matching_runtime_seconds_no_warning(self) -> None:
        """Matching runtime_seconds values must not produce a warning."""
        _make_archive(self.baseline, {"4096_1_randread": ["1", "2"]}, runtime="60")
        _make_archive(self.candidate, {"4096_1_randread": ["1", "2"]}, runtime="60")

        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setLevel(logging.WARNING)
        logger = logging.getLogger("cbt.regression.loader")
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)

        load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(stream.getvalue(), "", "No warning should be emitted when runtime_seconds matches")


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestLoadArchiveErrors(_ArchiveTestCase):
    """Tests for ArchiveLoadError conditions in load_archive_pairs()."""

    def test_nonexistent_baseline_raises(self) -> None:
        """A nonexistent baseline directory raises ArchiveLoadError."""
        _make_archive(self.candidate, {"4096_1_randread": ["1"]})

        with self.assertRaises(ArchiveLoadError, msg="Nonexistent baseline should raise"):
            load_archive_pairs(self.tmp / "does_not_exist", self.candidate)

    def test_nonexistent_candidate_raises(self) -> None:
        """A nonexistent candidate directory raises ArchiveLoadError."""
        _make_archive(self.baseline, {"4096_1_randread": ["1"]})

        with self.assertRaises(ArchiveLoadError, msg="Nonexistent candidate should raise"):
            load_archive_pairs(self.baseline, self.tmp / "does_not_exist")

    def test_no_intermediate_files_raises(self) -> None:
        """An archive with no intermediate JSON raises ArchiveLoadError naming fio_common_output_wrapper."""
        self.baseline.mkdir()
        _make_archive(self.candidate, {"4096_1_randread": ["1"]})

        with self.assertRaises(ArchiveLoadError) as ctx:
            load_archive_pairs(self.baseline, self.candidate)

        self.assertIn(
            "fio_common_output_wrapper",
            str(ctx.exception),
            "Error message must reference fio_common_output_wrapper.py",
        )

    def test_no_pairs_raises(self) -> None:
        """When no stems match at all, ArchiveLoadError is raised."""
        _make_archive(self.baseline, {"4096_1_randread": ["1"]})
        _make_archive(self.candidate, {"256k_1_randwrite": ["1"]})

        with self.assertRaises(ArchiveLoadError, msg="No paired workloads should raise"):
            load_archive_pairs(self.baseline, self.candidate)

    def test_duplicate_stem_in_baseline_warns_and_keeps_first(self) -> None:
        """Two files with the same stem in one archive logs a warning and keeps the first."""
        # Place the same stem in two different visualisation directories
        for operation, iops in (("randread", "1000"), ("randwrite", "2000")):
            data = _make_json(["1"])
            data["maximum_iops"] = iops
            _write_json(self.baseline / operation / "visualisation" / "4096_1_randread.json", data)
        _make_archive(self.candidate, {"4096_1_randread": ["1"]})

        with self.assertLogs("cbt.regression.loader", level="WARNING") as log_ctx:
            pairs, _missing, _extra = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0].baseline_data["maximum_iops"], "1000")
        self.assertTrue(
            any("duplicate stem '4096_1_randread'" in msg for msg in log_ctx.output),
            "Must log warning about duplicate stem",
        )

    def test_invalid_json_raises(self) -> None:
        """Corrupt JSON in the baseline raises ArchiveLoadError naming the file."""
        self._write_raw_baseline(b"{ not valid json")
        _make_archive(self.candidate, {"4096_1_randread": ["1"]})

        with self.assertRaises(ArchiveLoadError) as ctx:
            load_archive_pairs(self.baseline, self.candidate)

        self.assertIn("4096_1_randread", str(ctx.exception), "Error must name the corrupt file")

    def test_non_utf8_json_file_raises(self) -> None:
        """A non-UTF-8 intermediate file raises ArchiveLoadError."""
        self._write_raw_baseline(b"\x80\x81\xff\xfe")
        _make_archive(self.candidate, {"4096_1_randread": ["1"]})

        with self.assertRaises(ArchiveLoadError) as ctx:
            load_archive_pairs(self.baseline, self.candidate)

        self.assertIn("4096_1_randread", str(ctx.exception))

    def test_unreadable_json_file_raises(self) -> None:
        """An intermediate path that cannot be read as a file raises ArchiveLoadError."""
        # A directory named *.json is unreadable as a file for every user.
        (self.baseline / "visualisation" / "4096_1_randread.json").mkdir(parents=True)
        _make_archive(self.candidate, {"4096_1_randread": ["1"]})

        with self.assertRaises(ArchiveLoadError) as ctx:
            load_archive_pairs(self.baseline, self.candidate)

        self.assertIn("4096_1_randread", str(ctx.exception))

    def test_non_dict_json_file_raises(self) -> None:
        """A JSON file containing a list instead of a dict raises ArchiveLoadError."""
        self._write_raw_baseline(b"[1, 2, 3]")
        _make_archive(self.candidate, {"4096_1_randread": ["1"]})

        with self.assertRaises(ArchiveLoadError) as ctx:
            load_archive_pairs(self.baseline, self.candidate)

        self.assertIn("not a JSON object", str(ctx.exception))

    def test_empty_json_file_raises(self) -> None:
        """An empty JSON file raises ArchiveLoadError naming the file."""
        self._write_raw_baseline(b"")
        _make_archive(self.candidate, {"4096_1_randread": ["1"]})

        with self.assertRaises(ArchiveLoadError) as ctx:
            load_archive_pairs(self.baseline, self.candidate)

        self.assertIn("4096_1_randread", str(ctx.exception), "Error must name the empty file")


# ---------------------------------------------------------------------------
# Varied layout and stem shapes (no hardcoding)
# ---------------------------------------------------------------------------


class TestArchiveLayouts(_ArchiveTestCase):
    """Tests that load_archive_pairs() copes with the range of real archive layouts."""

    def test_multi_iteration_archive_loads_successfully(self) -> None:
        """Archives with multiple iterations (e.g. results/0 and results/1) load without error."""
        for iteration in ("0", "1"):
            _make_archive(self.baseline / "results" / iteration / "randread", {"4096_1_randread": ["1", "2"]})
        _make_archive(self.candidate, {"4096_1_randread": ["1", "2"]})

        pairs, _missing, _extra = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0].stem, "4096_1_randread")

    def test_bare_visualisation_folder_as_archive(self) -> None:
        """A baseline stored as a bare visualisation/ folder is found."""
        # The baseline IS the parent of visualisation/
        _make_archive(self.baseline, {"262144_1_randwrite": ["4", "8", "16"]})
        _make_archive(self.candidate, {"262144_1_randwrite": ["4", "8", "16"]})

        pairs, _missing, _extra = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0].stem, "262144_1_randwrite")

    def test_deeply_nested_archive_layout(self) -> None:
        """Workloads nested under results/<iter>/id-<hash>/op/visualisation/ are found."""
        stems = {"4194304_2_randwrite": ["8", "16", "32", "64"]}
        _make_archive(self.baseline / "results" / "0" / "id-xyz" / "randwrite", stems)
        _make_archive(self.candidate / "results" / "0" / "id-abc" / "randwrite", stems)

        pairs, missing, extra = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0].stem, "4194304_2_randwrite")
        self.assertEqual(missing, [])
        self.assertEqual(extra, [])

    def test_varied_stem_shapes_nothing_hardcoded(self) -> None:
        """
        Loader works for an entirely different set of workload names and counts
        than used in other tests — nothing is hardcoded.
        """
        stems = {
            "131072_4_read": ["2", "4"],
            "131072_4_write": ["2", "4"],
            "524288_8_randrw_50_50": ["2", "4"],
            "524288_8_randread": ["2", "4"],
            "1048576_1_randwrite": ["2", "4"],
        }
        _make_archive(self.baseline, stems)
        _make_archive(self.candidate, stems)

        pairs, missing, extra = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 5, "All five unusual stems should pair")
        self.assertEqual(missing, [])
        self.assertEqual(extra, [])

    def test_workload_pair_contains_parsed_data(self) -> None:
        """Each WorkloadPair holds the parsed JSON dicts for baseline and candidate."""
        _make_archive(self.baseline, {"4096_1_randread": ["1", "2"]})
        _make_archive(self.candidate, {"4096_1_randread": ["1", "2"]})

        pairs, _, _ = load_archive_pairs(self.baseline, self.candidate)

        self.assertEqual(len(pairs), 1)
        pair = pairs[0]
        self.assertIsInstance(pair, WorkloadPair)
        self.assertIsInstance(pair.baseline_data, dict, "baseline_data must be a dict")
        self.assertIsInstance(pair.candidate_data, dict, "candidate_data must be a dict")
        self.assertIn("1", pair.baseline_data, "Iodepth key '1' must be present")


if __name__ == "__main__":
    unittest.main()
