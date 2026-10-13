"""
Unit tests for the JSON output extraction logic in FioBenchmark.parse_results().

FioBenchmark.parse_results() scans ``output.*`` files in the archive directory
and writes the first JSON block (from ``{`` to ``}``) it finds into a matching
``json_output.*`` file alongside the source.

These tests verify the canonical {/} brace-matching extraction strategy used
by all FIO benchmark subclasses via FioBenchmark.

Coverage:
  - A clean JSON-only file is copied as-is.
  - Mixed stdout + JSON (e.g. FIO's json,normal format) → only the JSON block.
  - Multiple JSON blocks → only the first block is written.
  - File with no JSON block → output file is created but contains nothing.
  - Non-numeric suffixed files are ignored (not processed).
  - Multiple ``output.*`` files in the same directory are all handled.
  - Nested sub-directories are recursively scanned.
  - The JSON extracted is valid Python-parsable JSON.
"""

# pyright: strict, reportPrivateUsage=false

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from benchmark.base_fio import FioBenchmark

# ---------------------------------------------------------------------------
# Minimal concrete subclass so we can instantiate FioBenchmark
# ---------------------------------------------------------------------------


class _ConcreteFioBenchmark(FioBenchmark):
    @property
    def num_targets(self) -> int:
        return 1

    @property
    def benchmark_type(self) -> str:
        return "test"

    def setup_targets(self) -> None:
        pass

    def prefill_targets(self) -> None:
        pass

    def cleanup_targets(self) -> None:
        pass

    def _run_io_loop(self) -> None:
        pass


def _make_fio_benchmark(archive_dir: str) -> _ConcreteFioBenchmark:
    """Return a FioBenchmark instance pointing at archive_dir."""
    config: dict[str, Any] = {"iteration": 0, "mode": "write", "op_size": 4194304, "iodepth": 16, "numjobs": 1}
    cluster_mock = MagicMock()
    cluster_mock.config = {}
    with (
        patch("settings.getnodes", return_value="fake-client"),
        patch("settings.cluster", {"tmp_dir": "/tmp/cbt-test/"}),
        patch("common.get_osd_ra", return_value="0"),
    ):
        bm = _ConcreteFioBenchmark(archive_dir, cluster_mock, config)
    bm.archive_dir = archive_dir
    return bm


def _write_output(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _json_output_for(output_path: Path) -> Path:
    """Return the expected json_output sibling of an output file."""
    _stem, _, suffix = output_path.name.partition(".")
    return output_path.parent / f"json_output.{suffix}"


# Minimal valid FIO JSON block (trimmed for test clarity)
_VALID_JSON_OBJ: dict[str, Any] = {"fio version": "fio-3.30", "jobs": []}
_VALID_JSON_STR = json.dumps(_VALID_JSON_OBJ, indent=2)


class TestFioBenchmarkParseResultsCleanJson(unittest.TestCase):
    """A file containing only a JSON block is extracted unchanged."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.archive = os.path.join(self.tmp, "archive")
        os.makedirs(self.archive)
        self.bm = _make_fio_benchmark(self.archive)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_json_block_is_extracted(self) -> None:
        """Verify that a standalone JSON block is extracted into a json_output file."""
        out_file = Path(self.archive) / "output.0"
        _write_output(out_file, _VALID_JSON_STR)

        self.bm.parse_results()

        json_out = _json_output_for(out_file)
        self.assertTrue(json_out.exists(), f"Expected {json_out} to be created")

    def test_extracted_content_is_valid_json(self) -> None:
        """Verify that the extracted content parses as valid JSON matching the input."""
        out_file = Path(self.archive) / "output.0"
        _write_output(out_file, _VALID_JSON_STR)

        self.bm.parse_results()

        json_out = _json_output_for(out_file)
        content = json_out.read_text(encoding="utf-8")
        parsed = json.loads(content)
        self.assertIn("fio version", parsed)


class TestFioBenchmarkParseResultsMixedOutput(unittest.TestCase):
    """Mixed stdout + JSON (json,normal format): only the JSON block is kept."""

    _MIXED = (
        "fio: io_u error on file cbt-rbdfio-host-0: Connection refused: error\n"
        "Starting 1 process\n"
        "{\n"
        '  "fio version": "fio-3.30",\n'
        '  "jobs": []\n'
        "}\n"
        "fio: file specific write error: 5\n"
    )

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.archive = os.path.join(self.tmp, "archive")
        os.makedirs(self.archive)
        self.bm = _make_fio_benchmark(self.archive)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_only_json_block_in_output(self) -> None:
        """Verify that stdout output surrounding the JSON block is excluded."""
        out_file = Path(self.archive) / "output.0"
        _write_output(out_file, self._MIXED)

        self.bm.parse_results()

        json_out = _json_output_for(out_file)
        content = json_out.read_text(encoding="utf-8")
        # Non-JSON lines must not appear
        self.assertNotIn("Connection refused", content)
        self.assertNotIn("Starting", content)

    def test_extracted_mixed_is_parseable_json(self) -> None:
        """Verify that the extracted block from mixed output is parseable JSON."""
        out_file = Path(self.archive) / "output.0"
        _write_output(out_file, self._MIXED)

        self.bm.parse_results()

        json_out = _json_output_for(out_file)
        content = json_out.read_text(encoding="utf-8")
        parsed = json.loads(content)
        self.assertIn("fio version", parsed)


class TestFioBenchmarkParseResultsNoJson(unittest.TestCase):
    """Files with no JSON block produce an empty json_output file."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.archive = os.path.join(self.tmp, "archive")
        os.makedirs(self.archive)
        self.bm = _make_fio_benchmark(self.archive)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_no_json_skips_write_and_warns(self) -> None:
        """When no JSON block is found the json_output file is NOT created and a warning is logged."""
        out_file = Path(self.archive) / "output.0"
        _write_output(out_file, "fio: some log line\nfio: another log line\n")

        with self.assertLogs("cbt", level="WARNING") as cm:
            self.bm.parse_results()

        json_out = _json_output_for(out_file)
        self.assertFalse(json_out.exists(), "json_output file must NOT be created when no JSON found")
        self.assertTrue(any("no JSON block found" in msg for msg in cm.output))


class TestFioBenchmarkParseResultsMultipleBlocks(unittest.TestCase):
    """When multiple JSON blocks exist only the first is extracted."""

    _TWO_BLOCKS = (
        "{\n"
        + '  "fio version": "first"\n'
        + "}\n"
        + "some separator line\n"
        + "{\n"
        + '  "fio version": "second"\n'
        + "}\n"
    )

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.archive = os.path.join(self.tmp, "archive")
        os.makedirs(self.archive)
        self.bm = _make_fio_benchmark(self.archive)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_only_first_block_extracted(self) -> None:
        """Verify that only the first JSON block is extracted when multiple exist."""
        out_file = Path(self.archive) / "output.0"
        _write_output(out_file, self._TWO_BLOCKS)

        self.bm.parse_results()

        json_out = _json_output_for(out_file)
        content = json_out.read_text(encoding="utf-8")
        parsed = json.loads(content)
        self.assertEqual(parsed.get("fio version"), "first")
        self.assertNotIn("second", content)


class TestFioBenchmarkParseResultsMultipleFiles(unittest.TestCase):
    """All output.N files in the archive directory are processed."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.archive = os.path.join(self.tmp, "archive")
        os.makedirs(self.archive)
        self.bm = _make_fio_benchmark(self.archive)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_all_numeric_output_files_processed(self) -> None:
        """Verify that multiple output.N files in a directory are all parsed."""
        for i in range(4):
            obj = {"volume": i}
            content = json.dumps(obj, indent=2)
            out_file = Path(self.archive) / f"output.{i}"
            _write_output(out_file, content)

        self.bm.parse_results()

        for i in range(4):
            json_out = Path(self.archive) / f"json_output.{i}"
            self.assertTrue(json_out.exists(), f"Missing {json_out}")
            parsed = json.loads(json_out.read_text(encoding="utf-8"))
            self.assertEqual(parsed["volume"], i)


class TestFioBenchmarkParseResultsIgnoresNonNumericSuffix(unittest.TestCase):
    """Files whose suffix is not a plain integer are not processed."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.archive = os.path.join(self.tmp, "archive")
        os.makedirs(self.archive)
        self.bm = _make_fio_benchmark(self.archive)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_non_numeric_output_file_ignored(self) -> None:
        """Verify that output files with non-numeric extensions are ignored."""
        # output.log should NOT be processed
        bad_file = Path(self.archive) / "output.log"
        _write_output(bad_file, _VALID_JSON_STR)

        self.bm.parse_results()

        json_out = Path(self.archive) / "json_output.log"
        self.assertFalse(json_out.exists(), f"{json_out} should not be created for non-numeric suffix")


class TestFioBenchmarkParseResultsRecursiveSubdirs(unittest.TestCase):
    """parse_results() recurses into subdirectories and handles output files there."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.archive = os.path.join(self.tmp, "archive")
        os.makedirs(self.archive)
        self.bm = _make_fio_benchmark(self.archive)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_subdir_output_files_are_processed(self) -> None:
        """Verify that output files in nested subdirectories are found and parsed."""
        subdir = Path(self.archive) / "workload" / "rbdfio" / "numjobs-001" / "iodepth-000016"
        obj: dict[str, Any] = {"jobs": [{"vol": 0}]}
        out_file = subdir / "output.0"
        _write_output(out_file, json.dumps(obj, indent=2))

        self.bm.parse_results()

        json_out = _json_output_for(out_file)
        self.assertTrue(json_out.exists(), f"Expected {json_out}")
        parsed = json.loads(json_out.read_text(encoding="utf-8"))
        self.assertIn("jobs", parsed)


class TestFioBenchmarkParseResultsFormattingEdgeCases(unittest.TestCase):
    """Tests for single-line, inline braces, and complex brace formatting."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.archive = os.path.join(self.tmp, "archive")
        os.makedirs(self.archive)
        self.bm = _make_fio_benchmark(self.archive)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_single_line_json(self) -> None:
        """Verify that single-line JSON embedded in stdout is extracted."""
        out_file = Path(self.archive) / "output.0"
        _write_output(out_file, 'prefix stdout {"fio version": "fio-3.30", "jobs": []} suffix stdout')

        self.bm.parse_results()

        json_out = _json_output_for(out_file)
        parsed = json.loads(json_out.read_text(encoding="utf-8"))
        self.assertEqual(parsed["fio version"], "fio-3.30")

    def test_inline_first_and_last_lines(self) -> None:
        """Verify that JSON with inline first and last lines is extracted."""
        out_file = Path(self.archive) / "output.0"
        _write_output(out_file, '{"fio version": "fio-3.30",\n"jobs": [{"name": "test"}]}')

        self.bm.parse_results()

        json_out = _json_output_for(out_file)
        parsed = json.loads(json_out.read_text(encoding="utf-8"))
        self.assertEqual(parsed["jobs"][0]["name"], "test")

    def test_json_with_braces_in_strings(self) -> None:
        """Verify that literal braces inside string values do not break extraction."""
        out_file = Path(self.archive) / "output.0"
        _write_output(
            out_file,
            'header line\n{\n  "msg": "error with {placeholder} and extra } brace",\n  "jobs": []\n}\ntrailer\n',
        )

        self.bm.parse_results()

        json_out = _json_output_for(out_file)
        parsed = json.loads(json_out.read_text(encoding="utf-8"))
        self.assertEqual(parsed["msg"], "error with {placeholder} and extra } brace")


if __name__ == "__main__":
    unittest.main()
