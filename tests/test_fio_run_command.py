# pylint: disable=protected-access
"""
Tests for Fio.analyze() — JSON extraction from mixed fio stdout output.

Block-size precedence and flag assembly are tested at the Command level in
test_fio_command_generation.py.  _run_io_loop() dispatch is tested in
test_io_loop.py.  This file covers only the JSON extraction behaviour that is
common to all FIO benchmark subclasses via FioBenchmark.analyze().
"""

# pyright: strict, reportPrivateUsage=false

import json
import os
import shutil
import tempfile
import unittest
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from benchmark.fio import Fio

_FQDN_CMD = "hostname -f"


def _make_fio(extra: Optional[dict[str, Any]] = None) -> Fio:
    """Build a minimal Fio stub — enough for analyze() to work."""
    base_config: dict[str, Any] = {
        "mode": "write",
        "op_size": 4194304,
        "iodepth": 16,
        "numjobs": 1,
        "end_fsync": 0,
        "size": 4096,
        "procs_per_endpoint": 1,
    }
    if extra:
        base_config.update(extra)

    cluster_mock = MagicMock()
    cluster_mock.config = {}

    with patch("settings.getnodes", return_value="fake-client"), patch("common.get_fqdn_cmd", return_value=_FQDN_CMD):
        bm = Fio.__new__(Fio)
        with patch("benchmark.benchmark.Benchmark.__init__", return_value=None):
            bm.archive_dir = "/tmp/archive/"
            bm.run_dir = "/tmp/run/"
            bm.osd_ra = "0"
            bm.osd_ra_changed = False
            bm._base_archive_directory = "/tmp/archive/"
            bm._create_report = False
            bm.log_iops = True
            bm.log_bw = True
            bm.log_lat = True
            bm.cmd_path_full = ""
            bm.config = base_config
            bm.cluster = cluster_mock
            workloads_stub = MagicMock()
            workloads_stub.exist.return_value = False
            bm._workloads = workloads_stub
            Fio.__init__(bm, "/tmp/archive/", cluster_mock, base_config)
    return bm


class TestFioAnalyze(unittest.TestCase):
    """
    Fio.analyze() delegates to FioBenchmark.parse_results() which uses the
    { / } brace-matching strategy to extract JSON from mixed fio stdout.
    """

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.bm = _make_fio()
        self.bm.archive_dir = self.tmp
        self.bm.out_dir = self.tmp

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_output(self, content: str, suffix: str = "0") -> str:
        path = os.path.join(self.tmp, f"output.{suffix}")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def _json_output_path(self, suffix: str = "0") -> str:
        return os.path.join(self.tmp, f"json_output.{suffix}")

    def test_json_block_extracted(self) -> None:
        """A plain JSON output file is extracted correctly into json_output."""
        obj = {"fio version": "fio-3.30", "jobs": []}
        self._write_output(json.dumps(obj, indent=2))
        self.bm.analyze(self.tmp)
        self.assertTrue(os.path.exists(self._json_output_path()))
        with open(self._json_output_path(), encoding="utf-8") as fh:
            content = fh.read()
        self.assertIn("fio version", content)

    def test_content_before_brace_is_excluded(self) -> None:
        """Non-JSON lines before the opening brace are stripped from the output."""
        self._write_output(
            "fio: pid=1234, err=0\n"
            "Starting 1 process\n"
            "{\n"
            '  "fio version": "fio-3.30"\n'
            "}\n"
        )
        self.bm.analyze(self.tmp)
        with open(self._json_output_path(), encoding="utf-8") as fh:
            content = fh.read()
        self.assertNotIn("Starting", content)
        self.assertNotIn("pid=1234", content)
        self.assertIn("fio version", content)

    def test_no_json_skips_write_and_warns(self) -> None:
        """When no JSON block is found, json_output is not written and a warning is logged."""
        self._write_output("fio: some log line\nfio: another log line\n")
        with self.assertLogs("cbt", level="WARNING") as cm:
            self.bm.analyze(self.tmp)
        self.assertFalse(os.path.exists(self._json_output_path()))
        self.assertTrue(any("no JSON block found" in msg for msg in cm.output))


if __name__ == "__main__":
    unittest.main()
