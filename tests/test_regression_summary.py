"""
End-to-end tests for the regression pipeline via run_comparison().

Uses real JSON files in temp directories — same pattern as test_regression_loader.py.
"""

import json
import shutil
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from post_processing.post_processing_types import (
    Band,
    Decision,
    FlagResult,
    FlagTier,
    RunDecision,
    WorkloadDecision,
)
from post_processing.regression.runner import DEFAULT_THRESHOLDS, run_comparison
from post_processing.regression.summary import (
    format_flag_detail,
    format_run_summary,
    format_workload_name,
    sort_and_group_workload_decisions,
)


def _make_json(iodepths: Sequence[str], iops: str) -> dict[str, Any]:
    """Return a minimal intermediate-format dict with the given iodepth keys and maximum_iops."""
    data: dict[str, Any] = {}
    for depth in iodepths:
        data[depth] = {
            "bandwidth_bytes": "500000",
            "blocksize": "4096",
            "iops": "250",
            "latency": "4000000",
            "number_of_jobs": "1",
            "runtime_seconds": "60",
        }
    data.update(
        {
            "maximum_iops": iops,
            "maximum_bandwidth": "500000",
            "maximum_cpu_usage": "0.50",
            "maximum_memory_usage": "1024",
            "benchmark": "librbdfio",
        }
    )
    return data


def _make_archive(base: Path, iops_by_stem: dict[str, str], iodepths: Sequence[str] = ("1", "2", "4")) -> None:
    """Create an archive at base/visualisation/ holding one workload JSON per {stem: maximum_iops}."""
    vis = base / "visualisation"
    vis.mkdir(parents=True, exist_ok=True)
    for stem, iops in iops_by_stem.items():
        (vis / f"{stem}.json").write_text(json.dumps(_make_json(iodepths, iops)), encoding="utf8")


class TestRunComparison(unittest.TestCase):
    """End-to-end tests for run_comparison."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.baseline = self.tmp / "baseline"
        self.candidate = self.tmp / "candidate"

    def test_baseline_vs_itself_returns_0(self) -> None:
        """Comparing an archive against itself must return 0 (PASS)."""
        _make_archive(self.baseline, {"4096_1_randread": "1000"})
        code = run_comparison(self.baseline, self.baseline, DEFAULT_THRESHOLDS)
        self.assertEqual(code, 0, "Baseline vs itself must return 0")

    def test_single_weak_workload_of_several_returns_0(self) -> None:
        """RC=1 out of 4 workloads → PASS → return 0."""
        _make_archive(
            self.baseline,
            {
                "4096_1_randread": "1000",
                "4096_1_randwrite": "1000",
                "256k_1_randread": "1000",
                "256k_1_randwrite": "1000",
            },
        )
        _make_archive(
            self.candidate,
            {
                "4096_1_randread": "900",
                "4096_1_randwrite": "1000",
                "256k_1_randread": "1000",
                "256k_1_randwrite": "1000",
            },
        )  # -10% WEAK on 4096_1_randread only
        code = run_comparison(self.baseline, self.candidate, DEFAULT_THRESHOLDS)
        self.assertEqual(code, 0, "RC=1 must return 0")

    def test_two_weak_workloads_returns_1(self) -> None:
        """RC=2 → REVIEW → return 1."""
        _make_archive(self.baseline, {"4096_1_randread": "1000", "4096_1_randwrite": "1000", "256k_1_randread": "1000"})
        _make_archive(
            self.candidate, {"4096_1_randread": "900", "4096_1_randwrite": "900", "256k_1_randread": "1000"}
        )  # -10% WEAK on two workloads
        code = run_comparison(self.baseline, self.candidate, DEFAULT_THRESHOLDS)
        self.assertEqual(code, 1, "RC=2 must return 1")

    def test_strong_drop_returns_2(self) -> None:
        """
        FC >= fail_target → FAIL → return 2.
        N=2, fail_target=1; -25% IOPS on one workload → STRONG → FAIL.
        """
        _make_archive(self.baseline, {"4096_1_randread": "1000", "4096_1_randwrite": "1000"})
        _make_archive(self.candidate, {"4096_1_randread": "750", "4096_1_randwrite": "1000"})  # -25% STRONG
        code = run_comparison(self.baseline, self.candidate, DEFAULT_THRESHOLDS)
        self.assertEqual(code, 2, "FC >= fail_target must return 2")

    def test_nonexistent_baseline_returns_3(self) -> None:
        """A nonexistent baseline directory returns exit code 3 (input error)."""
        _make_archive(self.candidate, {"4096_1_randread": "1000"})
        code = run_comparison(self.tmp / "missing", self.candidate, DEFAULT_THRESHOLDS)
        self.assertEqual(code, 3)

    def test_no_intermediate_files_returns_3(self) -> None:
        """An archive directory with no intermediate JSON files returns exit code 3."""
        self.baseline.mkdir()
        _make_archive(self.candidate, {"4096_1_randread": "1000"})
        code = run_comparison(self.baseline, self.candidate, DEFAULT_THRESHOLDS)
        self.assertEqual(code, 3)


class TestSummaryFormattingHelpers(unittest.TestCase):
    """Test naming, sorting, and formatting helper functions."""

    def test_format_workload_name_standard(self) -> None:
        """Standard filename stems are formatted into human-readable workload names."""
        self.assertEqual(format_workload_name("4096_1_randread"), "4K randread")
        self.assertEqual(format_workload_name("1048576_1_write"), "1M write")
        self.assertEqual(format_workload_name("16384_1_70_30_randrw"), "16K 70/30 randrw")
        self.assertEqual(format_workload_name("4194304_1_randwrite (missing)"), "4M randwrite (missing)")

    def test_sort_and_group_workload_decisions_cbt_order(self) -> None:
        """Workload decisions are grouped into CBT canonical operation order."""
        stems = ["4096_1_randwrite", "1048576_1_read", "4096_1_read", "4096_1_randread", "1048576_1_write"]
        items = [WorkloadDecision(stem=stem, decision=Decision.PASS, flags=[]) for stem in stems]

        sections = sort_and_group_workload_decisions(items)
        titles = [title for title, _ in sections]

        # Sequential Read should come before Sequential Write before Random Read before Random Write
        self.assertIn("Sequential Read", titles)
        self.assertIn("Sequential Write", titles)
        self.assertIn("Random Read", titles)
        self.assertIn("Random Write", titles)

        read_sec = next(wds for t, wds in sections if t == "Sequential Read")
        self.assertEqual(read_sec[0].stem, "4096_1_read")
        self.assertEqual(read_sec[1].stem, "1048576_1_read")

    def test_format_run_summary_with_missing_and_extra(self) -> None:
        """Run summary includes missing count in review and extra count when present."""
        run = RunDecision(
            decision=Decision.REVIEW,
            num_workloads=3,
            fail_target=2,
            fail_count=0,
            review_count=2,
        )
        line = format_run_summary(run, missing_count=1, extra_count=2)
        self.assertIn("review=2 (incl. 1 missing)", line)
        self.assertIn("extra=2", line)

    def test_format_flag_detail_m1_m2_percent(self) -> None:
        """M1 and M2 detail lines display values formatted as percentages."""
        flag = FlagResult(
            tier=FlagTier.M,
            band=Band.WEAK,
            baseline_value=50.0,
            candidate_value=55.0,
            pct_change=0.10,
            reason=None,
            iodepth=None,
        )
        detail = format_flag_detail(flag, "M2")
        self.assertIn("baseline=50.00%", detail)
        self.assertIn("candidate=55.00%", detail)


if __name__ == "__main__":
    unittest.main()
