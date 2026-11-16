"""
Unit tests for post_processing/regression/decisions.py.

Tests cover:
- Workload decision: one case per H/M tier and band combination, plus precedence between them
- fail_target: max(1, min(3, ceil(N * 0.5))) at and either side of both bounds
- Run decision: FAIL/REVIEW/PASS for each fail_count and review_count combination, N <= 0 error
"""

import unittest

from post_processing.post_processing_types import (
    Band,
    Decision,
    FlagResult,
    FlagTier,
    Thresholds,
)
from post_processing.regression.decisions import (
    _compute_fail_target,
    compute_run_decision,
    compute_workload_decision,
)

_T = Thresholds(weak=0.10, strong=0.20)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _flag(tier: FlagTier, band: Band) -> FlagResult:
    """Return a FlagResult carrying only the tier and band; decisions ignore the measured values."""
    return FlagResult(
        tier=tier,
        band=band,
        baseline_value=1.0,
        candidate_value=1.0,
        pct_change=0.0,
        reason=None,
        iodepth=None,
    )


def _h(band: Band) -> FlagResult:
    """Return a high-confidence (H tier) flag in the given band."""
    return _flag(FlagTier.H, band)


def _m(band: Band) -> FlagResult:
    """Return a medium-confidence (M tier) flag in the given band."""
    return _flag(FlagTier.M, band)


# ---------------------------------------------------------------------------
# Workload decision
# ---------------------------------------------------------------------------


class TestWorkloadDecision(unittest.TestCase):
    """Tests for compute_workload_decision."""

    def test_h_strong_gives_fail(self) -> None:
        """Any H STRONG flag produces FAIL decision."""
        flags = [_h(Band.STRONG), _m(Band.VALID), _m(Band.VALID)]
        result = compute_workload_decision(flags)
        self.assertEqual(result.decision, Decision.FAIL, "H STRONG must give FAIL")

    def test_h_weak_gives_review(self) -> None:
        """An H WEAK flag produces REVIEW decision."""
        flags = [_h(Band.WEAK), _m(Band.VALID)]
        result = compute_workload_decision(flags)
        self.assertEqual(result.decision, Decision.REVIEW, "H WEAK must give REVIEW")

    def test_m_strong_gives_review(self) -> None:
        """An M STRONG flag produces REVIEW decision."""
        flags = [_h(Band.VALID), _m(Band.STRONG)]
        result = compute_workload_decision(flags)
        self.assertEqual(result.decision, Decision.REVIEW, "M STRONG must give REVIEW")

    def test_m_weak_gives_review(self) -> None:
        """An M WEAK flag produces REVIEW decision."""
        flags = [_h(Band.VALID), _m(Band.WEAK)]
        result = compute_workload_decision(flags)
        self.assertEqual(result.decision, Decision.REVIEW, "M WEAK must give REVIEW")

    def test_all_valid_gives_pass(self) -> None:
        """All VALID flags produce PASS decision."""
        flags = [_h(Band.VALID), _h(Band.VALID), _m(Band.VALID), _m(Band.VALID)]
        result = compute_workload_decision(flags)
        self.assertEqual(result.decision, Decision.PASS, "All VALID must give PASS")

    def test_not_evaluable_gives_review(self) -> None:
        """NOT_EVALUABLE contributes to REVIEW."""
        flags = [_h(Band.VALID), _m(Band.NOT_EVALUABLE)]
        result = compute_workload_decision(flags)
        self.assertEqual(result.decision, Decision.REVIEW, "NOT_EVALUABLE must give REVIEW")

    def test_not_evaluable_does_not_override_fail(self) -> None:
        """H STRONG gives FAIL even when other flags are NOT_EVALUABLE."""
        flags = [_h(Band.STRONG), _m(Band.NOT_EVALUABLE)]
        result = compute_workload_decision(flags)
        self.assertEqual(result.decision, Decision.FAIL, "FAIL takes precedence over NOT_EVALUABLE")

    def test_h_strong_overrides_m_weak(self) -> None:
        """H STRONG takes precedence over M WEAK."""
        flags = [_h(Band.STRONG), _m(Band.WEAK)]
        result = compute_workload_decision(flags)
        self.assertEqual(result.decision, Decision.FAIL)

    def test_workload_decision_stores_flags(self) -> None:
        """WorkloadDecision preserves input flag list."""
        flags = [_h(Band.VALID), _m(Band.VALID)]
        result = compute_workload_decision(flags)
        self.assertEqual(result.flags, flags)

    def test_empty_flags_gives_pass(self) -> None:
        """An empty flag list produces PASS decision."""
        result = compute_workload_decision([])
        self.assertEqual(result.decision, Decision.PASS)


# ---------------------------------------------------------------------------
# fail_target formula: max(1, min(3, ceil(N * 0.5)))
# ---------------------------------------------------------------------------


class TestFailTarget(unittest.TestCase):
    """Tests for _compute_fail_target calculation."""

    def test_fail_target_single_workload_requires_one_fail(self) -> None:
        """N=1: ceil(0.5) is 1, so fail_target is 1."""
        self.assertEqual(_compute_fail_target(1), 1)

    def test_fail_target_two_workloads_requires_one_fail(self) -> None:
        """N=2: ceil(1.0) is 1, so fail_target is 1."""
        self.assertEqual(_compute_fail_target(2), 1)

    def test_fail_target_three_workloads_requires_two_fails(self) -> None:
        """N=3: ceil(1.5) is 2, so fail_target is 2."""
        self.assertEqual(_compute_fail_target(3), 2)

    def test_fail_target_six_workloads_capped_at_three_fails(self) -> None:
        """N=6: ceil(3.0) is 3, so fail_target is 3, sitting exactly on the cap."""
        self.assertEqual(_compute_fail_target(6), 3)

    def test_fail_target_ten_workloads_capped_at_three_fails(self) -> None:
        """N=10: ceil(5.0) is 5, which the cap reduces to a fail_target of 3."""
        self.assertEqual(_compute_fail_target(10), 3)


# ---------------------------------------------------------------------------
# Run decision
# ---------------------------------------------------------------------------


class TestRunDecision(unittest.TestCase):
    """Tests for compute_run_decision."""

    def test_run_decision_fail_count_reaches_fail_target_evaluates_fail(self) -> None:
        """FC=3, N=8 → fail_target=3 → FAIL (FC >= fail_target)."""
        result = compute_run_decision(num_workloads=8, fail_count=3, review_count=0)
        self.assertEqual(result.decision, Decision.FAIL)
        self.assertEqual(result.fail_target, 3)

    def test_run_decision_fail_count_positive_below_target_evaluates_review(self) -> None:
        """FC=1, N=8 → K=3 → FC>0 → REVIEW."""
        result = compute_run_decision(num_workloads=8, fail_count=1, review_count=0)
        self.assertEqual(result.decision, Decision.REVIEW)

    def test_run_decision_single_workload_review_evaluates_pass(self) -> None:
        """RC=1, FC=0 → PASS (single REVIEW does not escalate run decision)."""
        result = compute_run_decision(num_workloads=8, fail_count=0, review_count=1)
        self.assertEqual(result.decision, Decision.PASS)

    def test_run_decision_multiple_workload_reviews_evaluates_review(self) -> None:
        """RC=2, FC=0 → REVIEW (RC > 1)."""
        result = compute_run_decision(num_workloads=8, fail_count=0, review_count=2)
        self.assertEqual(result.decision, Decision.REVIEW)

    def test_run_decision_single_review_in_small_run_evaluates_review(self) -> None:
        """RC=1, FC=0 → REVIEW when N < 3 (one review cannot be dismissed as noise), PASS from N=3."""
        for num_workloads, expected in ((1, Decision.REVIEW), (2, Decision.REVIEW), (3, Decision.PASS)):
            with self.subTest(num_workloads=num_workloads):
                result = compute_run_decision(num_workloads=num_workloads, fail_count=0, review_count=1)
                self.assertEqual(result.decision, expected, f"N={num_workloads}, RC=1 should be {expected.name}")

    def test_run_decision_all_workloads_passing_evaluates_pass(self) -> None:
        """FC=0, RC=0 produces PASS."""
        result = compute_run_decision(num_workloads=6, fail_count=0, review_count=0)
        self.assertEqual(result.decision, Decision.PASS)

    def test_run_decision_zero_workloads_raises_value_error(self) -> None:
        """N=0 must raise ValueError."""
        with self.assertRaises(ValueError, msg="N=0 must raise ValueError"):
            compute_run_decision(num_workloads=0, fail_count=0, review_count=0)

    def test_run_decision_negative_workloads_raises_value_error(self) -> None:
        """Negative num_workloads must raise ValueError."""
        with self.assertRaises(ValueError, msg="N<0 must raise ValueError"):
            compute_run_decision(num_workloads=-1, fail_count=0, review_count=0)

    def test_run_decision_fields_contain_computed_counts(self) -> None:
        """RunDecision fields preserve computed counts and targets."""
        result = compute_run_decision(num_workloads=6, fail_count=1, review_count=0)
        self.assertEqual(result.num_workloads, 6)
        self.assertEqual(result.fail_count, 1)
        self.assertEqual(result.review_count, 0)
        self.assertEqual(result.fail_target, 3)


if __name__ == "__main__":
    unittest.main()
