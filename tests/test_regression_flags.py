"""
Unit tests for post_processing/regression/flags.py.

Tests cover:
- Each flag (H1, H2, H3, M1, M2, M3): STRONG/WEAK/VALID/boundaries/NOT_EVALUABLE
- One-directional: improvements never fire
- Zero-baseline rules (direction-dependent)
- NOT_EVALUABLE: missing field, non-numeric value
- Iodepth group helpers: numeric ordering, group sizes for n=10 and n=7,
  different start points, worst-depth selection
- Fewer than 3 common iodepths → H3/M3 NOT_EVALUABLE
"""

import unittest

from post_processing.post_processing_types import Band, CommonFormatDataType, FlagTier, Thresholds
from post_processing.regression.flags import (
    compute_h1_max_iops,
    compute_h2_max_bandwidth,
    compute_h3_low_depth_latency,
    compute_m1_max_cpu,
    compute_m2_max_memory,
    compute_m3_high_depth_latency,
)

_T = Thresholds(weak=0.08, strong=0.12)

# ---------------------------------------------------------------------------
# Minimal data builders
# ---------------------------------------------------------------------------


def _headline(**overrides: str) -> CommonFormatDataType:
    """Return a dict with headline fields only (no iodepth entries)."""
    base: CommonFormatDataType = {
        "maximum_iops": "1000",
        "maximum_bandwidth": "500000",
        "maximum_cpu_usage": "0.50",
        "maximum_memory_usage": "1024",
        "benchmark": "librbdfio",
    }
    base.update(overrides)
    return base


def _iodepths(latency_by_depth: dict[int, int]) -> CommonFormatDataType:
    """
    Return a dict with iodepth entries only, from a {iodepth: latency_ns} sweep.

    H3 and M3 split the common depths into a low and a high group of max(1, len // 3)
    depths each, so a 6-depth sweep groups as low=[1, 2] and high=[16, 32].
    """
    data: CommonFormatDataType = {}
    for depth, latency in latency_by_depth.items():
        data[str(depth)] = {
            "bandwidth_bytes": "1000000",
            "blocksize": "4096",
            "iops": "250",
            "latency": str(latency),
            "number_of_jobs": "1",
            "runtime_seconds": "60",
        }
    return data


# ---------------------------------------------------------------------------
# H1: maximum_iops — fires when drops
# ---------------------------------------------------------------------------


class TestH1MaxIops(unittest.TestCase):
    """Tests for H1 maximum_iops flag."""

    def test_maximum_iops_strong_drop_fires_strong(self) -> None:
        """A 25% drop in maximum IOPS evaluates to STRONG (> 12%)."""
        result = compute_h1_max_iops(_headline(maximum_iops="1000"), _headline(maximum_iops="750"), _T)
        self.assertEqual(result.band, Band.STRONG, "25% drop should be STRONG")
        self.assertEqual(result.tier, FlagTier.H)

    def test_maximum_iops_weak_drop_fires_weak(self) -> None:
        """A 10% drop in maximum IOPS evaluates to WEAK (8% - 12%)."""
        result = compute_h1_max_iops(_headline(maximum_iops="1000"), _headline(maximum_iops="900"), _T)
        self.assertEqual(result.band, Band.WEAK, "10% drop should be WEAK")

    def test_maximum_iops_small_drop_below_weak_threshold_is_valid(self) -> None:
        """A 4.5% drop in maximum IOPS evaluates to VALID (< 8%)."""
        result = compute_h1_max_iops(_headline(maximum_iops="1000"), _headline(maximum_iops="955"), _T)
        self.assertEqual(result.band, Band.VALID, "4.5% drop should be VALID")

    def test_maximum_iops_increase_is_valid(self) -> None:
        """An increase in maximum IOPS evaluates to VALID."""
        result = compute_h1_max_iops(_headline(maximum_iops="1000"), _headline(maximum_iops="1300"), _T)
        self.assertEqual(result.band, Band.VALID, "Improvement must not fire H1")

    def test_maximum_iops_exact_weak_boundary_fires_weak(self) -> None:
        """An exact 8% drop in maximum IOPS evaluates to WEAK."""
        result = compute_h1_max_iops(_headline(maximum_iops="1000"), _headline(maximum_iops="920"), _T)
        self.assertEqual(result.band, Band.WEAK, "Exactly -8% should be WEAK")

    def test_maximum_iops_exact_strong_boundary_fires_strong(self) -> None:
        """An exact 12% drop in maximum IOPS evaluates to STRONG."""
        result = compute_h1_max_iops(_headline(maximum_iops="1000"), _headline(maximum_iops="880"), _T)
        self.assertEqual(result.band, Band.STRONG, "Exactly -12% should be STRONG")

    def test_maximum_iops_zero_baseline_and_zero_candidate_is_valid(self) -> None:
        """Zero baseline and zero candidate evaluates to VALID."""
        result = compute_h1_max_iops(_headline(maximum_iops="0"), _headline(maximum_iops="0"), _T)
        self.assertEqual(result.band, Band.VALID, "Both zero → VALID (no change)")

    def test_maximum_iops_zero_baseline_with_positive_candidate_is_valid(self) -> None:
        """Zero baseline with positive candidate is an improvement (VALID)."""
        result = compute_h1_max_iops(_headline(maximum_iops="0"), _headline(maximum_iops="1000"), _T)
        self.assertEqual(result.band, Band.VALID, "Zero baseline, positive candidate → improvement → VALID for H1")

    def test_maximum_iops_drop_to_zero_fires_strong(self) -> None:
        """Positive baseline dropped to zero evaluates to STRONG."""
        result = compute_h1_max_iops(_headline(maximum_iops="1000"), _headline(maximum_iops="0"), _T)
        self.assertEqual(result.band, Band.STRONG, "IOPS dropped to zero → STRONG")

    def test_maximum_iops_missing_field_evaluates_not_evaluable(self) -> None:
        """Missing field in baseline evaluates to NOT_EVALUABLE."""
        result = compute_h1_max_iops({}, _headline(), _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE, "Missing baseline field → NOT_EVALUABLE")
        self.assertIsNotNone(result.reason)

    def test_maximum_iops_non_numeric_field_evaluates_not_evaluable(self) -> None:
        """Non-numeric field value evaluates to NOT_EVALUABLE."""
        result = compute_h1_max_iops(_headline(maximum_iops="n/a"), _headline(), _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE, "Non-numeric field → NOT_EVALUABLE")


# ---------------------------------------------------------------------------
# H2: maximum_bandwidth — fires when drops
# ---------------------------------------------------------------------------


class TestH2MaxBandwidth(unittest.TestCase):
    """Tests for H2 maximum_bandwidth flag."""

    def test_maximum_bandwidth_strong_drop_fires_strong(self) -> None:
        """A 30% drop in bandwidth evaluates to STRONG."""
        baseline, candidate = _headline(maximum_bandwidth="500000"), _headline(maximum_bandwidth="350000")
        result = compute_h2_max_bandwidth(baseline, candidate, _T)
        self.assertEqual(result.band, Band.STRONG)
        self.assertEqual(result.tier, FlagTier.H)

    def test_maximum_bandwidth_weak_drop_fires_weak(self) -> None:
        """A 10% drop in bandwidth evaluates to WEAK."""
        baseline, candidate = _headline(maximum_bandwidth="500000"), _headline(maximum_bandwidth="450000")
        result = compute_h2_max_bandwidth(baseline, candidate, _T)
        self.assertEqual(result.band, Band.WEAK)

    def test_maximum_bandwidth_increase_is_valid(self) -> None:
        """An increase in bandwidth evaluates to VALID."""
        baseline, candidate = _headline(maximum_bandwidth="500000"), _headline(maximum_bandwidth="700000")
        result = compute_h2_max_bandwidth(baseline, candidate, _T)
        self.assertEqual(result.band, Band.VALID)

    def test_maximum_bandwidth_zero_baseline_with_positive_candidate_is_valid(self) -> None:
        """Zero baseline with positive candidate is an improvement (VALID)."""
        baseline, candidate = _headline(maximum_bandwidth="0"), _headline(maximum_bandwidth="500000")
        result = compute_h2_max_bandwidth(baseline, candidate, _T)
        self.assertEqual(result.band, Band.VALID, "Drop-fires flag: zero→positive is improvement → VALID")

    def test_maximum_bandwidth_drop_to_zero_fires_strong(self) -> None:
        """Positive baseline dropped to zero evaluates to STRONG."""
        baseline, candidate = _headline(maximum_bandwidth="500000"), _headline(maximum_bandwidth="0")
        result = compute_h2_max_bandwidth(baseline, candidate, _T)
        self.assertEqual(result.band, Band.STRONG, "Bandwidth dropped to zero → STRONG")

    def test_maximum_bandwidth_missing_field_evaluates_not_evaluable(self) -> None:
        """Missing field evaluates to NOT_EVALUABLE."""
        result = compute_h2_max_bandwidth({}, _headline(), _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE)


# ---------------------------------------------------------------------------
# M1: maximum_cpu_usage — fires when rises
# ---------------------------------------------------------------------------


class TestM1MaxCpu(unittest.TestCase):
    """Tests for M1 maximum_cpu_usage flag."""

    def test_maximum_cpu_usage_strong_rise_fires_strong(self) -> None:
        """A 30% rise in CPU usage evaluates to STRONG."""
        result = compute_m1_max_cpu(_headline(maximum_cpu_usage="0.50"), _headline(maximum_cpu_usage="0.65"), _T)
        self.assertEqual(result.band, Band.STRONG, "30% CPU rise should be STRONG")
        self.assertEqual(result.tier, FlagTier.M)

    def test_maximum_cpu_usage_weak_rise_fires_weak(self) -> None:
        """A 10% rise in CPU usage evaluates to WEAK."""
        result = compute_m1_max_cpu(_headline(maximum_cpu_usage="0.50"), _headline(maximum_cpu_usage="0.55"), _T)
        self.assertEqual(result.band, Band.WEAK, "10% rise should be WEAK")

    def test_maximum_cpu_usage_drop_is_valid(self) -> None:
        """A decrease in CPU usage evaluates to VALID."""
        result = compute_m1_max_cpu(_headline(maximum_cpu_usage="0.50"), _headline(maximum_cpu_usage="0.30"), _T)
        self.assertEqual(result.band, Band.VALID, "CPU drop is an improvement → VALID")

    def test_maximum_cpu_usage_rise_from_zero_is_not_evaluable(self) -> None:
        """Zero baseline with positive candidate evaluates to NOT_EVALUABLE for M1 (unmeasured baseline)."""
        result = compute_m1_max_cpu(_headline(maximum_cpu_usage="0"), _headline(maximum_cpu_usage="0.50"), _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE, "Zero baseline, positive candidate → NOT_EVALUABLE for M1")

    def test_maximum_cpu_usage_zero_baseline_and_zero_candidate_is_valid(self) -> None:
        """Zero baseline with zero candidate evaluates to VALID."""
        result = compute_m1_max_cpu(_headline(maximum_cpu_usage="0"), _headline(maximum_cpu_usage="0"), _T)
        self.assertEqual(result.band, Band.VALID, "Both zero → VALID")

    def test_maximum_cpu_usage_missing_field_evaluates_not_evaluable(self) -> None:
        """Missing field evaluates to NOT_EVALUABLE."""
        result = compute_m1_max_cpu({}, _headline(), _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE)

    def test_maximum_cpu_usage_nan_evaluates_not_evaluable(self) -> None:
        """NaN or Inf evaluates to NOT_EVALUABLE."""
        result = compute_m1_max_cpu(_headline(maximum_cpu_usage="NaN"), _headline(maximum_cpu_usage="0.50"), _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE)


# ---------------------------------------------------------------------------
# M2: maximum_memory_usage — fires when rises
# ---------------------------------------------------------------------------


class TestM2MaxMemory(unittest.TestCase):
    """Tests for M2 maximum_memory_usage flag."""

    def test_maximum_memory_usage_strong_rise_fires_strong(self) -> None:
        """A 27% rise in memory usage evaluates to STRONG (> 12%)."""
        baseline, candidate = _headline(maximum_memory_usage="1024"), _headline(maximum_memory_usage="1300")
        result = compute_m2_max_memory(baseline, candidate, _T)
        self.assertEqual(result.band, Band.STRONG)
        self.assertEqual(result.tier, FlagTier.M)

    def test_maximum_memory_usage_weak_rise_fires_weak(self) -> None:
        """A 10% rise in memory usage evaluates to WEAK (8% - 12%)."""
        baseline, candidate = _headline(maximum_memory_usage="1000"), _headline(maximum_memory_usage="1100")
        result = compute_m2_max_memory(baseline, candidate, _T)
        self.assertEqual(result.band, Band.WEAK, "10% memory rise should be WEAK")

    def test_maximum_memory_usage_small_rise_below_weak_threshold_is_valid(self) -> None:
        """A 5% rise in memory usage evaluates to VALID (< 8%)."""
        baseline, candidate = _headline(maximum_memory_usage="1000"), _headline(maximum_memory_usage="1050")
        result = compute_m2_max_memory(baseline, candidate, _T)
        self.assertEqual(result.band, Band.VALID, "5% memory rise should be VALID")

    def test_maximum_memory_usage_drop_is_valid(self) -> None:
        """A decrease in memory usage evaluates to VALID."""
        baseline, candidate = _headline(maximum_memory_usage="1024"), _headline(maximum_memory_usage="900")
        result = compute_m2_max_memory(baseline, candidate, _T)
        self.assertEqual(result.band, Band.VALID)

    def test_maximum_memory_usage_rise_from_zero_is_not_evaluable(self) -> None:
        """Zero baseline with positive candidate evaluates to NOT_EVALUABLE for M2 (unmeasured baseline)."""
        baseline, candidate = _headline(maximum_memory_usage="0"), _headline(maximum_memory_usage="1024")
        result = compute_m2_max_memory(baseline, candidate, _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE, "Zero baseline, positive candidate → NOT_EVALUABLE for M2")

    def test_maximum_memory_usage_missing_field_evaluates_not_evaluable(self) -> None:
        """Missing field evaluates to NOT_EVALUABLE."""
        result = compute_m2_max_memory({}, _headline(), _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE)

    def test_maximum_memory_usage_nan_evaluates_not_evaluable(self) -> None:
        """NaN or Inf evaluates to NOT_EVALUABLE."""
        baseline, candidate = _headline(maximum_memory_usage="inf"), _headline(maximum_memory_usage="1024")
        result = compute_m2_max_memory(baseline, candidate, _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE)


# ---------------------------------------------------------------------------
# Iodepth group tests (numeric ordering, group sizes, worst-depth)
# ---------------------------------------------------------------------------


class TestIodepthGroups(unittest.TestCase):
    """Tests for numeric grouping and ordering across iodepths."""

    def test_numeric_ordering_not_lexicographic(self) -> None:
        """
        '128' sorts after '16' numerically but before it lexicographically.
        With 8 depths the high group is ['64','128'], so a rise at '128' fires M3.
        """
        base = _iodepths({1: 1000, 2: 2000, 4: 3000, 8: 4000, 16: 5000, 32: 6000, 64: 7000, 128: 8000})
        cand = _iodepths({1: 1000, 2: 2000, 4: 3000, 8: 4000, 16: 5000, 32: 6000, 64: 7000, 128: 12000})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertNotEqual(result.band, Band.NOT_EVALUABLE)
        self.assertEqual(result.iodepth, "128", "Worst high-depth must be '128' (numeric sort)")

    def test_group_size_10_depths(self) -> None:
        """10 depths → group=3; low=[1,2,4], high=[96,128,256]."""
        base = _iodepths(
            {1: 1000, 2: 1100, 4: 1200, 8: 2000, 16: 3000, 32: 4000, 48: 5000, 96: 6000, 128: 7000, 256: 8000}
        )
        cand = _iodepths(
            {1: 1000, 2: 1100, 4: 2400, 8: 2000, 16: 3000, 32: 4000, 48: 5000, 96: 6000, 128: 7000, 256: 8000}
        )
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG, "100% rise in low group → STRONG")
        self.assertEqual(result.iodepth, "4", "Worst low-depth should be '4'")

    def test_group_size_7_depths(self) -> None:
        """7 depths → group=2; low=[1,2], high=[32,64]."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 1000, 16: 1000, 32: 1000, 64: 1000})
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 1000, 16: 1000, 32: 1000, 64: 1250})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG)
        self.assertEqual(result.iodepth, "64")

    def test_sweep_starting_at_4(self) -> None:
        """Sweep starting at 4 (not 1): 6 depths → group=2; low=[4,8], high=[64,128]."""
        base = _iodepths({4: 1000, 8: 1200, 16: 1500, 32: 2000, 64: 3000, 128: 4000})
        cand = _iodepths({4: 1400, 8: 1200, 16: 1500, 32: 2000, 64: 3000, 128: 4000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG)
        self.assertEqual(result.iodepth, "4")

    def test_worst_depth_selected_from_group(self) -> None:
        """Low group [1,2,4]; biggest rise at depth '2' → reported as iodepth '2'."""
        base = _iodepths(
            {1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000, 48: 5000, 96: 6000, 128: 7000, 256: 8000}
        )
        cand = _iodepths(
            {1: 1050, 2: 1500, 4: 1100, 8: 2000, 16: 3000, 32: 4000, 48: 5000, 96: 6000, 128: 7000, 256: 8000}
        )
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG)
        self.assertEqual(result.iodepth, "2")

    def test_same_band_tie_break_picks_worst_percentage(self) -> None:
        """Both depth 1 (+40% STRONG) and depth 2 (+300% STRONG) are STRONG; depth 2 is selected."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1400, 2: 4000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG)
        self.assertEqual(result.iodepth, "2", "Depth with largest worsening (+300%) must win tie-break")

    def test_two_common_depths_not_evaluable(self) -> None:
        """2 common depths evaluates to NOT_EVALUABLE."""
        base = _iodepths({1: 1000, 2: 2000})
        cand = _iodepths({1: 1200, 2: 2400})
        self.assertEqual(compute_h3_low_depth_latency(base, cand, _T).band, Band.NOT_EVALUABLE)
        self.assertEqual(compute_m3_high_depth_latency(base, cand, _T).band, Band.NOT_EVALUABLE)

    def test_zero_common_depths_not_evaluable(self) -> None:
        """Zero common depths evaluates to NOT_EVALUABLE."""
        base = _iodepths({1: 1000, 2: 2000})
        cand = _iodepths({4: 1000, 8: 2000})
        self.assertEqual(compute_h3_low_depth_latency(base, cand, _T).band, Band.NOT_EVALUABLE)

    def test_one_common_depth_not_evaluable(self) -> None:
        """One common depth evaluates to NOT_EVALUABLE."""
        base = _iodepths({1: 1000, 2: 2000, 4: 3000})
        cand = _iodepths({4: 6000, 8: 2000, 16: 3000})  # only "4" in common
        self.assertEqual(compute_h3_low_depth_latency(base, cand, _T).band, Band.NOT_EVALUABLE)


# ---------------------------------------------------------------------------
# H3: low-depth latency — fires on rise
# ---------------------------------------------------------------------------


class TestH3LowDepthLatency(unittest.TestCase):
    """Tests for H3 low-depth latency flag."""

    def test_strong_rise_in_low_group(self) -> None:
        """A 40% latency rise in low-depth group evaluates to STRONG."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1400, 2: 1400, 4: 1400, 8: 2000, 16: 3000, 32: 4000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG, "40% rise in low group should be STRONG (>= 35%)")
        self.assertEqual(result.tier, FlagTier.H)
        self.assertIsNotNone(result.iodepth)

    def test_weak_rise_in_low_group(self) -> None:
        """A 30% latency rise in the low-depth group is WEAK (>= 25% and < 35%)."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1300, 2: 1300, 4: 1300, 8: 2000, 16: 3000, 32: 4000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.WEAK, "30% rise in low group should be WEAK (>= 25% and < 35%)")

    def test_small_rise_in_low_group_is_valid(self) -> None:
        """A 20% latency rise in the low-depth group stays VALID (< 25%)."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1200, 2: 1200, 4: 1200, 8: 2000, 16: 3000, 32: 4000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.VALID, "20% rise in low group should be VALID (< 25%)")

    def test_exact_weak_boundary(self) -> None:
        """A rise of exactly 25% sits on the WEAK boundary and is inclusive."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1250, 2: 1250, 4: 1250, 8: 2000, 16: 3000, 32: 4000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.WEAK, "Exactly +25% rise should be WEAK")

    def test_exact_strong_boundary(self) -> None:
        """A rise of exactly 35% sits on the STRONG boundary and is inclusive."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1350, 2: 1350, 4: 1350, 8: 2000, 16: 3000, 32: 4000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG, "Exactly +35% rise should be STRONG")

    def test_general_thresholds_do_not_override_h3_fixed_thresholds(self) -> None:
        """
        H3 keeps its overriding 25% weak / 35% strong thresholds even when the
        general thresholds are stricter (e.g. weak=0.05, strong=0.10).
        """
        strict_t = Thresholds(weak=0.05, strong=0.10)
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand_20 = _iodepths({1: 1200, 2: 1200, 4: 1200, 8: 2000, 16: 3000, 32: 4000})
        result = compute_h3_low_depth_latency(base, cand_20, strict_t)
        self.assertEqual(result.band, Band.VALID, "+20% rise must be VALID for H3 even with strict general thresholds")

    def test_high_depth_rise_does_not_fire_h3(self) -> None:
        """A latency rise to the high-depth group must not fire H3."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 8000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.VALID, "High-depth rise must not fire H3")

    def test_improvement_in_low_group_is_valid(self) -> None:
        """A latency improvement in the low-depth group is VALID."""
        base = _iodepths({1: 5000, 2: 5000, 4: 5000, 8: 8000, 16: 9000, 32: 10000})
        cand = _iodepths({1: 3000, 2: 3000, 4: 3000, 8: 8000, 16: 9000, 32: 10000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.VALID)

    def test_zero_baseline_latency_across_whole_low_group_is_not_evaluable(self) -> None:
        """Zero baseline latency is unmeasured data, not a regression."""
        base = _iodepths({1: 0, 2: 0, 4: 0, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        result = compute_h3_low_depth_latency(base, cand, _T)
        self.assertEqual(
            result.band,
            Band.NOT_EVALUABLE,
            "Unmeasured baseline latency must route to review, not fail the candidate",
        )


# ---------------------------------------------------------------------------
# M3: high-depth latency — fires on rise
# ---------------------------------------------------------------------------


class TestM3HighDepthLatency(unittest.TestCase):
    """Tests for M3 high-depth latency flag."""

    def test_m3_latency_strong_rise_in_high_depth_group_fires_strong(self) -> None:
        """A 25% latency rise in high-depth group evaluates to STRONG (> 12%)."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 5000})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG)
        self.assertEqual(result.tier, FlagTier.M)
        self.assertIsNotNone(result.iodepth)

    def test_m3_latency_weak_rise_in_high_depth_group_fires_weak(self) -> None:
        """A 10% latency rise in high-depth group evaluates to WEAK (8% - 12%)."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4400})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.WEAK, "10% high-depth rise should be WEAK")

    def test_m3_latency_small_rise_below_weak_threshold_is_valid(self) -> None:
        """A 5% latency rise in high-depth group evaluates to VALID (< 8%)."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4200})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.VALID, "5% high-depth rise should be VALID")

    def test_m3_latency_exact_weak_boundary_fires_weak(self) -> None:
        """An exact +8% latency rise in high-depth group evaluates to WEAK."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4320})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.WEAK, "Exactly +8% should be WEAK")

    def test_m3_latency_exact_strong_boundary_fires_strong(self) -> None:
        """An exact +12% latency rise in high-depth group evaluates to STRONG."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4480})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG, "Exactly +12% should be STRONG")

    def test_m3_latency_low_depth_rise_does_not_fire_m3(self) -> None:
        """A latency rise at low depth does not trigger M3."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 5000, 2: 5000, 4: 5000, 8: 2000, 16: 3000, 32: 4000})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.VALID, "Low-depth rise must not fire M3")

    def test_m3_latency_drop_in_high_depth_group_is_valid(self) -> None:
        """A latency improvement in high-depth group evaluates to VALID."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 4000})
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 2000})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.VALID)

    def test_m3_unmeasured_depth_is_not_reported_as_valid(self) -> None:
        """An unchecked depth must not produce a clean VALID verdict."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 0})  # no baseline at 32
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 5000})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.NOT_EVALUABLE, "Depth 32 has no baseline, so the group is unverified")

    def test_m3_proven_regression_wins_over_unmeasured_depth(self) -> None:
        """A measured regression is still reported when a neighbouring depth is unmeasured."""
        base = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 3000, 32: 0})  # no baseline at 32
        cand = _iodepths({1: 1000, 2: 1000, 4: 1000, 8: 2000, 16: 6000, 32: 5000})
        result = compute_m3_high_depth_latency(base, cand, _T)
        self.assertEqual(result.band, Band.STRONG, "Depth 16 doubled; an unmeasured depth 32 must not mask it")


if __name__ == "__main__":
    unittest.main()
