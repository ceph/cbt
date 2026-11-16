"""
Regression flag computation.

Each flag is a pure function taking two parsed intermediate JSON dicts
(baseline and candidate) plus a Thresholds instance, returning a FlagResult.

Flag directions:
  H1, H2       — fire when value DROPS   (IOPS, bandwidth)
  H3, M1, M2, M3 — fire when value RISES  (latency, CPU, memory)

Zero-baseline rules (a zero baseline means the value was never measured):
  - drop-fires (H1, H2), baseline=0                      → VALID  (candidate is at least as good)
  - drop-fires (H1, H2), baseline>0, candidate=0         → STRONG (dropped to zero)
  - rise-fires (H3, M1, M2, M3), baseline=0, candidate>0 → NOT_EVALUABLE
  - rise-fires (H3, M1, M2, M3), baseline=0, candidate=0 → VALID  (both zero)

Iodepth groups:
  - H3 and M3 require at least 3 common iodepths to form bottom-third and top-third groups.
    If fewer than 3 common iodepths exist, NOT_EVALUABLE is returned.
  - A group reports VALID only if every depth in it was evaluable; a proven WEAK/STRONG
    at any depth still wins, so regressions are never masked by an unmeasured neighbour.

NOT_EVALUABLE is returned when a field is missing, non-numeric, or non-finite (NaN/Inf).
"""

import math
from collections.abc import Mapping
from typing import Optional

from post_processing.common import float_percent_difference
from post_processing.post_processing_types import (
    Band,
    CommonFormatDataType,
    FlagResult,
    FlagTier,
    Thresholds,
)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Ordering for band severity: used to pick the worst result across an iodepth group.
# NOT_EVALUABLE is ranked lowest (0) so that if any iodepths in a group have evaluable
# data, the group decision is driven by those valid results rather than marking the
# entire group un-evaluable due to a single missing or corrupt entry. If all depths in
# the group are non-evaluable, NOT_EVALUABLE is returned.
_BAND_ORDER = {
    Band.NOT_EVALUABLE: 0,
    Band.VALID: 1,
    Band.WEAK: 2,
    Band.STRONG: 3,
}


def _not_evaluable(tier: FlagTier, reason: str) -> FlagResult:
    return FlagResult(
        tier=tier,
        band=Band.NOT_EVALUABLE,
        baseline_value=None,
        candidate_value=None,
        pct_change=None,
        reason=reason,
        iodepth=None,
    )


def _read_float(data: Mapping[str, object], key: str) -> tuple[Optional[float], Optional[str]]:
    """
    Extract *key* from *data* as a float.

    Returns (value, None) on success, or (None, reason_string) on failure.
    """
    raw = data.get(key)
    if raw is None:
        return None, f"field '{key}' is missing"
    try:
        val = float(str(raw))
        if not math.isfinite(val):
            return None, f"field '{key}' is not finite (got {raw!r})"
        return val, None
    except (ValueError, TypeError):
        return None, f"field '{key}' is not numeric (got {raw!r})"


def _band_for_worsening(pct: float, thresholds: Thresholds) -> Band:
    """Map an absolute fractional worsening to a Band. pct must be >= 0."""
    if pct >= thresholds.strong:
        return Band.STRONG
    if pct >= thresholds.weak:
        return Band.WEAK
    return Band.VALID


def _evaluate_drop_fires(
    tier: FlagTier,
    baseline: float,
    candidate: float,
    thresholds: Thresholds,
) -> FlagResult:
    """
    Evaluate a flag that fires when the value DROPS (H1, H2).

    Zero-baseline: candidate >= baseline is VALID (improvement or tie).
    Nonzero-baseline → zero-candidate: STRONG.
    Otherwise: normal percentage comparison.
    """
    if baseline == 0.0:
        band = Band.VALID  # any candidate is at least as good (improvement or tie)
        pct: Optional[float] = None
    elif candidate == 0.0:
        band = Band.STRONG  # dropped to zero — worst regression
        pct = None
    else:
        frac = float_percent_difference(baseline, candidate)
        pct = frac
        worsening = -frac  # negative frac = drop = worsening for this flag
        band = _band_for_worsening(worsening, thresholds) if worsening > 0 else Band.VALID

    return FlagResult(
        tier=tier,
        band=band,
        baseline_value=baseline,
        candidate_value=candidate,
        pct_change=pct,
        reason=None,
        iodepth=None,
    )


def _evaluate_rise_fires(
    tier: FlagTier,
    baseline: float,
    candidate: float,
    thresholds: Thresholds,
    iodepth: Optional[str] = None,
) -> FlagResult:
    """
    Evaluate a flag that fires when the value RISES (H3, M1, M2, M3).

    Zero-baseline + positive candidate: NOT_EVALUABLE (baseline never measured).
    Zero-baseline + zero candidate: VALID.
    Otherwise: normal percentage comparison.
    """
    if baseline == 0.0 and candidate > 0.0:
        return FlagResult(
            tier=tier,
            band=Band.NOT_EVALUABLE,
            baseline_value=baseline,
            candidate_value=candidate,
            pct_change=None,
            reason="baseline is 0.0 (unsupported/unmeasured in baseline run)",
            iodepth=iodepth,
        )

    if baseline == 0.0:
        band = Band.VALID
        pct = None
    else:
        frac = float_percent_difference(baseline, candidate)
        pct = frac
        worsening = frac  # positive frac = rise = worsening for this flag
        band = _band_for_worsening(worsening, thresholds) if worsening > 0 else Band.VALID

    return FlagResult(
        tier=tier,
        band=band,
        baseline_value=baseline,
        candidate_value=candidate,
        pct_change=pct,
        reason=None,
        iodepth=iodepth,
    )


def _common_iodepths(
    baseline: CommonFormatDataType,
    candidate: CommonFormatDataType,
) -> list[str]:
    """
    Return iodepth keys common to both dicts, sorted numerically.
    Iodepth entries are the dict-valued keys; headline fields are plain strings.
    """
    b_keys = {k for k, v in baseline.items() if isinstance(v, dict) and k.isdecimal()}
    c_keys = {k for k, v in candidate.items() if isinstance(v, dict) and k.isdecimal()}
    common = b_keys & c_keys
    return sorted(common, key=int)


def _eval_depth_latency(
    tier: FlagTier,
    depth: str,
    b_entry: Mapping[str, object],
    c_entry: Mapping[str, object],
    thresholds: Thresholds,
) -> FlagResult:
    b_lat, b_err = _read_float(b_entry, "latency")
    c_lat, c_err = _read_float(c_entry, "latency")
    if b_lat is None or c_lat is None:
        return FlagResult(
            tier=tier,
            band=Band.NOT_EVALUABLE,
            baseline_value=None,
            candidate_value=None,
            pct_change=None,
            reason=b_err or c_err or "latency field missing",
            iodepth=depth,
        )
    return _evaluate_rise_fires(tier, b_lat, c_lat, thresholds, iodepth=depth)


def _latency_flag_for_group(
    tier: FlagTier,
    group: list[str],
    baseline: CommonFormatDataType,
    candidate: CommonFormatDataType,
    thresholds: Thresholds,
) -> FlagResult:
    """
    Evaluate a latency flag over a specific iodepth group.

    Picks the depth with the worst (highest) worsening and returns that flag.
    If all depths in the group have NOT_EVALUABLE latency, returns NOT_EVALUABLE.
    """
    worst_result: Optional[FlagResult] = None
    unevaluable_depths: list[str] = []

    for depth in group:
        b_entry = baseline.get(depth)
        c_entry = candidate.get(depth)
        if not isinstance(b_entry, dict) or not isinstance(c_entry, dict):
            continue

        res = _eval_depth_latency(tier, depth, b_entry, c_entry, thresholds)
        if res.band == Band.NOT_EVALUABLE:
            unevaluable_depths.append(depth)

        if worst_result is None:
            worst_result = res
        elif _BAND_ORDER[res.band] > _BAND_ORDER[worst_result.band]:
            worst_result = res
        elif _BAND_ORDER[res.band] == _BAND_ORDER[worst_result.band] and res.band in (Band.WEAK, Band.STRONG):
            w_pct = worst_result.pct_change
            if w_pct is not None and (res.pct_change is None or res.pct_change > w_pct):
                worst_result = res

    if worst_result is None:
        return _not_evaluable(tier, "no evaluable iodepth entries in group")

    # Only claim VALID if every depth was actually checked.
    if worst_result.band == Band.VALID and unevaluable_depths:
        return _not_evaluable(tier, f"iodepths not evaluable: {', '.join(unevaluable_depths)}")

    return worst_result


# ---------------------------------------------------------------------------
# Public flag functions
# ---------------------------------------------------------------------------


def compute_h1_max_iops(
    baseline: CommonFormatDataType,
    candidate: CommonFormatDataType,
    thresholds: Thresholds,
) -> FlagResult:
    """H1: maximum_iops — fires when drops."""
    b_val, b_err = _read_float(baseline, "maximum_iops")
    c_val, c_err = _read_float(candidate, "maximum_iops")
    if b_val is None or c_val is None:
        return _not_evaluable(FlagTier.H, b_err or c_err or "unknown error")
    return _evaluate_drop_fires(FlagTier.H, b_val, c_val, thresholds)


def compute_h2_max_bandwidth(
    baseline: CommonFormatDataType,
    candidate: CommonFormatDataType,
    thresholds: Thresholds,
) -> FlagResult:
    """H2: maximum_bandwidth — fires when drops."""
    b_val, b_err = _read_float(baseline, "maximum_bandwidth")
    c_val, c_err = _read_float(candidate, "maximum_bandwidth")
    if b_val is None or c_val is None:
        return _not_evaluable(FlagTier.H, b_err or c_err or "unknown error")
    return _evaluate_drop_fires(FlagTier.H, b_val, c_val, thresholds)


def compute_h3_low_depth_latency(
    baseline: CommonFormatDataType,
    candidate: CommonFormatDataType,
    thresholds: Thresholds,
) -> FlagResult:
    """H3: latency over the bottom third of common iodepths — fires when rises."""
    common = _common_iodepths(baseline, candidate)
    if len(common) < 3:
        return _not_evaluable(FlagTier.H, f"fewer than 3 common iodepths ({len(common)} found)")
    group_size = max(1, len(common) // 3)
    low_group = common[:group_size]
    h3_thresholds = Thresholds(weak=thresholds.h3_weak, strong=thresholds.h3_strong)
    return _latency_flag_for_group(FlagTier.H, low_group, baseline, candidate, h3_thresholds)


def compute_m1_max_cpu(
    baseline: CommonFormatDataType,
    candidate: CommonFormatDataType,
    thresholds: Thresholds,
) -> FlagResult:
    """M1: maximum_cpu_usage — fires when rises."""
    b_val, b_err = _read_float(baseline, "maximum_cpu_usage")
    c_val, c_err = _read_float(candidate, "maximum_cpu_usage")
    if b_val is None or c_val is None:
        return _not_evaluable(FlagTier.M, b_err or c_err or "unknown error")
    return _evaluate_rise_fires(FlagTier.M, b_val, c_val, thresholds)


def compute_m2_max_memory(
    baseline: CommonFormatDataType,
    candidate: CommonFormatDataType,
    thresholds: Thresholds,
) -> FlagResult:
    """M2: maximum_memory_usage — fires when rises."""
    b_val, b_err = _read_float(baseline, "maximum_memory_usage")
    c_val, c_err = _read_float(candidate, "maximum_memory_usage")
    if b_val is None or c_val is None:
        return _not_evaluable(FlagTier.M, b_err or c_err or "unknown error")
    return _evaluate_rise_fires(FlagTier.M, b_val, c_val, thresholds)


def compute_m3_high_depth_latency(
    baseline: CommonFormatDataType,
    candidate: CommonFormatDataType,
    thresholds: Thresholds,
) -> FlagResult:
    """M3: latency over the top third of common iodepths — fires when rises."""
    common = _common_iodepths(baseline, candidate)
    if len(common) < 3:
        return _not_evaluable(FlagTier.M, f"fewer than 3 common iodepths ({len(common)} found)")
    group_size = max(1, len(common) // 3)
    high_group = common[-group_size:]
    return _latency_flag_for_group(FlagTier.M, high_group, baseline, candidate, thresholds)
