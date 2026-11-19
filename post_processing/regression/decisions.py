"""
Decision functions for regression detection.

Turns flag results into a workload decision, and workload decisions into a
run decision.

Workload decision rules:
  FAIL   — any H flag is STRONG
  REVIEW — any H flag is WEAK, any M flag is WEAK/STRONG, or any flag is NOT_EVALUABLE
  PASS   — otherwise

Run decision rules (fail_target = max(1, min(3, ceil(num_workloads * 0.5)))):
  FAIL   — fail_count >= fail_target
  REVIEW — fail_count > 0 or review_count > 1
  PASS   — otherwise
"""

import math

from post_processing.post_processing_types import (
    Band,
    Decision,
    FlagResult,
    FlagTier,
    RunDecision,
    WorkloadDecision,
)


def _compute_fail_target(num_workloads: int) -> int:
    """Return fail_target = max(1, min(3, ceil(num_workloads * 0.5)))."""
    return max(1, min(3, math.ceil(num_workloads * 0.5)))


def compute_workload_decision(flags: list[FlagResult]) -> WorkloadDecision:
    """
    Derive a workload decision from a list of FlagResult objects.

    The returned WorkloadDecision has an empty stem; callers populate stem
    when associating the decision with a workload.
    """
    has_fail = any(f.tier == FlagTier.H and f.band == Band.STRONG for f in flags)
    has_review = any(
        (f.tier == FlagTier.H and f.band == Band.WEAK)
        or (f.tier == FlagTier.M and f.band in (Band.WEAK, Band.STRONG))
        or f.band == Band.NOT_EVALUABLE
        for f in flags
    )

    if has_fail:
        decision = Decision.FAIL
    elif has_review:
        decision = Decision.REVIEW
    else:
        decision = Decision.PASS

    return WorkloadDecision(stem="", decision=decision, flags=flags)


def compute_run_decision(num_workloads: int, fail_count: int, review_count: int) -> RunDecision:
    """
    Derive a run decision from workload counts.

    Raises ValueError if num_workloads is less than or equal to zero.
    """
    if num_workloads <= 0:
        raise ValueError("num_workloads must be greater than zero")

    fail_target = _compute_fail_target(num_workloads)

    if fail_count >= fail_target:
        decision = Decision.FAIL
    elif fail_count > 0 or review_count > 1:
        decision = Decision.REVIEW
    else:
        decision = Decision.PASS

    return RunDecision(
        decision=decision,
        num_workloads=num_workloads,
        fail_target=fail_target,
        fail_count=fail_count,
        review_count=review_count,
    )
