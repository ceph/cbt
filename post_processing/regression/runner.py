"""
Pipeline runner for regression detection.

Wires loader → flags → decisions → summary into a single callable
that returns an exit code (0-3).
"""

from logging import Logger, getLogger
from pathlib import Path

from post_processing.post_processing_types import (
    Decision,
    FlagResult,
    Thresholds,
    WorkloadDecision,
)
from post_processing.regression.decisions import compute_run_decision, compute_workload_decision
from post_processing.regression.flags import (
    compute_h1_max_iops,
    compute_h2_max_bandwidth,
    compute_h3_low_depth_latency,
    compute_m1_max_cpu,
    compute_m2_max_memory,
    compute_m3_high_depth_latency,
)
from post_processing.regression.loader import (
    ArchiveLoadError,
    WorkloadPair,
    load_archive_pairs,
)
from post_processing.regression.summary import print_summary

log: Logger = getLogger("cbt.regression")

DEFAULT_THRESHOLDS = Thresholds(weak=0.08, strong=0.12)


def _evaluate_pair(pair: WorkloadPair, thresholds: Thresholds) -> WorkloadDecision:
    """Compute flags and workload decision for a single paired workload."""
    flags: list[FlagResult] = [
        compute_h1_max_iops(pair.baseline_data, pair.candidate_data, thresholds),
        compute_h2_max_bandwidth(pair.baseline_data, pair.candidate_data, thresholds),
        compute_h3_low_depth_latency(pair.baseline_data, pair.candidate_data, thresholds),
        compute_m1_max_cpu(pair.baseline_data, pair.candidate_data, thresholds),
        compute_m2_max_memory(pair.baseline_data, pair.candidate_data, thresholds),
        compute_m3_high_depth_latency(pair.baseline_data, pair.candidate_data, thresholds),
    ]
    wd = compute_workload_decision(flags)
    return WorkloadDecision(stem=pair.stem, decision=wd.decision, flags=wd.flags)


def _evaluate_all_pairs(
    pairs: list[WorkloadPair],
    thresholds: Thresholds,
    initial_review_count: int,
) -> tuple[list[WorkloadDecision], int, int]:
    """Evaluate all pairs and return decisions and fail/review counts."""
    workload_decisions: list[WorkloadDecision] = []
    fail_count = 0
    review_count = initial_review_count

    for pair in pairs:
        wd = _evaluate_pair(pair, thresholds)
        workload_decisions.append(wd)
        if wd.decision == Decision.FAIL:
            fail_count += 1
        elif wd.decision == Decision.REVIEW:
            review_count += 1

    return workload_decisions, fail_count, review_count


def run_comparison(
    baseline_dir: Path,
    candidate_dir: Path,
    thresholds: Thresholds,
    use_color: bool = False,
) -> int:
    """
    Compare two archive directories and print a regression summary.

    Returns:
        0 — PASS
        1 — REVIEW
        2 — FAIL
        3 — input error
    """
    try:
        pairs, missing, extra = load_archive_pairs(baseline_dir, candidate_dir)
    except ArchiveLoadError as exc:
        log.error("Input error: %s", exc)
        return 3

    workload_decisions, fail_count, review_count = _evaluate_all_pairs(
        pairs, thresholds, initial_review_count=len(missing)
    )
    num_workloads = len(pairs) + len(missing)

    try:
        run = compute_run_decision(
            num_workloads=num_workloads,
            fail_count=fail_count,
            review_count=review_count,
        )
    except ValueError as exc:
        log.error("Input error: %s", exc)
        return 3

    for stem in missing:
        workload_decisions.append(WorkloadDecision(stem=f"{stem} (missing)", decision=Decision.REVIEW, flags=[]))

    print_summary(
        workload_decisions,
        run,
        missing_count=len(missing),
        extra_count=len(extra),
        use_color=use_color,
    )

    if run.decision == Decision.FAIL:
        return 2
    if run.decision == Decision.REVIEW:
        return 1
    return 0
