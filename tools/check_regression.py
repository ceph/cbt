#!/usr/bin/env -S python3 -B
"""
Check whether a candidate CBT archive has regressed against a baseline.

Usage:
    check_regression.py --baseline <dir> --candidate <dir>

Exit codes:
    0  PASS   — no significant regression
    1  REVIEW — possible regression; needs human review
    2  FAIL   — regression detected
    3  ERROR  — input problem (missing directory, no intermediate files, etc.)
"""

import sys
from argparse import ArgumentParser
from logging import Logger, getLogger
from pathlib import Path

from logging_configuration import setup_loggers
from post_processing.post_processing_types import Thresholds
from post_processing.regression.runner import run_comparison


def main() -> int:
    """Entry point for check_regression.py."""
    setup_loggers()
    log: Logger = getLogger("cbt.regression")

    parser = ArgumentParser(
        description="Compare two CBT archive directories and report whether the candidate has regressed."
    )
    parser.add_argument("--baseline", type=str, required=True, help="Path to the baseline archive directory")
    parser.add_argument("--candidate", type=str, required=True, help="Path to the candidate archive directory")
    parser.add_argument(
        "--weak",
        type=float,
        default=0.08,
        help="Weak regression threshold (fractional drop/rise, default: 0.08 / 8%%)",
    )
    parser.add_argument(
        "--strong",
        type=float,
        default=0.12,
        help="Strong regression threshold (fractional drop/rise, default: 0.12 / 12%%)",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color codes in output",
    )
    args = parser.parse_args()

    if not 0.0 < args.weak < args.strong:
        log.error(
            "Invalid thresholds: --weak (%s) must be > 0 and < --strong (%s)",
            args.weak,
            args.strong,
        )
        return 3

    # latency, CPU, memory can legitimately exceed 100%, so threshold above 1.0 is allowed
    if args.strong >= 1.0:
        log.warning(
            "Thresholds are fractions, not percentages: --strong %s means %.0f%%",
            args.strong,
            args.strong * 100,
        )

    log.info("Baseline:  %s", args.baseline)
    log.info("Candidate: %s", args.candidate)

    thresholds = Thresholds(weak=args.weak, strong=args.strong)
    use_color = not args.no_color and sys.stdout.isatty()

    return run_comparison(
        Path(args.baseline),
        Path(args.candidate),
        thresholds,
        use_color=use_color,
    )


if __name__ == "__main__":
    sys.exit(main())
