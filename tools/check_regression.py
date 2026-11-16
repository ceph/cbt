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

import math
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

    defaults = Thresholds()
    parser = ArgumentParser(
        description="Compare two CBT archive directories and report whether the candidate has regressed."
    )
    parser.add_argument("--baseline", type=str, required=True, help="Path to the baseline archive directory")
    parser.add_argument("--candidate", type=str, required=True, help="Path to the candidate archive directory")
    parser.add_argument(
        "--weak",
        type=float,
        default=defaults.weak,
        help="Weak regression threshold (fractional drop/rise, default: %(default)s)",
    )
    parser.add_argument(
        "--strong",
        type=float,
        default=defaults.strong,
        help="Strong regression threshold (fractional drop/rise, default: %(default)s)",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color codes in output",
    )
    try:
        args = parser.parse_args()
    except SystemExit as exc:
        # argparse exits 2 on a usage error, which this tool documents as FAIL. Report it as an input error (3).
        # --help exits 0 and is passed through.
        return 0 if exc.code == 0 else 3

    if not 0.0 < args.weak < args.strong < math.inf:
        log.error(
            "Invalid thresholds: --weak (%s) must be > 0 and < --strong (%s), and finite",
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
