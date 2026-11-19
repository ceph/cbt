"""
Summary formatting for regression detection output.

Produces readable output for the check_regression tool:
- Grouped by operation in CBT report order (Sequential Read, Write, etc.)
- Workloads sorted numerically by block size (4K, 8K, 16K, etc.)
- Display workload names (e.g. 4K read, 256K randwrite, 16K 70/30 randrw)
- Formatted values with appropriate units (IOPS, MB/s or GB/s, ms)
- Ansi color highlighting for PASS/REVIEW/FAIL
- Detail blocks for non-PASS workloads listing firing flags
- Final summary line with counts, colored run decision, and flag key
"""

from typing import Optional

from post_processing.post_processing_types import (
    Band,
    Decision,
    FlagResult,
    RunDecision,
    WorkloadDecision,
)

_DEFAULT_FLAG_LABELS = ["H1", "H2", "H3", "M1", "M2", "M3"]

_COLOR_GREEN = "\033[92m"
_COLOR_YELLOW = "\033[93m"
_COLOR_RED = "\033[91m"
_COLOR_BOLD = "\033[1m"
_COLOR_RESET = "\033[0m"

# Standard CBT report operation categories and display order
_OP_CATEGORIES: dict[str, tuple[int, str]] = {
    "read": (0, "Sequential Read"),
    "write": (1, "Sequential Write"),
    "readwrite": (2, "Sequential Read/Write"),
    "rw": (2, "Sequential Read/Write"),
    "randread": (3, "Random Read"),
    "randwrite": (4, "Random Write"),
    "randrw": (5, "Random Read/Write"),
}

_FLAG_LEGEND = (
    "\nFlag Key:\n"
    "  H1: Maximum IOPS (drop)            M1: Maximum CPU Usage (%) (rise)\n"
    "  H2: Maximum Bandwidth (drop)       M2: Maximum Memory Usage (%) (rise)\n"
    "  H3: Low-depth Latency (rise)       M3: High-depth Latency (rise)"
)


def _format_blocksize(blocksize_bytes: int) -> str:
    """Format bytes into readable blocksize (e.g. 4096 -> 4K, 1048576 -> 1M)."""
    if blocksize_bytes >= 1048576 and blocksize_bytes % 1048576 == 0:
        return f"{blocksize_bytes // 1048576}M"
    if blocksize_bytes >= 1024 and blocksize_bytes % 1024 == 0:
        return f"{blocksize_bytes // 1024}K"
    return f"{blocksize_bytes}B"


def parse_workload_stem(stem: str) -> tuple[int, int, str, str]:
    """
    Parse a workload stem into (category_index, blocksize_bytes, display_name, section_title).

    Examples:
        '4096_1_randread' -> (3, 4096, '4K randread', 'Random Read')
        '1048576_1_write' -> (1, 1048576, '1M write', 'Sequential Write')
        '16384_1_70_30_randrw' -> (5, 16384, '16K 70/30 randrw', 'Random Read/Write')
    """
    is_missing = stem.endswith(" (missing)")
    clean_stem = stem[:-10] if is_missing else stem
    parts = clean_stem.split("_")

    blocksize_bytes = 0
    if parts and parts[0].isdigit():
        blocksize_bytes = int(parts[0])
        blocksize_str = _format_blocksize(blocksize_bytes)
    else:
        blocksize_str = parts[0] if parts else clean_stem

    if len(parts) >= 5 and parts[2].isdigit() and parts[3].isdigit():
        operation = parts[4]
        display_name = f"{blocksize_str} {parts[2]}/{parts[3]} {operation}"
    elif len(parts) >= 3:
        operation = parts[2]
        display_name = f"{blocksize_str} {operation}"
    else:
        operation = clean_stem
        display_name = clean_stem

    if is_missing:
        display_name = f"{display_name} (missing)"

    category_index, section_title = _OP_CATEGORIES.get(operation, (99, "Other Workloads"))
    return category_index, blocksize_bytes, display_name, section_title


def format_workload_name(stem: str) -> str:
    """Return the display workload name for a stem."""
    _, _, display_name, _ = parse_workload_stem(stem)
    return display_name


def sort_and_group_workload_decisions(
    workload_decisions: list[WorkloadDecision],
) -> list[tuple[str, list[WorkloadDecision]]]:
    """
    Sort workload decisions in CBT report order (by operation category, then blocksize)
    and group them into section title -> list of decisions.
    """
    decorated: list[tuple[tuple[int, int, str], str, WorkloadDecision]] = []
    for wd in workload_decisions:
        category_index, blocksize_bytes, _, section_title = parse_workload_stem(wd.stem)
        sort_key = (category_index, blocksize_bytes, wd.stem)
        decorated.append((sort_key, section_title, wd))

    decorated.sort(key=lambda item: item[0])

    sections: list[tuple[str, list[WorkloadDecision]]] = []
    current_title: Optional[str] = None
    current_list: list[WorkloadDecision] = []

    for _, section_title, wd in decorated:
        if section_title != current_title:
            if current_title is not None:
                sections.append((current_title, current_list))
            current_title = section_title
            current_list = [wd]
        else:
            current_list.append(wd)

    if current_title is not None and current_list:
        sections.append((current_title, current_list))

    return sections


def _format_bandwidth(value: float) -> str:
    """Format bandwidth bytes/sec into readable unit string."""
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f} GB/s"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f} MB/s"
    if value >= 1_000:
        return f"{value / 1_000:.2f} KB/s"
    return f"{value:,.0f} B/s"


def _format_latency(value: float) -> str:
    """Format latency nanoseconds into readable unit string."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f} ms"
    if value >= 1_000:
        return f"{value / 1_000:.2f} us"
    return f"{value:,.0f} ns"


def _format_metric_value(label: str, value: Optional[float]) -> str:
    """Format raw metric values with commas or units according to flag type."""
    if value is None:
        return "n/a"
    if label == "H1":
        return f"{value:,.1f}"
    if label == "H2":
        return _format_bandwidth(value)
    if label in ("H3", "M3"):
        return _format_latency(value)
    if label in ("M1", "M2"):
        return f"{value:.2f}%"
    return f"{value:,.2f}"


def color_decision(decision: Decision, use_color: bool = False) -> str:
    """Return formatted decision name with optional bold ANSI color."""
    if not use_color:
        return decision.name
    if decision == Decision.PASS:
        return f"{_COLOR_BOLD}{_COLOR_GREEN}PASS{_COLOR_RESET}"
    if decision == Decision.REVIEW:
        return f"{_COLOR_BOLD}{_COLOR_YELLOW}REVIEW{_COLOR_RESET}"
    if decision == Decision.FAIL:
        return f"{_COLOR_BOLD}{_COLOR_RED}FAIL{_COLOR_RESET}"
    return decision.name


def format_workload_line(stem: str, decision: Decision, use_color: bool = False) -> str:
    """Return a single summary line for a workload with bold display name."""
    display_name = format_workload_name(stem)
    decision_str = color_decision(decision, use_color=use_color)
    if use_color:
        return f"  {_COLOR_BOLD}{display_name:<40}{_COLOR_RESET} {decision_str}"
    return f"  {display_name:<40} {decision_str}"


def format_flag_detail(flag: FlagResult, label: str) -> str:
    """Return a detail line for one flag."""
    if flag.band == Band.NOT_EVALUABLE:
        return f"    {label:<3} NOT_EVALUABLE  reason: {flag.reason}"

    if flag.band == Band.VALID:
        return ""

    pct = flag.pct_change
    pct_str = f"{pct:+.1%}" if pct is not None else "n/a"
    iodepth_str = f"  iodepth={flag.iodepth}" if flag.iodepth is not None else ""
    baseline_str = _format_metric_value(label, flag.baseline_value)
    candidate_str = _format_metric_value(label, flag.candidate_value)

    return (
        f"    {label:<3} baseline={baseline_str}  "
        f"candidate={candidate_str}  "
        f"change={pct_str}  "
        f"{flag.band.name}{iodepth_str}"
    )


def format_workload_block(
    wd: WorkloadDecision,
    flag_labels: Optional[list[str]] = None,
    use_color: bool = False,
) -> str:
    """
    Return the full output block for one workload: a summary line plus, for
    non-PASS workloads, detail lines for every flag that fired or is NOT_EVALUABLE.
    """
    lines = [format_workload_line(wd.stem, wd.decision, use_color=use_color)]
    labels = flag_labels if flag_labels is not None else _DEFAULT_FLAG_LABELS

    if wd.decision != Decision.PASS:
        for i, flag in enumerate(wd.flags):
            label = labels[i] if i < len(labels) else f"F{i}"
            detail = format_flag_detail(flag, label)
            if detail != "":
                lines.append(detail)

    return "\n".join(lines)


def format_run_summary(
    run: RunDecision,
    missing_count: int,
    extra_count: int = 0,
    use_color: bool = False,
) -> str:
    """Return the final summary line with the entire line colored if enabled."""
    review_str = (
        f"review={run.review_count} (incl. {missing_count} missing)" if missing_count else f"review={run.review_count}"
    )
    parts = [
        f"workloads={run.num_workloads}",
        f"fail_target={run.fail_target}",
        f"failed={run.fail_count}",
        review_str,
    ]
    if extra_count:
        parts.append(f"extra={extra_count}")
    counts = "  ".join(parts)

    if run.review_count == 1 and run.decision == Decision.PASS:
        note = "  (1 workload needs review)"
    elif run.review_count > 1:
        note = f"  ({run.review_count} workloads need review)"
    else:
        note = ""

    line = f"Result: {run.decision.name}  {counts}{note}"
    if use_color:
        if run.decision == Decision.PASS:
            line = f"{_COLOR_BOLD}{_COLOR_GREEN}{line}{_COLOR_RESET}"
        elif run.decision == Decision.REVIEW:
            line = f"{_COLOR_BOLD}{_COLOR_YELLOW}{line}{_COLOR_RESET}"
        elif run.decision == Decision.FAIL:
            line = f"{_COLOR_BOLD}{_COLOR_RED}{line}{_COLOR_RESET}"

    return f"\n{line}"


def print_summary(
    workload_decisions: list[WorkloadDecision],
    run_decision: RunDecision,
    missing_count: int,
    extra_count: int = 0,
    use_color: bool = False,
) -> None:
    """Print the full regression comparison summary to stdout grouped by operation."""
    grouped = sort_and_group_workload_decisions(workload_decisions)
    for section_title, wds in grouped:
        print(f"\n--- {section_title} ---")
        for wd in wds:
            print(format_workload_block(wd, _DEFAULT_FLAG_LABELS, use_color=use_color))

    print(format_run_summary(run_decision, missing_count, extra_count=extra_count, use_color=use_color))
    print(_FLAG_LEGEND)
