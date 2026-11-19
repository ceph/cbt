"""
Type definitions for post-processing data structures.

This module defines TypedDict types for various data formats used in the
post-processing pipeline, including the common intermediate format and
time-series format.
"""

from enum import Enum, auto
from typing import NamedTuple, Optional, TypedDict, Union

# Log setup types
HandlerType = dict[str, dict[str, str]]

# FIO json file data types
JobsDataType = list[dict[str, Union[str, dict[str, Union[int, float, dict[str, Union[int, float]]]]]]]

# Common formatter data types
IodepthDataType = dict[str, str]
CommonFormatDataType = dict[str, Union[str, IodepthDataType]]

# Common formatter internal data types
InternalBlocksizeDataType = dict[str, dict[str, Union[str, IodepthDataType]]]
InternalNumJobsDataType = dict[str, InternalBlocksizeDataType]
InternalFormattedOutputType = dict[str, InternalNumJobsDataType]

# Plotter types
PlotDataType = dict[str, dict[str, str]]


class CPUPlotType(Enum):
    """
    The different options for producing a plot of CPU data.
    """

    NOCPU = 0
    OVERALL = auto()
    OSD = auto()
    FIO = auto()
    NODES = auto()


class ReportType(Enum):
    """
    The different types of reports that can be generated.
    """

    SIMPLE = auto()
    COMPARISON = auto()
    TIMESERIES = auto()


class ReportOptions(NamedTuple):
    """
    This class is used to store the options required to create a report.
    """

    archives: list[str]
    output_directory: str
    results_file_root: str
    create_pdf: bool
    force_refresh: bool
    no_error_bars: bool
    report_type: ReportType
    plot_resources: bool


# New types for time-series format
class TimeSeriesDataPoint(TypedDict):
    """
    A single data point in a time-series.

    Represents aggregated metrics at a specific timestamp, typically
    aggregated across multiple volumes using time-window binning.
    """

    timestamp_sec: float
    iops: float
    bandwidth_bytes: float
    mean_latency_ms: float
    max_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    num_samples: int


class TimeSeriesMetadata(TypedDict):
    """
    Metadata about a time-series test run.

    Contains information about the test duration, sampling configuration,
    and number of volumes tested.
    """

    start_time_epoch: float
    end_time_epoch: float
    duration_seconds: float
    num_volumes: int
    sampling_interval_ms: int
    log_avg_msec: int


class TimeSeriesFormatType(TypedDict):
    """
    Complete time-series intermediate format.

    This format is used to store time-indexed performance data from
    benchmark runs. It is designed to be benchmark-agnostic, allowing
    different benchmarks (FIO, elbencho, etc.) to produce data in this
    format for consistent plotting.

    Includes pre-calculated maximum values for efficient report generation,
    following the same pattern as CommonFormatDataType.
    """

    benchmark: str
    operation: str
    blocksize: str
    numjobs: str
    iodepth: str  # total_iodepth if exists, otherwise iodepth
    metadata: TimeSeriesMetadata
    timeseries: list[TimeSeriesDataPoint]
    # Pre-calculated maximum values (same as CommonFormatDataType)
    maximum_iops: str
    maximum_bandwidth: str
    latency_at_max_iops: str
    latency_at_max_bandwidth: str
    timestamp_at_max_iops: str
    timestamp_at_max_bandwidth: str
    maximum_latency: str
    timestamp_at_max_latency: str
    maximum_cpu_usage: str
    maximum_memory_usage: str


# ---------------------------------------------------------------------------
# Regression detection types
# ---------------------------------------------------------------------------


class Band(Enum):
    """
    Result band for a single regression flag.

    VALID        — evaluated; worsening is below the weak threshold (no problem).
    WEAK         — evaluated; worsening is ≥ weak but < strong threshold.
    STRONG       — evaluated; worsening is ≥ strong threshold (or zero-crossing).
    NOT_EVALUABLE — could not be computed (missing field, non-numeric value);
                   contributes to REVIEW in the workload verdict.
    """

    VALID = auto()
    WEAK = auto()
    STRONG = auto()
    NOT_EVALUABLE = auto()


class FlagTier(Enum):
    """Tier of a flag: H (high-priority) or M (medium-priority)."""

    H = auto()
    M = auto()


class FlagResult(NamedTuple):
    """
    Result of evaluating a single regression flag.

    Fields:
        tier          — H or M.
        band          — VALID / WEAK / STRONG / NOT_EVALUABLE.
        baseline_value — raw baseline float (None when NOT_EVALUABLE due to bad data).
        candidate_value — raw candidate float (None when NOT_EVALUABLE due to bad data).
        pct_change    — unrounded fractional change (None when NOT_EVALUABLE).
        reason        — human-readable explanation, required for NOT_EVALUABLE.
        iodepth       — iodepth key reported for H3 / M3 (None for headline flags).
    """

    tier: FlagTier
    band: Band
    baseline_value: Optional[float]
    candidate_value: Optional[float]
    pct_change: Optional[float]
    reason: Optional[str]
    iodepth: Optional[str]


class Thresholds(NamedTuple):
    """
    Tunable thresholds for regression detection.

    weak      — fractional worsening at or above which a flag is WEAK (default 0.08).
    strong    — fractional worsening at or above which a flag is STRONG (default 0.12).
    h3_weak   — overriding weak threshold for H3 low-depth latency (default 0.25).
    h3_strong — overriding strong threshold for H3 low-depth latency (default 0.35).
    """

    weak: float = 0.08
    strong: float = 0.12
    h3_weak: float = 0.25
    h3_strong: float = 0.35


class Decision(Enum):
    """
    Overall decision for a workload or a run comparison.

    PASS   — no significant regression detected.
    REVIEW — possible regression; needs human review.
    FAIL   — regression detected above the strong threshold.
    """

    PASS = auto()
    REVIEW = auto()
    FAIL = auto()


class WorkloadDecision(NamedTuple):
    """Decision for a single paired workload."""

    stem: str
    decision: Decision
    flags: list[FlagResult]


class RunDecision(NamedTuple):
    """
    Decision for the overall run comparison.

    num_workloads — N: total baseline workloads (paired + missing).
    fail_target   — the number of FAILs needed to fail the run.
    fail_count    — FC: number of FAIL workloads.
    review_count  — RC: number of REVIEW workloads.
    """

    decision: Decision
    num_workloads: int
    fail_target: int
    fail_count: int
    review_count: int
