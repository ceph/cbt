"""
Type definitions for post-processing data structures.

This module defines TypedDict types for various data formats used in the
post-processing pipeline, including the common intermediate format and
time-series format.
"""

from enum import Enum, auto
from typing import NamedTuple, TypedDict, Union

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
