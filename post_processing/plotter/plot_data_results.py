"""
Data structures for storing plot data results.

Contains dataclasses for:
- DataPointResult: Result from processing a single data point
- PlotDataResult: Structured result from plot data extraction
"""

from dataclasses import dataclass


@dataclass
class DataPointResult:
    """Result from processing a single data point."""

    x_value: float
    x_label: str
    error_bar: float
    resource_available: bool


@dataclass
class PlotDataResult:
    """Structured result from plot data extraction."""

    x_data: list[float]
    error_bars: list[float]
    cap_size: int
    plot_resource_usage: bool
    x_label: str


# Made with Bob
