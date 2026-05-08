"""
A file to contain common type definitions for use in the post-processing
"""

from enum import Enum, auto
from typing import NamedTuple, Union


class CPUPlotType(Enum):
    """
    The different options for producing a plot of CPU data.
    """

    NOCPU = 0
    OVERALL = auto()
    OSD = auto()
    FIO = auto()
    NODES = auto()


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
    comparison: bool
    plot_resources: bool


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
