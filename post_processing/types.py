"""
A file to contain common type definitions for use in the post-processing
"""

from typing import Union

# Log setup types
HandlerType = dict[str, dict[str, str]]

# FIO json file data types
JobsDataType = list[dict[str, Union[str, dict[str, Union[int, float, dict[str, Union[int, float]]]]]]]

# Common formatter data types
IodepthDataType = dict[str, str]
CommonFormatDataType = dict[str, Union[str, IodepthDataType]]

# Common formatter internal data types
InternalBlocksizeDataType = dict[str, CommonFormatDataType]
InternalFormattedOutputType = dict[str, InternalBlocksizeDataType]

# Plotter types
PlotDataType = dict[str, dict[str, str]]
