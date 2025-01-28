"""
A file to contain common type definitions for use in the post-processing
"""

from typing import Union

# FIO json file data types
JOBS_DATA_TYPE = list[dict[str, Union[str, dict[str, Union[int, float, dict[str, Union[int, float]]]]]]]

# Common formatter data types
IODEPTH_DETAILS_TYPE = dict[str, str]
COMMON_FORMAT_FILE_DATA_TYPE = dict[str, Union[str, IODEPTH_DETAILS_TYPE]]

# Common formatter internal data types
INTERNAL_BLOCKSIZE_DATA_TYPE = dict[str, COMMON_FORMAT_FILE_DATA_TYPE]
INTERNAL_FORMATTED_OUTPUT_TYPE = dict[str, INTERNAL_BLOCKSIZE_DATA_TYPE]

# Plotter types
PLOT_DATA_TYPE = dict[str, dict[str, str]]
