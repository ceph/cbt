"""
A file to contain common type definitions for use in the post-processing
"""

from enum import Enum, auto


class BenchmarkType(Enum):
    """
    The different benchamrk types for a run
    """

    UNKNOWN = 0
    RBDFIO = auto()
    KVMRBDFIO = auto()
    RAWFIO = auto()
    RADOSBENCH = auto()
    COSBENCH = auto()
    HSBENCH = auto()
