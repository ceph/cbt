"""
Type definitions for the workloads classes
"""

from typing import Union

WorkloadType = dict[str, Union[str, list[str]]]
WorkloadYamlType = dict[str, WorkloadType]
BenchmarkConfigurationType = dict[str, WorkloadYamlType]
