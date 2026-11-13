"""
Type definitions for the workloads classes
"""


from typing import Optional, Union

WorkloadType = dict[str, Optional[Union[str, list[str]]]]
WorkloadYamlType = dict[str, WorkloadType]
BenchmarkConfigurationType = dict[str, WorkloadYamlType]
