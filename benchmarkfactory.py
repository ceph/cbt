"""
This module implements a 'factory' like behavior for all the supported benchmarks including:\n
- Rados bench
- RbdFio
- RawFio
- KvmRbdFio
- LibrbdFio
- Nullbench
- Cosbench
- CephTestRados
- Getput

The module is implemented to return 'generators' for all the benchmarks listed in the\n
YAML configuration. Generators make the whole benchmarking process MUCH faster.
"""
import copy
import itertools

# the YAML config parsing we setup
import settings

# get all the benchmark profiles needed
from benchmark.radosbench import Radosbench
from benchmark.fio import Fio
from benchmark.hsbench import Hsbench
from benchmark.rbdfio import RbdFio
from benchmark.rawfio import RawFio
from benchmark.kvmrbdfio import KvmRbdFio
from benchmark.librbdfio import LibrbdFio
from benchmark.nullbench import Nullbench
from benchmark.cosbench import Cosbench
from benchmark.cephtestrados import CephTestRados
from benchmark.getput import Getput

# return a 'generator' for each benchmarking object, to handle given benchmarks
def get_all(archive, cluster, iteration):
    """Returns a 'generator' for getting the 'benchmark object' depending\n
    on the type of benchmark listed in the YAML file."""

    # sort out all the benchmarks listed in the YAML
    for benchmark, config in sorted(settings.benchmarks.items()):
        # each benchmark needs to be performed iteration number of times
        default = {"benchmark": benchmark,
                   "iteration": iteration}
        # for each configuration of benchmark in the YAML, create a new generator for the caller
        for current in all_configs(config):
            current.update(default)
            # give a generator to the caller
            yield get_object(archive, cluster, benchmark, current)

# return permutations of the config parameters as 'generators' for faster processing
def all_configs(config):
    """
    return all parameter combinations for config
    config: dict - list of params
    iterate over all top-level lists in config
    """
    cycle_over_lists = []
    cycle_over_names = []
    default = {}
    # get the config parameters from the YAML 'object' to iterate on
    for param, value in config.items():
        # acceptable applies to benchmark as a whole, no need to it to
        # the set for permutation
        if param == 'acceptable':
            default[param] = value
        elif isinstance(value, list):
            cycle_over_lists.append(value)
            cycle_over_names.append(param)
        else:
            default[param] = value

    # iterate over all the possible iterations, and make 'generators' for the calling function
    for permutation in itertools.product(*cycle_over_lists):
        current = copy.deepcopy(default)
        current.update(zip(cycle_over_names, permutation))
        yield current

# return the benchmark object given the input string
def get_object(archive, cluster, benchmark, bconfig):
    """Return a benchmark object, formed by the cluster object and benchmark config from YAML"""
    benchmarks = {
        'nullbench': Nullbench,
        'radosbench': Radosbench,
        'fio': Fio,
        'hsbench': Hsbench,
        'rbdfio': RbdFio,
        'kvmrbdfio': KvmRbdFio,
        'rawfio': RawFio,
        'librbdfio': LibrbdFio,
        'cosbench': Cosbench,
        'cephtestrados': CephTestRados,
        'getput': Getput}
    try:
        return benchmarks[benchmark](archive, cluster, bconfig)
    except KeyError:
        return None
