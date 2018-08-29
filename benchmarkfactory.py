import copy
import itertools

import settings
from benchmark.radosbench import Radosbench
from benchmark.rbdfio import RbdFio
from benchmark.rawfio import RawFio
from benchmark.kvmrbdfio import KvmRbdFio
from benchmark.librbdfio import LibrbdFio
from benchmark.nullbench import Nullbench
from benchmark.cosbench import Cosbench
from benchmark.cephtestrados import CephTestRados
from benchmark.getput import Getput

def get_all(cluster, iteration):
    for benchmark, config in sorted(settings.benchmarks.iteritems()):
        default = {"benchmark": benchmark,
                   "iteration": iteration}
        for current in all_configs(config):
            current.update(default)
            yield get_object_with_config(benchmark, cluster, current)

def all_configs(config):
    """
    return all parameter combinations for config
    config: dict - list of params
    iterate over all top-level lists in config
    """
    cycle_over_lists = []
    cycle_over_names = []
    default = {}

    for param, value in config.iteritems():
        if isinstance(value, list):
            cycle_over_lists.append(value)
            cycle_over_names.append(param)
        else:
            default[param] = value

    for permutation in itertools.product(*cycle_over_lists):
        current = copy.deepcopy(default)
        current.update(zip(cycle_over_names, permutation))
        yield current

def get_object(benchmark):
    if benchmark == "nullbench":
        return Nullbench()
    if benchmark == "radosbench":
        return Radosbench()
    if benchmark == "rbdfio":
        return RbdFio()
    if benchmark == "kvmrbdfio":
        return KvmRbdFio()
    if benchmark == "rawfio":
        return RawFio()
    if benchmark == 'librbdfio':
        return LibrbdFio()
    if benchmark == 'cosbench':
        return Cosbench()
    if benchmark == 'cephtestrados':
        return CephTestRados()
    if benchmark == 'getput':
        return Getput()

def get_object_with_config(benchmark, cluster, bconfig):
    obj = get_object(benchmark)
    obj.load_config(cluster, bconfig)
    return obj
