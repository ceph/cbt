import copy
import itertools

import settings
from benchmark.radosbench import Radosbench
from benchmark.rbdfio import RbdFio
from benchmark.kvmrbdfio import KvmRbdFio
from benchmark.stdfiobench import StdFioBench
from benchmark.librbdfio import LibrbdFio
from benchmark.nullbench import Nullbench
from benchmark.cosbench import Cosbench
from benchmark.cephtestrados import CephTestRados


def get_all(cluster, iteration):
    for benchmark, config in sorted(settings.benchmarks.iteritems()):
        default = {"benchmark": benchmark,
                   "iteration": iteration}

        for current in all_configs(config):
            current.update(default)
            yield get_object(cluster, benchmark, current)


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


def get_object(cluster, benchmark, bconfig):
    if benchmark == "nullbench":
        return Nullbench(cluster, bconfig)
    if benchmark == "radosbench":
        return Radosbench(cluster, bconfig)
    if benchmark == "rbdfio":
        return RbdFio(cluster, bconfig)
    if benchmark == "kvmrbdfio":
        return KvmRbdFio(cluster, bconfig)
    if benchmark == "stdfiobench":
        return StdFioBench(cluster, bconfig)
    if benchmark == 'librbdfio':
        return LibrbdFio(cluster, bconfig)
    if benchmark == 'cosbench':
        return Cosbench(cluster, bconfig)
    if benchmark == 'cephtestrados':
        return CephTestRados(cluster, bconfig)
