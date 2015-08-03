import copy
import itertools


import settings
from benchmark.radosbench import Radosbench
from benchmark.rbdfio import RbdFio
from benchmark.kvmrbdfio import KvmRbdFio
from benchmark.librbdfio import LibrbdFio
from benchmark.nullbench import Nullbench
from benchmark.cosbench import Cosbench
from benchmark.cephtestrados import CephTestRados


def getAll(cluster, iteration):
    objects = []
    for benchmark, config in sorted(settings.benchmarks.iteritems()):
        objects.extend(get(cluster, benchmark, config, iteration))
    return objects


def generate_all_combinations(config):
    list_param_names = []
    list_values = []
    default = {}

    for param, value in config.iteritems():
        if isinstance(value, list):
            list_param_names.append(param)
            list_values.append(value)
        else:
            default[param] = value

    for permutation in itertools.product(list_values):
        combination = copy.deepcopy(default)
        combination.update(**zip(list_param_names, permutation))
        yield combination


def get(cluster, benchmark, config, iteration):
    for combination in generate_all_combinations(config):
        combination.update(benchmark=benchmark,
                           iteration=iteration)
        yield getObject(cluster, benchmark, combination)


# koder: benchmark name should be a benchmark class field

def getObject(cluster, benchmark, bconfig):
    if benchmark == "nullbench":
        return Nullbench(cluster, bconfig)
    if benchmark == "radosbench":
        return Radosbench(cluster, bconfig)
    if benchmark == "rbdfio":
        return RbdFio(cluster, bconfig)
    if benchmark == "kvmrbdfio":
        return KvmRbdFio(cluster, bconfig)
    if benchmark == 'librbdfio':
        return LibrbdFio(cluster, bconfig)
    if benchmark == 'cosbench':
        return Cosbench(cluster, bconfig)
    if benchmark == 'cephtestrados':
        return CephTestRados(cluster, bconfig)
