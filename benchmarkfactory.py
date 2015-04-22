import settings
import common
import copy
from benchmark.radosbench import Radosbench
from benchmark.rbdfio import RbdFio 
from benchmark.kvmrbdfio import KvmRbdFio
from benchmark.librbdfio import LibrbdFio
from benchmark.nullbench import Nullbench
from benchmark.cosbench import Cosbench

def getAll(cluster, iteration):
    objects = []
    for benchmark, config in sorted(settings.benchmarks.iteritems()):
        objects.extend(get(cluster, benchmark, config, iteration))
    return objects

def get(cluster, benchmark, config, iteration):
    objects = []
    default = {"benchmark":benchmark, "iteration":iteration}

    permutations = [default]
    for param, value in config.iteritems():
        if (isinstance(value, list)):
            localperms = []
            for lv in value:
                for p in permutations:
                    lp = copy.deepcopy(p)
                    lp[param] = lv
                    localperms.append(lp)
            permutations = localperms
        else:
            for p in permutations:
                p[param] = value

    for p in permutations:
        objects.append(getObject(cluster, benchmark, p))
    return objects

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
