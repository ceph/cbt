import settings
import common
import copy
from benchmark.radosbench import Radosbench
from benchmark.rbdfio import RbdFio 
from benchmark.kvmrbdfio import KvmRbdFio
#from benchmark import *

def getAll(iteration):
    objects = []
    for benchmark, config in sorted(settings.benchmarks.iteritems()):
        objects.extend(get(benchmark, config, iteration))
    return objects

def get(benchmark, config, iteration):
    objects = []
    default = {"benchmark":benchmark, "iteration":iteration}

    permutations = [default]
#    for param, value in sorted(config.iteritems(), reverse=True):
    for param, value in config.iteritems():
        if (isinstance(value, list)):
            localperms = []
#            for lv in sorted(value, reverse=True):
            for lv in value:
                for p in permutations:
                    lp = copy.deepcopy(p)
                    lp[param] = lv
                    localperms.append(lp)
            permutations = localperms;
        else:
            for p in permutations:
                p[param] = value

    for p in permutations:
        objects.append(getObject(benchmark, p))
    return objects

def getObject(benchmark, bconfig):
    if benchmark == "radosbench":
        return Radosbench(bconfig)
    if benchmark == "rbdfio":
        return RbdFio(bconfig)
    if benchmark == "kvmrbdfio":
        return KvmRbdFio(bconfig)
