import settings
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

def get_all(archive, cluster, iteration):
    for benchmark, config in sorted(settings.benchmarks.items()):
        default = {"benchmark": benchmark,
                   "iteration": iteration}
        for current in all_configs(config):
            current.update(default)
            yield get_object(archive, cluster, benchmark, current)

def get_object(archive, cluster, benchmark, bconfig):
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
