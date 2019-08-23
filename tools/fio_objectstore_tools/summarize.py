#!env python3

import os
import json
from run import get_fio_output, get_base_config

def populate_args(parser):
    parser.add_argument('target', metavar='T', type=str, help='target results directory')
    parser.add_argument('--match', type=str, help='json for matching', default='{}')
    parser.add_argument('--output', type=str, help='output directory')
    parser.add_argument('--generate-graphs', type=bool, help='generate graphs')
    parser.set_default(func=summarize)

def project(name, config, fio_stats, perf_stats):
    def f(op):
        return {
            'iops_min': op['iops_min'],
            'iops_max': op['iops_max'],
            'iops': op['iops'],
            'clat_min_ns': op['clat_ns']['min'],
            'clat_max_ns': op['clat_ns']['max'],
            'clat_mean_ns': op['clat_ns']['mean'],
            'clat_median_ns': op['clat_ns']['percentile']['50.000000'],
            'clat_99.9_ns': op['clat_ns']['percentile']['99.900000'],
            'slat_min_ns': op['slat_ns']['min'],
            'slat_max_ns': op['slat_ns']['max'],
            'slat_mean_ns': op['slat_ns']['mean'],
        }
    fio = dict(((op, f(fio_stats['jobs'][0][op])) for op in ['read', 'write']))

    wanted_perf = [
        'commit_lat',
        'kv_commit_lat',
        'kv_final_lat',
        'kv_flush_lat',
        'kv_sync_lat',
        'state_deferred_aio_wait_lat',
        'state_deferred_cleanup_lat',
        'state_deferred_queued_lat',
        'state_kv_committing_lat'
        ]

    perf = {
        k: v['avgtime'] for k, v in
        filter(lambda x: '_lat' in x[0],
               perf_stats['perfcounter_collection']['bluestore'].items())
        }

    return {
        'fio': fio,
        'config': config,
        'name': name,
        'perf': perf,
        }

def dump_target(name, directory):
    fio_output = {}
    with open(get_fio_output(directory)) as f:
        decoder = json.JSONDecoder()
        fio_output, _ = decoder.raw_decode(f.read())
        #fio_output = json.load(f)
    perf_output = {}
    with open(os.path.join(directory, 'perf_counters.json')) as f:
        perf_output = json.load(f)
    with open(get_base_config(directory)) as f:
        base_config = json.load(f)
    return project(name, base_config, fio_output, perf_output)

def generate_summary(filtered, match):
    def config_to_frozen(config, match):
        ret = dict(filter(lambda x: x[0] not in match, config.items()))
        if 'run' in ret:
            del ret['run']
        return frozenset(sorted(ret.items()))

    def group_by_config(input):
        grouped = {}
        for run in filtered:
            key = config_to_frozen(run['config'], match)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(run)
        return [{'config': dict(list(k)), 'runs': v} for k, v in grouped.items()]

    grouped = group_by_config(filtered)

    def union_top_n(group):
        ret = set()
        for run in group:
            ret = ret.union(
                [k for v, k in sorted(((a, b) for b, a in run['perf'].items()))][::-1][:5]
            )
        return ret

    def project_run(perfs):
        def ret(run):
            return {
                'tp': run['fio']['write']['iops'],
                'lat': run['fio']['write']['clat_mean_ns'] / 1000000000.0,
                'slat': run['fio']['write']['slat_mean_ns'] / 1000000000.0,
                'perf': dict(filter(lambda x: x[0] in perfs, run['perf'].items()))
            }
        return ret

    def sort_by(f, input):
        return [v for (_, _, v) in sorted(map(lambda x: (f(x[0]), x[1], x[0]), zip(input, range(len(input)))))]

    def project_group(group):
        perfs = union_top_n(group['runs'])
        return {
            'config': group['config'],
            'runs': sort_by(
                lambda x: x['tp'],
                list(map(project_run(perfs), group['runs'])))
        }

    return sort_by(
        lambda x: (x['config'].get('bs', 0), x['config'].get('size', 0)),
        list(map(project_group, grouped)))
