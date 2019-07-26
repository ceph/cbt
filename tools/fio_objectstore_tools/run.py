#!env python3

import os
import subprocess
import json
import time
import sys

BLUESTORE_CONF = """
[global]
	debug bluestore = 0/0
	debug bluefs = 0/0
	debug bdev = 0/0
	debug rocksdb = 0/0
	osd pool default pg num = 8
	osd op num shards = {numjobs}

[osd]
	osd objectstore = bluestore

	# use directory= option from fio job file
	osd data = {target_dir}
	osd journal = {target_dir}/journal

        #bluestore rocksdb options = compression=kNoCompression,max_write_buffer_number=4,min_write_buffer_number_to_merge=1,recycle_log_file_num=4,write_buffer_size=268435456,writable_file_max_buffer_size=0,compaction_readahead_size=2097152,stats_dump_period_sec=10

	# log inside fio_dir
	log file = {output_dir}/log
        bluestore_tracing = true
        bluestore_throttle_trace_rate = 200.0
        bluestore_throttle_bytes = 0
        bluestore_throttle_deferred_bytes = 0
        #rocksdb collect extended stats = true
        #rocksdb collect memory stats = true
        #rocksdb collect compaction stats = true
        #rocksdb perf = true
        bluefs_preextend_wal_files = {preextend}
        bluestore_throttle_cost_per_io_hdd = {tcio_hdd}
        bluestore_throttle_cost_per_io_ssd = {tcio_ssd}
        bluestore_fsck_on_mkfs = false
"""

def generate_ceph_conf(conf):
    ret = BLUESTORE_CONF.format(**conf)
    for k in ['block_path', 'block_wal_path', 'block_db_path']:
        v = conf['devices'][conf['target_device']].get(k, None)
        if v:
            ret += "        bluestore_" + k + " = " + v + "\n"
    for k in ['cache_size']:
        v = conf.get(k, None)
        if v:
            ret += "        bluestore_" + k + " = " + v + "\n"
    return ret


BLUESTORE_FIO_BASE = """
[global]
ioengine={lib}/libfio_ceph_objectstore.so

conf={output_dir}/ceph.conf
directory={target_dir}


oi_attr_len=320 # specifies OI(aka '_') attribute length range to couple
                # writes with. Default: 0 (disabled)

snapset_attr_len=35  # specifies snapset attribute length range to couple
                     # writes with. Default: 0 (disabled)
_fastinfo_omap_len=186 # specifies _fastinfo omap entry length range to
                       # couple writes with. Default: 0 (disabled)
pglog_simulation=1   # couples write and omap generation in OSD PG log manner.
                     # Ceph's osd_min_pg_log_entries, osd_pg_log_trim_min,
                     # osd_pg_log_dups_tracked settings control cyclic
                     # omap keys creation/removal.
                     # Following additional FIO pglog_ settings to apply too:

pglog_omap_len=173   # specifies PG log entry length range to couple
                     # writes with. Default: 0 (disabled)

pglog_dup_omap_len=57 # specifies duplicate PG log entry length range
                      # to couple writes with. Default: 0 (disabled)

perf_output_file={output_dir}/perf_counters.json
thread=1
group_reporting=1

nr_files={nr_files}
size={size}
filesize={filesize}
"""

def preprocess_fio_configs(conf):
    c = conf.copy()
    for k in ["deferred_", ""]:
        key = "bluestore_" + k + "throttle"
        c[key] = ','.join([str(int(x * (1<<20))) for x in c[key]])
    c['nr_files'] = str((conf['size'] << 30) // (conf['filesize'] << 20) // conf['numjobs'])
    c['size'] = str((conf['size'] << 30) // conf['numjobs'])
    c['filesize'] = str(conf['filesize']) + 'm'
    return c

BLUESTORE_FIO_POPULATE = """
[write]
bs=1m
iodepth=16
rw=write
time_based=0
numjobs={numjobs}
"""

def generate_fio_populate_conf(conf):
    c = preprocess_fio_configs(conf)
    return (BLUESTORE_FIO_BASE + BLUESTORE_FIO_POPULATE).format(**c)

BLUESTORE_FIO = """
[write]
preallocate_files=0
check_files=1
bluestore_throttle="{bluestore_throttle}"
bluestore_deferred_throttle="{bluestore_deferred_throttle}"
vary_bluestore_throttle_period={vary_bluestore_throttle_period}
rw=randwrite
iodepth={qd}
bs={bs}k
time_based=1
runtime={runtime}s
numjobs={numjobs}
"""

def generate_fio_job_conf(conf):
    c = preprocess_fio_configs(conf)
    return (BLUESTORE_FIO_BASE + BLUESTORE_FIO).format(**c)


DEFAULT = {
    'output_dir': os.path.join('../../output',time.strftime('%Y-%m-%d-%H:%M:%S')),
    'lib': '../../ceph/build/lib',
    'fio_bin': '../../ceph/build/bin/fio',
    'qd': 16,
    'runtime': 10,
    'bs': 4,
    'lttng': True,
    'preextend': 'false',
    'bluestore_throttle': [],
    'bluestore_deferred_throttle': [],
    'vary_bluestore_throttle_period': 0,
    'tcio_hdd': 670000,
    'tcio_ssd': 4000,
    'size': 1,
    'filesize': 4,
    'cache_size': None,
    'tcmalloc': True,
    'numjobs': 4
}

def get_fio_fn(base):
    return os.path.join(base, 'bluestore.fio')

def get_fio_populate_fn(base):
    return os.path.join(base, 'bluestore_populate.fio')

def get_ceph_fn(base):
    return os.path.join(base, 'ceph.conf')

def get_fio_output(base):
    return os.path.join(base, 'fio_output.json')

def get_fio_stdout(base):
    return os.path.join(base, 'fio.stdout')

def write_conf(conf):
    fio_fn = get_fio_fn(conf['output_dir'])
    fio_populate_fn = get_fio_populate_fn(conf['output_dir'])
    ceph_fn = get_ceph_fn(conf['output_dir'])
    for fn, func in [(fio_fn, generate_fio_job_conf),
                     (fio_populate_fn, generate_fio_populate_conf),
                     (ceph_fn, generate_ceph_conf)]:
        with open(fn, 'a') as f:
            f.write(func(conf))
    return fio_fn, fio_populate_fn

def setup_start_lttng(conf):
    if not conf.get('lttng', False):
        return
    tracedir = os.path.join(conf['output_dir'], 'trace')
    subprocess.run(['mkdir', tracedir])
    subprocess.run([
        'lttng', 'create', 'fio-bluestore',
        '--output', tracedir
        ])
    for event in ['state_duration', 'total_duration',
                  'initial_state', 'initial_state_rocksdb',
                  'commit_latency', 'kv_sync_latency', 'kv_submit_latency']:
        subprocess.run([
            'lttng', 'enable-event',
            '--session', 'fio-bluestore',
            '--userspace', 'bluestore:transaction_' + event
        ])
    subprocess.run(['lttng', 'start', 'fio-bluestore'])

def stop_destroy_lttng(conf):
    if not conf.get('lttng', False):
        return
    subprocess.run([
        'lttng', 'stop', 'fio-bluestore'
    ], check=False)
    subprocess.run([
        'lttng', 'destroy', 'fio-bluestore'
    ], check=False)

def run_fio(conf, fn):
    env = {
        'LD_LIBRARY_PATH': conf['lib']
    }
    output_json = get_fio_output(conf['output_dir'])
    cmd = [
        conf['fio_bin'],
        fn,
        '--alloc-size', '1048576',
        '--output', output_json,
        '--output-format', 'json+']

    if conf.get('tcmalloc', False):
        env['LD_PRELOAD'] = '/usr/lib64/libtcmalloc.so.4'
        env['TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES'] = '134217728'
    with open(get_fio_stdout(conf['output_dir']), 'a') as outf:
        subprocess.run(cmd, env=env, stdout=outf, stderr=outf)

def run_conf(conf):
    stop_destroy_lttng(conf)

    subprocess.run(['rm', '-rf', conf['output_dir']], check=False)
    subprocess.run(['mkdir', '-p', conf['output_dir']])
    fio_conf, fio_populate_conf = write_conf(conf)

    setup_start_lttng(conf)
    run_fio(conf, fio_conf)
    stop_destroy_lttng(conf)

def get_all_config_combos(configs):
    if len(configs) == 0:
        yield {}
    else:
        key = list(configs.keys())[0]
        vals = configs[key]
        sub = configs.copy()
        del sub[key]
        for val in vals:
            for subconfig in get_all_config_combos(sub):
                subconfig.update({key: val})
                yield subconfig

def generate_name_full_config(base, run):
    full_config = {}
    full_config.update(DEFAULT)
    full_config.update(base)
    full_config.update(run)
    if ('devices' in full_config.keys() or
        'target_device' in full_config.keys()):
        full_config['target_dir'] = \
            full_config['devices'][full_config['target_device']]['target_dir']
    name = "-".join(
        "{name}({val})".format(name=name, val=val)
        for name, val in run.items())
    return name, run, full_config

def get_base_config(base):
    return os.path.join(base, 'base_config.json')

def get_full_config(base):
    return os.path.join(base, 'full_config.json')

def write_obj(obj, fn):
    with open(fn, 'w') as f:
        json.dump(obj, f, sort_keys=True, indent=2)

def do_run(base, runs):
    ret = {}
    orig_output_dir = None

    for name, base_config, full_config in map(
            lambda x: generate_name_full_config(base, x),
            get_all_config_combos(runs)):
        if orig_output_dir is None:
            orig_output_dir = full_config['output_dir']
        full_config['output_dir'] = os.path.join(full_config['output_dir'], name)
        print("Running {name}".format(name=name))
        run_conf(full_config)
        write_obj(base_config, get_base_config(full_config['output_dir']))
        write_obj(full_config, get_full_config(full_config['output_dir']))
    return orig_output_dir

def do_initialize(base, runs, initialize):
    devices = set()
    if initialize == 'runs':
        devices = set([full_config['target_device'] for _, _, full_config
                       in [generate_name_full_config(base, x) for x in
                           get_all_config_combos(runs)]])
    elif initialize == 'all':
        devices = set(base['devices'].keys())
    else:
        devices = set(initialize.split(','))

    print("Initializing devices {}".format(devices))
    for name, base_config, full_config in [
            generate_name_full_config(base, { 'target_device': device })
            for device in devices]:
        full_config['output_dir'] = os.path.join(full_config['output_dir'], name)
        print("Initializing {name}".format(name=name))

        stop_destroy_lttng(full_config)

        for d in [full_config['output_dir'], full_config['target_dir']]:
            subprocess.run(['rm', '-rf', d], check=False)
            subprocess.run(['mkdir', '-p', d])

        fio_conf, fio_populate_conf = write_conf(full_config)
        run_fio(full_config, fio_populate_conf)

        write_obj(base_config, get_base_config(full_config['output_dir']))
        write_obj(full_config, get_full_config(full_config['output_dir']))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--run', action='store_true',
        help='execute runs in conf')
    group.add_argument(
        '--initialize', type=str,
        help='comma seperated list of devices or runs or all to be initialized')
    parser.add_argument('conf', metavar='C', type=str, nargs=1,
                        help='path to config file')
    args = parser.parse_args()

    if args.run or args.initialize:
        conf = {}
        with open(args.conf[0]) as f:
            conf = json.load(f)
            base = DEFAULT
            base.update(conf.get('base', {}))
            if args.run:
                do_run(base, conf['runs'])
            elif args.initialize:
                do_initialize(base, conf['runs'], args.initialize)
