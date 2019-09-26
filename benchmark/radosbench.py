import subprocess
import common
import settings
import monitoring
import os
import time
import threading
import logging
import re
import json

from cluster.ceph import Ceph
from .benchmark import Benchmark, Result
from .lis import Lispy, Env

logger = logging.getLogger("cbt")


class Radosbench(Benchmark):

    def __init__(self, archive_dir, cluster, config):
        super(Radosbench, self).__init__(archive_dir, cluster, config)

        self.tmp_conf = self.cluster.tmp_conf
        self.time =  str(config.get('time', '300'))
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.concurrent_ops = config.get('concurrent_ops', 16)
        self.pool_per_proc = config.get('pool_per_proc', False)  # default behavior used to be True
        self.write_only = config.get('write_only', False)
        self.write_time = config.get('write_time', self.time)
        self.read_only = config.get('read_only', False)
        self.read_time = config.get('read_time', self.time)
        self.op_size = config.get('op_size', 4194304)
        self.object_set_id = config.get('object_set_id', '')
        self.run_dir = os.path.join(self.run_dir,
                                    'osd_ra-{:0>8}'.format(self.osd_ra),
                                    'op_size-{:0>8}'.format(self.op_size),
                                    'concurrent_ops-{:0>8}'.format(self.concurrent_ops))
        self.out_dir = self.archive_dir
        self.pool_profile = config.get('pool_profile', 'default')
        self.cmd_path = config.get('cmd_path', self.cluster.rados_cmd)
        self.pool = config.get('target_pool', 'rados-bench-cbt')
        self.readmode = config.get('readmode', 'seq')
        self.max_objects = config.get('max_objects', None)
        self.write_omap = config.get('write_omap', False)
        self.prefill_time = config.get('prefill_time', None)
        self.prefill_objects = config.get('prefill_objects', None)

    def exists(self, expect_exists=False):
        if os.path.exists(self.out_dir):
            if not expect_exists:
                logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        else:
            if expect_exists:
                logger.info('test result does not exist in %s.', self.out_dir)
            return False

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self): 
        super(Radosbench, self).initialize()

        logger.info('Pausing for 60s for idle monitoring.')
        with monitoring.monitor("%s/idle_monitoring" % self.run_dir):
            time.sleep(60)

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def get_rados_version(self):
        stdout, _ = common.pdsh(settings.getnodes('head'), '%s -c %s -v' % (self.cmd_path, self.tmp_conf)).communicate()
        m = (re.findall("version (\d+)(?:.\d+)* \([0-9a-f]+\)", stdout) or
             re.findall("version v(\d+)(?:.\d+)* \([0-9a-f]+\)", stdout) or
             (255, 0))
        return int(m[0])

    def run(self):
        super(Radosbench, self).run()

        do_prefill = self.prefill_time or self.prefill_objects
        # sanity tests
        if self.read_only and self.write_only:
            logger.error('Both "read_only" and "write_only" are specified, '
                         'but they are mutually exclusive.')
            return
        elif self.read_only and not do_prefill:
            logger.error('Please prefill the testbench with "prefill_time" and/or '
                         '"prefill_objects" option for a "read_only" test');
            return

        # Remake the pools
        self.mkpools()

        # Run prefill
        if do_prefill:
            self._run(mode='prefill', run_dir='prefill', out_dir='prefill',
                      max_objects=self.prefill_objects,
                      runtime=self.prefill_time or self.time)
        # Run write test
        if not self.read_only:
            self._run(mode='write', run_dir='write', out_dir='write',
                      max_objects=self.max_objects,
                      runtime=self.write_time)
        # Run read test unless write_only
        if not self.write_only:
            self._run(mode=self.readmode, run_dir=self.readmode, out_dir=self.readmode,
                      max_objects=None,
                      runtime=self.read_time)

    def _run(self, mode, run_dir, out_dir, max_objects, runtime):
        # We'll always drop caches for rados bench
        self.dropcaches()

        if self.concurrent_ops:
            concurrent_ops_str = '--concurrent-ios %s' % self.concurrent_ops

        rados_version = self.get_rados_version()

        # Max Objects
        max_objects_str = ''
        if max_objects:
            if rados_version < 10:
                raise ValueError('max_objects not supported by rados_version < 10')
            max_objects_str = '--max-objects %s' % max_objects

        # Operation type 
        op_type = mode
        if mode is 'prefill':
            op_type = 'write'

        if op_type == 'write':
            op_size_str = '-b %s' % self.op_size
        else:
            op_size_str = ''  

        # Write to OMAP
        write_omap_str = ''
        if self.write_omap:
            if rados_version < 10:
                raise ValueError('write_omap not supported by rados_version < 10')
            write_omap_str = '--write-omap'

        run_dir = os.path.join(self.run_dir, run_dir)
        common.make_remote_dir(run_dir)

        # dump the cluster config
        self.cluster.dump_config(run_dir)

        # Run the backfill testing thread if requested (but not for prefill)
        if mode is not 'prefill' and 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(run_dir, recovery_callback)

        # Run rados bench
        with monitoring.monitor(run_dir) as monitor:
            logger.info('Running radosbench %s test.' % mode)
            ps = []
            for i in range(self.concurrent_procs):
                out_file = '%s/output.%s' % (run_dir, i)
                objecter_log = '%s/objecter.%s.log' % (run_dir, i)
                if self.pool_per_proc:
                    # support previous behavior of 1 storage pool per rados process
                    pool_name_cmd = 'rados-bench-`{fqdn_cmd}`-{i}'
                    pool_name = pool_name_cmd.format(fqdn_cmd=common.get_fqdn_cmd(), i=i)
                    run_name = ''
                else:
                    # default behavior is to use a single storage pool
                    pool_name = self.pool
                    run_name_fmt = '--run-name {object_set_id} `{fqdn_cmd}`-{i}'
                    run_name = run_name_fmt.format(
                        object_set_id=self.object_set_id,
                        fqdn_cmd=common.get_fqdn_cmd(),
                        i=i)
                rados_bench_cmd_fmt = \
                    '{cmd} -c {conf} -p {pool} bench {op_size_arg} {duration} ' \
                    '{op_type} {concurrent_ops_arg} {max_objects_arg} ' \
                    '{write_omap_arg} {run_name} --no-cleanup ' \
                    '2> {stderr} > {stdout}'
                rados_bench_cmd = rados_bench_cmd_fmt.format(
                    cmd=self.cmd_path_full,
                    conf=self.tmp_conf,
                    pool=pool_name,
                    op_size_arg=op_size_str,
                    duration=runtime,
                    op_type=op_type,
                    concurrent_ops_arg=concurrent_ops_str,
                    max_objects_arg=max_objects_str,
                    write_omap_arg=write_omap_str,
                    run_name=run_name,
                    stderr=objecter_log,
                    stdout=out_file)
                p = common.pdsh(settings.getnodes('clients'), rados_bench_cmd)
                ps.append(p)
            for p in ps:
                p.wait()

        # If we were doing recovery, wait until it's done (but not for prefill).
        if mode is not 'prefill' and 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(run_dir)

        out_dir = os.path.join(self.out_dir, out_dir)
        common.sync_files('%s/*' % run_dir, out_dir)
        self.analyze(out_dir)

    def mkpools(self):
        with monitoring.monitor("%s/pool_monitoring" % self.run_dir):
            if self.pool_per_proc: # allow use of a separate storage pool per process
                for i in range(self.concurrent_procs):
                    for node in settings.getnodes('clients').split(','):
                        node = node.rpartition("@")[2]
                        self.cluster.rmpool('rados-bench-%s-%s' % (node, i), self.pool_profile)
                        self.cluster.mkpool('rados-bench-%s-%s' % (node, i), self.pool_profile, 'radosbench')
            else: # the default behavior is to use a single Ceph storage pool for all rados bench processes
                self.cluster.rmpool('rados-bench-cbt', self.pool_profile)
                self.cluster.mkpool('rados-bench-cbt', self.pool_profile, 'radosbench')

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 rados').communicate()

    def parse(self, out_dir):
        for client in settings.getnodes('clients').split(','):
            host = settings.host_info(client)["host"]
            for i in range(self.concurrent_procs):
                result = {}
                found = 0
                out_file = '%s/output.%s.%s' % (out_dir, i, host)
                json_out_file = '%s/json_output.%s.%s' % (out_dir, i, host)
                with open(out_file) as fd:
                    for line in fd.readlines():
                        if found == 0:
                            if "Total time run" in line:
                                found = 1
                        if found == 1:
                            line = line.strip()
                            key, val = line.split(":")
                            result[key.strip()] = val.strip()
                with open(json_out_file, 'w') as json_fd:
                    json.dump(result, json_fd)


    def analyze(self, out_dir):
        logger.info('Convert results to json format.')
        self.parse(out_dir)

    def _compare_client_results(self, client_run, fnames):
        # normalize the names
        aliases = {'bandwidth': 'Bandwidth (MB/sec)',
                   'iops_avg': 'Average IOPS',
                   'iops_stddev': 'Stddev IOPS',
                   'latency_avg': 'Average Latency(s)'}
        json_outputs = []
        compare_results = []
        for fname in fnames:
            with open(fname) as f:
                json_outputs.append(json.load(f))
        for alias, stmt in self.acceptable.items():
            name = aliases[alias]
            result, baseline = [float(j[name]) for j in json_outputs]
            # safer than eval()
            env = Env(None, result=result, baseline=baseline)
            lispy = Lispy()
            accepted = lispy.eval(lispy.parse(stmt), env)
            result = Result(client_run, alias, result, baseline, stmt, accepted)
            compare_results.append(result)
        return compare_results

    def evaluate(self, baseline):
        runs = []
        if self.prefill_time or self.prefill_objects:
            runs.append('prefill')
        if not self.read_only:
            runs.append('write')
        if not self.write_only:
            runs.append(self.readmode)
        results = []
        for run in runs:
            out_dirs = [os.path.join(self.out_dir, run),
                        os.path.join(baseline.out_dir, run)]
            for client in settings.getnodes('clients').split(','):
                host = settings.host_info(client)["host"]
                for proc in range(self.concurrent_procs):
                    fname = 'json_output.{proc}.{host}'.format(proc=proc,
                                                               host=host)
                    client_run = '{run}/{client}/{proc}'.format(run=run, client=client, proc=proc)
                    fpaths  = [os.path.join(d, fname) for d in out_dirs]
                    compare_results = self._compare_client_results(client_run, fpaths)
                    rejected = sum(not result.accepted for result in compare_results)
                    results.extend(compare_results)
            # TODO: check results from monitors
        return results

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Radosbench, self).__str__())
