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
from benchmark import Benchmark

logger = logging.getLogger("cbt")


class Radosbench(Benchmark):

    def __init__(self, cluster, config):
        super(Radosbench, self).__init__(cluster, config)

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
        self.run_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.run_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.out_dir = self.archive_dir
        self.pool_profile = config.get('pool_profile', 'default')
        self.cmd_path = config.get('cmd_path', self.cluster.rados_cmd)
        self.pool = config.get('target_pool', 'rados-bench-cbt')
        self.readmode = config.get('readmode', 'seq')
        self.max_objects = config.get('max_objects', None)
        self.write_omap = config.get('write_omap', False)
        self.prefill_time = config.get('prefill_time', None)
        self.prefill_objects = config.get('prefill_objects', None)


    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self): 
        super(Radosbench, self).initialize()

        logger.info('Pausing for 60s for idle monitoring.')
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        time.sleep(60)
        monitoring.stop()

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

        return True

    def get_rados_version(self):
        output = ""
        stdout,stderr = common.pdsh(settings.getnodes('head'), '%s -c %s -v' % (self.cmd_path, self.tmp_conf)).communicate()
        return stdout

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
                      runtime=self.prefill_time or self.time)
        # Run write test
        if not self.read_only:
            self._run(mode='write', run_dir='write', out_dir='write',
                      runtime=self.read_time)
        # Run read test unless write_only
        if not self.write_only:
            self._run(mode=self.readmode, run_dir=self.readmode, out_dir=self.readmode,
                      runtime=self.write_time)

    def _run(self, mode, run_dir, out_dir, runtime):
        # We'll always drop caches for rados bench
        self.dropcaches()

        if self.concurrent_ops:
            concurrent_ops_str = '--concurrent-ios %s' % self.concurrent_ops

        #determine rados version
        rados_version_str = self.get_rados_version()

        m = re.findall("version (\d+)", rados_version_str)
        if not m:
           m = re.findall("version v(\d+)", rados_version_str)

        rados_version = int(m[0])

        # Max Objects
        max_objects = None
        if mode is 'prefill':
            max_objects = self.prefill_objects
        else:
            max_objects = self.max_objects
        max_objects_str = ''
        if max_objects:
            if rados_version < 10:
                raise ValueError('max_objects not supported by rados_version < 10')
            max_objects_str = '--max-objects %s' % max_objects

        # Operation type 
        op_type = mode
        if mode is 'prefill':
            op_type = 'write'

        if op_type in ['write'] or rados_version < 9:
            op_size_str = '-b %s' % self.op_size
        else:
            op_size_str = ''  

        # Write to OMAP
        write_omap_str = ''
        if self.write_omap and rados_version < 9:
           raise ValueError('write_omap not supported by rados_version < 9')
        if self.write_omap and rados_version > 9:
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
        monitoring.start(run_dir)
        logger.info('Running radosbench %s test.' % mode)
        ps = []
        for i in xrange(self.concurrent_procs):
            out_file = '%s/output.%s' % (run_dir, i)
            objecter_log = '%s/objecter.%s.log' % (run_dir, i)
            # default behavior is to use a single storage pool 
            pool_name = self.pool

            run_name = '--run-name %s`%s`-%s'%(self.object_set_id, common.get_fqdn_cmd(), i)
            if self.pool_per_proc: # support previous behavior of 1 storage pool per rados process
                pool_name = 'rados-bench-``-%s'% (common.get_fqdn_cmd(), i)
                run_name = ''
            rados_bench_cmd = '%s -c %s -p %s bench %s %s %s %s %s %s %s --no-cleanup 2> %s > %s' % \
                 (self.cmd_path_full, self.tmp_conf, pool_name, op_size_str, runtime, op_type, concurrent_ops_str, max_objects_str, write_omap_str, run_name, objecter_log, out_file)
            p = common.pdsh(settings.getnodes('clients'), rados_bench_cmd)
            ps.append(p)
        for p in ps:
            p.wait()
        monitoring.stop(run_dir)

        # If we were doing recovery, wait until it's done (but not for prefill).
        if mode is not 'prefill' and 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(run_dir)

        out_dir = os.path.join(self.out_dir, out_dir)
        common.sync_files('%s/*' % run_dir, out_dir)
        self.analyze(out_dir)

    def mkpools(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        if self.pool_per_proc: # allow use of a separate storage pool per process
            for i in xrange(self.concurrent_procs):
                for node in settings.getnodes('clients').split(','):
                    node = node.rpartition("@")[2]
                    self.cluster.rmpool('rados-bench-%s-%s' % (node, i), self.pool_profile)
                    self.cluster.mkpool('rados-bench-%s-%s' % (node, i), self.pool_profile, 'radosbench')
        else: # the default behavior is to use a single Ceph storage pool for all rados bench processes
            self.cluster.rmpool('rados-bench-cbt', self.pool_profile)
            self.cluster.mkpool('rados-bench-cbt', self.pool_profile, 'radosbench')
        monitoring.stop()

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 rados').communicate()

    def parse(self, out_dir):
        for client in settings.getnodes('clients').split(','):
            host = settings.host_info(client)["host"]
            for i in xrange(self.concurrent_procs):
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

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Radosbench, self).__str__())
