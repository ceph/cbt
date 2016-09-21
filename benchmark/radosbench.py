import subprocess
import common
import settings
import monitoring
import os
import time
import threading
import logging
import re

from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger("cbt")


class Radosbench(Benchmark):

    def __init__(self, cluster, config):

        self.super = super(Radosbench, self)
        self.super.__init__(cluster, config)
        self.tmp_conf = self.cluster.tmp_conf
        self.time =  str(config.get('time', '300'))
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.concurrent_ops = config.get('concurrent_ops', 16)
        self.pool_per_proc = config.get('pool_per_proc', False)  # default behavior used to be True
        self.write_only = config.get('write_only', False)
        self.op_size = config.get('op_size', 4194304)
        self.object_set_id = config.get('object_set_id', '')
        self.run_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.run_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.out_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.archive_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.pool_profile = config.get('pool_profile', 'default')
        self.cmd_path = config.get('cmd_path', '/usr/bin/rados')
        self.pool = config.get('target_pool', 'rados-bench-cbt')
        self.readmode = config.get('readmode', 'seq')

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self): 
        self.super.initialize()
        iteration = self.config.get('iteration')
        if iteration == 0:
            self.super.do_initial_monitoring()

        return True

    def run(self):
        self.super.run()
        
        # Remake the pools
        self.mkpools()

        # Run write test
        self._run('write', '%s/write' % self.run_dir, '%s/write' % self.out_dir)
        # Run read test unless write_only
        if self.write_only: return
        self._run(self.readmode, '%s/%s' % (self.run_dir, self.readmode), '%s/%s' % (self.out_dir, self.readmode))
        
    def _run(self, mode, run_dir, out_dir):

        # We'll always drop caches for rados bench
        self.dropcaches()

        if self.concurrent_ops:
            concurrent_ops_str = '--concurrent-ios %s' % self.concurrent_ops
        #determine rados version    
        rados_version_str = subprocess.check_output(["rados", "-v"])
        m = re.findall("version (\d+)", rados_version_str)
        rados_version = int(m[0])

        if mode in ['write'] or rados_version < 9:
            op_size_str = '-b %s' % self.op_size
        else:
            op_size_str = ''

        # dump the cluster config
        self.cluster.dump_config(run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(run_dir, recovery_callback)

        # Run rados bench
        monitoring.start(self.run_monitoring_list)
        logger.info('Running radosbench %s test.' % mode)
        ps = []
        for i in xrange(self.concurrent_procs):
            out_file = '%s/output.%s' % (run_dir, i)
            objecter_log = '%s/objecter.%s.log' % (run_dir, i)
            # default behavior is to use a single storage pool 
            pool_name = self.pool
            run_name = '--run-name %s`hostname -s`-%s'%(self.object_set_id, i)
            if self.pool_per_proc: # support previous behavior of 1 storage pool per rados process
                pool_name = 'rados-bench-`hostname -s`-%s'%i
                run_name = ''
            rados_bench_cmd = '%s -c %s -p %s bench %s %s %s %s %s --no-cleanup 2> %s > %s' % \
                 (self.cmd_path_full, self.tmp_conf, pool_name, op_size_str, self.time, mode, concurrent_ops_str, run_name, objecter_log, out_file)
            p = common.pdsh(settings.getnodes('clients'), rados_bench_cmd)
            ps.append(p)
        for p in ps:
            p.wait()
        monitoring.stop(self.run_monitoring_list)

        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        self.postprocess_all()

    def mkpools(self):
        monitoring.start(self.pool_monitoring_list)
        if self.pool_per_proc: # allow use of a separate storage pool per process
            for i in xrange(self.concurrent_procs):
                for node in settings.getnodes('clients').split(','):
                    node = node.rpartition("@")[2]
                    self.cluster.rmpool('rados-bench-%s-%s' % (node, i), self.pool_profile)
                    self.cluster.mkpool('rados-bench-%s-%s' % (node, i), self.pool_profile)
        else: # the default behavior is to use a single Ceph storage pool for all rados bench processes
            self.cluster.rmpool('rados-bench-cbt', self.pool_profile)
            self.cluster.mkpool('rados-bench-cbt', self.pool_profile)
        monitoring.stop(self.pool_monitoring_list)

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 rados').communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, str(self.super))
