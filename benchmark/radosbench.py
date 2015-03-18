import subprocess
import common
import settings
import monitoring
import os
import time
import threading

from cluster.ceph import Ceph
from benchmark import Benchmark

class Radosbench(Benchmark):

    def __init__(self, cluster, config):
        super(Radosbench, self).__init__(cluster, config)

        self.tmp_conf = self.cluster.tmp_conf
        self.time =  str(config.get('time', '300'))
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.concurrent_ops = config.get('concurrent_ops', 16)
        self.write_only = config.get('write_only', False)
        self.op_size = config.get('op_size', 4194304)
        self.run_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.run_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.out_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.archive_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.pool_profile = config.get('pool_profile', 'default')
        self.cmd_path = config.get('cmd_path', '/usr/bin/rados')

    def exists(self):
        if os.path.exists(self.out_dir):
            print 'Skipping existing test in %s.' % self.out_dir
            return True
        return False

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self): 
        super(Radosbench, self).initialize()

        print 'Running scrub monitoring.'
        monitoring.start("%s/scrub_monitoring" % self.run_dir)
        self.cluster.check_scrub()
        monitoring.stop()

        print 'Pausing for 60s for idle monitoring.'
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        time.sleep(60)
        monitoring.stop()

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

        return True

    def run(self):
        super(Radosbench, self).run()
        
        # Remake the pools
        self.mkpools()

        # Run write test
        self._run('write', '%s/write' % self.run_dir, '%s/write' % self.out_dir)
        # Run read test unless write_only
        if self.write_only: return
        self._run('seq', '%s/seq' % self.run_dir, '%s/seq' % self.out_dir)

    def _run(self, mode, run_dir, out_dir):
        # We'll always drop caches for rados bench
        self.dropcaches()

        if self.concurrent_ops:
            concurrent_ops_str = '--concurrent-ios %s' % self.concurrent_ops
        op_size_str = '-b %s' % self.op_size

        common.make_remote_dir(run_dir)

        # dump the cluster config
        self.cluster.dump_config(run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(run_dir, recovery_callback)

        # Run rados bench
        monitoring.start(run_dir)
        print 'Running radosbench read test.'
        ps = []
        for i in xrange(self.concurrent_procs):
            out_file = '%s/output.%s' % (run_dir, i)
            objecter_log = '%s/objecter.%s.log' % (run_dir, i)
            p = common.pdsh(settings.getnodes('clients'), '%s -c %s -p rados-bench-`hostname -s`-%s %s bench %s %s %s --no-cleanup 2> %s > %s' % (self.cmd_path_full, self.tmp_conf, i, op_size_str, self.time, mode, concurrent_ops_str, objecter_log, out_file))
            ps.append(p)
        for p in ps:
            p.wait()
        monitoring.stop(run_dir)

        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(run_dir)
        common.sync_files('%s/*' % run_dir, out_dir)

    def mkpools(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        for i in xrange(self.concurrent_procs):
            for node in settings.getnodes('clients').split(','):
                node = node.rpartition("@")[2]
                self.cluster.rmpool('rados-bench-%s-%s' % (node, i), self.pool_profile)
                self.cluster.mkpool('rados-bench-%s-%s' % (node, i), self.pool_profile)
        monitoring.stop()

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 rados')

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Radosbench, self).__str__())
