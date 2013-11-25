import subprocess
import common
import settings
import monitoring
import os
import time

from benchmark import Benchmark

class Radosbench(Benchmark):

    def __init__(self, config):
        super(Radosbench, self).__init__(config)
        self.time =  str(config.get('time', '300'))
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.concurrent_ops = config.get('concurrent_ops', 16)
        self.write_only = config.get('write_only', False)
        self.op_size = config.get('op_size', 4194304)
        self.pgs_per_pool = config.get('pgs_per_pool', 2048)
        self.run_dir = '%s/radosbench/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.tmp_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.out_dir = '%s/radosbench/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.archive_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.use_existing = config.get('use_existing', True)

    def exists(self):
        if os.path.exists(self.out_dir):
            print 'Skipping existing test in %s.' % self.out_dir
            return True
        return False

    def initialize(self): 
        super(Radosbench, self).initialize()
        common.cleanup_tests()
        if not self.use_existing:
            common.setup_cluster()
            common.setup_ceph()

            # Create the run directory
            common.make_remote_dir(self.run_dir)

            # Setup the pools

            monitoring.start("%s/pool_monitoring" % self.run_dir)
            for i in xrange(self.concurrent_procs):
                for node in settings.cluster.get('clients').split(','):
                    node = node.rpartition("@")[2]
                    common.pdsh(settings.cluster.get('head'), 'sudo ceph osd pool create rados-bench-%s-%s %d %d' % (node, i, self.pgs_per_pool, self.pgs_per_pool)).communicate()
                    common.pdsh(settings.cluster.get('head'), 'sudo ceph osd pool set rados-bench-%s-%s size 1' % (node, i)).communicate()
                    # check the health for each pool.
                    print 'Checking Healh after pool creation.'
                    common.check_health()
            monitoring.stop()

        print 'Running scrub monitoring.'
        monitoring.start("%s/scrub_monitoring" % self.run_dir)
        common.check_scrub()
        monitoring.stop()

        print 'Pausing for 60s for idle monitoring.'
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        time.sleep(60)
        monitoring.stop()

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

        return True

    def run(self):
        super(Radosbench, self).run()
        # Run write test
        self._run('write', '%s/write' % self.run_dir, '%s/write' % self.out_dir)
        # Run read test unless write_only 
        if self.write_only: return
        self.dropcaches()
        self._run('seq', '%s/seq' % self.run_dir, '%s/seq' % self.out_dir)

    def _run(self, mode, run_dir, out_dir):
        # We'll always drop caches for rados bench
        self.dropcaches()

        if self.concurrent_ops:
            concurrent_ops_str = '--concurrent-ios %s' % self.concurrent_ops
        op_size_str = '-b %s' % self.op_size

        common.make_remote_dir(run_dir)

        # dump the cluster config
        common.dump_config(run_dir)

        monitoring.start(run_dir)
        # Run rados bench
        print 'Running radosbench read test.'
        ps = []
        for i in xrange(self.concurrent_procs):
            out_file = '%s/output.%s' % (run_dir, i)
            objecter_log = '%s/objecter.%s.log' % (run_dir, i)
            p = common.pdsh(settings.cluster.get('clients'), '/usr/bin/rados -p rados-bench-`hostname -s`-%s %s bench %s %s %s --no-cleanup 2> %s > %s' % (i, op_size_str, self.time, mode, concurrent_ops_str, objecter_log, out_file))
            ps.append(p)
        for p in ps:
            p.wait()
        monitoring.stop(run_dir)

        # Get the historic ops
        common.dump_historic_ops(run_dir)
        common.sync_files('%s/*' % run_dir, out_dir)

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Radosbench, self).__str__())
