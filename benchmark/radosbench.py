import subprocess
import common
import settings
import monitoring
import os
import time
import threading

from cluster.ceph import Ceph
from benchmark import Benchmark

class BackfillThread(threading.Thread):
    def __init__(self, config, cluster):
        threading.Thread.__init__(self)
        self.config = config
        self.cluster = cluster
        self.state = 'pre'
        self.states = {'pre': self.pre, 'osdout': self.osdout, 'osdin':self.osdin, 'done':self.done}
        self.stoprequest = threading.Event()

    def logcmd(self, message):
        return 'echo "[`date`] %s" >> %s/backfill.log' % (message, self.config.get('run_dir'))

    def pre(self):
        pre_time = self.config.get("pre_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Starting Backfill Thread, waiting %s seconds.' % pre_time)).communicate()
        time.sleep(pre_time)
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s out." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd out %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd)).communicate()
        common.pdsh(settings.getnodes('head'), self.logcmd('Waiting for the cluster to break and heal')).communicate()

        self.state = 'osdout'

    def osdout(self):
        ret = self.cluster.check_health("%s/backfill.log" % self.config.get('run_dir'))
        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()
        if ret == 0:
            return # Cluster hasn't become unhealthy yet.

        common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s in." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd in %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd)).communicate()

        self.state = "osdin"

    def osdin(self):
        # Wait until the cluster is healthy.
        ret = self.cluster.check_health("%s/backfill.log" % self.config.get('run_dir'))
        if ret == 0:
            return # Cluster hasn't become unhealthy yet.
        post_time = self.config.get("post_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, completion in %s seconds.' % post_time)).communicate()
        time.sleep(post_time)
        self.state = "done"

    def done(self):
        common.pdsh(settings.getnodes('head'), self.logcmd("Done.  Killing RADOS Bench.")).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 rados').communicate()
        self.stoprequest.set()

    def join(self, timeout=None):
        common.pdsh(settings.getnodes('head'), self.logcmd('Backfill cancel event.  Will stop at next state.')).communicate()
        self.stoprequest.set()
        super(BackfillThread, self).join(timeout)

    def run(self):
        self.stoprequest.clear()
        while not self.stoprequest.isSet():
          self.states[self.state]()
        common.pdsh(settings.getnodes('head'), self.logcmd('Exiting BackfillThread.  Last state was: %s' % self.state)).communicate()

class Radosbench(Benchmark):

    def __init__(self, config):
        super(Radosbench, self).__init__(config)

        self.tmp_dir = self.cluster.tmp_dir
        self.tmp_conf = self.cluster.tmp_conf
        self.time =  str(config.get('time', '300'))
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.concurrent_ops = config.get('concurrent_ops', 16)
        self.write_only = config.get('write_only', False)
        self.op_size = config.get('op_size', 4194304)
        self.pgs_per_pool = config.get('pgs_per_pool', 2048)
        self.run_dir = '%s/radosbench/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.tmp_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.out_dir = '%s/radosbench/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.archive_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.use_existing = config.get('use_existing', True)
        self.erasure = config.get('erasure', False)
        self.pool_replication = config.get('pool_replication', 1)
        self.erasure_k = config.get('erasure_k', 6)
        self.erasure_m = config.get('erasure_m', 2)

    def exists(self):
        if os.path.exists(self.out_dir):
            print 'Skipping existing test in %s.' % self.out_dir
            return True
        return False

    def initialize(self): 
        super(Radosbench, self).initialize()
        
        self.cluster.cleanup()
        if not self.use_existing:
            self.cluster.initialize()

            # Create the run directory
            common.make_remote_dir(self.run_dir)

            # Setup the rules
            if self.erasure:
                common.pdsh(settings.getnodes('head'), 'ceph -c %s osd crush rule create-erasure cbt-erasure --property erasure-code-ruleset-failure-domain=osd --property erasure-code-m=%s --property erasure-code-k=%s' % (self.tmp_conf, self.erasure_m, self.erasure_k)).communicate()
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
        bft = None
        if 'backfill' in self.config:
            bf_config = self.config.get("backfill", {})
            bf_config['run_dir'] = run_dir
            bft = BackfillThread(bf_config, self.cluster)
            bft.start()

        # Run rados bench
        monitoring.start(run_dir)
        print 'Running radosbench read test.'
        ps = []
        for i in xrange(self.concurrent_procs):
            out_file = '%s/output.%s' % (run_dir, i)
            objecter_log = '%s/objecter.%s.log' % (run_dir, i)
            p = common.pdsh(settings.getnodes('clients'), '/usr/bin/rados -c %s -p rados-bench-`hostname -s`-%s %s bench %s %s %s --no-cleanup 2> %s > %s' % (self.tmp_conf, i, op_size_str, self.time, mode, concurrent_ops_str, objecter_log, out_file))
            ps.append(p)
        for p in ps:
            p.wait()
        monitoring.stop(run_dir)

        # If we were doing bf, wait until it's done.
        if bft:
            bft.join()

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(run_dir)
        common.sync_files('%s/*' % run_dir, out_dir)

    def mkpools(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        for i in xrange(self.concurrent_procs):
            for node in settings.getnodes('clients').split(','):
                node = node.rpartition("@")[2]
                erasure_line = ""
                if self.erasure:
                    erasure_line = 'erasure crush_ruleset=cbt-erasure --property erasure-code-ruleset-failure-domain=osd --property erasure-code-m=%s --property erasure-code-k=%s' % (self.erasure_m, self.erasure_k)
                common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool delete rados-bench-%s-%s rados-bench-%s-%s --yes-i-really-really-mean-it' % (self.tmp_conf, node, i, node, i)).communicate()
                common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool create rados-bench-%s-%s %d %d %s' % (self.tmp_conf, node, i, self.pgs_per_pool, self.pgs_per_pool, erasure_line)).communicate()
                if not self.erasure:
                    common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool set rados-bench-%s-%s size %d' % (self.tmp_conf, node, i, self.pool_replication)).communicate()

                # check the health for each pool.
                print 'Checking Healh after pool creation.'
                self.cluster.check_health()
        monitoring.stop()


    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Radosbench, self).__str__())
