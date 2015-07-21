import subprocess
import common
import settings
import monitoring
import os
import time
import threading

from cluster.ceph import Ceph
from benchmark import Benchmark

class CephTestRados(Benchmark):

    def __init__(self, cluster, config):
        super(CephTestRados, self).__init__(cluster, config)

        self.tmp_conf = self.cluster.tmp_conf

        self.object_size = int(config.get('object_size', 4000000))
        self.ec_pool = config.get('ec_pool', False)
        self.write_fadvise_dontneed = config.get('write_fadvise_dontneed', False)
        self.pool_snaps = config.get('pool_snaps', False)
        self.max_ops = str(config.get('ops', 10000))
        self.objects = str(config.get('objects', 500))
        self.max_in_flight = str(config.get('max_in_flight', 16))
        self.size = int(config.get('object_size', 4000000))
        self.min_stride_size = str(config.get('min_stride_size', self.object_size / 10))
        self.max_stride_size = str(config.get('max_stride_size', self.object_size / 5))
        self.max_seconds = str(config.get('max_seconds', 0))
        self.write_append_excl = str(config.get('write_append_excl', True))

        self.weights = {}
        self.weights['snap_create'] = int(config.get('snap_create_weight', None))
        self.weights['snap_remove'] = int(config.get('snap_remove_weight', None))
        self.weights['rollback'] = int(config.get('rollback_weight', None))
        self.weights['setattr'] = int(config.get('settattr_weight', None))
        self.weights['rmattr'] = int(config.get('rmattr_weight', None))
        self.weights['watch'] = int(config.get('watch_weight', None))
        self.weights['copy_from'] = int(config.get('copy_from_weight', None))
        self.weights['hit_set_list'] = int(config.get('hit_set_list_weight', None))
        self.weights['is_dirty'] = int(config.get('is_dirty_weight', None))
        self.weights['cache_flush'] = int(config.get('cache_flush_weight', None))
        self.weights['cache_try_flush'] = int(config.get('cache_try_flush_weight', None))
        self.weights['cache_evict'] = int(config.get('cache_evict_weight', None))
        self.weights['append'] = int(config.get('append_weight', None))
        if self.write_append_excl and self.weights['append_weight']:
            self.weights['append'] = self.weights['write'] / 2
            self.weights['append_excl'] = self.weights['write']

        self.weights['write'] = int(config.get('write_weight', 100))
        if self.write_append_excl and self.weights['wriite_weight']:
            self.weights['write'] = self.weights['write'] / 2
            self.weights['write_excl'] = self.weights['write']

        self.weights['read'] = int(config.get('read_weight', 100))
        self.weights['delete'] = int(config.get('delete_weight', 10))

#        self.weights = {k:v for k,v in self.weights.items() if v}
        
        for k,v in self.weights.items():
            
            weights[field] = op_weights[field]
        
        self.run_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.run_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.out_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_ops-%08d' % (self.archive_dir, int(self.osd_ra), int(self.op_size), int(self.concurrent_ops))
        self.pool_profile = config.get('pool_profile', 'default')
        self.cmd_path = config.get('cmd_path', '/usr/bin/ceph_test_rados')

    def exists(self):
        if os.path.exists(self.out_dir):
            print 'Skipping existing test in %s.' % self.out_dir
            return True
        return False

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self): 
        super(CephTestRados, self).initialize()
        common.sync_files('%s/*' % self.run_dir, self.out_dir)
        return True

    def run(self):
        super(CephTestRados, self).run()
        
        # Remake the pool
        self.mkpool()
        self.dropcaches()
        self.cluster.dump_config(self.run_dir)
        monitoring.start(self.run_dir)
        time.sleep(5)
        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        print 'Running rbd fio %s test.' % self.mode
        ps = []
        for i in xrange(1):
            p = common.pdsh(settings.getnodes('clients'), self.mkcmd())
            ps.append(p)
        for p in ps:
            p.wait()
        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def mkcmd(self):
        cmd = [self.cmd_path]       
        out_file = '%s/output' % self.run_dir
        for var in ['ec_pool', 'write_fadvise_dontneed', 'pool_snaps', 'max_ops', 'objects', 'max_in_flight', 'size', 'min_stride_size', 'max_stride_size', 'max_seconds']:
            value = locals()[var]
            if value:
                cmd.extend(['--%s' % var.replace('_', '-'), value])
        for op, weight in self.weights.iteritems():
            if weight:
                self.cmd.extend(['--op', str(weight)])
        cmd.extend(['--pool', 'ceph_test_rados'])
        cmd.extend(['>', out_file])
        return cmd.join(' ')
        
    def mkpool(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        self.cluster.rmpool('ceph_test_rados', self.pool_profile)
        self.cluster.mkpool('ceph_test_rados', self.pool_profile)
        monitoring.stop()

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 ceph_test_rados').communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(CephTestRados, self).__str__())
