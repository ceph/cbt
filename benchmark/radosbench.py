import subprocess
import common
import settings
import monitoring
import os
import threading
import logging
import re
import json

from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger("cbt")


class Radosbench(Benchmark):

    def __init__(self, cluster, config):
        super(Radosbench, self).__init__()

    def load_config(self, cluster, config):
        super(Radosbench, self).load_config(cluster, config)

        self.tmp_conf = self.cluster.tmp_conf
        self.time =  str(config.get('time', '300'))
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.concurrent_ops = config.get('concurrent_ops', 16)
        self.pool_per_proc = config.get('pool_per_proc', False)  # default behavior used to be True
        self.write_only = config.get('write_only', False)
        self.op_size = config.get('op_size', 4194304)
        self.object_set_id = config.get('object_set_id', '')
        self.pool_profile = config.get('pool_profile', 'default')
        self.cmd_path = config.get('cmd_path', self.cluster.rados_cmd)
        self.pool = config.get('target_pool', 'rados-bench-cbt')
        self.readmode = config.get('readmode', 'seq')
        self.max_objects = config.get('max_objects', None)
        self.write_omap = config.get('write_omap', False)
        self.run_dir_orig = self.run_dir
        self.archive_dir_orig = self.archive_dir

    def get_rados_version(self):
        stdout,stderr = common.pdsh(settings.getnodes('head'), '%s -c %s -v' % (self.cmd_path, self.tmp_conf)).communicate()
        return stdout

    def run(self):
        # Remake the pools
        self.mkpools()

        # Run write test
        self.run_dir = os.path.join(self.run_dir, 'write')
        self.archive_dir = os.path.join(self.archive_dir, 'write')
        self._run('write')

        # Run read test unless write_only
        if self.write_only:
            return

        self.run_dir = os.path.join(self.run_dir, 'read')
        self.archive_dir = os.path.join(self.archive_dir, 'read')
        self._run(self.readmode) 

        self.run_dir = self.run_dir_orig
        self.archive_dir = self.archive_dir_orig

    def _run(self, mode):
        self.pre_run()

        # Run rados bench
        logger.info('Running radosbench %s test.' % mode)
        ps = []
        for i in xrange(self.concurrent_procs):
            p = common.pdsh(settings.getnodes('clients'), self.make_command(i, mode))
            ps.append(p)
        for p in ps:
            p.wait()

        self.post_run()

    def post_run(self):
        super(Radosbench, self).post_run()
        self.analyze()

    def make_command(self, i, mode):
        if self.concurrent_ops:
            concurrent_ops_str = '--concurrent-ios %s' % self.concurrent_ops

        #determine rados version
        rados_version_str = self.get_rados_version()

        m = re.findall("version (\d+)", rados_version_str)
        if not m:
           m = re.findall("version v(\d+)", rados_version_str)

        rados_version = int(m[0])

        if mode in ['write'] or rados_version < 9:
            op_size_str = '-b %s' % self.op_size
        else:
            op_size_str = ''

        # Max Objects
        max_objects_str = ''
        if self.max_objects and rados_version < 9:
           raise ValueError('max_objects not supported by rados_version < 9')
        if self.max_objects and rados_version > 9:
           max_objects_str = '--max-objects %s' % self.max_objects

        # Write to OMAP
        write_omap_str = ''
        if self.write_omap and rados_version < 9:
           raise ValueError('write_omap not supported by rados_version < 9')
        if self.write_omap and rados_version > 9:
           write_omap_str = '--write-omap'

        out_file = '%s/output.%s' % (self.run_dir, i)
        objecter_log = '%s/objecter.%s.log' % (self.run_dir, i)
        # default behavior is to use a single storage pool 
        pool_name = self.pool

        run_name = '--run-name %s`%s`-%s'%(self.object_set_id, common.get_fqdn_cmd(), i)
        if self.pool_per_proc: # support previous behavior of 1 storage pool per rados process
            pool_name = 'rados-bench-``-%s'% (common.get_fqdn_cmd(), i)
            run_name = ''

        command = self.cmd_path_full
        command += ' -c %s' % self.tmp_conf
        command += ' -p %s' % pool_name
        command += ' bench'
        command += ' %s' % op_size_str
        command += ' %s' % self.time
        command += ' %s' % mode
        command += ' %s' % concurrent_ops_str
        command += ' %s' % max_objects_str
        command += ' %s' % write_omap_str
        command += ' %s' % run_name
        command += ' --nocleanup 2> %s' % objecter_log
        command += ' > %s' % out_file

        return command

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

    def parse(self):
        for client in settings.cluster.get('clients'):
            for i in xrange(self.concurrent_procs):
                result = {}
                found = 0
                out_file = '%s/output.%s.%s' % (self.archive_dir, i, client)
                json_out_file = '%s/json_output.%s.%s' % (self.archive_dir, i, client)
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


    def analyze(self):
        logger.info('Convert results to json format.')
        self.parse(self.archive_dir)

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.archive_dir, super(Radosbench, self).__str__())
