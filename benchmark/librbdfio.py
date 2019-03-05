import subprocess
import common
import settings
import monitoring
import os
import time
import threading
import logging
import json

from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger("cbt")


class LibrbdFio(Benchmark):

    def __init__(self, cluster, config):
        super(LibrbdFio, self).__init__(cluster, config)

        # FIXME there are too many permutations, need to put results in SQLITE3 
        self.cmd_path = config.get('cmd_path', '/usr/bin/fio')
        self.pool_profile = config.get('pool_profile', 'default')
        self.data_pool_profile = config.get('data_pool_profile', None)
        self.time =  str(config.get('time', None))
        self.time_based = bool(config.get('time_based', False))
        self.ramp = str(config.get('ramp', None))
        self.iodepth = config.get('iodepth', 16)
        self.numjobs = config.get('numjobs', 1)
        self.end_fsync = str(config.get('end_fsync', 0))
        self.mode = config.get('mode', 'write')
        self.rwmixread = config.get('rwmixread', 50)
        self.rwmixwrite = 100 - self.rwmixread
        self.log_avg_msec = config.get('log_avg_msec', None)
#        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.pgs = config.get('pgs', 2048)
        self.vol_size = config.get('vol_size', 65536)
        self.vol_order = config.get('vol_order', 22)
        self.volumes_per_client = config.get('volumes_per_client', 1)
        self.procs_per_volume = config.get('procs_per_volume', 1)
        self.random_distribution = config.get('random_distribution', None)
        self.rate_iops = config.get('rate_iops', None)
        self.pool_name = "cbt-librbdfio"
        self.fio_out_format = "json,normal"
        self.data_pool = None 
        self.use_existing_volumes = config.get('use_existing_volumes', False)

	self.total_procs = self.procs_per_volume * self.volumes_per_client * len(settings.getnodes('clients').split(','))
        self.run_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.run_dir, int(self.osd_ra), int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)
        self.out_dir = self.archive_dir

        self.norandommap = config.get("norandommap", False)
        # Make the file names string (repeated across volumes)
        self.names = ''
        for proc_num in xrange(self.procs_per_volume):
            rbd_name = 'cbt-librbdfio-`%s`-file-%d' % (common.get_fqdn_cmd(), proc_num)
            self.names += '--name=%s ' % rbd_name

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    def initialize(self): 
        super(LibrbdFio, self).initialize()

        # Clean and Create the run directory
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

        logger.info('Pausing for 60s for idle monitoring.')
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        monitoring.start_pbench("%s/idle" % self.out_dir)
        time.sleep(60)
        monitoring.stop()
        monitoring.stop_pbench("%s/idle" % self.out_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

        self.mkimages()

        # populate the fio files
        ps = []
        logger.info('Attempting to populating fio files...')
        if (self.use_existing_volumes == False):
          for volnum in xrange(self.volumes_per_client):
              rbd_name = 'cbt-librbdfio-`%s`-%d' % (common.get_fqdn_cmd(), volnum)
              pre_cmd = 'sudo %s --ioengine=rbd --clientname=admin --pool=%s --rbdname=%s --invalidate=0  --rw=write --numjobs=%s --bs=4M --size %dM %s --output-format=%s > /dev/null' % (self.cmd_path, self.pool_name, rbd_name, self.numjobs, self.vol_size, self.names, self.fio_out_format)
              p = common.pdsh(settings.getnodes('clients'), pre_cmd)
              ps.append(p)
          for p in ps:
              p.wait()
        return True

    def run(self):
        super(LibrbdFio, self).run()

        # We'll always drop caches for rados bench
        self.dropcaches()

        # dump the cluster config
        self.cluster.dump_config(self.run_dir)

        monitoring.start(self.run_dir)
        monitoring.start_pbench(self.out_dir)

        time.sleep(5)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        logger.info('Running rbd fio %s test.', self.mode)
        ps = []
        for i in xrange(self.volumes_per_client):
            fio_cmd = self.mkfiocmd(i)
            p = common.pdsh(settings.getnodes('clients'), fio_cmd)
            ps.append(p)
        for p in ps:
            p.wait()
        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        monitoring.stop_pbench(self.out_dir)
        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)
        self.analyze(self.out_dir)

    def mkfiocmd(self, volnum):
        rbdname = 'cbt-librbdfio-`%s`-%d' % (common.get_fqdn_cmd(), volnum)
        out_file = '%s/output.%d.`%s`' % (self.run_dir, volnum, common.get_fqdn_cmd())

        fio_cmd = 'sudo %s --ioengine=rbd --clientname=admin --pool=%s --rbdname=%s --invalidate=0' % (self.cmd_path_full, self.pool_name, rbdname)
        fio_cmd += ' --rw=%s' % self.mode
        fio_cmd += ' --output-format=%s' % self.fio_out_format
        if (self.mode == 'readwrite' or self.mode == 'randrw'):
            fio_cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
#        fio_cmd += ' --ioengine=%s' % self.ioengine
        if self.time is not None:
            fio_cmd += ' --runtime=%s' % self.time
        if self.time_based is True:
            fio_cmd += ' --time_based'
        if self.ramp is not None:
            fio_cmd += ' --ramp_time=%s' % self.ramp
        fio_cmd += ' --numjobs=%s' % self.numjobs
        fio_cmd += ' --direct=1'
        fio_cmd += ' --bs=%dB' % self.op_size
        fio_cmd += ' --iodepth=%d' % self.iodepth
        fio_cmd += ' --end_fsync=%s' % self.end_fsync
#        if self.vol_size:
#            fio_cmd += ' -- size=%dM' % self.vol_size
        if self.norandommap:
            fio_cmd += ' --norandommap' 
        fio_cmd += ' --write_iops_log=%s' % out_file
        fio_cmd += ' --write_bw_log=%s' % out_file
        fio_cmd += ' --write_lat_log=%s' % out_file
        if 'recovery_test' in self.cluster.config:
            fio_cmd += ' --time_based'
        if self.random_distribution is not None:
            fio_cmd += ' --random_distribution=%s' % self.random_distribution
        if self.log_avg_msec is not None:
            fio_cmd += ' --log_avg_msec=%s' % self.log_avg_msec
        if self.rate_iops is not None:
            fio_cmd += ' --rate_iops=%s' % self.rate_iops

        # End the fio_cmd
        fio_cmd += ' %s > %s' % (self.names, out_file)
        return fio_cmd

    def mkimages(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        if (self.use_existing_volumes == False):
          self.cluster.rmpool(self.pool_name, self.pool_profile)
          self.cluster.mkpool(self.pool_name, self.pool_profile, 'rbd')
          if self.data_pool_profile:
              self.data_pool = self.pool_name + "-data"
              self.cluster.rmpool(self.data_pool, self.data_pool_profile)
              self.cluster.mkpool(self.data_pool, self.data_pool_profile, 'rbd')
          for node in common.get_fqdn_list('clients'):
              for volnum in xrange(0, self.volumes_per_client):
                  node = node.rpartition("@")[2]
                  self.cluster.mkimage('cbt-librbdfio-%s-%d' % (node,volnum), self.vol_size, self.pool_name, self.data_pool, self.vol_order)
        monitoring.stop()

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -2 fio').communicate()

    def parse(self, out_dir):
        for client in settings.cluster.get('clients'):
            for i in xrange(self.volumes_per_client):
                found = 0
                out_file = '%s/output.%d.%s' % (out_dir, i, client)
                json_out_file = '%s/json_output.%d.%s' % (out_dir, i, client)
                with open(out_file) as fd:
                    with open(json_out_file, 'w') as json_fd:
                        for line in fd.readlines():
                            if len(line.strip()) == 0:
                                found = 0
                                break
                            if found == 1:
                                json_fd.write(line)
                            if found == 0:
                                if "Starting" in line:
                                    found = 1

    def analyze(self, out_dir):
        logger.info('Convert results to json format.')
        self.parse(out_dir)

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(LibrbdFio, self).__str__())
