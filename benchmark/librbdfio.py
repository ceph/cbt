import subprocess
import common
import settings
import monitoring
import os
import time
import threading
import logging

from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger("cbt")


class LibrbdFio(Benchmark):

    def __init__(self, cluster, config):
        super(LibrbdFio, self).__init__(cluster, config)

        # FIXME there are too many permutations, need to put results in SQLITE3 
        self.cmd_path = config.get('cmd_path', '/usr/bin/fio')
        self.pool_profile = config.get('pool_profile', 'default')
        self.time =  str(config.get('time', None))
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
        self.poolname = "cbt-librbdfio"
        self.use_existing_volumes = config.get('use_existing_volumes', False)

	self.total_procs = self.procs_per_volume * self.volumes_per_client * len(settings.getnodes('clients').split(','))
        self.run_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.run_dir, int(self.osd_ra), int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)
        self.out_dir = '%s/osd_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.archive_dir, int(self.osd_ra), int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)

        # Make the file names string (repeated across volumes)
        self.names = ''
        for i in xrange(self.procs_per_volume):
            self.names += '--name=librbdfio-`hostname -s`-%d ' % i

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    def initialize(self): 
        super(LibrbdFio, self).initialize()

        logger.info('Running scrub monitoring.')
        monitoring.start("%s/scrub_monitoring" % self.run_dir)
        self.cluster.check_scrub()
        monitoring.stop()

        logger.info('Pausing for 60s for idle monitoring.')
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        time.sleep(60)
        monitoring.stop()

        common.sync_files('%s/*' % self.run_dir, self.out_dir)

        self.mkimages()

        # Create the run directory
        common.make_remote_dir(self.run_dir)

        # populate the fio files
        ps = []
        logger.info('Attempting to populating fio files...')
        if (self.use_existing_volumes == False):
          for i in xrange(self.volumes_per_client):
              pre_cmd = 'sudo %s --ioengine=rbd --clientname=admin --pool=%s --rbdname=cbt-librbdfio-`hostname -s`-%d --invalidate=0  --rw=write --numjobs=%s --bs=4M --size %dM %s > /dev/null' % (self.cmd_path, self.poolname, i, self.numjobs, self.vol_size, self.names)
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

        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def mkfiocmd(self, volnum):
        rbdname = 'cbt-librbdfio-`hostname -s`-%d' % volnum
        out_file = '%s/output.%d' % (self.run_dir, volnum)

        fio_cmd = 'sudo %s --ioengine=rbd --clientname=admin --pool=%s --rbdname=%s --invalidate=0' % (self.cmd_path_full, self.poolname, rbdname)
        fio_cmd += ' --rw=%s' % self.mode
        if (self.mode == 'readwrite' or self.mode == 'randrw'):
            fio_cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
#        fio_cmd += ' --ioengine=%s' % self.ioengine
        if self.time is not None:
            fio_cmd += ' --runtime=%s' % self.time
        if self.ramp is not None:
            fio_cmd += ' --ramp_time=%s' % self.ramp
        fio_cmd += ' --numjobs=%s' % self.numjobs
        fio_cmd += ' --direct=1'
        fio_cmd += ' --bs=%dB' % self.op_size
        fio_cmd += ' --iodepth=%d' % self.iodepth
        fio_cmd += ' --end_fsync=%s' % self.end_fsync
#        if self.vol_size:
#            fio_cmd += ' -- size=%dM' % self.vol_size
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
          self.cluster.rmpool(self.poolname, self.pool_profile)
          self.cluster.mkpool(self.poolname, self.pool_profile)
          for node in settings.getnodes('clients').split(','):
              stdout,stderr = common.pdsh(node, 'hostname -s').communicate()
              nodename = stdout.split()[1]
              for volnum in xrange(0, self.volumes_per_client):
#                  common.pdsh(settings.getnodes('head'), '/usr/bin/rbd create cbt-librbdfio-%s-%d --size %s --pool %s --order %s' % (node, volnum, self.vol_size, self.poolname, self.vol_order)).communicate()
                  self.cluster.mkimage('cbt-librbdfio-%s-%d' % (nodename,volnum), self.vol_size, self.poolname, self.vol_order)
        monitoring.stop()

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 fio').communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(LibrbdFio, self).__str__())
