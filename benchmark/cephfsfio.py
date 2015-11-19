import subprocess
import common
import settings
import monitoring
import os
import time
import logging

from benchmark import Benchmark

logger = logging.getLogger("cbt")

class CephFsFio(Benchmark):

    def __init__(self, cluster, config):
        super(CephFsFio, self).__init__(cluster, config)

        # FIXME there are too many permutations, need to put results in SQLITE3
        self.cmd_path = config.get('cmd_path', '/usr/bin/fio')
        self.pool_profile = config.get('pool_profile', 'default')

        self.monaddr_mountpoint = config.get('monaddr_mountpoint')
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.total_procs = self.concurrent_procs * len(settings.getnodes('clients').split(','))
        self.time =  str(config.get('time', None))
        self.ramp = str(config.get('ramp', None))
        self.iodepth = config.get('iodepth', 16)
        self.numjobs = config.get('numjobs', 1)
        self.end_fsync = str(config.get('end_fsync', 0))
        self.mode = config.get('mode', 'write')
        self.rwmixread = config.get('rwmixread', 50)
        self.rwmixwrite = 100 - self.rwmixread
        self.log_avg_msec = config.get('log_avg_msec', None)
        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.vol_size = config.get('vol_size', 65536)
        self.vol_order = config.get('vol_order', 22)
        self.random_distribution = config.get('random_distribution', None)
        self.rbdadd_mons = config.get('rbdadd_mons')
        self.rbdadd_options = config.get('rbdadd_options', 'share')
        self.client_ra = config.get('client_ra', 128)
        self.datapoolname = "cbt-kernelcephfsfiodata"
        self.metadatapoolname = "cbt-kernelcephfsfiometadata"

        self.run_dir = '%s/cephfsfio/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.run_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.concurrent_procs), int(self.iodepth), self.mode)
        self.out_dir = '%s/cephfsfio/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.archive_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.concurrent_procs), int(self.iodepth), self.mode)

        # Make the file names string
        self.names = ''
        for i in xrange(self.concurrent_procs):
            self.names += '--name=%s/cbt-kernelcephfsfio-`hostname -s`/cbt-kernelcephfsfio-%d ' % (self.cluster.mnt_dir, i)

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    def initialize(self): 
        super(CephFsFio, self).initialize()

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
        logger.info('Attempting to populating fio files...')
        pre_cmd = 'sudo %s --ioengine=%s --rw=write --numjobs=%s --bs=4M --size %dM %s > /dev/null' % (self.cmd_path, self.ioengine, self.numjobs, self.vol_size*0.9, self.names)
        common.pdsh(settings.getnodes('clients'), pre_cmd).communicate()

        return True


    def run(self):
        super(CephFsFio, self).run()

        # Set client readahead
        #self.set_client_param('read_ahead_kb', self.client_ra)

        # We'll always drop caches for rados bench
        self.dropcaches()

        monitoring.start(self.run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        time.sleep(5)
        out_file = '%s/output' % self.run_dir
        fio_cmd = 'sudo %s' % (self.cmd_path_full)
        fio_cmd += ' --rw=%s' % self.mode
        if (self.mode == 'readwrite' or self.mode == 'randrw'):
            fio_cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
        fio_cmd += ' --ioengine=%s' % self.ioengine
        if self.time is not None:
            fio_cmd += ' --runtime=%s' % self.time
        if self.ramp is not None:
            fio_cmd += ' --ramp_time=%s' % self.ramp
        fio_cmd += ' --numjobs=%s' % self.numjobs
        fio_cmd += ' --direct=1'
        fio_cmd += ' --bs=%dB' % self.op_size
        fio_cmd += ' --iodepth=%d' % self.iodepth
        if self.vol_size:
            fio_cmd += ' --size=%dM' % (int(self.vol_size) * 0.9)
        fio_cmd += ' --write_iops_log=%s' % out_file
        fio_cmd += ' --write_bw_log=%s' % out_file
        fio_cmd += ' --write_lat_log=%s' % out_file
        if 'recovery_test' in self.cluster.config:
            fio_cmd += ' --time_based'
        if self.random_distribution is not None:
            fio_cmd += ' --random_distribution=%s' % self.random_distribution
        fio_cmd += ' %s > %s' % (self.names, out_file)
        if self.log_avg_msec is not None:
            fio_cmd += ' --log_avg_msec=%s' % self.log_avg_msec
        logger.info('Running cephfs fio %s test.', self.mode)
        common.pdsh(settings.getnodes('clients'), fio_cmd).communicate()

        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def cleanup(self):
        super(CephFsFio, self).cleanup()
	common.pdsh(settings.getnodes('clients'), 'sudo umount %s/cbt-kernelcephfsfio-`hostname -s`' % self.cluster.mnt_dir).communicate()

    #def set_client_param(self, param, value):
	#Not needed because this is no longer a rbd benchmark
        #common.pdsh(settings.getnodes('clients'), 'find /sys/block/rbd* -exec sudo sh -c "echo %s > {}/queue/%s" \;' % (value, param)).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(CephFsFio, self).__str__())

    def mkimages(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        self.cluster.rmpool(self.datapoolname, self.pool_profile)
        self.cluster.rmpool(self.metadatapoolname, self.pool_profile)
        self.cluster.mkpool(self.datapoolname, self.pool_profile)
        self.cluster.mkpool(self.metadatapoolname, self.pool_profile)
	stdout, self.adminkeyerror = common.pdsh(settings.getnodes('head'), 'ceph-authtool /tmp/cbt/ceph/keyring -p').communicate()
	self.adminkey = stdout.split(':')[1]
	self.adminkey = self.adminkey.strip()
	common.pdsh(settings.getnodes('head'), 'ceph -c /tmp/cbt/ceph/ceph.conf fs new testfs %s %s' % (self.metadatapoolname, self.datapoolname)).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p -m0755 -- %s/cbt-kernelcephfsfio-`hostname -s`' % self.cluster.mnt_dir).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mount -t ceph %s %s/cbt-kernelcephfsfio-`hostname -s` -o name=admin,secret=%s' % (self.monaddr_mountpoint, self.cluster.mnt_dir, self.adminkey)).communicate()
        monitoring.stop()

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 fio').communicate()
