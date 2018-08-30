import subprocess
import common
import settings
import monitoring
import os
import logging

from benchmark import Benchmark

logger = logging.getLogger("cbt")

class RbdFio(Benchmark):

    def __init__(self, cluster, config):
        super(RbdFio, self).__init__()

    def load_config(self, cluster, config):
        super(RbdFio, self).load_config(cluster, config)

        # FIXME there are too many permutations, need to put results in SQLITE3
        self.cmd_path = config.get('cmd_path', '/usr/bin/fio')
        self.pool_profile = config.get('pool_profile', 'default')

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
        self.direct = config.get('direct', 1)
        self.poolname = "cbt-kernelrbdfio"

        # Make the file names string
        self.names = ''
        for i in xrange(self.concurrent_procs):
            self.names += '--name=%s/cbt-kernelrbdfio-`hostname -s`/cbt-kernelrbdfio-%d ' % (self.cluster.mnt_dir, i)

    def exists(self):
        if os.path.exists(self.archive_dir):
            logger.info('Skipping existing test in %s.', self.archive_dir)
            return True
        return False

    def initialize(self): 
        super(RbdFio, self).initialize()
        self.mkimages()
 
        # populate the fio files
        logger.info('Attempting to populating fio files...')
        size = self.vol_size * 0.9 / self.concurrent_procs
        pre_cmd = 'sudo %s --ioengine=%s --rw=write --numjobs=%s --bs=4M --size %dM %s > /dev/null' % (self.cmd_path, self.ioengine, self.numjobs, size, self.names)
        common.pdsh(settings.getnodes('clients'), pre_cmd).communicate()

        return True

    def pre_run(self):
        super(RbdFio, self).pre_run()

        # Set client readahead
        self.set_client_param('read_ahead_kb', self.client_ra)


    def run(self):
        self.pre_run()

        logger.info('Running rbd fio %s test.', self.mode)
        ps = []
        for i in xrange(self.concurrent_procs):
            p = common.pdsh(settings.getnodes('clients'), self.make_command(i))
            ps.append(p)
        for p in ps:
            p.wait()

        self.post_run()

    def make_command(self, i):
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
        fio_cmd += ' --direct=%s' % self.direct
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

        return fio_cmd

    def cleanup(self):
        super(RbdFio, self).cleanup()

    def set_client_param(self, param, value):
        common.pdsh(settings.getnodes('clients'), 'find /sys/block/rbd* -exec sudo sh -c "echo %s > {}/queue/%s" \;' % (value, param)).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.archive_dir, super(RbdFio, self).__str__())

    def mkimages(self):
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        self.cluster.rmpool(self.poolname, self.pool_profile)
        self.cluster.mkpool(self.poolname, self.pool_profile, 'rbd')
        common.pdsh(settings.getnodes('clients'), '/usr/bin/rbd create cbt-kernelrbdfio-`hostname -s` --size %s --pool %s' % (self.vol_size, self.poolname)).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo rbd map cbt-kernelrbdfio-`hostname -s` --pool %s --id admin' % self.poolname).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mkfs.xfs /dev/rbd/cbt-kernelrbdfio/cbt-kernelrbdfio-`hostname -s`').communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p -m0755 -- %s/cbt-kernelrbdfio-`hostname -s`' % self.cluster.mnt_dir).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mount -t xfs -o noatime,inode64 /dev/rbd/cbt-kernelrbdfio/cbt-kernelrbdfio-`hostname -s` %s/cbt-kernelrbdfio-`hostname -s`' % self.cluster.mnt_dir).communicate()
        monitoring.stop()

    def recovery_callback(self): 
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 fio').communicate()
