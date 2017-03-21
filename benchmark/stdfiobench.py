import subprocess
import common
import settings
import monitoring
import os
import time
import string
import logging
from benchmark import Benchmark

logger = logging.getLogger("cbt")

class StdFioBench(Benchmark):

    def __init__(self, cluster, config):
        super(StdFioBench, self).__init__(cluster, config)
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.total_procs = self.concurrent_procs * len(settings.getnodes('clients').split(','))
        self.time =  str(config.get('run_time', '10'))
        self.ramp = str(config.get('ramp_time', '0'))
        self.iodepth = config.get('iodepth', 16)
        self.numjobs = config.get('numjobs', 1)
        self.mode = config.get('mode', 'write')
        self.rwmixread = config.get('rwmixread', 50)
        self.rwmixwrite = 100 - self.rwmixread
        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.vol_size = config.get('vol_size', 65536) * 0.9
        self.client_ra = config.get('client_ra', '128')
        self.fio_cmd = config.get('fio_path', '/usr/bin/fio')
        self.block_dev_name = config.get('block_device', 'vd')
        self.block_device = config.get('block_device', 'vd')
        self.mount_point_name = config.get('mount_point_name', '/mnt/stdfiobench')
        self.filesystem = config.get('filesystem', 'xfs')
        self.use_existing = config.get('use_existing', 'False')
        self.output_format = config.get('output_format', 'terse')

        # FIXME there are too many permutations, need to put results in SQLITE3 
        self.run_dir = '%s/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.run_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)
        self.out_dir = '%s/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.archive_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)

        self.names = ''
        for i in xrange(self.concurrent_procs):
            self.names += '--name=%s/`hostname -s`-0/cbt-stdfiobench-%d ' % (self.mount_point_name, i)
    
        self.block_dev_name = '/dev/' + self.block_dev_name

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    def initialize(self): 
        super(StdFioBench, self).initialize()
        for i in xrange(1):
             letter = string.ascii_lowercase[i+1]
	     if not self.use_existing:
               common.pdsh(settings.getnodes('clients'), 'sudo umount -f %s' % (self.block_dev_name)).communicate()
               common.pdsh(settings.getnodes('clients'), 'sudo mkfs.%s -f  %s' % (self.filesystem, self.block_dev_name)).communicate()
             common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p %s ' % (self.mount_point_name)).communicate()
             common.pdsh(settings.getnodes('clients'), 'sudo mount -t %s -o noatime %s %s' % (self.filesystem, self.block_dev_name, self.mount_point_name)).communicate()
             common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p %s/`hostname -s`-%d' % (self.mount_point_name, i)).communicate()

        # Create the run directory
        common.make_remote_dir(self.run_dir)

        # populate the fio files
        logger.info('Attempting to populating fio files...')
        pre_cmd = 'sudo %s --rw=write --ioengine=sync --numjobs=%s --bs=8M --size %dM %s > /dev/null ' % (self.fio_cmd, self.numjobs, self.vol_size, self.names)
        common.pdsh(settings.getnodes('clients'), pre_cmd).communicate()


    def run(self):
        super(StdFioBench, self).run()
        # Set client readahead
        self.set_client_param('read_ahead_kb', self.client_ra)

        # We'll always drop caches for rados bench
        self.dropcaches()

        monitoring.start(self.run_dir)

        time.sleep(5)
        out_file = '%s/output' % self.run_dir
        fio_cmd = 'sudo %s' % self.fio_cmd
        fio_cmd += ' --rw=%s' % self.mode
        if (self.mode == 'readwrite' or self.mode == 'randrw'):
            fio_cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
        fio_cmd += ' --ioengine=%s' % self.ioengine
        fio_cmd += ' --runtime=%s' % self.time
        fio_cmd += ' --ramp_time=%s' % self.ramp
        fio_cmd += ' --numjobs=%s' % self.numjobs
        fio_cmd += ' --direct=1'
        fio_cmd += ' --randrepeat=0'
        fio_cmd += ' --group_reporting'
        fio_cmd += ' --bs=%dB' % self.op_size
        fio_cmd += ' --iodepth=%d' % self.iodepth
        fio_cmd += ' --size=%dM' % self.vol_size 
        fio_cmd += ' --output-format=%s' % self.output_format
        if (self.output_format == 'normal'):
          fio_cmd += ' --write_iops_log=%s' % out_file 
          fio_cmd += ' --write_bw_log=%s' % out_file
          fio_cmd += ' --write_lat_log=%s' % out_file
        if 'recovery_test' in self.cluster.config:
            fio_cmd += ' --time_based'
        fio_cmd += ' %s > %s 2> %s/error_log' % (self.names, out_file, self.run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        logger.info('Running fio %s test.', self.mode)
        common.pdsh(settings.getnodes('clients'), fio_cmd).communicate()
     
        # FIO output Parsing logic
        if (self.output_format == 'terse'):
	  hostname = '`hostname -s`'
          parse_cmd = 'sudo sed "s/$/;%s;%s;%s;%s;%s;%s;%s/" ' % (hostname, self.mode, self.op_size, self.iodepth, self.numjobs, self.client_ra, self.concurrent_procs)
          parse_cmd += ' %s > %s/terse_output' % (out_file, self.run_dir )
          common.pdsh(settings.getnodes('clients'), parse_cmd).communicate()

        monitoring.stop(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def cleanup(self):
         super(StdFioBench, self).cleanup()
         common.pdsh(settings.getnodes('clients'), 'sudo rm -rf %s/`hostname -s`*' % (self.mount_point_name)).communicate()
         if not self.use_existing:
           common.pdsh(settings.getnodes('clients'), 'sudo umount -f %s' % (self.block_dev_name)).communicate()

    def set_client_param(self, param, value):
         cmd = 'sudo sh -c "echo %s > /sys/block/%s/queue/%s"' % (value, self.block_device, param)
         common.pdsh(settings.getnodes('clients'), cmd).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(StdFioBench, self).__str__())

    def recovery_callback(self):
        common.pdsh(settings.getnodes('clients'), 'sudo killall fio').communicate()

