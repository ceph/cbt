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

class RawFio(Benchmark):

    def __init__(self):
        super(RawFio, self).__init__()

    def load_config(self, cluster, config):
        super(RawFio, self).load_config(cluster, config)
        # comma-separated list of block devices to use inside the client host/VM/container
        self.block_device_list = config.get('block_devices', '/dev/vdb' )
        self.block_devices = [ d.strip() for d in self.block_device_list.split(',') ]
        self.concurrent_procs = config.get('concurrent_procs', len(self.block_devices))
        self.total_procs = self.concurrent_procs * len(settings.getnodes('clients').split(','))
        self.fio_out_format = "json"
        self.time =  str(config.get('time', '300'))
        self.ramp = str(config.get('ramp', '0'))
        self.startdelay = config.get('startdelay', None)
        self.rate_iops = config.get('rate_iops', None)
        self.iodepth = config.get('iodepth', 16)
        self.direct = config.get('direct', 1)
        self.numjobs = config.get('numjobs', 1)
        self.mode = config.get('mode', 'write')
        self.rwmixread = config.get('rwmixread', 50)
        self.rwmixwrite = 100 - self.rwmixread
        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.vol_size = config.get('vol_size', 65536) * 0.9
        self.fio_cmd = config.get('fio_cmd', 'sudo /usr/bin/fio')

    def initialize(self): 
        super(RawFio, self).initialize()
        common.pdsh(settings.getnodes('clients'),
                    'sudo rm -rf %s' % self.run_dir,
                    continue_if_error=False).communicate()
        common.make_remote_dir(self.run_dir)
        clnts = settings.getnodes('clients')
        logger.info('creating mountpoints...')

        logger.info('Attempting to initialize fio files...')
        initializer_list = []
        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            fiopath = b
            pre_cmd = 'sudo %s --rw=write -ioengine=%s --bs=%s ' % (self.fio_cmd, self.ioengine, self.op_size)
            pre_cmd = '%s --size %dM --name=%s --output-format=%s> /dev/null' % (
                       pre_cmd, self.vol_size, fiopath, self.fio_out_format)
            initializer_list.append(common.pdsh(clnts, pre_cmd,
                                    continue_if_error=False))
        for p in initializer_list:
             p.communicate()

        # Create the run directory
        common.pdsh(clnts, 'rm -rf %s' % self.run_dir,
                    continue_if_error=False).communicate()
        common.make_remote_dir(self.run_dir)

    def run(self):
        super(RawFio, self).run()
        # Set client readahead
        clnts = settings.getnodes('clients')

        # We'll always drop caches for rados bench
        self.dropcaches()

        monitoring.start(self.run_dir)

        time.sleep(5)

        logger.info('Starting raw fio %s test.', self.mode)

        fio_process_list = []
        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            fiopath = b
            out_file = '%s/output.%d' % (self.run_dir, i)
            fio_cmd = 'sudo %s' % self.fio_cmd
            fio_cmd += ' --rw=%s' % self.mode
            if (self.mode == 'readwrite' or self.mode == 'randrw'):
                fio_cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
            fio_cmd += ' --ioengine=%s' % self.ioengine
            fio_cmd += ' --runtime=%s' % self.time
            fio_cmd += ' --ramp_time=%s' % self.ramp
            if self.startdelay:
                fio_cmd += ' --startdelay=%s' % self.startdelay
            if self.rate_iops:
                fio_cmd += ' --rate_iops=%s' % self.rate_iops
            fio_cmd += ' --numjobs=%s' % self.numjobs
            fio_cmd += ' --direct=%s' % self.direct
            fio_cmd += ' --bs=%dB' % self.op_size
            fio_cmd += ' --iodepth=%d' % self.iodepth
            fio_cmd += ' --size=%dM' % self.vol_size 
            fio_cmd += ' --write_iops_log=%s' % out_file
            fio_cmd += ' --write_bw_log=%s' % out_file
            fio_cmd += ' --write_lat_log=%s' % out_file
            fio_cmd += ' --output-format=%s' % self.fio_out_format
            if 'recovery_test' in self.cluster.config:
                fio_cmd += ' --time_based'
            fio_cmd += ' --name=%s > %s' % (fiopath, out_file)
            logger.debug("FIO CMD: %s" % fio_cmd)
            fio_process_list.append(common.pdsh(clnts, fio_cmd, continue_if_error=False))
        for p in fio_process_list:
            p.communicate()
        monitoring.stop(self.run_dir)
        logger.info('Finished raw fio test')

        common.sync_files('%s/*' % self.run_dir, self.archive_dir)

    def cleanup(self):
         super(RawFio, self).cleanup()
         clnts = settings.getnodes('clients')

         logger.debug("Kill fio: %s" % clnts)
         common.pdsh(clnts, 'killall fio').communicate()
         time.sleep(3)
         common.pdsh(clnts, 'killall -9 fio').communicate()

    def set_client_param(self, param, value):
         cmd = 'find /sys/block/vd* ! -iname vda -exec sudo sh -c "echo %s > {}/queue/%s" \;' % (value, param)
         common.pdsh(settings.getnodes('clients'), cmd).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.archive_dir, super(RawFio, self).__str__())

    def recovery_callback(self):
        common.pdsh(settings.getnodes('clients'), 'sudo killall fio').communicate()

