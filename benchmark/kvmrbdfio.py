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

class KvmRbdFio(Benchmark):

    def __init__(self, cluster, config):
        self.super = super(KvmRbdFio, self)
        self.super.__init__(cluster, config)
        # comma-separated list of block devices to use inside the client host/VM/container
        self.block_device_list = config.get('block_devices', '/dev/vdb' )
        self.block_devices = [ d.strip() for d in self.block_device_list.split(',') ]
        self.concurrent_procs = config.get('concurrent_procs', len(self.block_devices))
        self.total_procs = self.concurrent_procs * len(settings.getnodes('clients').split(','))

        self.time =  str(config.get('time', '300'))
        self.ramp = str(config.get('ramp', '0'))
        self.startdelay = config.get('startdelay', None)
        self.rate_iops = config.get('rate_iops', None)
        self.iodepth = config.get('iodepth', 16)
        self.numjobs = config.get('numjobs', 1)
        self.mode = config.get('mode', 'write')
        self.rwmixread = config.get('rwmixread', 50)
        self.rwmixwrite = 100 - self.rwmixread
        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.pgs = config.get('pgs', 2048)
        self.vol_size = config.get('vol_size', 65536) * 0.9
        self.rep_size = config.get('rep_size', 1)
        self.rbdadd_mons = config.get('rbdadd_mons')
        self.rbdadd_options = config.get('rbdadd_options')
        self.client_ra = config.get('client_ra', '128')
        self.fio_cmd = config.get('fio_cmd', '/usr/bin/fio')
        # FIXME there are too many permutations, need to put results in SQLITE3 
        self.run_dir = '%s/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.run_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)
        self.out_dir = '%s/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.archive_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)

    def initialize(self): 
        self.super.initialize()
        clnts = settings.getnodes('clients')
        logger.info('creating mountpoints...')
        for b in self.block_devices:
            bnm = os.path.basename(b)
            mtpt = '/srv/rbdfio-`hostname -s`-%s' % bnm
            common.pdsh(clnts, 'sudo mkfs.ext4 %s' % b,
                        continue_if_error=False).communicate()
            common.pdsh(clnts, 'sudo mkdir -p %s' % mtpt,
                        continue_if_error=False).communicate()
            common.pdsh(clnts, 'sudo mount -t ext4 -o noatime %s %s' % (b,mtpt),
                        continue_if_error=False).communicate()
        logger.info('Attempting to initialize fio files...')
        initializer_list = []
        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            bnm = os.path.basename(b)
            mtpt = '/srv/rbdfio-`hostname -s`-%s' % bnm
            fiopath = os.path.join(mtpt, 'fio%d.img' % i)
            pre_cmd = 'sudo %s --rw=write -ioengine=sync --bs=4M ' % self.fio_cmd
            pre_cmd = '%s --size %dM --name=%s > /dev/null' % (
                       pre_cmd, self.vol_size, fiopath)
            initializer_list.append(common.pdsh(clnts, pre_cmd,
                                    continue_if_error=False))
        for p in initializer_list:
             p.communicate()

    def run(self):
        self.super.run()
        # Set client readahead
        self.set_client_param('read_ahead_kb', self.client_ra)
        clnts = settings.getnodes('clients')

        # We'll always drop caches for rados bench
        self.dropcaches()

        monitoring.start(self.run_monitoring_list)

        time.sleep(5)
        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        logger.info('Starting rbd fio %s test.', self.mode)

        fio_process_list = []
        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            bnm = os.path.basename(b)
            mtpt = '/srv/rbdfio-`hostname -s`-%s' % bnm
            fiopath = os.path.join(mtpt, 'fio%d.img' % i)
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
            fio_cmd += ' --direct=1'
            fio_cmd += ' --bs=%dB' % self.op_size
            fio_cmd += ' --iodepth=%d' % self.iodepth
            fio_cmd += ' --size=%dM' % self.vol_size 
            fio_cmd += ' --write_iops_log=%s' % out_file
            fio_cmd += ' --write_bw_log=%s' % out_file
            fio_cmd += ' --write_lat_log=%s' % out_file
            if 'recovery_test' in self.cluster.config:
                fio_cmd += ' --time_based'
            fio_cmd += ' --name=%s > %s' % (fiopath, out_file)
            fio_process_list.append(common.pdsh(clnts, fio_cmd, continue_if_error=False))
        for p in fio_process_list:
            p.communicate()
        monitoring.stop(self.run_monitoring_list)

        self.super.postprocess_all()

    def cleanup(self):
         self.super.cleanup()
         clnts = settings.getnodes('clients')
         common.pdsh(clnts, 'killall fio').communicate()
         time.sleep(3)
         common.pdsh(clnts, 'killall -9 fio').communicate()
         time.sleep(3)
         common.pdsh(clnts, 'rm -rf /srv/*/*',
                     continue_if_error=False).communicate()
         common.pdsh(clnts, 'sudo umount /srv/* || echo -n').communicate()

    def set_client_param(self, param, value):
         cmd = 'find /sys/block/vd* ! -iname vda -exec sudo sh -c "echo %s > {}/queue/%s" \;' % (value, param)
         common.pdsh(settings.getnodes('clients'), cmd).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, self.super.__str__())

    def recovery_callback(self):
        common.pdsh(settings.getnodes('clients'), 'sudo killall fio').communicate()

