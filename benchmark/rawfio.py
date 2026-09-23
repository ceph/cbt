import os
import re
import json
import hashlib
import common
import settings
from monitoring.monitoring_factory import MonitoringFactory
import time
import logging
from pathlib import Path

from .benchmark import Benchmark

logger = logging.getLogger("cbt")


class RawFio(Benchmark):

    def __init__(self, archive_dir, cluster, config):
        cbt_logger = logging.getLogger("cbt")
        original_level = cbt_logger.level
        # Suppress the spurious per-iodepth "Results dir" log from base class
        cbt_logger.setLevel(logging.WARNING)
        try:
            super(RawFio, self).__init__(archive_dir, cluster, config)
        finally:
            cbt_logger.setLevel(original_level if original_level != logging.NOTSET else logging.DEBUG)
        # Recompute archive_dir hash excluding iodepth so all iodepth permutations share same archive_dir
        config_without_iodepth = {k: v for k, v in self.config.items() if k not in ('iodepth', 'total_iodepth')}
        hashable = json.dumps(sorted(config_without_iodepth.items())).encode()
        digest = hashlib.sha1(hashable).hexdigest()[:8]
        # archive_dir mirrors the librbdfio layout: <base>/results/<iteration>/id-<hash>
        self.archive_dir = os.path.join(self._base_archive_directory,
                                        'results',
                                        '{:0>8}'.format(self.config.get('iteration')),
                                        'id-{}'.format(digest))
        # comma-separated list of block devices to use inside the client host/VM/container
        _block_devices = config.get('block_devices', '/dev/vdb')
        if isinstance(_block_devices, list):
            self.block_devices = [d.strip() for d in _block_devices]
        else:
            self.block_devices = [d.strip() for d in _block_devices.split(',')]
        self.block_device_list = ','.join(self.block_devices)
        self.concurrent_procs = config.get('concurrent_procs', len(self.block_devices))
        self.total_procs = self.concurrent_procs * len(settings.getnodes('clients').split(','))
        # Use json,normal so the output format matches librbdfio and parse() can extract
        # the JSON block correctly from the mixed output.
        self.fio_out_format = "json,normal"
        self.time = str(config.get('time', '300'))
        self.ramp = str(config.get('ramp', '0'))
        self.invalidate = config.get('invalidate', 0)
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
        self.vol_size = config.get('vol_size', 65536)
        self.fio_cmd = config.get('fio_cmd', 'sudo /usr/bin/fio')
        # FIXME there are too many permutations, need to put results in SQLITE3
        self.run_dir += (f'op_size-{int(self.op_size):08d}/'
                         f'concurrent_procs-{int(self.total_procs):03d}/'
                         f'{self.mode}/iodepth-{int(self.iodepth):03d}')
        # out_dir = archive_dir/mode/iodepth-NNN so that:
        #   1. Each iodepth run archives to its own subdir (no overwrite).
        #   2. The formatter receives archive_dir/mode as the io_pattern_dir
        #      and recurses into iodepth-NNN/ finding all json_output files.
        #   3. FIO._get_iodepth() reads the correct iodepth from the path.
        self.out_dir = os.path.join(self.archive_dir, self.mode,
                                    f'iodepth-{int(self.iodepth):03d}')
        logger.info("Results dir: %s", self.out_dir)

    def exists(self):
        marker = os.path.join(self._base_archive_directory, 'rawfio.initialized')
        if os.path.exists(marker):
            os.makedirs(self.out_dir, exist_ok=True)
            return True
        logger.info('rawfio exists returning False')
        return False

    def initialize(self):
        super(RawFio, self).initialize()
        marker = os.path.join(self._base_archive_directory, 'rawfio.initialized')
        open(marker, 'w').close()

    def prefill(self):
        clnts = settings.getnodes('clients')
        logger.info('Attempting to prefill fio devices...')
        initializer_list = []

        logger.info('%s', self.block_devices)

        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            fiopath = b
            pre_cmd = 'sudo %s --rw=write -ioengine=%s --numjobs=1 --bs=65536 ' % (self.fio_cmd, self.ioengine)
            pre_cmd += '--size %dM --invalidate=%s --name=%s --output-format=%s> /dev/null' % (
                self.vol_size, self.invalidate, fiopath, self.fio_out_format)

            initializer_list.append(common.pdsh(clnts, pre_cmd,
                                                continue_if_error=False))

        for p in initializer_list:
            p.communicate()

        # Recreate the run directory after prefill
        common.pdsh(clnts, 'rm -rf %s' % self.run_dir,
                    continue_if_error=False).communicate()
        common.make_remote_dir(self.run_dir)

    def run(self):
        super(RawFio, self).run()
        clnts = settings.getnodes('clients')

        # We'll always drop caches for rados bench
        self.dropcaches()

        MonitoringFactory.start(self.run_dir)

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
            if self.invalidate:
                fio_cmd += ' --invalidate=%s' % self.invalidate
            if self.startdelay:
                fio_cmd += ' --startdelay=%s' % self.startdelay
            if self.rate_iops:
                fio_cmd += ' --rate_iops=%s' % self.rate_iops
            fio_cmd += ' --numjobs=%s' % self.numjobs
            fio_cmd += ' --direct=%s' % self.direct
            fio_cmd += ' --bs=%dB' % self.op_size
            fio_cmd += ' --iodepth=%d' % self.iodepth
            if self.log_iops:
                fio_cmd += ' --write_iops_log=%s' % out_file
            if self.log_bw:
                fio_cmd += ' --write_bw_log=%s' % out_file
            if self.log_lat:
                fio_cmd += ' --write_lat_log=%s' % out_file
            if 'recovery_test' in self.cluster.config:
                fio_cmd += ' --time_based'
            fio_cmd += ' --output-format=%s' % self.fio_out_format
            fio_cmd += ' --name=%s > %s' % (fiopath, out_file)
            logger.debug("FIO CMD: %s" % fio_cmd)
            fio_process_list.append(common.pdsh(clnts, fio_cmd, continue_if_error=False))
        for p in fio_process_list:
            p.communicate()
        MonitoringFactory.stop(self.run_dir)
        logger.info('Finished raw fio test')

        common.sync_files('%s/*' % self.run_dir, self.out_dir)
        self.analyze(self.out_dir)

    def parse(self, out_dir):
        # rpdcp appends the client hostname to every synced file, turning
        # "output.0" into "output.0.hostname.domain".  We match both the
        # plain form (output.<digits>) and the suffixed form
        # (output.<digits>.<hostname>), then always write the output as the
        # plain "json_output.<digits>" so fio_common_output_wrapper can find it.
        archive_path = Path(out_dir)
        files_to_process = [
            f for f in archive_path.glob("**/output.*")
            if re.search(r"output\.\d+", str(f))
            and not re.search(r"output\.\d+_", str(f))  # exclude _bw/_iops/_lat etc.
        ]
        for file in files_to_process:
            # Extract the numeric index from the filename regardless of any
            # trailing hostname suffix, e.g. "output.7.soko07.front.sepia.ceph.com" -> "7"
            m = re.search(r"output\.(\d+)", file.name)
            if not m:
                continue
            index = m.group(1)
            output_file_name = f"{file.parent}/json_output.{index}"
            output_path = Path(output_file_name)
            found = False
            with file.open("r", encoding="utf-8") as input_file:
                with output_path.open("w", encoding="utf-8") as output_file:
                    for line in input_file.readlines():
                        if re.search("^{$", line):
                            # Write the opening brace and mark that we are inside the JSON block.
                            output_file.write(line)
                            found = True
                            continue
                        if re.search("^}$", line):
                            output_file.write(line)
                            found = False
                            break
                        if found:
                            output_file.write(line)

    def analyze(self, out_dir):
        logger.info('Convert results to json format.')
        self.parse(out_dir)

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
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(RawFio, self).__str__())

    def recovery_callback(self):
        common.pdsh(settings.getnodes('clients'), 'sudo killall fio').communicate()
