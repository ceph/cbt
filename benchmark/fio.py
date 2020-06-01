import common
import settings
import monitoring
import os
import time
import logging
import client_endpoints_factory

from .benchmark import Benchmark

logger = logging.getLogger("cbt")


class Fio(Benchmark):
    def __init__(self, archive_dir, cluster, config):
        super(Fio, self).__init__(archive_dir, cluster, config)

        # FIXME there are too many permutations, need to put results in SQLITE3
        self.cmd_path = config.get('cmd_path', '/usr/bin/fio')
        self.direct = config.get('direct', 1)
        self.time = config.get('time', None)
        self.time_based = bool(config.get('time_based', False))
        self.ramp = config.get('ramp', None)
        self.iodepth = config.get('iodepth', 16)
        self.numjobs = config.get('numjobs', 1)
        self.end_fsync = config.get('end_fsync', 0)
        self.mode = config.get('mode', 'write')
        self.rwmixread = config.get('rwmixread', 50)
        self.rwmixwrite = 100 - self.rwmixread
        self.logging = config.get('logging', True)
        self.log_avg_msec = config.get('log_avg_msec', None)
        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.size = config.get('size', 4096)
        self.procs_per_endpoint = config.get('procs_per_endpoint', 1)
        self.random_distribution = config.get('random_distribution', None)
        self.rate_iops = config.get('rate_iops', None)
        self.fio_out_format = "json,normal"
        self.prefill = config.get('prefill', True)
        self.norandommap = config.get("norandommap", False)
        self.out_dir = self.archive_dir
        self.client_endpoints = config.get("client_endpoints", None)
        self.recov_test_type = config.get('recov_test_type', 'blocking')

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    def initialize(self):
        super(Fio, self).initialize()

        # Clean and Create the run directory
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

    def initialize_endpoints(self):
        super(Fio, self).initialize_endpoints()

        # Get the client_endpoints and set them up
        if self.client_endpoints is None:
            raise ValueError('No client_endpoints defined!')
        self.client_endpoints_object = client_endpoints_factory.get(self.cluster, self.client_endpoints)

        # Create the recovery image based on test type requested
        if 'recovery_test' in self.cluster.config and self.recov_test_type == 'background':
            self.client_endpoints_object.create_recovery_image()
        else:
            self.create_endpoints()

    def create_endpoints(self):
        new_ep = False
        if not self.client_endpoints_object.get_initialized():
            self.client_endpoints_object.initialize()
            new_ep = True

        self.endpoint_type = self.client_endpoints_object.get_endpoint_type()
        self.endpoints_per_client = self.client_endpoints_object.get_endpoints_per_client()
        self.endpoints = self.client_endpoints_object.get_endpoints()

        # Error out if the aggregate fio size is going to be larger than the endpoint size
        aggregate_size = self.numjobs * self.procs_per_endpoint * self.size
        endpoint_size = self.client_endpoints_object.get_endpoint_size()
        if aggregate_size > endpoint_size:
            raise ValueError("Aggregate fio data size (%dKB) exceeds end_point size (%dKB)! Please check numjobs, procs_per_endpoint, and size settings." % (aggregate_size, endpoint_size))

        if self.endpoint_type == 'rbd' and self.ioengine != 'rbd':
            logger.warn('rbd endpoints must use the librbd fio engine! Setting ioengine=rbd')
            self.ioengine = 'rbd'
        if self.endpoint_type == 'rbd' and self.direct != '1':
            logger.warn('rbd endpoints must use O_DIRECT. Setting direct=1')
            self.direct = '1'

        # Prefill Data
        if new_ep and self.prefill:
            self.prefill_data()

    def fio_command_extra(self, ep_num):
        cmd = ''

        # typical directory endpoints
        if self.endpoint_type == 'directory':
            for proc_num in range(self.procs_per_endpoint):
                cmd += ' --name=%s/`%s`-%s-%s' % (self.endpoints[ep_num], common.get_fqdn_cmd(), ep_num, proc_num)

        # handle rbd endpoints with the librbbd engine.
        elif self.endpoint_type == 'rbd':
            pool_name, rbd_name = self.endpoints[ep_num].split("/")
            cmd += ' --clientname=admin'
            cmd += ' --pool=%s' % pool_name
            cmd += ' --rbdname=%s' % rbd_name
            cmd += ' --invalidate=0'
            for proc_num in range(self.procs_per_endpoint):
                rbd_name = '%s-%d' % (self.endpoints[ep_num], proc_num)
                cmd += ' --name=%s' % rbd_name
        return cmd

    def prefill_command(self, ep_num):
        cmd = 'sudo %s' % self.cmd_path
        cmd += ' --ioengine=%s' % self.ioengine
        cmd += ' --rw=write'
        cmd += ' --numjobs=%d' % self.numjobs
        cmd += ' --bs=4M'
        cmd += ' --size %dM' % self.size
        cmd += ' --output-format=%s' % self.fio_out_format
        cmd += self.fio_command_extra(ep_num)
        return cmd

    def prefill_data(self):
        # populate the fio files
        ps = []
        logger.info('Attempting to populating fio files...')
        for ep_num in range(self.endpoints_per_client):
            p = common.pdsh(settings.getnodes('clients'), self.prefill_command(ep_num))
            ps.append(p)
        for p in ps:
            p.wait()

    def run_command(self, ep_num):
        out_file = '%s/output.%d' % (self.run_dir, ep_num)

        # cmd_path_full includes any valgrind or other preprocessors vs cmd_path
        cmd = 'sudo %s' % self.cmd_path_full

        # IO options
        cmd += ' --ioengine=%s' % self.ioengine
        cmd += ' --direct=%s' % self.direct
        cmd += ' --bs=%dB' % self.op_size
        cmd += ' --iodepth=%d' % self.iodepth
        cmd += ' --end_fsync=%d' % self.end_fsync
        cmd += ' --rw=%s' % self.mode
        if (self.mode == 'readwrite' or self.mode == 'randrw'):
            cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
        if self.random_distribution is not None:
            cmd += ' --random_distribution=%s' % self.random_distribution
        if self.rate_iops is not None:
            cmd += ' --rate_iops=%d' % self.rate_iops
        if self.norandommap:
            cmd += ' --norandommap'

        # Set the output size
        if self.size:
            cmd += ' --size=%dM' % self.size
        cmd += ' --numjobs=%d' % self.numjobs

        # Time options
        if self.time is not None:
            cmd += ' --runtime=%d' % self.time
        if self.time_based is True:
            cmd += ' --time_based'
        if self.ramp is not None:
            cmd += ' --ramp_time=%d' % self.ramp

        # Put extra options before logging and output for conveneince of debugging
        cmd += self.fio_command_extra(ep_num)

        # Logging and output options
        if self.logging:
            cmd += ' --write_iops_log=%s' % out_file
            cmd += ' --write_bw_log=%s' % out_file
            cmd += ' --write_lat_log=%s' % out_file
            if self.log_avg_msec is not None:
                cmd += ' --log_avg_msec=%d' % self.log_avg_msec
        cmd += ' --output-format=%s' % self.fio_out_format

        # End the fio_cmd
        cmd += ' > %s' % (out_file)
        return cmd

    def run(self):
        super(Fio, self).run()

        # We'll always drop caches for rados bench
        self.dropcaches()

        # Create the run directory
        common.make_remote_dir(self.run_dir)

        # dump the cluster config
        self.cluster.dump_config(self.run_dir)

        time.sleep(5)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            if self.recov_test_type == 'blocking':
                recovery_callback = self.recovery_callback_blocking
            elif self.recov_test_type == 'background':
                recovery_callback = self.recovery_callback_background
            self.cluster.create_recovery_test(self.run_dir, recovery_callback, self.recov_test_type)

        if 'recovery_test' in self.cluster.config and self.recov_test_type == 'background':
            # Wait for signal to create the image & start client IO
            self.cluster.wait_start_io()
            self.create_endpoints()

        monitoring.start(self.run_dir)

        logger.info('Running fio %s test.', self.mode)
        ps = []
        for i in range(self.endpoints_per_client):
            p = common.pdsh(settings.getnodes('clients'), self.run_command(i))
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
        self.analyze(self.out_dir)

    def recovery_callback_blocking(self):
        common.pdsh(settings.getnodes('clients'), 'sudo killall -2 fio').communicate()

    def recovery_callback_background(self):
        logger.info('Recovery thread completed!')

    def analyze(self, out_dir):
        logger.info('Convert results to json format.')
        for client in settings.getnodes('clients').split(','):
            host = settings.host_info(client)["host"]
            for i in range(self.endpoints_per_client):
                found = 0
                out_file = '%s/output.%d.%s' % (out_dir, i, host)
                json_out_file = '%s/json_output.%d.%s' % (out_dir, i, host)
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

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Fio, self).__str__())
