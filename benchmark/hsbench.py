import common
import settings
import monitoring
import os
import logging
import pathlib
import client_endpoints_factory

from .benchmark import Benchmark

logger = logging.getLogger("cbt")


class Hsbench(Benchmark):

    def __init__(self, archive_dir, cluster, config):
        super(Hsbench, self).__init__(archive_dir, cluster, config)
        self.cmd_path = config.get('cmd_path', '/usr/local/bin/hsbench')
        self.tmp_conf = self.cluster.tmp_conf
        self.buckets = config.get('buckets', None)
        self.bucket_prefix = config.get('bucket_prefix', None)
        self.duration = config.get('duration', None)
        self.loop = config.get('loop', None)
        self.modes = config.get('modes', None)
        self.max_keys = config.get('max_keys', None)
        self.objects = config.get('objects', None)
        self.object_prefix = config.get('object_prefix', None)
        self.region = config.get('region', None)
        self.report_intervals = config.get('report_intervals', None)
        self.threads = config.get('threads', None)
        self.size = config.get('size', None)
        self.out_dir = self.archive_dir
        self.client_endpoints = config.get("client_endpoints", None)

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self):
        super(Hsbench, self).initialize()

        # Clean and Create the run directory
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

    def initialize_endpoints(self):
        super(Hsbench, self).initialize_endpoints()
        if self.client_endpoints is None:
            raise ValueError('No client_endpoints defined!')
        self.client_endpoints_object = client_endpoints_factory.get(self.cluster, self.client_endpoints)

        if not self.client_endpoints_object.get_initialized():
            self.client_endpoints_object.initialize()

        self.endpoint_type = self.client_endpoints_object.get_endpoint_type()
        self.endpoints_per_client = self.client_endpoints_object.get_endpoints_per_client()
        self.endpoints = self.client_endpoints_object.get_endpoints()

    def mkcredfiles(self):
        for i in range(0, len(self.auth_urls)):
            cred = "export ST_AUTH=%s\\nexport ST_USER=%s\\nexport ST_KEY=%s" % (self.auth_urls[i], self.subuser, self.key)
            common.pdsh(settings.getnodes('clients'), 'echo -e "%s" > %s/gw%02d.cred' % (cred, self.run_dir, i)).communicate()

    def run_command(self, ep_num):
        out_csv = '%s/output.%d.csv' % (self.run_dir, ep_num)
        out_json = '%s/output.%d.json' % (self.run_dir, ep_num)

        cmd = 'sudo %s' % self.cmd_path_full
        if self.buckets:
            cmd += ' -b %d' % self.buckets
        if self.bucket_prefix:
            cmd += ' -bp %s' % self.bucket_prefix
        if self.duration:
            cmd += ' -d %d' % self.duration
        if self.loop:
            cmd += ' -l %d' % self.loop
        if self.modes:
            cmd += ' -m %s' % self.modes
        if self.max_keys:
            cmd += ' -mk %d' % self.max_keys
        if self.objects:
            cmd += ' -n %d' % self.objects
        if self.object_prefix:
            cmd += ' -op %s' % self.object_prefix
        if self.region:
            cmd += ' -r %s' % self.region
        if self.report_intervals:
            cmd += ' -ri %s' % self.report_intervals
        if self.threads:
            cmd += ' -t %d' % self.threads
        if self.size:
            cmd += ' -z %s' % self.size
        cmd += ' -o %s' % out_csv
        cmd += ' -j %s' % out_json
        cmd += ' -s %s' % self.endpoints[ep_num % len(self.endpoints)]["secret_key"]
        cmd += ' -a %s' % self.endpoints[ep_num % len(self.endpoints)]["access_key"]
        cmd += ' -u %s' % self.endpoints[ep_num % len(self.endpoints)]["url"]

        return cmd

    def run(self):
        super(Hsbench, self).run()

        # We'll always drop caches
        self.dropcaches()

        # dump the cluster config
        self.cluster.dump_config(self.run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        monitoring.start(self.run_dir)
        logger.info('Running hsbench %s test.' % self.modes)
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

        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def recovery_callback(self):
        self.cleanup()

    def cleanup(self):
        cmd_name = pathlib.PurePath(self.cmd_path).name
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 %s' % cmd_name).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Hsbench, self).__str__())
