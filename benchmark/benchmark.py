import subprocess
import logging

import settings
import common
import monitoring
import hashlib
import os
import yaml
import json
import time

logger = logging.getLogger('cbt')

class Benchmark(object):
    def __init__(self):
        pass

    def load_config(self, cluster, config):
        self.config = config
        self.cluster = cluster
        self.config_dict = dict(benchmark=self.config.copy())
        self.config_dict.update(cluster=self.cluster.config.copy())
        # Use a 128-bit truncated sha256 to keep the DB from getting too big.
        bench_hash = hashlib.sha256(json.dumps(self.config_dict, sort_keys=True)).hexdigest()[:32]

        archive_results  = os.path.join(settings.general.get('archive_dir'), 'results')
        self.archive_dir = os.path.join(archive_results, str(bench_hash))

        tmp_results = os.path.join(settings.general.get('tmp_dir'), 'results')
        self.run_dir = os.path.join(tmp_results, str(bench_hash))

        self.osd_ra = config.get('osd_ra', None)
        self.cmd_path = ''
        self.valgrind = config.get('valgrind', None)
        self.cmd_path_full = '' 
        if self.valgrind is not None:
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)

        self.osd_ra_changed = False
        if self.osd_ra:
            self.osd_ra_changed = True
        else:
            self.osd_ra = common.get_osd_ra()

    def getclass(self):
        return self.__class__.__name__

    def reset_run_dir(self):
        # Wipe and create the run directory
        logger.debug('Cleaning and recreating: %s', self.run_dir)
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self):
        use_existing = settings.cluster.get('use_existing', True)
        if not use_existing:
            self.cluster.initialize()

    def dump_config(self):
        # Store the parameters of the test run
        logger.debug('Dumping benchmark_config.yaml to %s' % self.archive_dir)
        config_file = os.path.join(self.archive_dir, 'benchmark_config.yaml')
        if not os.path.exists(self.archive_dir):
            os.makedirs(self.archive_dir)
        if not os.path.exists(config_file):
            with open(config_file, 'w') as fd:
                yaml.dump(self.config_dict, fd, default_flow_style=False)

    def pre_run(self):
        self.reset_run_dir()
        self.dump_config()

        if self.osd_ra and self.osd_ra_changed:
            logger.info('Setting OSD Read Ahead to: %s', self.osd_ra)
            self.cluster.set_osd_param('read_ahead_kb', self.osd_ra)

        if self.valgrind is not None:
            logger.debug('Adding valgrind to the command path.')
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)

        # Set the full command path
        self.cmd_path_full += self.cmd_path

        # We'll always drop caches before running benchmarks 
        self.dropcaches()

        # dump the cluster config
        self.cluster.dump_config(self.run_dir)

        monitoring.start(self.run_dir)

        time.sleep(5)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

    def run(self):
        pass

    def post_run(self):
        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.archive_dir)


    def exists(self):
        if os.path.exists(self.archive_dir):
            logger.info('Skipping existing test in %s.', self.archive_dir)
            return True
        return False

    def cleanup(self):
        pass

    def dropcaches(self):
        nodes = settings.getnodes('clients', 'osds') 

        common.pdsh(nodes, 'sync').communicate()
        common.pdsh(nodes, 'echo 3 | sudo tee /proc/sys/vm/drop_caches').communicate()

    def __str__(self):
        return str(self.config)
