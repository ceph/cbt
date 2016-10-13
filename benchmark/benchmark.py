import os
import subprocess
import logging
import monitoring
import monitoring_factory
import settings
import common
import pprint

logger = logging.getLogger('cbt')

class Benchmark(object):
    def __init__(self, cluster, config):
        self.config = config
        self.cluster = cluster
#        self.cluster = Ceph(settings.cluster)
        self.archive_dir = "%s/%08d/%s" % (settings.cluster.get('archive_dir'), config.get('iteration'), self.getclass())
        self.run_dir = "%s/%08d/%s" % (settings.cluster.get('tmp_dir'), config.get('iteration'), self.getclass())
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

        # so fields are part of instance
        self.run_monitoring_list = None
        self.scrub_monitoring_list = None
        self.pool_monitoring_list = None

    def getclass(self):
        return self.__class__.__name__

    def initialize(self):
        logger.debug("benchmarks:\n    %s",
                     pprint.pformat(self.config, indent=8, width=132, depth=1))
        common.make_remote_dir(self.run_dir)
        use_existing = settings.cluster.get('use_existing', True)
        if not use_existing:
            self.cluster.initialize()

        # must re-init monitoring lists every iteration 
        # so that plug-ins can append to them
        config = self.config
        self.scrub_monitoring_list = monitoring_factory.factory(
                                      config.get('scrub_monitoring_list', ''),
                                      os.path.join(self.run_dir, 'scrub_monitoring'))
        self.pool_monitoring_list = monitoring_factory.factory(
                                      config.get('pool_monitoring_list', ''),
                                      os.path.join(self.run_dir, 'pool_monitoring'))


        # dump the cluster config
        self.cluster.dump_config(self.run_dir)

        self.cleanup()

    def run(self):

        # Create the run directory
        common.make_remote_dir(self.run_dir)

        self.run_monitoring_list = monitoring_factory.factory(
                                      self.config.get('run_monitoring_list', ''),
                                      os.path.join(self.run_dir, 'run_monitoring'))

        # drop Linux buffer cache and set up readahead if needed
        self.dropcaches()
        if self.osd_ra and self.osd_ra_changed:
            logger.info('Setting OSD Read Ahead to: %s', self.osd_ra)
            self.cluster.set_osd_param('read_ahead_kb', self.osd_ra)

        if self.valgrind is not None:
            logger.debug('Adding valgrind to the command path.')
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)
        # Set the full command path
        self.cmd_path_full += self.cmd_path

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    def cleanup(self):
        pass

    def do_initial_monitoring(self):
        if self.scrub_monitoring_list:
            logger.info('Running scrub monitoring.')
            monitoring.start(self.scrub_monitoring_list)
            self.cluster.check_scrub()
            monitoring.stop(self.scrub_monitoring_list)

    # no need to have every benchmark do this when we can do it all here

    def postprocess_all(self):
        self.cluster.dump_historic_ops(self.run_dir)
        monitoring.postprocess(self.run_monitoring_list, self.out_dir)
        if self.config.get('iteration') == 0: # Question: why scrub on iteration 0?
            for lst in [ self.scrub_monitoring_list, self.pool_monitoring_list ]:
                monitoring.postprocess(lst, self.out_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def dropcaches(self):
        nodes = settings.getnodes('clients', 'osds') 

        common.pdsh(nodes, 'sync').communicate()
        common.pdsh(nodes, 'echo 3 | sudo tee /proc/sys/vm/drop_caches').communicate()

    def __str__(self):
        return str(self.config)
