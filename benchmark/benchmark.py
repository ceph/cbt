import subprocess
import logging

import settings
import common
import monitoring

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


    def getclass(self):
        return self.__class__.__name__

    def initialize(self):
        self.cluster.cleanup()
        use_existing = settings.cluster.get('use_existing', True)
        if not use_existing:
            self.cluster.initialize()

        self.cleanup()
        # Create the run directory
        common.make_remote_dir(self.run_dir)

    def run(self):
        if self.osd_ra and self.osd_ra_changed:
            logger.info('Setting OSD Read Ahead to: %s', self.osd_ra)
            self.cluster.set_osd_param('read_ahead_kb', self.osd_ra)

        logger.debug('Cleaning existing temporary run directory: %s', self.run_dir)
        common.pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws'), 'sudo rm -rf %s' % self.run_dir).communicate()
        if self.valgrind is not None:
            logger.debug('Adding valgrind to the command path.')
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)
        # Set the full command path
        self.cmd_path_full += self.cmd_path

    def exists(self):
        return False

    def cleanup(self):
        pass

    def dropcaches(self):
        nodes = settings.getnodes('clients', 'osds') 

        common.pdsh(nodes, 'sync').communicate()
        common.pdsh(nodes, 'echo 3 | sudo tee /proc/sys/vm/drop_caches').communicate()

    def __str__(self):
        return str(self.config)
