import subprocess

import settings
import common
import monitoring

class Benchmark(object):
    def __init__(self, cluster, config):
        self.config = config
        self.cluster = cluster
#        self.cluster = Ceph(settings.cluster)
        self.archive_dir = "%s/%08d/%s" % (settings.cluster.get('archive_dir'), config.get('iteration'), self.getclass())
        self.run_dir = "%s/%08d/%s" % (settings.cluster.get('tmp_dir'), config.get('iteration'), self.getclass())
        self.osd_ra = config.get('osd_ra', 128)
        self.cmd_path = ''
        self.valgrind = config.get('valgrind', None)
        self.cmd_path_full = '' 
        if self.valgrind is not None:
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)


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
        print 'Setting OSD Read Ahead to: %s' % self.osd_ra
        self.cluster.set_osd_param('read_ahead_kb', self.osd_ra)
        print 'Cleaning existing temporary run directory: %s' % self.run_dir
        common.pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws'), 'sudo rm -rf %s' % self.run_dir)
        if self.valgrind is not None:
            print 'Adding valgrind to the command path.'
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)
        # Set the full command path
        self.cmd_path_full += self.cmd_path

    def cleanup(self):
        pass

    def dropcaches(self):
        nodes = settings.getnodes('clients', 'osds') 

        common.pdsh(nodes, 'sync')
        common.pdsh(nodes, 'echo 3 | sudo tee /proc/sys/vm/drop_caches')

    def __str__(self):
        return str(self.config)
