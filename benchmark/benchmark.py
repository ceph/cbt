import subprocess

import settings
import common
import monitoring

class Benchmark(object):
    def __init__(self, config):
        self.config = config
        self.tmp_dir = "%s/%08d" % (settings.cluster.get('tmp_dir'), config.get('iteration'))
        self.archive_dir = "%s/%08d" % (settings.cluster.get('archive_dir'), config.get('iteration'))

    def initialize(self):
        if settings.cluster.get('rebuild_every_test', False):
            common.setup_ceph()

    def run(self):
        pass

    def cleanup(self):
        pass

    def dropcaches(self):
        nodes = common.get_nodes([settings.cluster.get('clients'), settings.cluster.get('servers')])
        common.pdsh(nodes, 'sync').communicate()
        common.pdsh(nodes, 'echo 3 | sudo tee /proc/sys/vm/drop_caches').communicate()

    def __str__(self):
        return str(self.config)
