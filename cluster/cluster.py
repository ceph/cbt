import subprocess

import settings
import common

class Cluster(object):
    def __init__(self, config):
        self.config = config
        base_tmp = config.get('tmp_dir', '/tmp/cbt')
        self.base_dir = base_tmp
        self.mnt_dir = config.get('mnt_dir', "%s/%s" % (base_tmp, 'mnt'))
        self.tmp_dir = "%s/%s" % (base_tmp, config.get('clusterid'))
        self.archive_dir = "%s/%s" % (config.get('archive_dir'), config.get('clusterid'))

    def getclass(self):
        return self.__class__.__name__

    def initialize(self):
        pass

    def cleanup(self):
        pass

    def __str__(self):
        return str(self.config)
