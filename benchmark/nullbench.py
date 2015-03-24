import subprocess
import common
import settings
import monitoring
import os

from cluster.ceph import Ceph
from benchmark import Benchmark

class Nullbench(Benchmark):

    def __init__(self, cluster, config):
        super(Nullbench, self).__init__(cluster, config)

    def initialize(self): 
        super(Nullbench, self).initialize()
        return True

    def run(self):
        super(Nullbench, self).run()
        
    def recovery_callback(self): 
        pass

    def __str__(self):
        super(Nullbench, self).__str__()
