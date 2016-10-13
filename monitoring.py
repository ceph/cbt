import common
import settings
import os
import logging

logger = logging.getLogger("cbt")

class MonitorException(Exception):
    def __init__(self, msg):
        super(Exception, self).__init__(msg)

# monitoring.py has become a base class
# that defines an API for adding new monitoring tools to CBT

class CBTMonitoring:
    def __init__(self, directory):
        self.directory = directory
        self.nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')
        self.settings = settings
        self.classname = self.__class__.__name__
        self.pdsh_threads = []  # start() method appends common.pdsh objects to this
        self.subdirectory = os.path.join(self.directory, self.classname)
        logger.info('monitor for class %s in directory %s' %
                    (self.classname, self.subdirectory))

    def __str__(self):
        return 'monitor class %s directory %s' % (
               self.__class__.__name__, self.directory)

    def start(self):
        common.pdsh(self.nodes, 'mkdir -p %s' % self.subdirectory, 
                    continue_if_error=False).communicate()

    def stop(self):
        for t in self.pdsh_threads:
            # reap any dead collection threads
            t.communicate()

    def postprocess(self, out_dir):
        d1 = os.path.basename(self.subdirectory)
        d2 = os.path.basename(os.path.dirname(self.subdirectory))
        copy_to_dir = os.path.join(os.path.join(out_dir, d2), d1)
        common.sync_files(self.subdirectory, copy_to_dir)


# launch monitor tools

def start(cbt_monitoring_list):
    if cbt_monitoring_list:  # if it's not None
      for m in cbt_monitoring_list:
        m.start()

# shut down monitor tools

def stop(cbt_monitoring_list):
    if cbt_monitoring_list:  # if it's not None
      for m in cbt_monitoring_list:
        m.stop()

# run any necessary postprocessing steps and copy files back to test driver

def postprocess(cbt_monitoring_list, out_dir):
    if cbt_monitoring_list:  # if it's not None
      for m in cbt_monitoring_list:
        m.postprocess(out_dir)

