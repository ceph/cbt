import common
import settings
import monitoring

# this module implements a collectl monitoring only
# using the monitoring.py base class

# edit this to change the set of block devices that will be monitored
# by collectl

rawdskfilt = 'cciss/c\d+d\d+ |hd[ab] | sd[a-z]+ |dm-\d+ |xvd[a-z] |fio[a-z]+ | vd[a-z]+ |emcpower[a-z]+ |psv\d+ |nvme[0-9]n[0-9]+p[0-9]+ '
collectl_cmd = 'collectl -s+mYZ -i 1:10 --rawdskfilt "%s" -F0 -f %s'

class monitor_collectl(monitoring.CBTMonitoring):
    def __init__(self, directory):
        #super(monitor_collectl, self).__init__(directory)
        monitoring.CBTMonitoring.__init__(self, directory)
        self.monitor_threads = None

    def start(self):
        monitoring.CBTMonitoring.start(self)
        self.monitor_threads = common.pdsh(self.nodes, 
                    collectl_cmd % (rawdskfilt, self.subdirectory), 
                    continue_if_error=False)

    def stop(self):
        common.pdsh(self.nodes, 'pkill -SIGINT -f collectl').communicate()
        self.monitor_threads.communicate()
        monitoring.CBTMonitoring.stop(self)

    def postprocess(self, out_dir):
        monitoring.CBTMonitoring.postprocess(self, out_dir)
