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

    def start(self):
        monitoring.CBTMonitoring.start(self)
        self.pdsh_threads.append(
            common.pdsh(self.nodes, 
                collectl_cmd % (rawdskfilt, self.subdirectory)))

    def stop(self):
        common.pdsh(self.nodes, 'pkill -SIGINT -f collectl').communicate()
        monitoring.CBTMonitoring.stop(self)

