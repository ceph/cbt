import time
import os
import common
import settings
import monitoring
from monitoring import CBTMonitoring

# this module is a subclass of the monitoring.py base class that
# runs the Linux perf tool

class monitor_perf(CBTMonitoring):
    def __init__(self, directory):
        CBTMonitoring.__init__(self, directory)
        self.perfrun_num = 0
        self.fn = ''

    def start(self):
        CBTMonitoring.start(self)
        self.perfrun_num += 1
        self.fn = os.path.join(self.subdirectory, 'perf.data.run%d' % self.perfrun_num)
        self.pdsh_threads.append(
            common.pdsh(self.nodes, 
                #'cd %s;sudo perf record -g -f -a -F 100 -o perf.data' % 
                'sudo perf record -g -a -F 100 -o %s' % self.fn))

    def stop(self):
        common.pdsh(self.nodes, 'sudo pkill -SIGINT -f perf').communicate()
        time.sleep(1)
        common.pdsh(self.nodes, 'sudo pkill -SIGKILL -f perf').communicate()
        time.sleep(1)

        # don't call superclass until it is shut down

        CBTMonitoring.stop(self)
        sc = self.settings.cluster
        u = sc.get('user')
        common.pdsh(self.nodes, 
                'sudo chown %s.%s %s' % (u, u, self.fn), 
                 continue_if_error=False)
