import common
import settings
import monitoring
from monitoring import CBTMonitoring

# this module is a subclass of the monitoring.py base class that
# runs the Linux perf tool

class monitor_perf(monitoring.CBTMonitoring):

    def start(self):
        monitoring.CBTMonitoring.start(self)
        self.pdsh_threads.append(
            common.pdsh(self.nodes, 
                #'cd %s;sudo perf record -g -f -a -F 100 -o perf.data' % 
                'cd %s;sudo perf record -g -a -F 100 -o perf.data' % 
                 self.subdirectory))

    def stop(self):
        monitoring.CBTMonitoring.stop(self)
        common.pdsh(self.nodes, 'sudo pkill -SIGINT -f perf').communicate()
        common.pdsh(self.nodes, 'sudo pkill -SIGINT -f perf').communicate()
        for t in self.pdsh_threads: t.communicate()
        sc = self.settings.cluster
        u = sc.get('user')
        common.pdsh(self.nodes, 
                'cd %s;sudo chown %s.%s perf.data' % (
                 self.subdirectory, u, u))

    def postprocess(self, out_dir):
        monitoring.CBTMonitoring.postprocess(self, out_dir)
