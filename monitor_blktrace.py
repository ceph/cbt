import common
import settings
import monitoring
blktrace_cmd = \
 'cd %s;sudo blktrace -o device%s -d /dev/disk/by-partlabel/osd-device-%s-data'

# this subclass of monitoring class invokes blktrace on ceph OSDs

class monitor_blktrace(monitoring.CBTMonitoring):

    def start(self):
        monitoring.CBTMonitoring.start(self)
        osds_per_node = int(self.settings.cluster.get('osds_per_node'))
        osds = self.settings.getnodes('osds')
        self.pdsh_threads = \
            [ common.pdsh( osds,
                           blktrace_cmd % (self.subdirectory, device, device)) \
              for device in range(0, osds_per_node) ]


    def stop(self):
        osds = self.settings.getnodes('osds')
        common.pdsh(osds, 'sudo pkill -SIGINT -f blktrace').communicate()
        for thrds in self.pdsh_threads:
            thrds.communicate()
        sc = self.settings.cluster
        u = sc.get('user')
        common.pdsh(osds, 
                    'sudo chown -R %s.%s %s' % (
                    u, u, self.subdirectory))
        monitoring.CBTMonitoring.stop(self)

    def postprocess(self, out_dir):
        sc = self.settings.cluster
        seekwatcher = '/home/%s/bin/seekwatcher' % sc.get('user')
        blktrace_dir = self.subdirectory
    
        for device in range(sc.get('osds_per_node')):
            common.pdsh(self.settings.getnodes('osds'), 
                        'cd %s;%s -t device%s -o device%s.mpg --movie' %
                        (blktrace_dir, seekwatcher, device, device)).communicate()
        monitoring.CBTMonitoring.postprocess(self, out_dir)
