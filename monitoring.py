import common
import settings

class Monitoring(object):
    def __init__(self, mconfig):
        # the initializers should be the very single places interrogating
        # settings for the sake of explicitness
        nodes_list = mconfig.get('nodes', self._get_default_nodes())
        self.nodes = settings.getnodes(*nodes_list)

    @staticmethod
    def _get_all():
        for monitoring, mconfig in sorted(settings.monitoring_profiles.items()):
            yield Monitoring._get_object(monitoring, mconfig)

    @staticmethod
    def _get_object(monitoring, mconfig):
        if monitoring == 'collectl':
            return CollectlMonitoring(mconfig)
        if monitoring == 'perf':
            return PerfMonitoring(mconfig)
        if monitoring == 'blktrace':
            return BlktraceMonitoring(mconfig)

class CollectlMonitoring(Monitoring):
    def __init__(self, mconfig):
        super(CollectlMonitoring, self).__init__(mconfig)

        self.args = mconfig.get('args', '-s+mYZ -i 1:10 -F0 -f {collectl_dir} '
                                        '--rawdskfilt \"+cciss/c\d+d\d+ |hd[ab] | sd[a-z]+ |dm-\d+ |xvd[a-z] |fio[a-z]+ | vd[a-z]+ |emcpower[a-z]+ |psv\d+ |nvme[0-9]n[0-9]+p[0-9]+ \"')

    def start(self, directory):
        collectl_dir = '%s/collectl' % directory
        common.pdsh(self.nodes, 'mkdir -p -m0755 -- %s' % collectl_dir).communicate()
        common.pdsh(self.nodes, ['collectl', self.args.format(collectl_dir=collectl_dir)])

    def stop(self, directory):
        common.pdsh(self.nodes, 'killall -SIGINT -f collectl').communicate()

    @staticmethod
    def _get_default_nodes():
        return ['clients', 'osds', 'mons', 'rgws']

class PerfMonitoring(Monitoring):
    def __init__(self, mconfig):
        super(PerfMonitoring, self).__init__(mconfig)
        self.pid_dir = settings.cluster.get('pid_dir')
        self.user = settings.cluster.get('user')
        self.args_template = mconfig.get('args')

    def start(self, directory):
        perf_dir = '%s/perf' % directory
        common.pdsh(self.nodes, 'mkdir -p -m0755 -- %s' % perf_dir).communicate()

        # ${pid} will be handled by remote's sh
        perf_args = self.args_template.format(perf_dir=perf_dir, pid='${pid}')
        common.pdsh(self.nodes, ['for pid in `cat %s/osd.*.pid`;' % self.pid_dir,
                                    'do', 'perf %s &' % perf_args,
                                'done'])
    def stop(self, directory):
        common.pdsh(self.nodes, 'sudo pkill -SIGINT -f perf\ ').communicate()
        if directory:
            sc = settings.cluster
            common.pdsh(self.nodes, 'sudo chown {user}.{user} {dir}/perf/perf.data'.format(
                    user=self.user, dir=directory))
            common.pdsh(self.nodes, 'sudo chown {user}.{user} {dir}/perf/perf_stat.*'.format(
                    user=self.user, dir=directory))

    @staticmethod
    def _get_default_nodes():
        return ['osds']

class BlktraceMonitoring(Monitoring):
    def __init__(self, mconfig):
        super(BlktraceMonitoring, self).__init__(mconfig)
        self.osds_per_node = settings.cluster.get('osds_per_node')
        self.use_existing = settings.cluster.get('use_existing', True)
        self.user = settings.cluster.get('user')

    def start(self, directory):
        blktrace_dir = '%s/blktrace' % directory
        common.pdsh(self.nodes, 'mkdir -p -m0755 -- %s' % blktrace_dir).communicate()
        for device in range(0, self.osds_per_node):
            common.pdsh(self.nodes, 'cd %s;sudo blktrace -o device%s -d /dev/disk/by-partlabel/osd-device-%s-data'
                     % (blktrace_dir, device, device))

    def stop(self, directory):
        common.pdsh(self.nodes, 'sudo pkill -SIGINT -f blktrace').communicate()
        if directory and not self.use_existing:
            self._make_movies(directory)

    def _make_movies(self, directory):
        seekwatcher = '/home/%s/bin/seekwatcher' % self.user
        blktrace_dir = '%s/blktrace' % directory

        for device in range(self.osds_per_node):
            common.pdsh(self.nodes, 'cd %s;%s -t device%s -o device%s.mpg --movie' %
                        (blktrace_dir, seekwatcher, device, device)).communicate()

    @staticmethod
    def _get_default_nodes():
        return ['osds']

def start(directory):
    for m in Monitoring._get_all():
        m.start(directory)

def stop(directory=None):
    for m in Monitoring._get_all():
        m.stop(directory)
