import common
import settings
import subprocess

def start(directory):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')
    collectl_dir = '%s/collectl' % directory
    perf_dir = '%s/perf' % directory
    blktrace_dir = '%s/blktrace' % directory

    # collectl
    common.pdsh(nodes, 'mkdir -p -m0755 -- %s' % collectl_dir).communicate()
    # don't block on this
    common.pdsh(nodes, 'collectl -s+mYZ -i 1:10 -F0 -f %s' % collectl_dir, continue_if_error=True)

    # perf
#    common.pdsh(nodes), 'mkdir -p -m0755 -- %s' % perf_dir).communicate()
#    common.pdsh(nodes), 'cd %s;sudo perf_3.6 record -g -f -a -F 100 -o perf.data' % perf_dir)

    # blktrace
#    common.pdsh(osds, 'mkdir -p -m0755 -- %s' % blktrace_dir).communicate()
#    for device in xrange (0,osds_per_node):
#        common.pdsh(osds, 'cd %s;sudo blktrace -o device%s -d /dev/disk/by-partlabel/osd-device-%s-data' % (blktrace_dir, device, device))



def stop(directory=None):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')
    for pattern in [ 'collectl', 'perf_3.6', 'blktrace' ]:
        common.pkill(nodes, 'INT', pattern)
    if directory:
        sc = settings.cluster
        common.pdsh(nodes, 'cd %s/perf;sudo chown %s.%s perf.data' % (directory, sc.get('user'), sc.get('user')))
        make_movies(directory)

def make_movies(directory):
    sc = settings.cluster
    seekwatcher = '/home/%s/bin/seekwatcher' % sc.get('user')
    blktrace_dir = '%s/blktrace' % directory

    for device in xrange (0,sc.get('osds_per_node')):
        common.pdsh(settings.getnodes('osds'), 'cd %s;%s -t device%s -o device%s.mpg --movie' % (blktrace_dir,seekwatcher,device,device), continue_if_error=True).communicate()

