"""
This module handles starting/stopping the monitoring service provided by CBT in form of 'plugins'.
It can work with 'collectl', 'perf' and 'blktrace'.
It can also create movies from the blktrace data using 'seekwatcher' for visualization.
"""
import common
import settings

# start the monitoring stack
def start(directory):
    """Start collectl, perf and blktrace on all nodes in the given directory."""

    # get names of all nodes
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')
    # setup the directories to be used by each monitoring tool
    collectl_dir = '%s/collectl' % directory
    # perf_dir = '%s/perf' % directory
    # blktrace_dir = '%s/blktrace' % directory

    # collectl
    # a perl regex filter to acquire data of only drive/partition from /proc/diskstats that match
    # unmatched drive/part data is never collected
    # + tells it collectl to make this the default filter
    rawdskfilt = '+cciss/c\d+d\d+ |hd[ab] | sd[a-z]+ |dm-\d+ |xvd[a-z] |fio[a-z]+ | vd[a-z]+ |emcpower[a-z]+ |psv\d+ |nvme[0-9]n[0-9]+p[0-9]+ '
    # create the collectl directory in the remote hosts
    common.pdsh(nodes, 'mkdir -p -m0755 -- %s' % collectl_dir)
    # run collectl remotely with following parameters:
    # -s -> specify the subsystem to sample data from, by default this is 'cdn' (CPU, disk, network)
    # +mYZ makes it add subsystems, namely 'm->memory', 'Y->Slabs in detail', 'Z->Processes in detail'
    # -i -> interval of sampling 1:10 means 1sample-diskstats-etc/second and 1sample-processes/10seconds
    # --rawdskfilt -> already explained above
    # -F0 -> flush the output buffers immediately at each data sampling time
    # -f -> file to use for dumping output data in case of "Record Mode"
    common.pdsh(nodes, 'collectl -s+mYZ -i 1:10 --rawdskfilt "%s" -F0 -f %s' % (rawdskfilt, collectl_dir))

    # perf
    # create the directory to hold all the perf data
    # common.pdsh(nodes), 'mkdir -p -m0755 -- %s' % perf_dir).communicate()
    # remotely call perf with the following parameters:
    # record -> run command in 'record' mode for storing in 'perf.data'
    # -g -> enable call-graph; which is basically a node-edge based graph of procedure calls during the program
    # static call-graph -> list all possible patterns of procedure calls
    # dynamic call-graph -> list procedure calls of one run of a program (mostly used for profiling)
    # -f -> don't know what this means yet
    # -a -> profile all CPUs
    # -F -> frequency of profiling (probably samples/second)
    # -o -> output file name to dump data into
    # common.pdsh(nodes), 'cd %s;sudo perf_3.6 record -g -f -a -F 100 -o perf.data' % perf_dir)

    # blktrace
    # create the directory to hold the data
    # common.pdsh(osds, 'mkdir -p -m0755 -- %s' % blktrace_dir).communicate()
    # run blktrace on each node with arguments
    # -o -> set the output file base-name, this will become 'device0, device1' etc
    # actual format of file will be <base-name>.blk-trace.<cpu-num> in the current dir
    # -d -> device to trace the block requests
    # for device in xrange (0,osds_per_node):
    #     common.pdsh(osds, 'cd %s;sudo blktrace -o device%s -d /dev/disk/by-partlabel/osd-device-%s-data'
    #                 % (blktrace_dir, device, device))

# stop the monitoring stack
def stop(directory=None):
    """Stops the monitoring stack (collectl, perf, blktrace) on all nodes.
    Additionally 'chown's the blktrace directory to the ceph user if given as argument."""

    # get the names of all nodes
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')

    # killall kills processes by name, so we're sending 'SIGINT -> keyboard interrupt signal' to all instances of collectl
    common.pdsh(nodes, 'killall -SIGINT -f collectl').communicate()
    # same with perf
    common.pdsh(nodes, 'sudo pkill -SIGINT -f perf_3.6').communicate()
    # the same with blktrace
    common.pdsh(settings.getnodes('osds'), 'sudo pkill -SIGINT -f blktrace').communicate()
    
    # if a directory was given
    if directory:
        # get the cluster handle
        sc = settings.cluster
        # chown the blktrace data, so it can be used by 'seekwatcher' to make movies
        common.pdsh(nodes, 'cd %s/perf;sudo chown %s.%s perf.data' % (directory, sc.get('user'), sc.get('user')))
        make_movies(directory)

# visualize the blktrace data
def make_movies(directory):
    """ This visualizes the blktrace data in the given directory using 'seekwatcher' utility"""
    # if we're using an existing cluster, no need to make movies
    use_existing = settings.cluster.get('use_existing', True)
    if use_existing:
        return None
    
    # get the cluster 'handle'
    sc = settings.cluster

    # setup the seekwatcher directory
    seekwatcher = '/home/%s/bin/seekwatcher' % sc.get('user')

    # setup the blktrace directory
    blktrace_dir = '%s/blktrace' % directory

    # make 'movies' for all OSDs on all nodes
    for device in range(sc.get('osds_per_node')):
        # call the 'seekwatcher' on all remote nodes with parameters
        # -t -> blktrace file path
        # -o -> output file name
        # --movie -> generate an io movie
        common.pdsh(settings.getnodes('osds'), 'cd %s;%s -t device%s -o device%s.mpg --movie' %
                    (blktrace_dir, seekwatcher, device, device)).communicate()
