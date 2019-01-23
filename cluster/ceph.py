"""
The biggest piece of code in the entire CBT framework!
"""

import subprocess
import common
import settings
import monitoring
import os
import time
import uuid
import threading
import logging

from cluster import Cluster

# acquire the pointer to the logger for logging
logger = logging.getLogger("cbt")

# return the SSH target string with given username and hostname
def sshtarget(user, host):
    """simply return a string with ssh-target"""
    h = host
    if user:
        h = '%s@%s' % (user, host)
    return h

# to bring an OSD up, this sequence of steps must be performed in this order
# but there are no cross-OSD dependencies so we can bring up multiple OSDs
# in parallel.

class OsdThread(threading.Thread):
    """A thread to bring up an OSD.
    The sequence in which these steps are performed is important.
    Since each OSD process is independent of the other, multiple OSDs can be brought up independently. 
    Hence, the option for multithreading."""

    # initialize the OSD thread with given data from the caller
    def __init__(self, cl_obj, devnumstr, osdnum, clusterid, host, osduuid, osddir):
        # initialize and give thread name using parent constructor
        threading.Thread.__init__(self, name='OsdThread-%d'%osdnum)

        # determine start time for timestamping? or perhaps duration measurement
        self.start_time = time.time()
        
        # response time from the disk?
        self.response_time = -1.0
        
        # cluster object on which to operate
        self.cl_obj = cl_obj

        # device number string
        self.devnumstr = devnumstr
        # osd number, simple integer
        self.osdnum = osdnum
        # UUID of cluster as created during the installation
        self.clusterid = clusterid
        # hostname on which the OSD process is going to run
        self.host = host
        # OSD UUID for ceph purposes
        self.osduuid = osduuid
        # directory on which the OSD data partition is going to be mounted
        self.osddir = osddir

        # the executable program to be run by this thread, initially NULL
        self.exc = None

    # all the action of the thread
    def run(self):
        """Run the OSD thread to bring up a certain OSD process.
        Do so by means of elevating the shell resource privileges, and then creating a new 
        keyring for this OSD Process as well.
        In addition, going to setup 'STDERR loggin' in a file in the given temp_dir to the program."""
        try:
            # get the keyring
            key_fn = '%s/keyring'%self.osddir
            # get the ceph.conf file from the cluster object (which is going to hold the location as provided in YAML)
            ceph_conf = self.cl_obj.tmp_conf
            # determine the SSH target string
            phost = sshtarget(settings.cluster.get('user'), self.host)
            # run the ceph command with pdsh and given parameters as:
            #   command string (probably 'ceph')
            #   config file (-c /ceph.conf)
            #   osd number (osd.%d)
            #   host (host=)
            #   along with some default crush values, ensuring failure domains
            common.pdsh(phost, 'sudo %s -c %s osd crush add osd.%d 1.0 host=%s rack=localrack root=default' % (self.cl_obj.ceph_cmd, ceph_conf, self.osdnum, self.host)).communicate()
            # setting the shell upper limits to increase shell resources
            # n -> number of concurrent open file descriptors
            # c -> maximum size of core files created
            # exec -> replaces this shell process with the program given with the 'command' as the arguments of the program
            # in our case, we're creating a new OSD process, and replacing the shell by it           
            cmd='ulimit -n 16384 && ulimit -c unlimited && exec %s -c %s -i %d --mkfs --mkkey --osd-uuid %s' % (self.cl_obj.ceph_osd_cmd, ceph_conf, self.osdnum, self.osduuid)
            # execute the made command through pdsh
            common.pdsh(phost, 'sudo sh -c "%s"' % cmd).communicate()
            # create a new keyring for this OSD
            common.pdsh(phost, 'sudo %s -c %s -i %s auth add osd.%d osd "allow *" mon "allow profile osd" mgr "allow"' % (self.cl_obj.ceph_cmd, ceph_conf, key_fn, self.osdnum)).communicate()
 
            # Start the OSD
            pidfile="%s/ceph-osd.%d.pid" % (self.cl_obj.pid_dir, self.osdnum)
            cmd = '%s -c %s -i %d --pid-file=%s' % (self.cl_obj.ceph_osd_cmd, ceph_conf, self.osdnum, pidfile)
            # if valgrind 'debugging' is enabled
            if self.cl_obj.osd_valgrind:
                # setup the valgrind setup
                cmd = common.setup_valgrind(self.cl_obj.osd_valgrind, 'osd.%d' % self.osdnum, self.cl_obj.tmp_dir) + ' ' + cmd
            else:
                # otherwise, simply run the given command directly
                cmd = '%s %s' % (self.cl_obj.ceph_run_cmd, cmd)
            # create a new file to act as STDERR stream, to dump all the errors into
            stderr_file = "%s/osd.%d.stderr" % (self.cl_obj.tmp_dir, self.osdnum)
            # run the shell with elevated resources just like before, but this time, redirect the STDERR to the given file descriptor
            # which in our case is attached to a regular file, hence this will act as sort of a 'log' for the OSD Process that's going
            # to run on the host using pdsh
            common.pdsh(phost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s 2> %s"' % (cmd, stderr_file)).communicate()
        except Exception as e:
            # handle any exceptions that might occur, this can be a file descriptor problem, a pdsh problem, anything
            self.exc = e
        finally:
            # if there's an exception or not, always determine the response time of bringing up this OSD Process
            self.response_time = time.time() - self.start_time

    # print this thread object's "ID" as a string, standard procedure
    def __str__(self):
        """Standard function to print object as a string"""
        return 'osd thrd %d %s %s'%(self.osdnum, self.host, self.osduuid)

    # this is intended to be called by parent thread after join()
    def postprocess(self):
        """To be called after parent thread join().
        Handles exception by logging it in the CBT logger, and updates information in the logger as well,
        regarding the process duration."""
        # If an exception was thrown by the 'run' function, log it here in the 'cbt' logger
        if not (self.exc is None):
            logger.error('thread %s: %s' % (self.name, str(self.exc)))
            raise Exception('OSD %s creation did not complete' % self.osdnum)
        # add consequent extra information as required
        logger.info('thread %s completed creation of OSD %d elapsed time %f'%(self.name, self.osdnum, self.response_time))

# This class basically builds a whole cluster from scratch, get's it up and running to start
# performing benchmarking immediately. This code also serves as a 'stencil' for making a cluster
# ready fo benchmarking even if we're using an existing cluster.

class Ceph(Cluster):
    """A particular instance of the given cluster in the YAML file to perform benchmarking onto."""
    def __init__(self, config):
        """Good old constructor"""

        # call the Cluster constructor and initialize all the config file related parameters
        super(Ceph, self).__init__(config)

        # setting up cluster parameters, in case they dont' exist, set them to the given parameters
        # time to wait for before checking cluster health, this let's the new settings take affect, 'settle down'
        self.health_wait = config.get('health_wait', 5)

        # the daemon executables to run 
        self.ceph_osd_cmd = config.get('ceph-osd_cmd', '/usr/bin/ceph-osd')
        self.ceph_mon_cmd = config.get('ceph-mon_cmd', '/usr/bin/ceph-mon')
        self.ceph_run_cmd = config.get('ceph-run_cmd', '/usr/bin/ceph-run')
        self.ceph_rgw_cmd = config.get('ceph-rgw_cmd', '/usr/bin/radosgw')
        self.ceph_mgr_cmd = config.get('ceph-mgr_cmd', '/usr/bin/ceph-mgr')
        self.radosgw_admin_cmd = config.get('radosgw-admin_cmd', '/usr/bin/radosgw-admin')
        self.ceph_cmd = config.get('ceph_cmd', '/usr/bin/ceph')
        self.rados_cmd = config.get('rados_cmd', '/usr/bin/rados')
        self.rbd_cmd = config.get('rbd_cmd', '/usr/bin/rbd')

        # create required directories in the given temp directory
        self.log_dir = config.get('log_dir', "%s/log" % self.tmp_dir)
        self.pid_dir = config.get('pid_dir', "%s/pid" % self.tmp_dir)
        self.core_dir = config.get('core_dir', "%s/core" % self.tmp_dir)
        self.monitoring_dir = "%s/monitoring" % self.tmp_dir
        
        # create required filenames needed for the cluster
        self.keyring_fn = "%s/keyring" % self.tmp_dir
        self.osdmap_fn = "%s/osdmap" % self.tmp_dir
        self.monmap_fn = "%s/monmap" % self.tmp_dir

        # other control parameters
        self.use_existing = config.get('use_existing', True)

        # deprecated work around to use bluestore with a filestore cluster, for more details see
        # http://lists.ceph.com/pipermail/cbt-ceph.com/2016-April/000142.html
        self.newstore_block = config.get('newstore_block', False)

        self.version_compat = config.get('version_compat', '')
        
        # these parameters control parallel OSD build 
        self.ceph_osd_online_rate = config.get('osd_online_rate', 10)
        self.ceph_osd_online_tmo = config.get('osd_online_timeout', 120)
        self.ceph_osd_parallel_creates = config.get('osd_parallel_creates')

        # If making the cluster, use the ceph.conf file distributed by initialize to the tmp_dir
        self.tmp_conf = '%s/ceph.conf' % self.tmp_dir
        # If using an existing cluster, defualt to /etc/ceph/ceph.conf
        if self.use_existing:
            self.tmp_conf = '/etc/ceph/ceph.conf'

        # other control parameters
        self.osd_valgrind = config.get('osd_valgrind', None)
        self.mon_valgrind = config.get('mon_valgrind', None)
        self.rgw_valgrind = config.get('rgw_valgrind', None)
        self.mgr_valgrind = config.get('mgr_valgrind', None)
        self.tiering = config.get('tiering', False)
        self.ruleset_map = {}
        self.cur_ruleset = 1
        self.idle_duration = config.get('idle_duration', 0)
        self.use_existing = config.get('use_existing', True)

        # thread controllers
        self.stoprequest = threading.Event()
        self.haltrequest = threading.Event()

        # list of all the auth URLs to be used by RGW
        self.auth_urls = []
        self.osd_count = config.get('osds_per_node') * len(settings.getnodes('osds'))

    # initialize a cluster and get it up and running!
    def initialize(self):
        """Doing cluster init work. Cleaning up old stuff, creating new stuff like temp dirs.
        ceph.conf distribution on each node, writing new FSs to each OSD, starting all the daemons,
        disabling scrubbing etc etc."""
        # Reset the rulesets
        self.ruleset_map = {}
        self.cur_ruleset = 1

        # safety check to make sure we don't blow away an existing cluster!
        if self.use_existing:
             raise RuntimeError('initialize was called on an existing cluster! Avoiding touching anything.') 

        super(Ceph, self).initialize()

        # unmount any kernel rbd volumes
        self.rbd_unmount()

        # shutdown any old processes
        self.shutdown()

        # Cleanup old junk and create new junk
        self.cleanup()
        
        common.mkdir_p(self.tmp_dir)
        # create new directories for handling stuff on each node listed
        common.pdsh(settings.getnodes('head', 'clients', 'mons', 'osds', 'rgws', 'mds', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.tmp_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.pid_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.log_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.monitoring_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.core_dir).communicate()
        # distribute the ceph.conf file to each node on /etc/ceph/
        self.distribute_conf()

        # Set the core directory, this will store the coredump for each node in case there's a crash
        # the given format string is the format of the coredump which is to be used when 'dumping the core' to the disk
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds', 'mgrs'), 'echo "%s/core.%%e.%%p.%%h.%%t" | sudo tee /proc/sys/kernel/core_pattern' % self.tmp_dir).communicate()

        # Create the filesystems
        self.setup_fs()

        # Build the cluster
        monitoring.start('%s/creation' % self.monitoring_dir)
        self.make_mons()
        self.start_mgrs()
        self.make_osds()
        monitoring.stop()

        # Check Health
        monitoring.start('%s/initial_health_check' % self.monitoring_dir)
        self.check_health()
        monitoring.stop()

        # Disable scrub and wait for any scrubbing to complete 
        self.disable_scrub()
        # FIXME with no PGs, osd pg dump appears to hang now.

        # Disable this since it was a workaround for an old problem from the cuttlefish era.
        # self.check_scrub()

        # Make the crush and erasure profiles
        self.make_profiles()

        # Start any higher level daemons
        self.start_rgw()

        # Peform Idle Monitoring
        if self.idle_duration > 0:
            monitoring.start("%s/idle_monitoring" % self.monitoring_dir)
            time.sleep(self.idle_duration)
            monitoring.stop()

        return True

    # kill all benchmarking processes on all nodes
    def shutdown(self):
        """Kills all processes including: 
        \nValgrind modes
        \nCeph daemons (mons, osds, mds, mgr, rgw)
        \nPDSH instances
        In addition, stop all the monitoring stack(collectl, perf, blktrace) as well."""

        # get all the node names in a list
        nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds', 'mgrs')

        # send signal 9 (kill) to all the processes which need to be terminated
        common.pdsh(nodes, 'sudo killall -9 massif-amd64-li').communicate()
        common.pdsh(nodes, 'sudo killall -9 memcheck-amd64-').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-osd').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-mon').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-mds').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-mgr').communicate()
        common.pdsh(nodes, 'sudo killall -9 rados').communicate()
        common.pdsh(nodes, 'sudo killall -9 rest-bench').communicate()
        common.pdsh(nodes, 'sudo killall -9 radosgw').communicate()
        common.pdsh(nodes, 'sudo killall -9 radosgw-admin').communicate()
        common.pdsh(nodes, 'sudo /etc/init.d/apache2 stop').communicate()
        common.pdsh(nodes, 'sudo killall -9 pdsh').communicate()
        monitoring.stop()

    # cleaning up when done
    def cleanup(self):
        """Get rid of all the data in the tmp_dir on all nodes"""
        # retrieve node names
        nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds', 'mgrs')
        # log before destruction
        logger.info('Deleting %s', self.tmp_dir)

        # add exception handling in case of very first test_run
        try:
            # attempt to delete previous stuff
            common.pdsh(nodes, 'sudo rm -rf %s' % self.tmp_dir).communicate()
        except OSError as e:
            # put a message on the console
            logger.warning("Exception in ceph.py @cleanup %s" %  e.message)

    # setting up filesystem on the OSD created
    def setup_fs(self):
        """Handle file system creation on the OSD nodes if not using existing node.
        In case of fs=tmpfs, nothing needs to be done.
        In case of zfs= zpool is used to manage all the pools and stuff using the underlying OSD partitions as vdevs, along 
        with setup of ZFS Intent Log (ZIL) cache.
        In case of anything else (probably xfs) populate a thread array to create osd-x on all OSD nodes in parallel.
        Also does logging on the CBT logger."""
        # determine if we need to use an existing cluster
        use_existing = settings.cluster.get('use_existing', True)
        # if we're using an existing cluster, no need to do any FS work, simply return
        if use_existing:
            return None

        # cluster on which to operate (just a dictionary of YAML data)
        sc = settings.cluster
        # fs tag which was given in the YAML for CBT
        fs = sc.get('fs')
        # get the filesystem options from the mkfs_opts tag of YAML
        mkfs_opts = sc.get('mkfs_opts', '')
        # get the mounting options from the mount_opts tag of YAML
        mount_opts = sc.get('mount_opts', '')

        # if a filesystem isn't specified, exit with a message
        if fs == '':
             settings.shutdown("No OSD filesystem specified.  Exiting.")

        # list to hold all the objects of pdsh which will do the filesystem setup stuff on all OSD nodes
        mkfs_threads = []

        # recursively create each OSD FS per node, the mount points are named sequentially 0->osds_per_node
        for device in xrange (0,sc.get('osds_per_node')):
            # get all the OSD nodes in the cluster
            osds = settings.getnodes('osds')
            # unmount any older osd on the node
            common.pdsh(osds, 'sudo umount /dev/disk/by-partlabel/osd-device-%s-data' % device).communicate()
            # remove all the older OSD data if it exists
            common.pdsh(osds, 'sudo rm -rf %s/osd-device-%s-data' % (self.mnt_dir, device)).communicate()
            # create a new mount-point for starting all the action
            common.pdsh(osds, 'sudo mkdir -p -m0755 -- %s/osd-device-%s-data' % (self.mnt_dir, device)).communicate()

            # if using a simple tmpfs in memory, no need for anything, go away with an empty thread list
            if fs == 'tmpfs':
                logger.info('using tmpfs osds, not creating a file system.')
            # if using the special Sun ZFS, going to be using the zpool manager to manage the creation/deletion of Filesystems
            # since ZFS combines filesystem as well as volume management, there's no need for an mkfs in such a scenario
            # the 'zpool' manager can be used to do all the work mkfs does. treating all the disks as a 'pool' and then creating 
            # filesystems as required on any of the underlying devices in the pool   
            elif fs == 'zfs':
                logger.info('ruhoh, zfs detected.  No mkfs for you!')
                # get rid of a previous osd pool if it exists
                common.pdsh(osds, 'sudo zpool destroy osd-device-%s-data' % device).communicate()
                # create a new pool of storage, the options are
                # -f -> force the addition of all vdevs even if there are conflicts and such
                # -O -> specify the filesystem attributes to give to the pool
                # xattr=sa -> specify 'system attributes' as xattr type. See man zfs "/xattr=on"
                # -m -> mount point of the pool to be 'legacy' this allows the filesystem to stay mounted to it's location
                # osd-device-%s-data -> is the name of the pool to be created
                # /dev/whatever -> is the vdev partition/device which is going to act as the 'data dump' for this pool
                common.pdsh(osds, 'sudo zpool create -f -O xattr=sa -m legacy osd-device-%s-data /dev/disk/by-partlabel/osd-device-%s-data' % (device, device)).communicate()
                # Add the vdevs to the newly created pool
                # osd-device-%s-data -> name of the pool
                # log -> the 'ZFS Intent Log' vdev, which is a special device used for caching writes in a ZFS system
                # *-zil -> is that partition/device which will act as this 'write cache' for the ZFS pool
                common.pdsh(osds, 'sudo zpool add osd-device-%s-data log /dev/disk/by-partlabel/osd-device-%s-zil' % (device, device)).communicate()
                # mount the newly created zfs on the mount point given
                common.pdsh(osds, 'sudo mount %s -t zfs osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, device, self.mnt_dir, device)).communicate()
            else: 
                # do mkfs and mount in 1 long command
                # alternative is to wait until make_osds to mount it
                mkfs_cmd='sudo sh -c "mkfs.%s %s /dev/disk/by-partlabel/osd-device-%s-data' % (fs, mkfs_opts, device)
                mkfs_cmd += '; mount %s -t %s /dev/disk/by-partlabel/osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, fs, device, self.mnt_dir, device)
                
                # newstore is simply 'bluestore', which requires a different structure of partitions in OSD mount points 
                # make a symlink for block if using newstore+block
                if self.newstore_block:
                    mkfs_cmd += ' ; sudo ln -s /dev/disk/by-partlabel/osd-device-%s-block %s/osd-device-%s-data/block' % (device, self.mnt_dir, device)
                mkfs_cmd += '"'

                # populate the threads array with pdsh objects for each osd on all nodes
                # since each node is going to be having the same number of OSDs
                mkfs_threads.append((device, common.pdsh(osds, mkfs_cmd)))
        # do all the action on the osd nodes        
        for device, t in mkfs_threads:  # for tmpfs and zfs cases, thread list is empty
            logger.info('for device %d on all hosts awaiting mkfs and mount'%device)
            # run the pdsh thread, and wait for it to finish
            t.communicate()

    # distribute ceph.conf to all the ceph nodes
    def distribute_conf(self):
        """Distribute the ceph.conf file to each node in the cluster in /etc/ceph/ceph.conf.
        If the file already exists, make a backup of existing conf, and make a symlink of tmp/conf into /etc/ceph.
        If the file deosn't exist, copy over a version from the head node onto the clients."""
        # get all the node hostnames
        nodes = settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws')
        # get the conf_file location
        conf_file = self.config.get("conf_file")
        # do the basic logging
        logger.info("Distributing %s.", conf_file)

        # if a cluster needs to be created
        # create a directory if it doesn't exist
        common.pdsh(nodes, 'mkdir -p -m0755 /etc/ceph').communicate()
        # copy the conf file to each node
        common.pdcp(nodes, '', conf_file, self.tmp_conf).communicate()

        # if a cluster already exists
        # create a backup of existing file
        common.pdsh(nodes, 'sudo mv /etc/ceph/ceph.conf /etc/ceph/ceph.conf.cbt.bak').communicate()
        # create a symlink of the actual conf file into the temp directory
        common.pdsh(nodes, 'sudo ln -s %s /etc/ceph/ceph.conf' % self.tmp_conf).communicate()

    # create the MON daemons on all the given 'MON' nodes
    def make_mons(self):
        """Create a new MON daemon on each MON node mentioned in the YAML file.\n
        Create necessary keyrings, copy config files as well as the monmap. Finally, spawn the daemons."""
        # Build and distribute the keyring
        # Create a monitor keyring with the given filename and allow * on MONs
        common.pdsh(settings.getnodes('head'), 'ceph-authtool --create-keyring --gen-key --name=mon. %s --cap mon \'allow *\'' % self.keyring_fn).communicate()
        # Create an admin keyring with the given filename and allow * on all daemons
        common.pdsh(settings.getnodes('head'), 'ceph-authtool --gen-key --name=client.admin --set-uid=0 --cap mon \'allow *\' --cap osd \'allow *\' --cap mds \'allow *\' --cap mgr \'allow *\' %s' % self.keyring_fn).communicate()
        # Copy the new keyring from the ceph nodes back to the admin node as tmp file
        common.rscp(settings.getnodes('head'), self.keyring_fn, '%s.tmp' % self.keyring_fn).communicate()
        # Copy the new keyring from admin node to all the nodes as tmp file
        common.pdcp(settings.getnodes('mons', 'osds', 'rgws', 'mds', 'mgrs'), '', '%s.tmp' % self.keyring_fn, self.keyring_fn).communicate()


        # Build the monmap, retrieve it, and distribute it
        mons = settings.getnodes('mons').split(',')
        # create new mon map
        cmd = 'monmaptool --create --clobber'
        # get list of all mons hostnames from the YAML file
        monhosts = settings.cluster.get('mons')
        # log the list of all mon hosts
        logger.info(monhosts)
        # update the command to add a mon entry for each mon(daemon) on each node(monhost) along with an address.
        for monhost, mons in monhosts.iteritems():
           for mon, addr in mons.iteritems():
                cmd = cmd + ' --add %s %s' % (mon, addr)
        # print the monmap after creating it
        cmd = cmd + ' --print %s' % self.monmap_fn
        # send the command on all head nodes
        common.pdsh(settings.getnodes('head'), cmd).communicate()
        # backup the monmap on the head nodes
        common.rscp(settings.getnodes('head'), self.monmap_fn, '%s.tmp' % self.monmap_fn).communicate()
        # copy the backup monmaps to all mons
        common.pdcp(settings.getnodes('mons'), '', '%s.tmp' % self.monmap_fn, self.monmap_fn).communicate()

        # Build the ceph-mons
        # get the name of the 'ceph-user'
        user = settings.cluster.get('user')
        # iterate over all the mon nodes
        for monhost, mons in monhosts.iteritems():
            # if a user is defined, edit the 'hostname' accordingly
            if user:
                monhost = '%s@%s' % (user, monhost)
            # for each monitor node, 
            for mon, addr in mons.iteritems():
                # clear out older mon stuff in the temp_dir
                common.pdsh(monhost, 'sudo rm -rf %s/mon.%s' % (self.tmp_dir, mon)).communicate()
                # create a new temp dir
                common.pdsh(monhost, 'mkdir -p %s/mon.%s' % (self.tmp_dir, mon)).communicate()
                # run the monitor command on all nodes, same old sh resource escalation followed by 'exec' to run the MON daemon on each node
                common.pdsh(monhost, 'sudo sh -c "ulimit -c unlimited && exec %s --mkfs -c %s -i %s --monmap=%s --keyring=%s"' % (self.ceph_mon_cmd, self.tmp_conf, mon, self.monmap_fn, self.keyring_fn)).communicate()
                # copy the mon keyring to the remote MON nodes
                common.pdsh(monhost, 'cp %s %s/mon.%s/keyring' % (self.keyring_fn, self.tmp_dir, mon)).communicate()
            
        # Start the mons
        # Similar process as setting up mons, this time, need a pidfile to keep track of the mon PIDs for log analysis later on
        for monhost, mons in monhosts.iteritems():
            if user:
                monhost = '%s@%s' % (user, monhost)
            for mon, addr in mons.iteritems():
                pidfile="%s/%s.pid" % (self.pid_dir, monhost)
                cmd = 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s -c %s -i %s --keyring=%s --pid-file=%s"' % (self.ceph_mon_cmd, self.tmp_conf, mon, self.keyring_fn, pidfile)
                if self.mon_valgrind:
                    cmd = "%s %s" % (common.setup_valgrind(self.mon_valgrind, 'mon.%s' % monhost, self.tmp_dir), cmd)
                else:
                    cmd = '%s %s' % (self.ceph_run_cmd, cmd)
                common.pdsh(monhost, 'sudo %s' % cmd).communicate()

    # create OSD daemons and using multiple threads to get it done faster
    def make_osds(self):
        """Spawn the previously created OSD 'setups' into OSD daemons. Uses parallelism to allow faster bringing up of OSDS. """
        # 
        osdnum = 0
        # get necessary params form the YAML file
        osdhosts = settings.cluster.get('osds')
        clusterid = self.config.get('clusterid')
        user = settings.cluster.get('user')
        # list to keep all threads together
        thread_list = []

        # set up degree of OSD creation parallelism
        # log some required stuff, for OSD making process
        logger.info('OSD creation rate: < %d OSDs/sec , join timeout %d, parallel creates < %s'%(
                        self.ceph_osd_online_rate,
                        self.ceph_osd_online_tmo,
                        str(self.ceph_osd_parallel_creates)))

        # a predetermined value, to be able to control the process duration
        osd_online_interval = 1.0 / self.ceph_osd_online_rate
        # determine max thread count (each spawn of OSD on each node is one thread e-g)
        max_parallel_creates = settings.cluster.get('osds_per_node') * len(osdhosts)
        
        if self.ceph_osd_parallel_creates:
            max_parallel_creates = int(self.ceph_osd_parallel_creates)

        # build OSDs in parallel, except for "ceph osd create" command
        # which must be 1 at a time

        threads_finished = 0
        for host in osdhosts:
            for devnumstr in xrange(0, settings.cluster.get('osds_per_node')):            
                pdshhost = sshtarget(user, host)
                # Build the OSD
                osduuid = str(uuid.uuid4())
        #       osddir='/var/lib/ceph/osd/%s-%d'%(clusterid, osdnum)
                osddir='%s/osd-device-%s-data' % (self.mnt_dir, devnumstr)
                # create the OSD first, so we know what number it has been assigned.
                common.pdsh(pdshhost, 'sudo %s -c %s osd create %s' % (self.ceph_cmd, self.tmp_conf, osduuid)).communicate()
                # bring the OSD online in background while continuing to create OSDs in foreground
                thrd = OsdThread(self, devnumstr, osdnum, clusterid, host, osduuid, osddir)
                logger.info('starting creation of OSD %d ' % osdnum)
                thrd.start()
                thread_list.append(thrd)

                # only allow up to max_parallel_creates threads to be active
                active_thread_count = len(thread_list) - threads_finished
                if active_thread_count >= max_parallel_creates:
                    thrd = thread_list[threads_finished]
                    thrd.join(self.ceph_osd_online_tmo)
                    thrd.postprocess()
                    threads_finished += 1
                time.sleep(osd_online_interval) # don't flood Ceph with OSD commands
                osdnum += 1

        # wait for rest of them to finish
        for thrd in thread_list[threads_finished:]:
            # an exception is thrown if the thread failed, hopefully
            thrd.join(self.ceph_osd_online_tmo)
            thrd.postprocess()

    # create MGRs on all the given nodes 
    def start_mgrs(self):
        """Start the MGR daemons on the hosts. This also sets up the Valgrind setup on each MGR node if desired."""
        # get the required stuff form the cluster YAML file
        user = settings.cluster.get('user')
        mgrhosts = settings.cluster.get('mgrs')

        # if no MGRs listed (as in pre-luminous version), do nothing!
        if not mgrhosts:
            return

        # same old way of iterating over the given hosts
        for mgrhost, manager in mgrhosts.iteritems():
            for mgrname, mgrsettings in manager.iteritems():
                # initialize the mgr
                cmd = '%s -i %s' % (self.ceph_mgr_cmd, mgrname)
                # setup valgrind for the MGR nodes if required
                if self.mgr_valgrind:
                    cmd = "%s %s" % (common.setup_valgrind(self.mgr_valgrind, mgrname, self.tmp_dir), cmd)
                else:
                    cmd = "%s %s" % (self.ceph_run_cmd, cmd)
                # if a ceph-user was mentioned, edit the SSH command accordingly
                if user:
                    pdshhost = '%s@%s' % (user, mgrhost)
                # bombs away!
                # setup shell limits, exec to run the given command and acquire the shell process
                common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s"' % cmd).communicate()

    # create RGWS on all the given nodes
    def start_rgw(self):
        """Start RGW daemons on the given nodes."""

        # get the required stuff from YAML given
        user = settings.cluster.get('user')
        rgwhosts = settings.cluster.get('rgws')

        # if RGW setup isn't necessary, bail out
        if not rgwhosts:
            return

        # If we are starting rGW, make the RGW pools
        self.make_rgw_pools()

        # for each radosgateway node, just like we've been iterating over all the x nodes
        for rgwhost, gateways in rgwhosts.iteritems():
            for rgwname, rgwsettings in gateways.iteritems():
                # get the hostname and port number
                host = rgwsettings.get('host', rgwhost)
                port = rgwsettings.get('port', None)
                # get ssl certificate if using TLS/SSL
                ssl_certificate = rgwsettings.get('ssl_certificate', None)

                # Build the auth_url
                auth_url = "http://" if ssl_certificate is None else "https://"
                auth_url += host
                auth_url += ":7480" if port is None else ":%s" % port
                auth_url += "/auth/v1.0"
                self.auth_urls.append(auth_url)

                # set the rgw_frontends
                rgw_frontends = None
                # see if SSL was setup
                if ssl_certificate is not None:
                    rgw_frontends = "civetweb ssl_certificate=%s" % ssl_certificate
                # see if a port number was given
                if port is not None:
                    # default frontend is civetweb
                    if rgw_frontends is None:
                        rgw_frontends = "civetweb"
                    rgw_frontends += " port=%s" % port

                # form the basic RGW 'boot-up' command
                cmd = '%s -c %s -n %s --log-file=%s/rgw.log' % (self.ceph_rgw_cmd, self.tmp_conf, rgwname, self.log_dir)
                # if a custom supported list of rgw-frontends is given, append that to the command as well
                if rgw_frontends is not None:
                    cmd += " --rgw-frontends='%s'" % rgw_frontends
                # if valgrind needs to be setup in the RGW daemon executable, the valgrind part of the command
                # will precede all the other commands, this will allow valgrind logging of the running 'probed' process
                if self.rgw_valgrind:
                    cmd = "%s %s" % (common.setup_valgrind(self.rgw_valgrind, 'rgw.%s' % host, self.tmp_dir), cmd)
                else:
                    cmd = '%s %s' % (self.ceph_run_cmd, cmd)

                # see if a ceph-user was mentioned in the YAML
                if user:
                    pdshhost = '%s@%s' % (user, rgwhost)
                
                # bombs away!
                common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s"' % cmd).communicate()

                # set min_size of pools to 1, when there is only one osd
                num_osds = len(settings.cluster.get('osds'))
                rgw_default_pools = ['.rgw.root', 'default.rgw.control', 'default.rgw.meta', 'default.rgw.log']
                pool_min_repl_size = 1

                if num_osds == 1:
                    # let the daemon breath in for a second, and setup
                    time.sleep(5)
                    for pool in rgw_default_pools:
                        common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s min_size %d' % (self.ceph_cmd, self.tmp_conf, pool, pool_min_repl_size),
                        continue_if_error=False).communicate()
                        # let the daemon breath out now :D
                        time.sleep(5)

    # disable scrubbing as a prereq to the benchmark setup
    def disable_scrub(self):
        """Disable scrubbing in the cluster to minimize the amount of background\n
        'reconstruction' traffic during the benchmarking process."""
        common.pdsh(settings.getnodes('head'), "ceph osd set noscrub; ceph osd set nodeep-scrub").communicate()

    # check the cluster health, proceed with deeper checks, and output in log if needed
    def check_health(self, check_list=None, logfile=None):
        """Check cluster health, give output to the given logfile.\n
        Can give a check_list to check as well, for only specific statuses like unclean, inactive etc."""
        # Wait for a defined amount of time in case ceph health is delayed
        time.sleep(self.health_wait)
        logline = ""
        if logfile:
            logline = "| tee -a %s" % logfile
        
        # probably measuring the number of 'ticks' the cluster needs to settle down
        ret = 0

        # Match any of these things to continue checking health
        check_list = ["degraded", "peering", "recovery_wait", "stuck", "inactive", "unclean", "recovery", "stale"]
        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), '%s -c %s health %s' % (self.ceph_cmd, self.tmp_conf, logline)).communicate()
            # if state is not mentioned in the checklist, we don't need to check it
            if check_list and not any(x in stdout for x in check_list):
                break
            # if state if OK, we don't need to check it either
            if "HEALTH_OK" in stdout:
                break
            else:
                ret = ret + 1
            # keep logging stuff as well
            logger.info("%s", stdout)
            # probe every second
            time.sleep(1)
        return ret

    # probe node to determine if they're scrubbing, wait until they finish
    def check_scrub(self):
        """Probe the nodes every second to determine if any scrubbing is going on, don't stop until they're all finished."""
        logger.info('Waiting until Scrubbing completes...')
        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), '%s -c %s pg dump | cut -f 16 | grep "0.000000" | wc -l' % (self.ceph_cmd, self.tmp_conf)).communicate()
            if " 0\n" in stdout:
                break
            else:
                logger.info(stdout)
            time.sleep(1)

    # dump the config of the admin daemon given the admin-socket
    def dump_config(self, run_dir):
        """Use the given admin socket (asok) to dump the configuration to a file in the given run_dir"""
        common.pdsh(settings.getnodes('osds'), 'sudo %s -c %s --admin-daemon /var/run/ceph/ceph-osd.0.asok config show > %s/ceph_settings.out' % (self.ceph_cmd, self.tmp_conf, run_dir)).communicate()

    # dump the historic ops given the admin socket
    def dump_historic_ops(self, run_dir):
        """Dump the historic ops using admin socket in a dump file in the given run_dir"""
        common.pdsh(settings.getnodes('osds'), 'find "/var/run/ceph/ceph-osd*.asok" -maxdepth 1 -exec sudo %s --admin-daemon {} dump_historic_ops \; > %s/historic_ops.out' % (self.ceph_cmd, run_dir)).communicate()

    # setup the disk parameters for all OSD disks on all OSD nodes
    def set_osd_param(self, param, value):
        """
        This command works as follows:
        - Find the osd 'data' disks in /dev/disk/by-partlabel/, these will be symlinks
        - 'Readlink' to resolve those names to get the actual locations
        - Parse the output to get only the disk name string like 'sda1', 'sda2' etc
        - Remove the number from the end
        - xargs is used to 'execute arguments' to it
        - xargs -I option performs 'replace_str', in our case, we're replacing the drive 'sda' etc
        - xargs will then execute the rest of the thing with the {} replaced by the string given
          to it in stdin, which in our case was say 'sda'
        - This makes the final command as a shell command which is doing the basic parameter
        - Writing 'value' to the /sys/block/<drive letter from xargs>/queue/<parameter>
        """        
        common.pdsh(settings.getnodes('osds'), 'find /dev/disk/by-partlabel/osd-device-*data -exec readlink {} \; | cut -d"/" -f 3 | sed "s/[0-9]$//" | xargs -I{} sudo sh -c "echo %s > /sys/block/\'{}\'/queue/%s"' % (value, param))

    # cool error handling
    def __str__(self):
        """Cool way of error handling, I guess"""
        return "foo"

    # create a recovery test thread and start it
    def create_recovery_test(self, run_dir, callback):
        """Create a 'RecoveryTestThread' and run it with the given callback."""
        rt_config = self.config.get("recovery_test", {})
        rt_config['run_dir'] = run_dir
        # create a new thread to start
        self.rt = RecoveryTestThread(rt_config, self, callback, self.stoprequest, self.haltrequest)
        self.rt.start()

    # let all the spawned recovery threads to finish up
    def wait_recovery_done(self):
        """"Wait for all the recovery threads to finish up. Setup the 'stop req event'"""
        self.stoprequest.set()
        while True:
            threads = threading.enumerate()
            if len(threads) == 1: break
            self.rt.join(1)

    # FIXME: This is a total hack that assumes there is only 1 existing ruleset!
    # Will change pending a fix for http://tracker.ceph.com/issues/8060
    def set_ruleset(self, name):
        name = str(name)
        if name in self.ruleset_map:
            raise Exception('A rule named %s already exists!' % name)
        self.ruleset_map[name] = self.cur_ruleset
        self.cur_ruleset += 1

    # get a ruleset from a rule_set map
    def get_ruleset(self, name):
        """Get the given ruleset from the ruleset_map"""
        name = str(name)
        # log the ruleset map
        logger.info("%s", self.ruleset_map)
        return self.ruleset_map[name]

    # make new CRUSH profiles if necessary
    def make_profiles(self):
        """Creates CRUSH and EC pool profiles. In the cluster reading information from the YAML file."""
        # get the crush_profiles from the config, if defined
        crush_profiles = self.config.get('crush_profiles', {})

        # create the profile for each profile CRUSH profile name given in the list
        for name,profile in crush_profiles.items():
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush add-bucket %s-root root' % (self.ceph_cmd, self.tmp_conf, name)).communicate()
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush add-bucket %s-rack rack' % (self.ceph_cmd, self.tmp_conf, name)).communicate()
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush move %s-rack root=%s-root' % (self.ceph_cmd, self.tmp_conf, name, name)).communicate()
            # FIXME: We need to build a dict mapping OSDs to hosts and create a proper hierarchy!
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush add-bucket %s-host host' % (self.ceph_cmd, self.tmp_conf, name)).communicate()
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush move %s-host rack=%s-rack' % (self.ceph_cmd, self.tmp_conf, name, name)).communicate()

            # check if OSDs are defined    
            osds = profile.get('osds', None)
            if not osds:
                raise Exception("No OSDs defined for crush profile, bailing!")
            # apply the crush profile to OSDs
            for i in osds:
                common.pdsh(settings.getnodes('head'), '%s -c %s osd crush set %s 1.0 host=%s-host' % (self.ceph_cmd, self.tmp_conf, i, name)).communicate()
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush rule create-simple %s %s-root osd' % (self.ceph_cmd, self.tmp_conf, name, name)).communicate()
            self.set_ruleset(name)

        # get names of any EC profiles in the YAML
        erasure_profiles = self.config.get('erasure_profiles', {})
        
        # create each profile
        for name,profile in erasure_profiles.items():
            k = profile.get('erasure_k', 6)
            m = profile.get('erasure_m', 2)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd erasure-code-profile set %s crush-failure-domain=osd k=%s m=%s' % (self.ceph_cmd, self.tmp_conf, name, k, m)).communicate()
            self.set_ruleset(name)

    # making a pool in the remote cluster
    def mkpool(self, name, profile_name, application, base_name=None):
        """Make a pool with the given name, profile application, and basename (in case of cache tier configuration).\n
        Frequently check cluster heatlh after every operation as well."""

        # get the list of all the pool profiles listed in the YAML file
        pool_profiles = self.config.get('pool_profiles', {'default': {}})
        
        # set the required pool profile by the given name
        profile = pool_profiles.get(profile_name, {})

        # FIXME: Set the pg and pgp size as read from the pool profile in the YAML file!

        # set pg, and pgp size for the pools
        pg_size = profile.get('pg_size', 128)
        pgp_size = profile.get('pgp_size', 128)
        
        # get the erasur eprofile if any
        erasure_profile = profile.get('erasure_profile', '')
        
        # replication 2, 3 etc
        replication = str(profile.get('replication', None))

        # other parameters to make fine tuned pool creation
        ec_overwrites = profile.get('ec_overwrites', False)
        cache_profile = profile.get('cache_profile', None)

        # Options for cache tiering
        crush_profile = profile.get('crush_profile', None)
        cache_mode = profile.get('cache_mode', None)
        hit_set_type = profile.get('hit_set_type', None)
        hit_set_count = profile.get('hit_set_count', None)
        hit_set_period = profile.get('hit_set_period', None)
        target_max_objects = profile.get('target_max_objects', None)
        target_max_bytes = profile.get('target_max_bytes', None)
        min_read_recency_for_promote = profile.get('min_read_recency_for_promote', None)
        min_write_recency_for_promote = profile.get('min_write_recency_for_promote', None)
        # Options for prefilling objects
        prefill_objects = profile.get('prefill_objects', 0)
        prefill_object_size = profile.get('prefill_object_size', 0)
        prefill_time = profile.get('prefill_time', 0)

        # setup an erasure coded pool
        if replication and replication == 'erasure':
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool create %s %d %d erasure %s' % (self.ceph_cmd, self.tmp_conf, name, pg_size, pgp_size, erasure_profile),
                        continue_if_error=False).communicate()
            if ec_overwrites is True:
                common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s allow_ec_overwrites true' % (self.ceph_cmd, self.tmp_conf, name), continue_if_error=False).communicate()
        else:
            # print("ceph_cmd:{}\ntmp_conf:{}\nname:{}\npg_size:{}\npgp_size:{}".format(self.ceph_cmd, self.tmp_conf, name, pg_size, pgp_size))
            (stdout, stderr) = common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool create %s %d %d' % (self.ceph_cmd, self.tmp_conf, name, pg_size, pgp_size),
                        continue_if_error=False).communicate()
            # print("returned data: {}".format((stdout, stderr)))

        # in case of newer versions (post luminous) pool 'application' option is available to optimize pool usage
        if self.version_compat not in ['argonaut', 'bobcat', 'cuttlefish', 'dumpling', 'emperor', 'firefly', 'giant', 'hammer', 'infernalis', 'jewel']:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool application enable %s %s' % (self.ceph_cmd, self.tmp_conf, name, application), continue_if_error=False).communicate()

        # setup replicated pool if mentioned
        if replication and replication.isdigit():
            pool_repl_size = int(replication)
            pool_min_repl_size = 1
            if (pool_repl_size > 2):
                pool_min_repl_size = pool_repl_size - 1

            # basic command running, this time to create a rep pool
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s size %s' % (self.ceph_cmd, self.tmp_conf, name, replication),
                        continue_if_error=False).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s min_size %d' % (self.ceph_cmd, self.tmp_conf, name, pool_min_repl_size),
                        continue_if_error=False).communicate()

        # in case of a crush profile given, setup the crush 'ruleset' on the created pool
        if crush_profile:
            try:
              rule_index = int(crush_profile)
              # set crush profile using the integer 0-based index of crush rule
              # displayed by: ceph osd crush rule ls
              ruleset = crush_profile
            except ValueError as e:
              ruleset = self.get_ruleset(crush_profile)
            
            # bombs away!
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s crush_ruleset %s' % (self.ceph_cmd, self.tmp_conf, name, crush_profile),
                        continue_if_error=False).communicate()

        logger.info('Checking Healh after pool creation.')
        self.check_health()

        # write data to the pool at the time of creation, this is useful if only running reading benchmarks
        # or can be any other reason just to get some data going into the pool
        if prefill_objects > 0 or prefill_time > 0:
            logger.info('prefilling %s %sbyte objects into pool %s' % (prefill_objects, prefill_object_size, name))
            common.pdsh(settings.getnodes('head'), 'sudo %s -p %s bench %s write -b %s --max-objects %s --no-cleanup' % (self.rados_cmd, name, prefill_time, prefill_object_size, prefill_objects)).communicate()
            # check health to see if you messed something up by the writes :D
            self.check_health()

        # set up cache tiering if needed (setup different cache tiers in case of highly hierarchical storage architecture)
        if base_name and cache_mode:
            logger.info("Adding %s as cache tier for %s.", name, base_name)
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier add %s %s' % (self.ceph_cmd, self.tmp_conf, base_name, name)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier cache-mode %s %s' % (self.ceph_cmd, self.tmp_conf, name, cache_mode)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier set-overlay %s %s' % (self.ceph_cmd, self.tmp_conf, base_name, name)).communicate()

        # continuing with the cache tier setup 
        if hit_set_type:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s hit_set_type %s' % (self.ceph_cmd, self.tmp_conf, name, hit_set_type)).communicate()
        if hit_set_count:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s hit_set_count %s' % (self.ceph_cmd, self.tmp_conf, name, hit_set_count)).communicate()
        if hit_set_period:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s hit_set_period %s' % (self.ceph_cmd, self.tmp_conf, name, hit_set_period)).communicate()
        if target_max_objects:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s target_max_objects %s' % (self.ceph_cmd, self.tmp_conf, name, target_max_objects)).communicate()
        if target_max_bytes:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s target_max_bytes %s' % (self.ceph_cmd, self.tmp_conf, name, target_max_bytes)).communicate()
        if min_read_recency_for_promote:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s min_read_recency_for_promote %s' % (self.ceph_cmd, self.tmp_conf, name, min_read_recency_for_promote)).communicate()
        if min_write_recency_for_promote:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s min_write_recency_for_promote %s' % (self.ceph_cmd, self.tmp_conf, name, min_write_recency_for_promote)).communicate()

        logger.info('Final Pool Health Check.')
        # really seeing if we messed up
        self.check_health()

        # If there is a cache profile assigned, make a cache pool
        if cache_profile:
            cache_name = '%s-cache' % name
            self.mkpool(cache_name, cache_profile, name, application)

    # delete a pool in the remote cluster
    def rmpool(self, name, profile_name):
        """Delete an existing pool with a given name and profile. Also handles cache tier configurations for the pools,"""
        # get pool profile array
        pool_profiles = self.config.get('pool_profiles', {'default': {}})
        # get the required profile if available
        profile = pool_profiles.get(profile_name, {})
        # get a cache profile if there
        cache_profile = profile.get('cache_profile', None)
        # handle flushing of the cache-tier
        if cache_profile:
            cache_name = '%s-cache' % name

            # flush and remove the overlay and such
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier cache-mode %s forward' % (self.ceph_cmd, self.tmp_conf, cache_name)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s -p %s cache-flush-evict-all' % (self.rados_cmd, self.tmp_conf, cache_name)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier remove-overlay %s' % (self.ceph_cmd, self.tmp_conf, name)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier remove %s %s' % (self.ceph_cmd, self.tmp_conf, name, cache_name)).communicate()

            # delete the cache pool
            self.rmpool(cache_name, cache_profile)
        # bombs away!
        common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool delete %s %s --yes-i-really-really-mean-it' % (self.ceph_cmd, self.tmp_conf, name, name),
                    continue_if_error=False).communicate()

    # unmount rbd mounted volumes from the loadgenerators
    def rbd_unmount(self):
        """Unmount all the rbd mounted drives in the all the clients (load generators)"""
        common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec umount \'{}\' \;').communicate()
        # common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec rbd -c %s unmap \'{}\' \;' % self.tmp_conf).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo service rbdmap stop').communicate()

    # make a new image in RBD pool with the given params
    def mkimage(self, name, size, pool, data_pool, object_size):
        """Simply create a new image in the RBD pool"""
        dp_option = ''
        if data_pool:
            dp_option = "--data-pool %s" % data_pool
        common.pdsh(settings.getnodes('head'), '%s -c %s create %s --size %s --pool %s %s --object-size %s' % (self.rbd_cmd, self.tmp_conf, name, size, pool, dp_option, object_size)).communicate()

    # auth_urls needed for RADOSGW related stuff
    def get_auth_urls(self):
        return self.auth_urls

    # create a new swift use particular to the benchmark 
    def add_swift_user(self, user, subuser, key):
        """Add a swift user with given credentials into the 'current' cluster."""
        # only if the auth_urls have been defined
        if self.auth_urls:
            # command to execute
            cmd = "%s" % self.radosgw_admin_cmd
            # all the 'head' nodes - CBT mngmnt nodes
            node = settings.getnodes('head')
            # create a user
            common.pdsh(node, '%s -c %s user create --uid=%s --display-name=%s' % (cmd, self.tmp_conf, user, user)).communicate()
            # create a subuser because of swift multi-tier auth-style
            common.pdsh(node, '%s -c %s subuser create --uid=%s --subuser=%s --access=full' % (cmd, self.tmp_conf, user, subuser)).communicate()
            # create a key for the new subuser
            common.pdsh(node, '%s -c %s key create --subuser=%s --key-type=swift --secret=%s' % (cmd, self.tmp_conf, subuser, key)).communicate()
            # limit the amount of resources available to the user
            common.pdsh(node, '%s -c %s user modify --uid=%s --max-buckets=0' % (cmd, self.tmp_conf, user)).communicate()

    # make 3 pools needed for RGW
    def make_rgw_pools(self):
        """Create the three required pools of RGW daemon for object storage with Ceph."""
        # get names of RGW pools if defined in the YAML
        rgw_pools = self.config.get('rgw_pools', {})
        # make a pool with name, profiles, as well as an application name
        # RGW needs 3 pools, one to store the buckets, as a default pool
        # another to store the index data of all the bucket metadata
        # and third to store the actual bucket data
        self.mkpool('default.rgw.buckets', rgw_pools.get('buckets', 'default'), 'rgw')
        self.mkpool('default.rgw.buckets.index', rgw_pools.get('buckets_index'), 'default', 'rgw')
        self.mkpool('default.rgw.buckets.data', rgw_pools.get('buckets_data'), 'default', 'rgw')

# thread to bring up a screwed cluster
class RecoveryTestThread(threading.Thread):
    """
        This thread performs the cluster recovery by handling OSD errors, makes the\n
         cluster go through a series of states testing health at each one and in each \n
         state a particular aspect of the OSDs is changed, in whatever state the cluster \n
         appears to be healed, events are 'set'. At the end, the control is returned to the \n
         callback.
        
        Attributes are:
        - config - the YAML configuration file
        - cluster - the cluster definition
        - callback - the callback function to return to
        - stoprequest - the Threading.Event object to signal the recovery request stopped
        - haltrequest - the Threading.Event object to signal the recovery was interrupted
    """
    def __init__(self, config, cluster, callback, stoprequest, haltrequest):
        threading.Thread.__init__(self)
        self.config = config
        self.cluster = cluster
        self.callback = callback
        self.state = 'pre'
        self.states = {'pre': self.pre, 'markdown': self.markdown, 'osdout': self.osdout, 'osdin':self.osdin, 'post':self.post, 'done':self.done}
        self.stoprequest = stoprequest
        self.haltrequest = haltrequest
        self.outhealthtries = 0
        self.inhealthtries = 0
        self.maxhealthtries = 60
        self.health_checklist = ["degraded", "peering", "recovery_wait", "stuck", "inactive", "unclean", "recovery"]
        self.ceph_cmd = self.cluster.ceph_cmd

    def logcmd(self, message):
        return 'echo "[`date`] %s" >> %s/recovery.log' % (message, self.config.get('run_dir'))

    def pre(self):
        pre_time = self.config.get("pre_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Starting Recovery Test Thread, waiting %s seconds.' % pre_time)).communicate()
        time.sleep(pre_time)
        lcmd = self.logcmd("Setting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), '%s -c %s osd set noup;%s' % (self.ceph_cmd, self.cluster.tmp_conf, lcmd)).communicate()
        self.state = 'markdown'

    def markdown(self):
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s down." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd down %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s out." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd out %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
        common.pdsh(settings.getnodes('head'), self.logcmd('Waiting for the cluster to break and heal')).communicate()

        self.state = 'osdout'

    def osdout(self):
        ret = self.cluster.check_health(self.health_checklist, "%s/recovery.log" % self.config.get('run_dir'))
        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()

        if self.outhealthtries < self.maxhealthtries and ret == 0:
            self.outhealthtries = self.outhealthtries + 1
            return # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()

        lcmd = self.logcmd("Unsetting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), '%s -c %s osd unset noup;%s' % (self.ceph_cmd, self.cluster.tmp_conf, lcmd)).communicate()
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s up." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd up %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s in." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd in %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()

        self.state = "osdin"

    def osdin(self):
        # Wait until the cluster is healthy.
        ret = self.cluster.check_health(self.health_checklist, "%s/recovery.log" % self.config.get('run_dir'))
        if self.inhealthtries < self.maxhealthtries and ret == 0:
            self.inhealthtries = self.inhealthtries + 1
            return # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()
        self.state = "post"

    def post(self):
        if self.stoprequest.isSet():
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but stoprequest is set, finishing now.')).communicate()
            self.haltrequest.set()
            return

        if self.config.get("repeat", False):
            # reset counters
            self.outhealthtries = 0
            self.inhealthtries = 0

            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but repeat is set.  Moving to "markdown" state.')).communicate()
            self.state = "markdown"
            return

        post_time = self.config.get("post_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, completion in %s seconds.' % post_time)).communicate()
        time.sleep(post_time)
        self.state = "done"

    def done(self):
        common.pdsh(settings.getnodes('head'), self.logcmd("Done.  Calling parent callback function.")).communicate()
        self.callback()
        self.haltrequest.set()

    def join(self, timeout=None):
        common.pdsh(settings.getnodes('head'), self.logcmd('Received notification that parent is finished and waiting.')).communicate()
        super(RecoveryTestThread, self).join(timeout)

    def run(self):
        self.haltrequest.clear()
        self.stoprequest.clear()
        while not self.haltrequest.isSet():
          self.states[self.state]()
        common.pdsh(settings.getnodes('head'), self.logcmd('Exiting recovery test thread.  Last state was: %s' % self.state)).communicate()

