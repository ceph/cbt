"""
This module implements the RBDFIO benchmark, 
"""

# standard imports
import subprocess
import os
import time
import logging

from .benchmark import Benchmark

logger = logging.getLogger("cbt")

class RbdFio(Benchmark):
    """This class runs the RBDFIO benchmark, gets the config options from\n
    the given YAML and updates the 'object'. Then setups up the required directories and file names."""
    def __init__(self, archive_dir, cluster, config):
        super(RbdFio, self).__init__(archive_dir, cluster, config)=======

        # FIXME there are too many permutations, need to put results in SQLITE3
        # path of the FIO binary
        self.cmd_path = config.get('cmd_path', '/usr/bin/fio')
        # setup the pool profile given in YAML conf
        self.pool_profile = config.get('pool_profile', 'default')
        # print("pool_profile: {}".format(self.pool_profile))
        # number of concurrent processes to run, default 1
        self.concurrent_procs = config.get('concurrent_procs', 1)
        # total number of processes to run
        self.total_procs = self.concurrent_procs * len(settings.getnodes('clients').split(','))
        
        # FIO Configuration, 'import' the config into the class variables
        # the duration of the test
        self.time =  str(config.get('time', None))
        # the duration to treat as 'ramp' time, let cluster settle before store results
        self.ramp = str(config.get('ramp', None))
        # length of the IO cache of the disk, number of 'IO operations'
        self.iodepth = config.get('iodepth', 16)
        # how many times this 'job - particular config' test needs to be run
        self.numjobs = config.get('numjobs', 1)
        # perform an fsync() at the end (letting the drive become consistent)
        self.end_fsync = str(config.get('end_fsync', 0))
        # operation mode, read/write
        self.mode = config.get('mode', 'write')
        # ratio of read in read/write
        self.rwmixread = config.get('rwmixread', 50)
        # ratio of write in read/write
        self.rwmixwrite = 100 - self.rwmixread
        # perform a log 'event' after log_avg_msec milliseconds
        self.log_avg_msec = config.get('log_avg_msec', None)
        # this is asynchronous IO read/write
        self.ioengine = config.get('ioengine', 'libaio')
        # set the 'buffer size'. It's same as the block size of the device, basically all the operations are performed in
        # chunks of this 'buffer size' hence, acts like a block size from application point of view
        self.op_size = config.get('op_size', 4194304)
        # size of the test data to create in megabytes, default 64K MBs
        self.vol_size = config.get('vol_size', 65536)
        # size of object/block to determine 'r/w transaction size'
        self.vol_object_size = config.get('vol_object_size', '4M')
        # if to use random distribution of bits in the generated data
        self.random_distribution = config.get('random_distribution', None)
        # not sure that these two are for
        self.rbdadd_mons = config.get('rbdadd_mons')
        self.rbdadd_options = config.get('rbdadd_options', 'share')
        # client node Read Ahead size in bytes
        self.client_ra = config.get('client_ra', 128)
        # use direct I/O, bypassing the kernel I/O Management modules to let 
        # fio handle the management, is crucial for correct benchmarking that
        # the benchmarking application talks directly to the underlying storage
        self.direct = config.get('direct', 1)
        
        # the pool name to be used by this benchmark
        self.poolname = "cbt-kernelrbdfio"

        # dir hierarchy for the temp/out directories for easier tracking of multiple benchmark results
        self.run_dir = '%s/rbdfio/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.run_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.concurrent_procs), int(self.iodepth), self.mode)
        self.out_dir = '%s/rbdfio/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.archive_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.concurrent_procs), int(self.iodepth), self.mode)

        # Make the file names string
        self.names = ''
        for i in xrange(self.concurrent_procs):
            self.names += '--name=%s/cbt-kernelrbdfio-`hostname -s`/cbt-kernelrbdfio-%d ' % (self.cluster.mnt_dir, i)

    # check if the test was already performed before, it exists
    # this can be used in conjuction with the 'rebuild_every_test' option
    # of the YAML
    def exists(self):
        """Check if the test already exists, checks by checking whether\n
        an output directory exists for the test profile provided"""
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    # init the test setup
    def initialize(self):
        """Initialize the benchmark. This includes\n
        - Perform idle monitoring, to determine running status of host
        - Syncing existing stuff
        - Making new images
        - Creating remote run_dirs
        - Creating local FIO files that will serve as the 'data' in R/W"""
        super(RbdFio, self).initialize()

        # do idle monitoring to determine 'normal' operation metrics
        # these will serve as the 'point of comparison' from the test results
        logger.info('Pausing for 10s for idle monitoring.')
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        # time to perform idle monitoring for
        time.sleep(10)
        monitoring.stop()

        # need to handle in case of not using a monitoring stack issued by the CBT by default
        try:
            # this directory won't exist if there was no monitoring performed, need to handle the exceptions
            common.sync_files('%s/*' % self.run_dir, self.out_dir)
        except Exception as e:
            logger.warning("Exception in rbdfio @initialize: {}".format(e.message))

        self.mkimages()
 
        # Create the run directory
        common.make_remote_dir(self.run_dir)

        # populate the fio files
        # this is pre-testing setup, basically creating files locally which will then
        # be used to do the testing, this will exlude the time in creating the data
        # which is very unrealisted way of testing.
        logger.info('Attempting to populating fio files...')
        size = self.vol_size * 0.9 / self.concurrent_procs
        pre_cmd = 'sudo %s --ioengine=%s --rw=write --numjobs=%s --bs=4M --size %dM %s > /dev/null' % (self.cmd_path, self.ioengine, self.numjobs, size, self.names)
        common.pdsh(settings.getnodes('clients'), pre_cmd).communicate()


    # Since this is an 'overloaded' method, the run method being called from cbt.py
    # has functionality of 'Benchmark.run()' as well as this specific run
    def run(self):
        """Run the benchmark by doing pre-test stuff, start monitoring, build the fio command\n
         given the params, run the test, wait for it to end, stop logging and be done."""

        # setup the stuff common to each benchmark (dir setup and such)
        super(RbdFio, self).run()

        # Set client readahead
        self.set_client_param('read_ahead_kb', self.client_ra)

        # We'll always drop caches for rados bench
        self.dropcaches()

        # start monitoring setup (collectl, perf, blktrace)
        monitoring.start(self.run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            # creates a RecoveryTestThread (in ceph.py) and runs it with given callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        # wait a little before dropping the bombs
        time.sleep(5)

        # setup output file base-name
        out_file = '%s/output' % self.run_dir
        
        # Start constructing the fio command given all the arguments
        
        # cmd_path_full has fio executable, as well as valgrind and everything setup
        fio_cmd = 'sudo %s' % (self.cmd_path_full)
        # set the mode
        fio_cmd += ' --rw=%s' % self.mode
        # in case of mixed mode, set appropriate r/w ratios
        if (self.mode == 'readwrite' or self.mode == 'randrw'):
            fio_cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
        # set the io-engine
        fio_cmd += ' --ioengine=%s' % self.ioengine
        
        # set the test duration (optional)
        if self.time is not None:
            fio_cmd += ' --runtime=%s' % self.time
            # option to run time based tests implicitly set if a runtime is given
            fio_cmd += ' --time_based'
        # set the ramp duration (optional)
        if self.ramp is not None:
            fio_cmd += ' --ramp_time=%s' % self.ramp
        
        # set the number of jobs of this 'profile' to perform in parallel
        fio_cmd += ' --numjobs=%s' % self.numjobs
        # set the Direct I/O options
        fio_cmd += ' --direct=%s' % self.direct
        # set the buffer size
        fio_cmd += ' --bs=%dB' % self.op_size
        # I/O queue depth to be used
        fio_cmd += ' --iodepth=%d' % self.iodepth
        # set the volume size using 90% volume specified, probably a safety measure
        if self.vol_size:
            fio_cmd += ' --size=%dM' % (int(self.vol_size) * 0.9)
            
        # log iops, bandwidth and latency in file with basename given
        if self.log_iops:
            fio_cmd += ' --write_iops_log=%s' % out_file
        if self.log_bw:
            fio_cmd += ' --write_bw_log=%s' % out_file
        if self.log_lat:
            fio_cmd += ' --write_lat_log=%s' % out_file
        # fio needs to be time based in case of a recovery test, not sure why though
        if 'recovery_test' in self.cluster.config:
            fio_cmd += ' --time_based'
        
        # set random distribution of binary data
        if self.random_distribution is not None:
            fio_cmd += ' --random_distribution=%s' % self.random_distribution

        # set the output file names, as well as set output dumping file
        fio_cmd += ' %s > %s' % (self.names, out_file)
        
        # set log probing period
        if self.log_avg_msec is not None:
            fio_cmd += ' --log_avg_msec=%s' % self.log_avg_msec
        
        # log some stuff
        logger.info('Running rbd fio %s test.', self.mode)

        # run the command on the remote cluster
        common.pdsh(settings.getnodes('clients'), fio_cmd, continue_if_error=False).communicate()

        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        # stop monitoring the cluster
        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    # cleanup stuff , dirs etc
    def cleanup(self):
        super(RbdFio, self).cleanup()

    # set the drive parameters to each block drive mounted on the client
    def set_client_param(self, param, value):
        """Take the 'value' and store it in the \n
        /sys/block/<drive>/queue/<param> file. Setting up drive parameters as per YAML."""
        common.pdsh(settings.getnodes('clients'), 'find /sys/block/rbd* -exec sudo sh -c "echo %s > {}/queue/%s" \;' % (value, param)).communicate()

    # format for identifying a particular test of a benchmark
    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(RbdFio, self).__str__())

    # make images for the test
    def mkimages(self):
        """Create RBD images in the given pool, map them to clients\n
        format them with xfs and mount with all the optimization options."""
        monitoring.start("%s/pool_monitoring" % self.run_dir)
        try:
            self.cluster.rmpool(self.poolname, self.pool_profile)
        except OSError as e:
            logger.warning("Exception in rbdfio.py @mkimages")

        self.cluster.mkpool(self.poolname, self.pool_profile, 'rbd')
        common.pdsh(settings.getnodes('clients'), '/usr/bin/rbd create cbt-kernelrbdfio-`hostname -s` --size %s --pool %s' % (self.vol_size, self.poolname), continue_if_error=False).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo rbd map cbt-kernelrbdfio-`hostname -s` --pool %s --id admin' % self.poolname, continue_if_error=False).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mkfs.xfs /dev/rbd/cbt-kernelrbdfio/cbt-kernelrbdfio-`hostname -s`', continue_if_error=False).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p -m0755 -- %s/cbt-kernelrbdfio-`hostname -s`' % self.cluster.mnt_dir, continue_if_error=False).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo mount -t xfs -o noatime,inode64 /dev/rbd/cbt-kernelrbdfio/cbt-kernelrbdfio-`hostname -s` %s/cbt-kernelrbdfio-`hostname -s`' % self.cluster.mnt_dir, continue_if_error=False).communicate()
        # notify successful mounting of image
        logger.info("Image mounted on host!")
        monitoring.stop()

    # kill them all! in case something goes wrong - Game of Thrones style
    def recovery_callback(self):
        """Emergency callback to kill all running fio processes (byname)"""
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 fio').communicate()
