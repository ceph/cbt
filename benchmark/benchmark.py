"""
This module implements the 'benchmark' class which includes all the methods that are common to each CBT benchmark.\n
This includes cleanup methods, initialization work, as well as the 'run' module and others.
"""
import subprocess
import logging

import settings
import common
import monitoring
import hashlib
import os
import json
import yaml

logger = logging.getLogger('cbt')

# the big guy of the module
class Benchmark(object):
    """The class implements all the functionality common between all the benchmarks.\n
    This includes basic init, and cleanup setup, run method, pre/post stuff."""
    
    # good ol' constructor
    def __init__(self, archive_dir, cluster, config):
        # the YAML given
        self.acceptable = config.pop('acceptable', {})
        self.config = config
        # the cluster to use, can be the newly constructed or the existing cluster
        self.cluster = cluster
        hashable = json.dumps(sorted(self.config.items())).encode()
        digest = hashlib.sha1(hashable).hexdigest()[:8]
        # setup the dir structure to store the results of each component of the benchmark
        self.archive_dir = os.path.join(archive_dir,
                                        'results',
                                        '{:0>8}'.format(config.get('iteration')),
                                        'id-{}'.format(digest))
        # setup the temp_dir to store all the temp data like ceph.conf, keyrings etc etc 
        self.run_dir = os.path.join(settings.cluster.get('tmp_dir'),
                                    '{:0>8}'.format(config.get('iteration')),
                                    self.getclass())
        # get OSD readahead, if any
        self.osd_ra = config.get('osd_ra', None)
        # this will hold the executable which is to be run
        self.cmd_path = ''
        # wanna use valgrind?
        self.valgrind = config.get('valgrind', None)
        # to accomodate the valgrind thing, need to append to the path
        self.cmd_path_full = '' 
        self.log_iops = config.get('log_iops', True)
        self.log_bw = config.get('log_bw', True)
        self.log_lat = config.get('log_lat', True)
        # change the path if it's valgrind
        if self.valgrind is not None:
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)
        # if a separate Read Ahead was mentioned in the config file, use that
        self.osd_ra_changed = False
        if self.osd_ra:
            self.osd_ra_changed = True
        else:
            self.osd_ra = common.get_osd_ra()

    # cleanup the run_dir of the benchmark
    def cleandir(self):
        """Wipe and create the run directory of the current benchmark"""
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

    # name says it all
    def getclass(self):
        return self.__class__.__name__

    # initialize the cluster, pretty simple stuff
    def initialize(self):
        """Init the cluster with the cluter 'initialize' function"""
        self.cluster.cleanup()
        use_existing = settings.cluster.get('use_existing', True)
        if not use_existing:
            self.cluster.initialize()
        self.cleanup()
    def initialize_endpoints(self):
        pass
    def run(self):
        """Start the benchmark. \nSetup OSD RA from YAML if given.\n
        Clean remote run_dirs, setup valgrind if necessary.\n
        Create and setup results directory along with dumping the config in that dir."""

        # handle a change in OSD RA
        if self.osd_ra and self.osd_ra_changed:
            logger.info('Setting OSD Read Ahead to: %s', self.osd_ra)
            self.cluster.set_osd_param('read_ahead_kb', self.osd_ra)
        # log some stuff
        logger.debug('Cleaning existing temporary run directory: %s', self.run_dir)
        # cleanup the rundir on remote nodes
        common.pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws'), 'sudo rm -rf %s' % self.run_dir).communicate()

        # Create a new empty run_dir after this for the benchmarking
        logger.debug('Creating new run directory: %s', self.run_dir)
        common.make_remote_dir(self.run_dir)

        # handle valgrind stuff
        if self.valgrind is not None:
            logger.debug('Adding valgrind to the command path.')
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)
        # Set the full command path
        self.cmd_path_full += self.cmd_path

        # Store the parameters of the test run
        config_file = os.path.join(self.archive_dir, 'benchmark_config.yaml')
        if not os.path.exists(self.archive_dir):
            os.makedirs(self.archive_dir)
        if not os.path.exists(config_file):
            config_dict = dict(cluster=self.config)
            with open(config_file, 'w') as fd:
                yaml.dump(config_dict, fd, default_flow_style=False)
        
        print dir(self)
    # the benchmark already exists, it doesn't! It's being created.
    def exists(self):
        return False

    def compare(self, baseline):
        logger.warn('%s does not support "compare" yet', self.getclass())

    def cleanup(self):
        pass

    # good ol' cache drop!
    def dropcaches(self):
        """Clean disk cache on the client (load generators) as well as the OSD nodes.\n
        This interferes with the read/write operations, that's why it's necessary before\n
        running each benchmark."""

        # get the node list
        nodes = settings.getnodes('clients', 'osds') 

        # sync the drive and cache, to avoid inconsistencies
        common.pdsh(nodes, 'sync').communicate()
        # dropping ALL cache, drop_caches can take three values
        # 1 - page cache
        # 2 - d-entries and inode-entries
        # 3 - both
        common.pdsh(nodes, 'echo 3 | sudo tee /proc/sys/vm/drop_caches').communicate()

    # 'stringifying' the object
    def __str__(self):
        return str(self.config)

class Result:
    def __init__(self, run, alias, result, baseline, stmt, accepted):
        self.run = run
        self.alias = alias
        self.result = result
        self.baseline = baseline
        self.stmt = stmt
        self.accepted = accepted

    def __str__(self):
        fmt = '{run}: {alias}: {stmt}:: {result}/{baseline}  => {status}'
        return fmt.format(run=self.run, alias=self.alias, stmt=self.stmt,
                          result=self.result, baseline=self.baseline,
                          status="accepted" if self.accepted else "rejected")
