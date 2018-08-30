import subprocess
import common
import settings
import monitoring
import os
import time
import threading
import logging
import re

from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger("cbt")


class Getput(Benchmark):

    def __init__(self):
        super(Getput, self).__init__()

    def load_config(self, cluster, config):
        super(Getput, self).load_config(cluster, config)

        self.tmp_conf = self.cluster.tmp_conf
        self.runtime =  config.get('runtime', None)
        self.container_prefix = config.get('container_prefix', 'cbt-getput')
        self.object_prefix = config.get('object_prefix', 'cbt-getput')
        self.procs = config.get('procs', 1)
        self.ops_per_proc = config.get('ops_per_proc', None)
        self.test = config.get('test', "p")
        self.op_size = config.get('op_size', 4194304)
        self.ctype = config.get('ctype', None)
        self.debug = config.get('debug', None)
        self.logops = config.get('logops', None)
        self.grace = config.get('grace', None)
        self.pool_profile = config.get('pool_profile', 'default')
        self.cmd_path = config.get('cmd_path', "/usr/bin/getput")
        self.user = config.get('user', 'cbt')
        self.subuser = '%s:swift' % self.user
        self.key = config.get('key', 'vzCEkuryfn060dfee4fgQPqFrncKEIkh3ZcdOANY') # dummy key from ceph radosgw docs
        self.auth_urls = config.get('auth', self.cluster.get_auth_urls())
        self.cleanup()
        self.cleandir()

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self): 
        super(Getput, self).initialize()

        # create the user and key
        self.cluster.add_swift_user(self.user, self.subuser, self.key)

    def mkcredfiles(self):
        for i in xrange(0, len(self.auth_urls)):
            cred = "export ST_AUTH=%s\\nexport ST_USER=%s\\nexport ST_KEY=%s" % (self.auth_urls[i], self.subuser, self.key)
            common.pdsh(settings.getnodes('clients'), 'echo -e "%s" > %s/gw%02d.cred' % (cred, self.run_dir, i)).communicate()

    def mkgetputcmd(self, cred_file, gwnum):
        # grab the executable to use
        getput_cmd = '%s ' % self.cmd_path

        # Set the options
        if self.container_prefix is not None:
            container_prefix_flag = '-c%s' % self.container_prefix
            if self.ctype == 'byproc' or self.ctype == 'bynodegw':
                container_prefix_flag = '%s-gw%s' % (container_prefix_flag, gwnum)
            getput_cmd += '%s ' % container_prefix_flag

        # For now we'll only test distinct objects per client/gw 
        if self.object_prefix is not None:
            getput_cmd += '-o%s-`%s`-gw%s ' % (self.object_prefix, common.get_fqdn_cmd(), gwnum)
        else:
            getput_cmd += '-o`%s`-gw%s ' % (common.get_fqdn_cmd(), gwnum)

        getput_cmd += '-s%s ' % self.op_size
        getput_cmd += '-t%s ' % self.test
        getput_cmd += '--procs %s ' % self.procs
        if self.ops_per_proc is not None:
            getput_cmd += '-n%s ' % self.ops_per_proc
        if self.runtime is not None:
            getput_cmd += '--runtime %s ' % self.runtime
        if self.ctype is not None:
            getput_cmd += '--ctype %s ' % self.ctype
        if self.debug is not None:
            getput_cmd += '--debug %s ' % self.debug
        if self.logops is not None:
            getput_cmd += '--logops %s ' % self.logops
        if self.grace is not None:
            getput_cmd += '--grace %s ' % self.grace

        getput_cmd += '--cred %s ' % cred_file

        # End the getput_cmd
        getput_cmd += '> %s/output.gw%s' % (self.run_dir, gwnum)

        return getput_cmd

    def run(self):
        # First create a credential file for each gateway
        self.mkcredfiles()

        # We'll always drop caches for rados bench
        self.dropcaches()
        
        # dump the cluster config
        self.cluster.dump_config(self.run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        # Run getput 
        monitoring.start(self.run_dir)
        logger.info('Running getput %s test.' % self.test)

        ps = []
        for i in xrange(0, len(self.auth_urls)):
            cmd = self.mkgetputcmd("%s/gw%02d.cred" % (self.run_dir, i), i)
            p = common.pdsh(settings.getnodes('clients'), cmd)
            ps.append(p)
        for p in ps:
            p.wait()
        monitoring.stop(self.run_dir)

        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.archive_dir)

    def recovery_callback(self): 
        self.cleanup()

    def cleanup(self):
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 getput').communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.archive_dir, super(Getput, self).__str__())
