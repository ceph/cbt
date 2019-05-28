# Benchmark subclass to invoke smallfile
# this benchmark will iterate over smallfile test parameters
# something that smallfile cannot do today
#
# see examples/smallfile.yaml for how to use it 
#
# at present, this does not create the filesystem or mount it,
# all clients and head node must have filesystem mounted
#
# it assumes that all hosts are accessed with the same user account
# so that user@hostname pdsh syntax is not needed 
#
# it has only been tested with a single Cephfs mountpoint/host

import copy
import common
import monitoring
import os
import time
import logging
import settings
import yaml
import json
import subprocess

from benchmark import Benchmark

logger = logging.getLogger("cbt")

# we do this so source of exception is really obvious
class CbtSmfExc(Exception):
    pass

class Smallfile(Benchmark):

    def __init__(self, cluster, config):
        super(Smallfile, self).__init__(cluster, config)
        self.out_dir = self.archive_dir
        self.config = config
        mons = settings.getnodes('mons').split(',')
        self.any_mon = mons[0]
        self.clients = settings.getnodes('clients').split(',')
        self.any_client = self.clients[0]
        self.head = settings.getnodes('head')
        self.cephfs_data_pool_name = config.get('data_pool_name', 'cephfs_data')
        self.cleandir()

    
    # this function uses "ceph df" output to monitor
    # cephfs_data pool object count, when that stops going down
    # then the pool is stable and it's ok to start another test

    def get_cephfs_data_objects(self):
        (cephdf_out, cephdf_err) = common.pdsh(
            self.any_mon, 'ceph -f json df', continue_if_error=False).communicate()
        # pdsh prepends JSON output with IP address of host that did the command, 
        # we have to strip the IP address off before JSON parser will accept it
        start_of_json = cephdf_out.index('{')
        json_str = cephdf_out[start_of_json:]
        cephdf = json.loads(json_str)
        cephfs_data_objs = -1
        for p in cephdf['pools']:
            if p['name'] == self.cephfs_data_pool_name:
                cephfs_data_objs = int(p['stats']['objects'])
                break
        if cephfs_data_objs == -1:
            raise CbtSmfExc('could not find cephfs_data pool in ceph -f json df output')
        logger.info('cephfs_data pool object count = %d' % cephfs_data_objs)
        return cephfs_data_objs

    def run(self):
        super(Smallfile, self).run()

        # someday we might want to allow the option 
        # to NOT drop cache
        self.dropcaches()
        # FIXME: if desired, drop cache on OSDs
        # FIXME: if desired, drop cache on MDSs
        
        # dump the cluster config
        self.cluster.dump_config(self.run_dir)

        # input YAML parameters for smallfile are subset
        # extract parameters that you need

        smfparams = copy.deepcopy(self.config)
        del smfparams['benchmark']
        del smfparams['iteration']
        try:
            del smfparams['data_pool_name']
        except KeyError:
            pass
        operation = smfparams['operation']
        topdir = smfparams['top'].split(',')[0]
        yaml_input_pathname = os.path.join(self.out_dir, 'smfparams.yaml')
        with open(yaml_input_pathname, 'w') as yamlf:
            yamlf.write(yaml.dump(smfparams, default_flow_style=False))

        # generate client list

        client_list_path = os.path.join(self.out_dir, 'client.list')
        with open(client_list_path, 'w') as client_f:
            for c in self.clients:
                client_f.write(c + '\n')

        # ensure SMF directory exists
        # for shared filesystem, we only need 1 client to 
        # initialize it

        logger.info('using client %s to initialize shared filesystem' % self.any_client)
        common.pdsh(self.any_client, 'mkdir -p -v -m 0777 ' + topdir, continue_if_error=False).communicate()

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        # Run smallfile
        monitoring.start(self.run_dir)
        monitoring.start_pbench(self.out_dir)
        logger.info('Running smallfile test, see %s for parameters' % yaml_input_pathname)
        smfcmd = [ 'smallfile_cli.py', 
                   '--host-set', client_list_path,
                   '--response-times', 'Y',
                   '--yaml-input-file', yaml_input_pathname, 
                   '--verbose', 'Y', 
                   '--output-json', '%s/smfresult.json' % self.out_dir ]
        logger.info('smallfile command: %s' % ' '.join(smfcmd))
        logger.info('YAML inputs: %s' % yaml.dump(smfparams))
        smf_out_path = os.path.join(self.out_dir, 'smf-out.log')
        (smf_out_str, smf_err_str) = common.pdsh(self.head, ' '.join(smfcmd), continue_if_error=False).communicate()
        with open(smf_out_path, 'w') as smf_outf:
            smf_outf.write(smf_out_str + '\n')
        logger.info('smallfile result: %s' % smf_out_path)
        monitoring.stop_pbench(self.out_dir)
        monitoring.stop(self.run_dir)


        # save response times
        rsptimes_target_dir = os.path.join(self.out_dir, 'rsptimes')
        common.mkdir_p(rsptimes_target_dir)
        common.rpdcp(self.head, '', 
                     os.path.join(os.path.join(topdir, 'network_shared'), 'rsptimes*csv'), 
                     rsptimes_target_dir)

        if operation == 'cleanup':
            common.pdsh(self.any_client, 'rm -rf ' + topdir, continue_if_error=False).communicate()
            common.pdsh(self.any_client, 'mkdir -v -m 0777 ' + topdir, continue_if_error=False).communicate()
            # wait until cephfs_data pool stops decreasing
            logger.info('wait for cephfs_data pool to empty')
            pool_shrinking = True
            old_data_objs = self.get_cephfs_data_objects()
            while pool_shrinking:
                time.sleep(10)
                data_objs = self.get_cephfs_data_objects()
                if old_data_objs == data_objs:
                    logger.info('pool stopped shrinking')
                    pool_shrinking = False
                else:
                    logger.info('pool shrank by %d objects', old_data_objs - data_objs)
                    old_data_objs = data_objs

        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files(self.run_dir, self.out_dir)

    def recovery_callback(self): 
        pass

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Smallfile, self).__str__())
