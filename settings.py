import argparse
import yaml
import sys
import os
import socket
import logging


logger = logging.getLogger("cbt")

cluster = {}
client_endpoints = {}
benchmarks = {}
monitoring_profiles = {}

def _handle_monitoring_legacy():
    """
    Inject collectl even if the config says nothing about it to preserve
    compatibility with current CBT's configuration files.
    """
    global monitoring_profiles
    if 'collectl' not in monitoring_profiles:
        monitoring_profiles['collectl'] = {}

def initialize(ctx):
    global cluster, client_endpoints, benchmarks, monitoring_profiles

    config = {}
    try:
        with open(ctx.config_file) as f:
            config = yaml.safe_load(f)
    except IOError as e:
        raise argparse.ArgumentTypeError(str(e))

    cluster = config.get('cluster', {})
    client_endpoints = config.get('client_endpoints', {})
    benchmarks = config.get('benchmarks', {})
    monitoring_profiles = config.get('monitoring_profiles', dict(collectl={}))

    if not cluster:
        shutdown('No cluster section found in config file, bailing.')

    if not benchmarks:
        shutdown('No benchmarks section found in config file, bailing.')

    # set the archive_dir from the commandline if present
    if ctx.archive:
        cluster['archive_dir'] = ctx.archive
    if 'archive_dir' not in cluster:
        shutdown('No archive dir has been set.')

    _handle_monitoring_legacy()

    # store cbt configuration in the archive directory
    cbt_results = os.path.join(cluster['archive_dir'], 'results')
    config_file = os.path.join(cbt_results, 'cbt_config.yaml')
    if not os.path.exists(cluster['archive_dir']):
        os.makedirs(cluster['archive_dir'])
    if not os.path.exists(cbt_results):
        os.makedirs(cbt_results)
    if not os.path.exists(config_file):
        config_dict = dict(cluster=cluster, benchmarks=benchmarks)
        with open(config_file, 'w') as fd:
            yaml.dump(config_dict, fd, default_flow_style=False)

    # set the tmp_dir if not set.
    if 'tmp_dir' not in cluster:
        cluster['tmp_dir'] = '/tmp/cbt.%s' % os.getpid()

    # set the ceph.conf file from the commandline if present
    if ctx.conf:
        cluster['conf_file'] = ctx.conf
    # If no conf file is set, default to /etc/ceph/ceph.conf
    # FIXME: We shouldn't have cluster specific defaults in settings.
    # Eventually make a base class with specific cluster implementations.
    if 'conf_file' not in cluster:
        cluster['conf_file'] = '/etc/ceph/ceph.conf'
    try:
        f = open(cluster['conf_file'])
        f.close()
    except IOError as e:
        shutdown('Was not able to access conf file: %s' % cluster['conf_file'])

def host_info(host):
    ret = {}
    user = cluster.get('user')

    if '@' in host:
        user, host = host.split('@')
        ret['user'] = user
    if user:
        ret['user'] = user
    ret['host'] = host
    ret['addr'] = socket.gethostbyname(host)
    return ret

def getnodes(*nodelists):
    nodes = []

    for nodelist in nodelists:
        cur = cluster.get(nodelist, [])
        if isinstance(cur, str):
            nodes.append(cur)
        elif isinstance(cur, dict):
            nodes.extend(cur.keys())
        elif isinstance(cur, list):
            nodes.extend(cur)
        else:
            raise ValueError("Can't process nodes of type %s - unknown set type: %r",
                             nodelist, cur)

    str_nodes = ','.join(uniquenodes(nodes))
    logger.debug("Nodes : %s", str_nodes)
    return str_nodes


def uniquenodes(nodes):
    unique = [node for node in nodes if node]
    ret = []

    for host in unique:
        info = host_info(host)
        host_str = info['host']
        if 'user' in info:
            host_str = "%s@%s" % (info['user'], host_str)
        ret.append(host_str)
    return set(ret)

def shutdown(message):
    sys.exit(message)
