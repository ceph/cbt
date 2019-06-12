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

def initialize(ctx):
    global cluster, client_endpoints, benchmarks

    config = {}
    try:
        with file(ctx.config_file) as f:
            map(config.update, yaml.safe_load_all(f))
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))

    cluster = config.get('cluster', {})
    client_endpoints = config.get('client_endpoints', {})
    benchmarks = config.get('benchmarks', {})

    if not cluster:
        shutdown('No cluster section found in config file, bailing.')

    if not benchmarks:
        shutdown('No benchmarks section found in config file, bailing.')

    # store cbt configuration in the archive directory
    cbt_results = os.path.join(ctx.archive, 'results')
    config_file = os.path.join(cbt_results, 'cbt_config.yaml')
    if not os.path.exists(ctx.archive):
        os.makedirs(ctx.archive)
    if not os.path.exists(cbt_results):
        os.makedirs(cbt_results)
    if not os.path.exists(config_file):
        config_dict = dict(cluster=cluster, benchmarks=benchmarks)
        with open(config_file, 'w') as fd:
            yaml.dump(config_dict, fd, default_flow_style=False)

    # set the tmp_dir if not set.
    if 'tmp_dir' not in cluster:
        cluster['tmp_dir'] = '/tmp/cbt.%s' % os.getpid()

    # set the ceph.conf file from the commandline, yaml, or default
    if ctx.conf:
        cluster['conf_file'] = ctx.conf
    elif 'conf_file' not in cluster:
        cluster['conf_file'] = "%s/ceph.conf" % (cluster.get('conf_file'),)

    if ctx.archive:
        cluster['archive_dir'] = ctx.archive

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
