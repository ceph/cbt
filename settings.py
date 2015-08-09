import argparse
import yaml
import sys
import os
import logging


logger = logging.getLogger("cbt")

cluster = {}
benchmarks = {}


def initialize(ctx):
    global cluster, benchmarks

    config = {}
    try:
        with file(ctx.config_file) as f:
            map(config.update, yaml.safe_load_all(f))
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))

    cluster = config.get('cluster', {})
    benchmarks = config.get('benchmarks', {})

    if not cluster:
        shutdown('No cluster section found in config file, bailing.')

    if not benchmarks:
        shutdown('No benchmarks section found in config file, bailing.')

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
    ret = [node for node in nodes if node]

    user = cluster.get('user')
    if user is not None:
        ret = ['%s@%s' % (user, node) for node in ret]

    return set(ret)


def shutdown(message):
    sys.exit(message)
