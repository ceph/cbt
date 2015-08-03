import os
import sys
import logging
import argparse

import yaml

logger = logging.getLogger("cbt")

cluster = {}
benchmarks = {}


def initialize(ctx):
    global cluster, benchmarks

    config = {}
    try:
        with file(ctx.config_file) as f:
            map(config.update, yaml.safe_load_all(f))
    except IOError as e:
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
        cluster['conf_file'] = "%s/ceph.conf" % cluster.get('conf_file')

    if ctx.archive:
        cluster['archive_dir'] = ctx.archive


def getnodes(*nodelists):
    nodes = []
    for nodelist in nodelists:
        cur = cluster.get(nodelist, [])
        if isinstance(cur, str):
            cur = [cur]
        if isinstance(cur, dict):
            cur = cur.keys()

        nodes.extend(cur)

    nodes_str = ','.join(uniquenodes(nodes))
    logger.debug("Nodes : %s", nodes_str)
    return nodes_str


def uniquenodes(nodes):
    user = cluster.get('user')

    nodes = [node for node in nodes if node]
    if user:
        nodes = ['%s@%s' % (user, node) for node in nodes]

    uniq_nodes = list(set(nodes))
    return uniq_nodes


def shutdown(message):
    sys.exit(message)
