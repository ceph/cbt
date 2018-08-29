import argparse
import yaml
import sys
import os
import logging


logger = logging.getLogger("cbt")

general = {}
cluster = {}
benchmarks = {}

def shutdown(message):
    sys.exit(message)

# Shamelessly borrowed from Stefan Nordhausen's code at
# https://github.com/ImmobilienScout24/yamlreader and
# http://stackoverflow.com/questions/7204805/python-dictionaries-of-dictionaries-merge
# licensed under Apache 2.0:
def data_merge(a, b):
    """merges b into a and return merged result
    based on http://stackoverflow.com/questions/7204805/python-dictionaries-of-dictionaries-merge
    and extended to also merge arrays and to replace the content of keys with the same name
    NOTE: tuples and arbitrary objects are not handled as it is totally ambiguous what should happen"""
    key = None
    # ## debug output
    # sys.stderr.write("DEBUG: %s to %s\n" %(b,a))
    try:
        if a is None or isinstance(a, unicode) or isinstance(a, int) or isinstance(a, long) or isinstance(a, float): 
            # border case for first run or if a is a primitive
            a = b
        elif isinstance(a, list):
            # lists can be only appended
            if isinstance(b, list):
                # merge lists
                a.extend(b)
            else:
                # append to list
                a.append(b)
        elif isinstance(a, dict):
            # dicts must be merged
            if isinstance(b, dict):
                for key in b:
                    if key in a:
                        a[key] = data_merge(a[key], b[key])
                    else:
                        a[key] = b[key]
            else:
                shutdown('Settings: Cannot merge non-dict "%s" into dict "%s"' % (b, a))
        else:
            shutdown('Settings: NOT IMPLEMENTED "%s" into "%s"' % (b, a))
    except TypeError as e:
        shutdown('Settings: TypeError "%s" in key "%s" when merging "%s" into "%s"' % (e, key, b, a))
    return a

def initialize_file(config_file, archive_dir):
    global general, cluster, benchmarks

    print "loading config_file: %s" % config_file

    config = {}
    try:
        with file(config_file) as f:
            map(config.update, yaml.safe_load_all(f))
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))

    general = data_merge(general, config.get('general', {}))
    cluster = data_merge(cluster, config.get('cluster', {}))
    benchmarks = data_merge(benchmarks, config.get('benchmarks', {}))

    # store cbt configuration in the archive directory
    cbt_results = os.path.join(archive_dir, 'results')
    archive_config = os.path.join(cbt_results, os.path.basename(config_file))
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)
    if not os.path.exists(cbt_results):
        os.makedirs(cbt_results)
    if not os.path.exists(archive_config):
        config_dict = dict(cluster=cluster, benchmarks=benchmarks)
        with open(archive_config, 'w') as fd:
            yaml.dump(config_dict, fd, default_flow_style=False)

def initialize(ctx):
    global cluster, benchmarks, general

    if ctx.config_files:
        for config_file in ctx.config_files:
            initialize_file(config_file, ctx.archive)
        
    # set the tmp_dir if not set.
    if 'tmp_dir' not in general:
        general['tmp_dir'] = '/tmp/cbt.%s' % os.getpid()

    # set the ceph.conf file from the commandline, yaml, or default
    if ctx.conf:
        cluster['conf_file'] = ctx.conf
    elif 'conf_file' not in cluster:
        cluster['conf_file'] = "%s/ceph.conf" % (cluster.get('conf_file'),)
    if ctx.query:
        general['query'] = ctx.query
    if ctx.format:
        general['format'] = ctx.format
    if ctx.rebuild:
        general['rebuild'] = True
    if ctx.archive:
        general['archive_dir'] = ctx.archive



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
