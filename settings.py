import argparse
import yaml
import sys

cluster = {}
benchmarks = {}

def initialize(ctx):
    global cluster, benchmarks
 
    config = {}
    try:
        with file(ctx.config_file) as f:
            g = yaml.safe_load_all(f)
            for new in g:
                config.update(new)
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))

    cluster = config.get('cluster', {})
    benchmarks = config.get('benchmarks', {})

    if not (cluster):
        shutdown('No cluster section found in config file, bailing.')
    if not (benchmarks):
        shutdown('No benchmarks section found in config file, bailing.')

    # overlod the yaml if a ceph.conf file is specified on the command line
    if ctx.conf:
        cluster['ceph.conf'] = ctx.conf
    if ctx.archive:
        cluster['archive_dir'] = ctx.archive

def getnodes(*nodelists):
    nodes = []
    for nodelist in nodelists:
        cur = cluster.get(nodelist, [])
        if isinstance(cur, str):
            cur = [cur]

        if cur:
            nodes = nodes + cur
    print nodes
    return ','.join(uniquenodes(nodes))

def uniquenodes(nodes):
    ret = [] 
    for node in nodes:
        if node and not node in ret:
            ret.append(node)
    print ret
    return ret
 
def shutdown(message):
    sys.exit(message)

