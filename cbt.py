#!/usr/bin/python

import argparse
import yaml
import os
import settings
import common
import logging
import benchmarkfactory
from cluster.ceph import Ceph
from log_support import setup_loggers

logger = logging.getLogger("cbt")

def parse_args():
    parser = argparse.ArgumentParser(description='Continuously run ceph tests.')
    parser.add_argument(
        '--archive',
        required = True,
        help = 'Directory where the results should be archived.',
        )

    parser.add_argument(
        '--conf',
        required = False,
        help = 'The ceph.conf file to use.',
        )
    parser.add_argument(
        'config_file',
        help = 'YAML config file.',
        )
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    setup_loggers()
    ctx = parse_args()
    settings.initialize(ctx)

    iteration = 0
    logger.debug("Settings.cluster: %s", settings.cluster)
    global_init = {} 
    # FIXME: Create ClusterFactory and parametrically match benchmarks and clusters.
    cluster = Ceph(settings.cluster)
    while (iteration < settings.cluster.get("iterations", 0)):
        benchmarks = benchmarkfactory.getAll(cluster, iteration)
        for b in benchmarks:
            if b.exists():
                continue
            # Tell the benchmark to initialize unless it's in the skip list.
            if not b.getclass() in global_init:
                b.initialize()
                # Skip future initializations unless rebuild requested.
                if not settings.cluster.get('rebuild_every_test', False):
                    global_init[b.getclass()] = b
            b.run()
            if not b.getclass() in global_init:
                b.cleanup()
        iteration += 1
    for k,b in global_init.items():
        b.cleanup()
