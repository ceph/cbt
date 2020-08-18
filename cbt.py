#!/usr/bin/python3
import argparse
import collections
import logging
import pprint
import sys

import settings
import benchmarkfactory
from cluster.ceph import Ceph, discover_mon_nodes, discover_osd_nodes
from log_support import setup_loggers

logger = logging.getLogger("cbt")


def parse_args(args):
    parser = argparse.ArgumentParser(description='Continuously run ceph tests.')
    parser.add_argument(
        '-a', '--archive',
        required=True,
        help='Directory where the results should be archived.',
        )

    parser.add_argument(
        '-c', '--conf',
        required=False,
        help='The ceph.conf file to use.',
        )

    parser.add_argument(
        'config_file',
        help='YAML config file.',
        )

    return parser.parse_args(args[1:])


def main(argv):
    setup_loggers()
    ctx = parse_args(argv)
    settings.initialize(ctx)

    if 'head' not in settings.cluster:
        print "Must specify 'head' node in config!"
        exit(1)

    if 'osds' not in settings.cluster:
        settings.cluster['osds'] = discover_osd_nodes(settings.cluster['head'])

    if 'mons' not in settings.cluster:
        settings.cluster['mons'] = discover_mon_nodes(settings.cluster['head'])

    iteration = 0
    logger.debug("Settings.cluster:\n    %s",
                 pprint.pformat(settings.cluster).replace("\n", "\n    "))

    global_init = collections.OrderedDict()

    # FIXME: Create ClusterFactory and parametrically match benchmarks and clusters.
    cluster = Ceph(settings.cluster)

    # E_OK
    return_code = 0

    try:
        for iteration in range(settings.cluster.get("iterations", 0)):
            archive_dir = settings.cluster.get('archive_dir')
            benchmarks = benchmarkfactory.get_all(archive_dir, cluster, iteration)
            for b in benchmarks:
                if b.exists():
                    continue

                # Tell the benchmark to initialize unless it's in the skip list.
                if b.getclass() not in global_init:
                    b.initialize()

                    # Skip future initializations unless rebuild requested.
                    if not settings.cluster.get('rebuild_every_test', False):
                        global_init[b.getclass()] = b

                # always try to initialize endpoints.
                b.initialize_endpoints()


                try:
                    b.run()
                finally:
                    if b.getclass() not in global_init:
                        b.cleanup()
    except:
        return_code = 1  # FAIL
        logger.exception("During tests")
    finally:
        for k, b in list(global_init.items()):
            try:
                b.cleanup()
            except:
                logger.exception("During %s cleanup", k)
                return_code = 1  # FAIL

    return return_code


if __name__ == '__main__':
    exit(main(sys.argv))
