#!/usr/bin/python
import sys
import logging
import argparse
import collections

import settings
import benchmarkfactory
from cluster.ceph import Ceph
from log_support import setup_loggers


logger = logging.getLogger("cbt")


def parse_args(argv):
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
    return parser.parse_args(argv)


def main(argv):
    setup_loggers()
    ctx = parse_args(argv[1:])
    settings.initialize(ctx)

    logger.debug("Cluster: {0!r}".format(settings.cluster))

    # koder: ordered dict used to preserve cleanup
    global_init = collections.OrderedDict()

    # FIXME: Create ClusterFactory and parametrically match benchmarks and clusters.
    cluster = Ceph(settings.cluster)
    res_code = 0

    try:
        for iteration in range(settings.cluster.get("iterations", 0)):
            benchmarks = benchmarkfactory.getAll(cluster, iteration)
            for b in benchmarks:
                if b.exists():
                    continue

                # Tell the benchmark to initialize unless it's in the skip list.
                if b.getclass() not in global_init:
                    b.initialize()
                    # Skip future initializations unless rebuild requested.
                    if not settings.cluster.get('rebuild_every_test', False):
                        global_init[b.getclass()] = b

                b.run()

                if b.getclass() not in global_init:
                    b.cleanup()
    except:
        logger.exception("During tests")
        res_code = 1
    finally:
        for test_cls_name, b in reversed(global_init.items()):
            try:
                b.cleanup()
            except:
                logger.exception("During cleanup " + test_cls_name)
                res_code = 1

    return res_code


if __name__ == '__main__':
    exit(main(sys.argv))
