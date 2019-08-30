#!/usr/bin/python3

import argparse
import os
import logging
import sys
import yaml

import settings
import benchmarkfactory
from cluster.ceph import Ceph
from log_support import setup_loggers

logger = logging.getLogger("cbt")

def main():
    setup_loggers()
    parser = argparse.ArgumentParser(description='query and compare CBT test results')
    parser.add_argument(
        '-a', '--archive',
        required=True,
        help='Directory where the results to be compared are archived.')
    parser.add_argument(
        '-b', '--baseline',
        required=True,
        help='Directory where the baseline results are archived.')
    ctx = parser.parse_args(sys.argv[1:])
    # settings.initialize() expects ctx.config_file and ctx.conf
    ctx.config_file = os.path.join(ctx.archive, 'results', 'cbt_config.yaml')
    ctx.conf = None
    settings.initialize(ctx)

    rejected = 0
    for iteration in range(settings.cluster.get('iterations', 0)):
        cluster = Ceph(settings.cluster)
        benchmarks = zip(benchmarkfactory.get_all(ctx.archive, cluster, iteration),
                         benchmarkfactory.get_all(ctx.baseline, cluster, iteration))
        for current, baseline in benchmarks:
            if not current.exists():
                logger.error("tested: %s result does not exist in %s",
                             current, ctx.archive)
                break
            if not baseline.exists():
                logger.error("baseline: %s result does not exist in %s",
                             baseline, ctx.baseline)
                break
            rejected += current.evaluated(baseline)
    if rejected > 0:
        logger.warn("%d metrics failed to pass the check", rejected)
        sys.exit(1, "")
    else:
        logger.info("all tests passed.")

if __name__ == '__main__':
    main()
