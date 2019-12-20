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
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='be chatty')
    ctx = parser.parse_args(sys.argv[1:])
    # settings.initialize() expects ctx.config_file and ctx.conf
    ctx.config_file = os.path.join(ctx.archive, 'results', 'cbt_config.yaml')
    ctx.conf = None
    settings.initialize(ctx)

    results = []
    for iteration in range(settings.cluster.get('iterations', 0)):
        cluster = Ceph(settings.cluster)
        benchmarks = list(zip(benchmarkfactory.get_all(ctx.archive, cluster, iteration),
                         benchmarkfactory.get_all(ctx.baseline, cluster, iteration)))
        for current, baseline in benchmarks:
            if not current.exists(True):
                logger.error("tested: %s result does not exist in %s",
                             current, ctx.archive)
                break
            if not baseline.exists(True):
                logger.error("baseline: %s result does not exist in %s",
                             baseline, ctx.baseline)
                break
            results.extend(current.evaluate(baseline))

    accepted = sum(result.accepted for result in results)
    if ctx.verbose:
        for result in results:
            if result.accepted:
                logger.info(result)
            else:
                logger.warning(result)

    rejected = len(results) - accepted
    if rejected > 0:
        logger.warning("%d tests failed out of %d", rejected, len(results))
        sys.exit(1)
    else:
        logger.info("All %d tests passed.", len(results))

if __name__ == '__main__':
    main()
