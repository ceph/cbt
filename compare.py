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


# Github Flavored Markdown elements
class Table:
    def __init__(self):
        self.text = ''
        self.cols = 0

    def add_headers(self, *headers):
        text = ' | '.join(headers) + '\n'
        text += ' | '.join('-' * len(h) for h in headers) + '\n'
        self.text += text
        self.cols = len(headers)

    def add_cells(self, *cells):
        assert(self.cols == len(cells))
        text = ' | '.join(str(c) for c in cells) + '\n'
        self.text += text

    def __str__(self):
        return self.text


class Heading:
    def __init__(self, level, text):
        self.text = '#' * level + ' ' + text + '\n'

    def __str__(self):
        return self.text


class Heading3(Heading):
    def __init__(self, text):
        super().__init__(3, text)


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
    parser.add_argument(
        '--output',
        help='write result in markdown to specified file',
        type=argparse.FileType('w'))
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

    nr_accepted = sum(result.accepted for result in results)
    if ctx.verbose:
        for result in results:
            if result.accepted:
                logger.info(result)
            else:
                logger.warning(result)

    nr_tests = len(results)
    nr_rejected = nr_tests - nr_accepted

    if ctx.output:
        heading = None
        if nr_rejected:
            heading = Heading3(f'{nr_rejected} out of {nr_tests} failed')
        else:
            heading = Heading3(f'all {nr_tests} tests passed')
        ctx.output.write(str(heading))

        table = Table()
        table.add_headers('run', 'metric', 'baseline', 'result', 'accepted')
        for r in results:
            table.add_cells(r.run, r.alias, r.baseline, r.result,
                            '  ' if r.accepted else ':x:')
        ctx.output.write(str(table))

    if nr_rejected > 0:
        logger.warning("%d tests failed out of %d", nr_rejected, len(results))
        sys.exit(1)
    else:
        logger.info("All %d tests passed.", len(results))


if __name__ == '__main__':
    main()
