#!/usr/bin/python

"""
    This is where all the action happens!
"""
import argparse
import collections
import logging
import pprint
import sys

import settings
import benchmarkfactory
from cluster.ceph import Ceph
from log_support import setup_loggers

# The get the pointer to the logger instance named 'cbt' for usage
logger = logging.getLogger("cbt")

# using the Argument Parser class, to create custom instance for program
def parse_args(args):
    """Simple argument parsing, usual stuff"""

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

    # call the argparse.ArgumentParser.parse_args function on the instance of the class
    return parser.parse_args(args[1:])


def main(argv):
    """The main function which does all the action"""

    # setup all the logging functionality of the 'cbt' logger
    setup_loggers()

    # perform the argument parsing stuff
    ctx = parse_args(argv)

    # initialize the benchmarking by handling CLI args, creating files/dirs given in the YAML(s)
    settings.initialize(ctx)

    # iteration counter for each iteration of benchmarking to perform as per the settings
    iteration = 0

    # pretty printing into the debug function itself, instead of on any file descriptor
    # printing all the cluster configs given to CBT
    logger.debug("Settings.cluster:\n    %s",
                 pprint.pformat(settings.cluster).replace("\n", "\n    "))

    # dictionary to keep track of all benchmarks that have been initialized already
    global_init = collections.OrderedDict()

    # FIXME: Create ClusterFactory and parametrically match benchmarks and clusters.
    cluster = Ceph(settings.cluster)

    # E_OK
    return_code = 0

    try:
        # perform all the iterations of the benchmarking
        for iteration in range(settings.cluster.get("iterations", 0)):
            # get all the benchmarks objects from the 'factory' given the YAML config
            benchmarks = benchmarkfactory.get_all(cluster, iteration)
            # iterate over the generator to get each benchmkar object
            for b in benchmarks:
                # a benchmark 'run_dir' already exists, with the exact test profile, skip it!
                if b.exists():
                    continue

                # Tell the benchmark to initialize unless it's in the skip list.
                if b.getclass() not in global_init:
                    b.initialize()

                    # Skip future initializations unless rebuild requested.
                    if not settings.cluster.get('rebuild_every_test', False):
                        global_init[b.getclass()] = b

                try:
                    b.run()
                finally:
                    # unless rebuilding every test, cleanup the benchmark when done
                    if b.getclass() not in global_init:
                        b.cleanup()
    except:
        return_code = 1  # FAIL
        logger.exception("During tests")
    finally:
        for k, b in global_init.items():
            try:
                b.cleanup()
            except:
                logger.exception("During %s cleanup", k)
                return_code = 1  # FAIL

    return return_code


if __name__ == '__main__':
    exit(main(sys.argv))
