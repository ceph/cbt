#!/usr/bin/python

import argparse
import yaml
import os
import settings
import common
import benchmarkfactory

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
    ctx = parse_args()
    settings.initialize(ctx)

    common.setup_cluster()
    if not settings.cluster.get("rebuild_every_test", False):
        common.setup_ceph()
#        setup_radosbench(rb_config)
        print 'Checking Health.'
        check_health()

    iteration = 0
    print settings.cluster
    while (iteration < settings.cluster.get("iterations", 0)):
        if os.path.exists(os.path.join(settings.cluster.get("archive_dir"), '%08d' % iteration)):
            print 'Skipping existing iteration %d.' % iteration
            iteration += 1
            continue

        benchmarks = benchmarkfactory.getAll(iteration)
        for b in benchmarks:
#             print b
            b.initialize()
            b.run()
            b.cleanup()
        iteration += 1
