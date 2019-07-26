#!env python3

import json
import os
import sys
import itertools
import argparse
import sys
import subprocess
import numpy as np

from summarize import dump_target, generate_summary
from traces import open_trace, iterate_structured_trace
from graph import graph, Scatter, Histogram

parser = argparse.ArgumentParser()
parser.add_argument(
    'target', metavar='T', type=str, help='target results directory')
parser.add_argument(
    '--match', type=str, help='json for matching', default='{}')
parser.add_argument(
    '--output', type=str, help='output directory')
parser.add_argument(
    '--generate-graphs', action='store_true', help='generate graphs')
parser.add_argument(
    '--detailed', action='store_true',
    help='generate more detailed graphs')
parser.add_argument(
    '--drop-first', type=float,
    help='drop', default=10.0)
parser.add_argument(
    '--drop-after', type=float,
    help='drop')
parser.add_argument(
    '--filter-latency-above', type=float,
    help='filter out latency above given percentile')
parser.add_argument(
    '--filter-latency-below', type=float,
    help='filter out latency below given percentile')


def get_targets(directory):
    contents = os.listdir(directory)
    if 'ceph.conf' in contents:
        return [(os.path.basename(directory), directory)]
    else:
        return [(x, os.path.join(directory, x)) for x in contents]


args = parser.parse_args()

match = json.loads(args.match)
targets = get_targets(args.target)
projected = [dump_target(name, target) for name, target in targets]

def do_filter(match, input):
    def cond(x):
        return all(x[1]['config'].get(k) == v for k, v in match.items())
    return filter(cond, input)

filtered_targets, filtered = zip(*do_filter(match, zip(targets, projected)))

summary = generate_summary(filtered, match)

graph_filename = lambda x: None
if args.output:
    subprocess.run(['mkdir', '-p', args.output], check=False)
    graph_filename = lambda x: os.path.join(args.output, x + '.png')

def do_mask(above, below):
    def f(lat):
        l, u = np.percentile(
            lat,
            [below if below else 0.0,
             above if above else 100.0],
            interpolation='linear')

        return (lat > l) & (lat < u)
    return f

masker = None
mask_params = None

if args.filter_latency_above or args.filter_latency_below:
    mask_params = ['latency']
    masker = do_mask(args.filter_latency_above, args.filter_latency_below)

TO_GRAPH_SMALL = [
    [Scatter(x, 'commit_latency_no_throttle', ymax=0.99) for x in
     ['current_kv_throttle_cost', 'current_deferred_throttle_cost']],
    [Scatter(x, 'throughput', ymax=0.99) for x in
     ['current_kv_throttle_cost', 'current_deferred_throttle_cost']]
]

TO_GRAPH_LARGE = [
    [Scatter(*x, ymax=0.99) for x in
     [('current_kv_throttle_cost', 'commit_latency_no_throttle'),
      ('current_deferred_throttle_cost', 'commit_latency_no_throttle'),
      ('total_throttle', 'commit_latency_no_throttle')]],
    [Scatter(*x, ymax=0.99) for x in
     [('current_kv_throttle_cost', 'throughput'),
      ('current_deferred_throttle_cost', 'throughput'),
      ('total_throttle', 'throughput')]],
    [Histogram(x) for x in
     ['current_kv_throttle_cost', 'current_deferred_throttle_cost', 'total_throttle']]
]

if args.generate_graphs:
    for name, path in filtered_targets:
        print("Generating graph for {}, path: {}".format(name, path))
        events = iterate_structured_trace(open_trace(path))
        if args.drop_first:
            events = itertools.dropwhile(
                lambda x: x.get_start() < args.drop_first, events)
        if args.drop_after:
            events = itertools.takewhile(
                lambda x: x.get_start() < args.drop_after, events)

        graph(
            events, name, graph_filename(name),
            TO_GRAPH_LARGE if args.detailed else TO_GRAPH_SMALL,
            mask_params, masker)

json.dump(summary, sys.stdout, sort_keys=True, indent=2)
