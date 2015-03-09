#!/usr/bin/python
import argparse
import re
import sys
import math
import numpy
import json

COUNTS_DICT = {
    'up_primaries':'OSDs in Primary Role (Up)',
    'up_secondaries':'OSDs in Secondary Role (Up)',
    'up_totals':'OSDs in All Roles (Up)',
    'acting_primaries':'OSDs in Primary Role (Acting)',
    'acting_secondaries':'OSDs in Secondary Role (Acting)',
    'acting_totals':'OSDs in All Roles (Acting)'}

def parse_args():
    parser = argparse.ArgumentParser(description='Display statistics about the PG Map.')
    parser.add_argument(
        'filename',
        nargs='?'
        )
        
    args = parser.parse_args()
    if args.filename:
        args.pg_map = open(args.filename, "r")
    elif not sys.stdin.isatty():
        args.pg_map = sys.stdin
    else:
        parser.print_help()
        sys.exit(1)
    return args

def add_count(key, dictionary):
   key = int(key)
   if key in dictionary:
       dictionary[key] += 1
   else:
       dictionary[key] = 1

def get_max(data):
    return numpy.max(data)

def get_min(data):
    return numpy.min(data)


def get_mean(data):
    return numpy.mean(data)

def get_std(data):
    return numpy.std(data)

def get_sum(data):
    return numpy.sum(data)

def dev_from_max(data):
    values = data.values()
    maxval = get_max(values)
    total = get_sum(values)
    pct = total / (1.0*maxval*len(values))
    return "Avg Deviation from Most Subscribed OSD: %.1f%%" % (100 * (1 - pct))

def efficiency_score(data, weights):
    values = data.values()
    avgval = get_mean(values)
    maxval = avgval

    for osd,pgs in data.iteritems():
        weight = 1.0
        if weights and osd in weights:
             weight = weights[osd]
        if weight*pgs > maxval:
             maxval = weight*pgs
    
    return 100.0*(avgval/maxval)

def pgs_per_osd(data):
    values = data.values()
    if values:
       return "Actual PGs Per OSD: Min: %d, Max: %d, Mean: %.1f, Std Dev: %.1f" % (get_min(values), get_max(values), get_mean(values), get_std(values))
    else:
       return "No OSDs acting in this capacity."

def expected_pgs_per_osd(data):
    values = data.values()
    if values:
        pgs = get_sum(values)
        osds = len(values)

        mean = 1.0 * pgs / osds
        min_exp = (pgs / osds) - math.sqrt(2*pgs*math.log(osds)/osds)
        max_exp = (pgs / osds) + math.sqrt(2*pgs*math.log(osds)/osds)
        std_dev = 1.0 * (max_exp - min_exp) / 4

        return "Expected PGs Per OSD: Min: %d, Max: %d, Mean: %.1f, Std Dev: %.1f" % (min_exp, max_exp, mean, std_dev)


def most_used_osds(data):
    count = min(5, len(data))
    out_array = []
    for element in get_top(count, data):
        out_array.append("%s(%s)" % (element[0], element[1]))
    return "%d Most Subscribed OSDs: %s" % (count, ", ".join(out_array))

def least_used_osds(data):
    count = min(5, len(data))
    out_array = []
    for element in get_bottom(count, data):
        out_array.append("%s(%s)" % (element[0], element[1]))
    return "%d Least Subscribed OSDs: %s" % (count, ", ".join(out_array))

def print_report(pool_counts, total_counts, input_type):
    print div()
    print format_line("Detected input as %s" % input_type)
    print div()
    for pool,data in sorted(pool_counts.iteritems()):
        weights = pool_weights[pool]
        print_data(data, pool_weights, total_weights)
        print_weights(data, pool_weights[pool])
        print div()
    print_data(total_counts, pool_weights, total_weights)
    print_weights(total_counts, total_weights)
    print div()

def div():
    return "+" + "-" * 76 + "+"

def format_line(line):
    return str("| %s" % line).ljust(77) + "|"

def print_data(data, pool_weights, total_weights):
    print ''
    print div() 
    print format_line("Pool ID: " + str(data['name']))
    print div() 

    osds = 0
    for clist in COUNTS_DICT.keys():
       cur = len(data[clist].keys())
       if cur > osds: osds = cur

    print format_line("Participating OSDs: " + str(osds))
    print format_line("Participating PGs: " + str(data['pgs']))

    # Calculate the expected maximally loaded OSD:
    pgs = int(data['pgs'])

    print div()
    for name,desc in sorted(COUNTS_DICT.iteritems()):
        print format_line(desc)
        if data[name].values():
            print format_line(expected_pgs_per_osd(data[name]))
            print format_line(pgs_per_osd(data[name]))
            print format_line(most_used_osds(data[name]))
            print format_line(least_used_osds(data[name]))
            print format_line(dev_from_max(data[name]))
            print format_line("") 
            print format_line("Efficiency score using equal weights: %.1f%%" % efficiency_score(data[name], {}))
            for pool,weights in pool_weights.iteritems():
                print format_line("Efficiency score using optimal weights for pool %s: %.1f%%" % (pool, efficiency_score(data[name], weights['acting_totals'])))
#            print format_line(efficiency_score(data[name]))
            print format_line("Efficiency score using optimal weights for all pools: %.1f%%" % efficiency_score(data[name], total_weights['acting_totals']))
        else:
            print format_line("No OSDs found in this capacity")
        print div()

def print_weights(data, weights):
    if data['acting_totals'].values():
        print format_line("Optimal OSD Weights for Pool ID: %s" % str(data['name']))
        print format_line("")
        section_weights = weights['acting_totals']
        for osd,weight in section_weights.iteritems():
            print format_line("OSD %s: %.2f" % (osd, weight))
    

def get_top(count, data):
    keys=list(data.keys())
    values=list(data.values())
    top = [] 
    for x in xrange(0, count):
        value = get_max(values)
        index = values.index(value)
        key = keys[index]
        top.append([key, value])

        del keys[index]
        del values[index]
    return top

def get_bottom(count, data):
    keys=list(data.keys())
    values=list(data.values())
    top = []
    for x in xrange(0, count):
        value = get_min(values)
        index = values.index(value)
        key = keys[index]
        top.append([key, value])

        del keys[index]
        del values[index]
    return top

def add_counts(pool, uplist, actinglist):
    if not pool in pool_counts:
       pool_counts[pool] = {'pgs':0,'name':pool}
       for clist in COUNTS_DICT.keys():
           pool_counts[pool][clist] = {}
           total_counts[clist] = {}
    pool_counts[pool]['pgs'] += 1
    total_counts['pgs'] += 1

    add_count(uplist[0], pool_counts[pool]['up_primaries'])
    add_count(uplist[0], total_counts['up_primaries'])

    for x in range (1, len(uplist)):
        add_count(uplist[x], pool_counts[pool]['up_secondaries'])
        add_count(uplist[x], total_counts['up_secondaries'])

    for x in range (0, len(uplist)):
        add_count(uplist[x], pool_counts[pool]['up_totals'])
        add_count(uplist[x], total_counts['up_totals'])

    add_count(actinglist[0], pool_counts[pool]['acting_primaries'])
    add_count(actinglist[0], total_counts['acting_primaries'])

    for x in range (1, len(actinglist)):
        add_count(actinglist[x], pool_counts[pool]['acting_secondaries'])
        add_count(actinglist[x], total_counts['acting_secondaries'])

    for x in range (0, len(actinglist)):
        add_count(actinglist[x], pool_counts[pool]['acting_totals'])
        add_count(actinglist[x], total_counts['acting_totals'])

def fill_weights():
    for pool,data in sorted(pool_counts.iteritems()):
        pool_weights[pool] = {}
        for name,desc in sorted(COUNTS_DICT.iteritems()):
            pool_weights[pool][name] = {}
            mean = get_mean(data[name].values())
            for osd,pgs in sorted(data[name].iteritems()):
                pool_weights[pool][name][int(osd)] = 1.0*mean/pgs
    for name,desc in sorted(COUNTS_DICT.iteritems()):
        total_weights[name] = {} 
        mean = get_mean(total_counts[name].values())
        for osd,pgs in sorted(total_counts[name].iteritems()):
            total_weights[name][int(osd)] = 1.0*mean/pgs

def parse_json(data):
    try:
        json_data = json.loads(data)
    except ValueError, e:
        parse_text(data)
        return
    for pg in json_data['pg_stats']:
        match = re.search(r"^(\d+)\.(\w{1,2})", pg['pgid'])
        pool = match.group(1)
        uplist = pg['up']
        actinglist = pg['acting']
        add_counts(pool, uplist, actinglist)
    fill_weights()
    print_report(pool_counts, total_counts, "JSON")

def parse_text(data):
    upnum = 0
    actingnum = 0

    for line in data.splitlines():
        parts = line.rstrip().split('\t')
        if parts[0] == "pg_stat":
            upnum = parts.index("up")
            actingnum = parts.index("acting")
            continue
        match = re.search(r"^(\d+)\.(\w{1,2})", parts[0])
        if match:
             pool = match.group(1)
             uplist = parts[upnum].translate(None, '[]').split(',')
             actinglist = parts[actingnum].translate(None, '[]').split(',')
             add_counts(pool, uplist, actinglist)
    if upnum == 0 or actingnum == 0:
        raise ValueError('could not parse the input as a plain ceph pg dump')
    fill_weights()
    print_report(pool_counts, total_counts, "plain")

if __name__ == '__main__':
    ctx = parse_args()
    pool_counts = {}
    total_counts = {'pgs':0, 'name':'Totals (All Pools)'}
    pool_weights = {}
    total_weights = {}
    data = ctx.pg_map.read()
    try:
        parse_json(data) 
    except ValueError, e:
        try:
            parse_text(data)
        except ValueError, e:
            print "Failed to read the input as either JSON or plain text."
            sys.exit(1)
       
