#!/usr/bin/python
import argparse
import re
import sys
import numpy

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

def dev_from_max(data):
    values = data.values()
    maxval = get_max(values)
    total = 0
    for value in values:
        total += value
    pct = total / (1.0*maxval*len(values))
    return "Avg Deviation from Most Subscribed OSD: %.1f%%" % (100 * (1 - pct))

def pgs_per_osd(data):
    values = data.values()
    if values:
       return "PGs Per OSD: Min: %.1f, Max: %.1f, Mean: %.1f, Std Dev: %.1f" % (get_min(values), get_max(values), get_mean(values), get_std(values))
    else:
       return "No OSDs acting in this capacity."

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

def print_report(pool_counts, total_counts):
    for pool,data in sorted(pool_counts.iteritems()):
        print_data(data)
    print_data(total_counts)

def div():
    return "+" + "-" * 76 + "+"

def format_line(line):
    return str("| %s" % line).ljust(77) + "|"

def print_data(data):
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
    print div()
    for name,desc in sorted(COUNTS_DICT.iteritems()):
        print format_line(desc)
        if data[name].values():
            print format_line(pgs_per_osd(data[name]))
            print format_line(most_used_osds(data[name]))
            print format_line(least_used_osds(data[name]))
            print format_line(dev_from_max(data[name]))
        else:
            print format_line("No OSDs found in this capacity")
        print div()
    print ''

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


if __name__ == '__main__':
    ctx = parse_args()
    pool_counts = {}
    total_counts = {'pgs':0, 'name':'Totals (All Pools)'}
    up_primary_osd_counts = {}
    up_secondary_osd_counts = {}
    acting_primary_osd_counts = {}
    acting_secondary_osd_counts = {} 

    for line in ctx.pg_map:
       parts = line.rstrip().split('\t')
       match = re.search(r"^(\d+)\.(\w{1,2})", parts[0])
       if match:
           pool = match.group(1)
           uplist = parts[13].translate(None, '[]').split(',')
           actinglist = parts[14].translate(None, '[]').split(',')

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

    print_report(pool_counts, total_counts)
