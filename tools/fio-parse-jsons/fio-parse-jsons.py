#!/usr/bin/python3
#
# fio-parse-jsons.py - script to parse workloads generation results produced by
# FIO in JSON format
#
# Input parameters:
#
# -c <file_name>- a file name containing a list of JSONs FIO output
#  result files, each following the naming convention:
#  <prefix>_\d+job_d+io_<suffix>
#
# -t <title> - a string describing the Title for the chart, eg.
# 'Crimson 4k Random Write (Single 200GB OSD)'
#
# -a <file_name> - a JSON filename with the CPU average list, normally
# produced by the parse-top.pl script.
#
# -d <dir> - work directory
#
# -m - flag to indicate whether the run is for MultiFIO
#
# Example:
# python3 fio-parse-jsons.py -c \
#  crimson4cores_200gb_1img_4k_1procs_randwrite_list -t \
#  'Crimson 4k Random Write (Single 200GB OSD)' -a \
#  crimson4cores_200gb_1img_4k_1procs_randwrite_avg.json
#

import glob, os, sys
import pprint
import json
import argparse
import re
import math
import functools
from operator import add

# Predefined dictionary of metrics (query paths on the ouput FIO .json) for typical workloads
# Keys are metric names, vallues are the string path in the .json to seek
# For MultiFIO JSON files, the jobname no neccessarily matches any of the predef_dict keys,
# so we need instead to use a separate query:
job_type='jobs/jobname=*/job options/rw'
# All the following should be within the path
#  'jobs/jobname=*/read/iops'
predef_dict = {
          'randwrite': {
              'iops' : 'write/iops',
              'total_ios' : 'write/total_ios',
              'clat_ms' : 'write/clat_ns',
              'clat_stdev' : 'write/clat_ns',
              'usr_cpu': 'usr_cpu',
              'sys_cpu': 'sys_cpu'
              },
          'randread': {
              'iops' : 'read/iops',
              'total_ios' : 'read/total_ios',
              'clat_ms' : 'read/clat_ns',
              'clat_stdev' : 'read/clat_ns',
              'usr_cpu': 'usr_cpu',
              'sys_cpu': 'sys_cpu'
              },
          'seqwrite': {
              'bw' : 'write/bw',
              'total_ios' : 'write/total_ios',
              'clat_ms' : 'write/clat_ns',
              'clat_stdev' : 'write/clat_ns',
              'usr_cpu': 'usr_cpu',
              'sys_cpu': 'sys_cpu'
              },
          'seqread': {
              'bw' : 'read/bw',
              'total_ios' : 'read/total_ios',
              'clat_ms' : 'read/clat_ns',
              'clat_stdev' : 'read/clat_ns',
              'usr_cpu': 'usr_cpu',
              'sys_cpu': 'sys_cpu'
              }
          }

def filter_json_node(next_branch, jnode_list_in):
    """"
    Traverse the JSON jnode_list_in according to the next_branch:
    jnode_list_in: [dict]
    Assumption: json output of non-leaf nodes consists of either
    - dictionary - key field selects sub-value
    - sequence - key field syntax is name=value, where
              name is a dictionary key of sequence elements, and
              value is the desired value to select a sequence element
    """
    next_node_list = []
    # Nothing to do if the input next_branch is empty
    if not next_branch:
        return next_node_list
    for n in jnode_list_in:
        dotlist = next_branch.split('=')
        if len(dotlist) > 2:
            print( f"unrecognized syntax at {next_branch}")
            return []
        if len(dotlist) == 1:
            assert isinstance(n, dict)
            next_node_list.append(n[next_branch])
        else: # must be a sequence, take any element with key matching value
            select_key = dotlist[0]
            select_value = dotlist[1]
            assert isinstance(n, list)
            for e in n:
                # n is a list
                # print 'select with key %s value %s sequence
                # element %s'%(select_key, select_value, e)
                if select_value == '*':
                    next_node_list.append(e)
                else:
                    v = e[select_key]
                    if v == select_value:
                        next_node_list.append(e)
                        #print('selecting: %s'%str(e))
            if len(next_node_list) == 0:
                print(f"{select_key}={select_value} not found")
                return []
    return next_node_list

def process_fio_item(k, next_node_list):
    """
    Dict of results:
    file: { /path/: value, ...}
    For default (empty paths) queries:
    file: { /workload-type/: wrte: iops, latency_ms: (sort list by value, get top) }
    To coalesce the results of several files:
    use the timestamp to groups json files -- pending
    For IOPs or BW: sum the values together from all the json files for the
    same timestamp
    For latency: multiply this value by IOPs, sum these values from all the
    json files for the same timestamp and then divide by total IOPs to get
    an average latency
    """
    #    match k: # Python version on the SV1 node does not support 'match'
    #    case 'iops' | 'usr_cpu' | 'sys_cpu':
    if re.search('iops|usr_cpu|sys_cpu|iodepth|total_ios', k):
        return next_node_list[0]
    if k == 'bw':
        return next_node_list[0]/1000
    if k == 'latency_ms':
    #    case 'latency_ms':
        unsorted_dict=next_node_list[0]
        sorted_dict=dict(sorted(unsorted_dict.items(), key=lambda x:x[1], reverse=True))
        firstk=list(sorted_dict.keys())[0]
        return firstk
    if k == 'clat_ms':
    #    case 'clat_ns':
        unsorted_dict=next_node_list[0]
        clat_ms=unsorted_dict['mean']/1e6
        return clat_ms
    if k == 'clat_stdev':
    #    case 'clat_ns':
        unsorted_dict=next_node_list[0]
        clat_stdev=unsorted_dict['stddev']/1e6
        return clat_stdev

def combined_mean(a, b):
    """
    Calculates the combined mean of two groups:
    X_c = frac{ n_1*mean(X_1)+n_2*mean(X_2)) }{ (n_1+n_2) }
    FIO already provides the (mean,stdev) of completion latency per sample
    Expects two tuples: (mx_1, n_1) and (mx_2,n_2), and returns a tuple.
    """
    mx_1,n_1 = a
    mx_2,n_2 = b
    n_c = n_1 + n_2
    return ( (n_1 * mx_1 + n_2 * mx_2)/n_c, n_c)

def combined_std_dev(a,b):
    """
    Calculats the combined std dev, normally for the completion latency
    Expects a,b to be tuples (s_i,x_i) std dev and mean, respectively,
    and returns a tuple.
    """
    y_1,n_1 = a
    y_2,n_2 = b
    s_1,mx_1 = y_1
    s_2,mx_2 = y_2
    mx_c,_nc = combined_mean( (mx_1,n_1), (mx_2,n_2) )
    v_1 = s_1 * s_1
    v_2 = s_2 * s_2
    q_1 = (n_1 - 1.0) * v_1 + n_1 * (mx_1 * mx_1)
    q_2 = (n_2 - 1.0) * v_2 + n_2 * (mx_2 * mx_2)
    q_c = q_1 + q_2
    n_c = n_1 + n_2
    return ((math.sqrt( (q_c - n_c * mx_c * mx_c )/(n_c - 1.0) ), mx_c), n_c)

def apply_reductor(result_dict, metric):
    """
    Applies the particular reduction to the list of values.
    Returns a value (scalar numeric)
    """
    if re.search('iops|usr_cpu|sys_cpu|bw|total_ios', metric):
        return  functools.reduce( add, result_dict[metric])
    if metric == 'clat_ms':
        z = zip(result_dict['clat_ms'], result_dict['total_ios'])
        mx,_ = functools.reduce( lambda x,y : combined_mean(x,y), z)
        return mx
    if metric == 'clat_stdev':
        z = zip(result_dict['clat_stdev'], result_dict['clat_ms'])
        zz = zip(z, result_dict['total_ios'])
        zc,_ = functools.reduce( lambda x,y : combined_std_dev(x,y), zz)
        sc,_ = zc
        return sc

def reduce_result_list(result_dict, jobname):
    """
    Applies a reduction to each of the lists of the result_dict:
    - IOPS/BW is the cummulative (sum)
    - avg (completion) latency is the combined avg
    - clat std dev is the combined std dev -- for the last two, we need
    the number of samples from FIO, which is "total_ios"
    """
    _res = {}
    for metric in predef_dict[jobname].keys():
        _res[metric] = apply_reductor(result_dict, metric)
    return _res

def process_fio_json_file(json_file, json_tree_path):
    """
    Collect metrics from an individual JSON file, which might
    contain several entries, one per job
    """
    with open(json_file, 'r') as json_data:
        result_dict = {}
        # check for empty file
        f_info = os.fstat(json_data.fileno())
        if f_info.st_size == 0:
            print( f'JSON input file {json_file} is empty')
            return result_dict
        # parse the JSON object
        node = json.load(json_data)
        # Extract the json timestamp: useful for matching same workloads from
        # different FIO processes
        result_dict['timestamp'] = str(node['timestamp'])
        result_dict['iodepth'] = node['global options']['iodepth']
        # Use the jobname to index the predef_dict for the json query
        jobs_list = node['jobs']
        print(f"Num jobs: {len(jobs_list)}")
        job_result = {}
        for _i,job in enumerate(jobs_list):
            jobname = str(job['jobname'])
            if jobname in predef_dict:
                # this gives the paths to query for the metrics
                query_dict = predef_dict[jobname]
            else:
                jobname = job['job options']['rw']
                query_dict = predef_dict[jobname]
            result_dict['jobname'] = jobname
            for k in query_dict.keys():
                json_tree_path = query_dict[k].split('/')
                next_node_list = [job]

                for next_branch in json_tree_path:
                    next_node_list = filter_json_node(next_branch, next_node_list)
                item = process_fio_item(k, next_node_list)
                if k not in job_result:
                    job_result[k] = []
                job_result[k].append(item)

        reduced = reduce_result_list(job_result, result_dict['jobname'])
        merged = { **result_dict, **reduced }
        return merged

def traverse_files(sdir, config, json_tree_path):
    """
    Traverses the JSON files given in the config
    """
    os.chdir(sdir)
    try:
        config_file = open(config, "r")
    except IOError as e:
        raise argparse.ArgumentTypeError(str(e))
    json_files = config_file.read().splitlines()
    print(json_files)
    config_file.close()
    print(f"loading {len(json_files)} .json files ...")
    pp = pprint.PrettyPrinter(width=41, compact=True)
    dict_new = {}
    for fname in json_files:
        node_list = process_fio_json_file(fname,json_tree_path)
        dict_new[fname] = node_list
        print(f"== {fname} ==")
        pp.pprint(node_list)
    return dict_new

def gen_plot(config, data, list_subtables, title):
    """
    Generate a gnuplot script and .dat files -- assumes OSD CPU util only
    """
    plot_dict = {
    # Use the dict key as the suffix for the output file .png,
    # the .dat file is the same for the different charts
        'iops_vs_lat_vs_cpu_sys': {
            'ylabel': "Latency (ms)",
            'ycolumn': '4',
            'y2label': "OSD CPU system",
            'y2column': '9'},
        'iops_vs_lat_vs_cpu_usr': {
            'ylabel': "Latency (ms)",
            'ycolumn': '4',
            'y2label': "OSD CPU user",
            'y2column': '8'}
    }
    header = """
set terminal pngcairo size 650,420 enhanced font 'Verdana,10'
set key box left Left noreverse title 'Iodepth'
set datafile missing '-'
set key outside horiz bottom center box noreverse noenhanced autotitle
set grid
set autoscale
#set logscale
# Hockey stick graph:
set style function linespoints
"""

    template = ""
    out_plot = config.replace("_list",".plot")
    out_data = config.replace("_list",".dat")
    # Gnuplot quirk: '_' is interpreted as a sub-index:
    _title   = title.replace("_","-")

    with open(out_plot, 'w') as f:
        f.write(header)
        for pk in plot_dict.keys():
            out_chart = config.replace("list", pk + ".png")
            ylabel = plot_dict[pk]['ylabel']
            ycol = plot_dict[pk]['ycolumn']
            y2label = plot_dict[pk]['y2label']
            y2col = plot_dict[pk]['y2column']
            template += f"""
set ylabel "{ylabel}"
set xlabel "IOPS"
set y2label "{y2label}"
set ytics nomirror
set y2tics
set tics out
set autoscale y
set autoscale y2
set output '{out_chart}'
set title "{_title}"
"""
            # To plot CPU util in the same response curve, we need the extra axis
            # This list_subtables indicates how many sub-tables the .datfile will have
            # The stdev is the error column:5 
            if len(list_subtables) > 0:
                head = f"plot '{out_data}' index 0 using 2:{ycol}:5 t '{list_subtables[0]} q-depth' w yerr axes x1y1"
                head += f",\\\n '' index 0 using 2:{ycol}:5 notitle w lp axes x1y1"
                head += f",\\\n '' index 0 using 2:{y2col} w lp axes x1y2 t 'CPU%'"
                tail = ",\\\n".join([ f"  '' index {i} using 2:{ycol} t '{list_subtables[i]} q-depth' w lp axes x1y1"
                     for i in range(1,len(list_subtables))])
                template += ",\\\n".join([head, tail])

        f.write(template)
        f.close()
    with open(out_data, 'w') as f:
        f.write(data)
        f.close()

def initial_fio_table(dict_files, multi):
    """
    Construct a table from the input mesurements FIO json
    If multi=True, the avg list reduces the samples from dict_files into a single row
    """
    table = {}
    avg = {}
    # Traverse each json sample
    for name in dict_files.keys():
        item = dict_files[name]
        jobname = item['jobname']
        subdict = predef_dict[jobname]
        _keys = [ 'iodepth' ] + [ *subdict.keys() ]
        # Each k is a FIO metric (iops, lat, etc)
        for k in _keys:
            if not k in table:
                table[k] = []
            table[k].append(item[k])
            if multi:
                if not k in table:
                    avg[k] = 0.0
                avg[k] += item[k]
    # Probably might use some other avg rather than arithmetic avg
    # For a sinlge data point, table == avg
    # For multiple FIO: probably want to do the same reduction as multi job
    # For response time curves, we want to have the sequence of increasing queue depth
    for k in avg.keys():
        avg[k] /= len(table[k])
    return table, avg

def aggregate_cpu_avg(avg, table, avg_cpu):
    """
    Depending of whether this set of results are from a Multi FIO or a typical
    response curve, aggregate the OSD CPU avg measurements into the main table
    """
    # Note: if num_files  > len(avg_cpu): this is a MultiFIO
    if len(avg_cpu):
        print(f" avg_cpu list has: {len(avg_cpu)} items")
        # The number of CPU items should be the same as the number of dict_files.keys()
        for cpu_item in avg_cpu:
            for k in cpu_item.keys(): # 'sys', 'us' normally from the OSD
                cpu_avg_k = 0
                samples = cpu_item[k]
                for cpu in samples.keys():
                    cpu_avg_k += samples[cpu]
                cpu_avg_k /= len(samples.keys())
                # Aggregate the CPU values in the avg table
                if not k in avg:
                    avg[k] = 0
                avg[k] += cpu_avg_k
                # Aggregate the CPU values in the FIO table
                if not k in table:
                    table[k] = []
                table[k].append(cpu_avg_k)

        pp = pprint.PrettyPrinter(width=41, compact=True)
        print("Table (after aggregating OSD CPU avg data):")
        pp.pprint(table)

def gen_table(dict_files, config, title, avg_cpu, multi=False):
    """
    Construct a table from the predefined keys, sorted according to the
    file naming convention:
    fio_[d+]cores_[d+]img_[d+]job_[d+]io_[4k|64k|128k|256k]_[rw|rr|sw|sr].json
    Each row of the table is the results from a run in a single .json

    The avg_cpu is a list of dict each with keys 'sys' and 'us', containing values per cpu core

    The optional multi option is used to differentiate between multiple FIO instances,
    which involve several .json files all from a concurrent execution (within the same timespan),
    as opposed to a response curve run which also involve
    several json files but within its own timespan.

    For a response curve over a double range (that is num_jobs and iodepth), the data for gnuplot is
    separated into its own table, so the .dat file ends up with one table per num_job.
    """
    table, avg = initial_fio_table(dict_files, multi)
    table_iters = {}
    list_subtables = []
    num_files = len(dict_files.keys())
    # The following produces a List of dicts, each with keys 'sys', 'us',
    # each sample with list of CPU (eg. 0-3, and 4-7)
    # Aggregate osd_cpu_us and osd_cpu_sys into the main table
    aggregate_cpu_avg(avg, table, avg_cpu)
    # Note: in general the CPU measurements are global across the test time
    # for all FIO processes, so in the MultiFIO case we need to reduce the FIO measurements first

    # Construct headers -- this is the same for both cases
    # For the gnuplot .dat, each subtable ranges over num_jobs(threads)
    # whereas each row within a table ranges over iodepth
    gplot_hdr = "# "
    gplot_hdr += ' '.join(table.keys())
    gplot_hdr += "\n"
    gplot = ""

    wiki = r"""{| class="wikitable"
|-
! colspan="7"  | """ + config.replace("_list","") + """
! colspan="2"  | OSD CPU%
|-
! """
    wiki += ' !! '.join(table.keys())
    wiki += "\n|-\n"

    for k in table.keys():
        table_iters[k] = iter(table[k])
    # Construct the wiki table: the order given by the file names is important here,
    # we need to identify where a block break in the data table is needed. We use
    # our naming convention to identify this:
    # fio_crimson_1osd_default_8img_fio_unrest_2job_16io_4k_randread_p5.json
    for name in dict_files.keys():
        m = re.match(r".*(?P<job>\d+)job_(?P<io>\d+)io_",name)
        if m:
            # Note: 'job' (num threads) is constant within  each table,
            # each row corresponds to increasing the iodepth, that is, each
            # sample run
            job = int(m.group('job')) # m.group(1)
            _io =  int(m.group('io'))  # m.group(2)
            if job not in list_subtables:
                # Add a gnuplot table break (ie new block)
                if len(list_subtables) > 0:
                    gplot += "\n\n"
                gplot += f"## num_jobs: {job}\n"
                gplot += gplot_hdr
                list_subtables.append(job)

            for k in table.keys():
                item = next(table_iters[k])
                if k == 'iodepth': #This metric is the first column
                    gplot += f" {item} "
                    wiki += f' | {item} '
                else:
                    gplot += f" {item:.2f} "
                    wiki += f' || {item:.2f} '
            gplot += "\n"
            wiki += "\n|-\n"
    if multi:
        wiki += '! Avg:'
        for k in avg.keys():
            wiki += f' || {avg[k]:.2f} '
        wiki += "\n|-\n"
        if 'iops' in avg.keys():
            total = avg['iops'] * num_files
        else:
            total = avg['bw'] * num_files
        wiki += f'! Total: || {total:.2f} '
        wiki += "\n|-\n"

    # format_numeric = lambda num: f"{num:e}" if isinstance(num, int) else f"{num:,.2f}"
    wiki += "|}\n"
    print(f" Wiki table: {title}")
    print(wiki)
    gen_plot(config, gplot, list_subtables, title)
    print('Done')

def main(directory, config, json_query):
    """
    Entry point: an initial path is an inheritance of the original script from which this tool
    evolved
    """
    if not bool(json_query):
        json_query='jobs/jobname=*'
    json_tree_path = json_query.split('/')
    dicto_files = traverse_files(directory, config, json_tree_path)
    print('Note: clat_ns has been converted to milliseconds')
    print('Note: bw has been converted to MiBs')
    return dicto_files

def load_avg_cpu_json(json_fname):
    """
    Load a .json file containing the CPU avg samples -- normally produced by the script
    parse-top.pl
    """
    try:
        with open(json_fname, "r") as json_data:
            cpu_avg_list = []
            # check for empty file
            f_info = os.fstat(json_data.fileno())
            if f_info.st_size == 0:
                print(f'JSON input file {json_fname} is empty')
                return cpu_avg_list
            # parse the JSON: list of dicts with keys 'sys' and 'us'
            cpu_avg_list = json.load(json_data)
            return cpu_avg_list
    except IOError as e:
        raise argparse.ArgumentTypeError(str(e))

def parse_args():
    """
    As it says on the tin
    """
    parser = argparse.ArgumentParser(description='Parse set of output json FIO results.')
    parser.add_argument(
            "-c", "--config", type=str,
            required=True,
            help="Name of the file with the list of JSON files names to examine", default="")
    parser.add_argument(
            "-t", "--title", type=str,
            required=True,
            help="Title for the response curve gnuplot chart", default="")
    parser.add_argument(
            "-a", "--average", type=str,
            help="Name of the JSON file with the CPU avg", default="")
    parser.add_argument(
            "-d", "--directory", type=str,
            help="result directory to evaluate", default="./")
    parser.add_argument(
            "-q", "--query", type=str,
            required=False,
            help="JSON query", default="jobs/jobname=*")
    parser.add_argument(
            '-m', '--multi', action='store_true',
            required=False,
            help="Indicate multiple FIO instance as opposed to response curves", default=False)
    args = parser.parse_args()
    return args

if __name__=='__main__':
    args = parse_args()
    dict_files = main(args.directory, args.config, args.query)
    avg_cpu = load_avg_cpu_json(args.average)
    gen_table(dict_files, args.config, args.title, avg_cpu, args.multi)
