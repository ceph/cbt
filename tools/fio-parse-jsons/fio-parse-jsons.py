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
# -t <title> - a string describning the Title for the chart, eg.
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

# Predefined dictionary of metrics (query paths on the ouput FIO .json) for typical workloads
# Keys are metric names, vallues are the string path in the .json to seek
predef_dict = {
          'randwrite': {
              'iodepth': 'global options/iodepth',
              'iops' : 'jobs/jobname=*/write/iops',
              'clat_ms' : 'jobs/jobname=*/write/clat_ns',
              'clat_stdev' : 'jobs/jobname=*/write/clat_ns',
              'usr_cpu': 'jobs/jobname=*/usr_cpu',
              'sys_cpu': 'jobs/jobname=*/sys_cpu'
              },
          'randread': {
              'iodepth': 'global options/iodepth',
              'iops' : 'jobs/jobname=*/read/iops',
              'clat_ms' : 'jobs/jobname=*/read/clat_ns',
              'clat_stdev' : 'jobs/jobname=*/read/clat_ns',
              'usr_cpu': 'jobs/jobname=*/usr_cpu',
              'sys_cpu': 'jobs/jobname=*/sys_cpu'
              },
          'seqwrite': {
              'iodepth': 'global options/iodepth',
              'bw' : 'jobs/jobname=*/write/bw',
              'clat_ms' : 'jobs/jobname=*/write/clat_ns',
              'clat_stdev' : 'jobs/jobname=*/write/clat_ns',
              'usr_cpu': 'jobs/jobname=*/usr_cpu',
              'sys_cpu': 'jobs/jobname=*/sys_cpu'
              },
          'seqread': {
              'iodepth': 'global options/iodepth',
              'bw' : 'jobs/jobname=*/read/bw',
              'clat_ms' : 'jobs/jobname=*/read/clat_ns',
              'clat_stdev' : 'jobs/jobname=*/read/clat_ns',
              'usr_cpu': 'jobs/jobname=*/usr_cpu',
              'sys_cpu': 'jobs/jobname=*/sys_cpu'
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
    if re.search('iops|usr_cpu|sys_cpu|iodepth', k):
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

def process_fio_json_file(json_file, json_tree_path):
    """
    Collect metrics from an individual JSON file
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
        # Use the jobname to index the predef_dict for the query
        jobname = str(node['jobs'][0]['jobname'])
        result_dict['jobname'] = jobname
        subdict = predef_dict[jobname]
        for k in subdict.keys():
            json_tree_path = subdict[k].split('/')
            next_node_list = [node]

            for next_branch in json_tree_path:
                next_node_list = filter_json_node(next_branch, next_node_list)
            item = process_fio_item(k, next_node_list)
            result_dict[k] = item

        return result_dict

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
        print(fname)
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
            'ycolumn': '3',
            'y2label': "OSD CPU system",
            'y2column': '8'},
        'iops_vs_lat_vs_cpu_usr': {
            'ylabel': "Latency (ms)",
            'ycolumn': '3',
            'y2label': "OSD CPU user",
            'y2column': '7'}
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
set autoscale  y
set autoscale y2
set output '{out_chart}'
set title "{_title}"
"""
            # To plot CPU util in the same response curve, we need the extra axis
            # This list_subtables indicates how many sub-tables the .datfile will have
            if len(list_subtables) > 0:
                head = f"plot '{out_data}' index 0 using 2:{ycol}:4 t '{list_subtables[0]} q-depth' w yerr axes x1y1"
                head += f",\\\n '' index 0 using 2:{ycol}:4 notitle w lp axes x1y1"
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
        # Each k is a FIO metric (iops, lat, etc)
        for k in subdict.keys():
            if not k in table:
                table[k] = []
            table[k].append(item[k])
            if multi:
                if not k in table:
                    avg[k] = 0.0
                avg[k] += item[k]
    # Probably might use some other avg rather than arithmetic avg
    # For a sinlge data point, table == avg
    # For multiple FIO, we always want to coalesce the table into avg
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
        pp.pprint(table)
        print("Avg after aggregating CPU avg data:\n")
        pp.pprint(avg)

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
! colspan="6"  | """ + config.replace("_list","") + """
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
            io =  int(m.group('io'))  # m.group(2)
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
