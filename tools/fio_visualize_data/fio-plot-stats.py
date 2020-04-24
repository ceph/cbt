#!/usr/bin/python3
#
# fio-plot-stats.py - script to parse fio workload generation results
# for interesting statistics and generate PyPlot graphs. Idea is to
# parse results generated in json and csv by fio and make use of PyPlot
# module to generate graphs that will provide insights on Ceph performance
#
# input parameters:
# The input to this script can be a directory containing fio log file(s)
# in either,
# - JSON format
# - CSV format
#
# The input file format option is mandatory. Depending on this, additonal
# options if preferred may be provided to override the default behavior.
# The default behavior is to treat each output file in the source
# directory and generate comparison graphs.

# assumption: All fio files in the source directory having 'json'
# string in filename are treated as JSON files. Otherwise, the file is
# assumed to be of type CSV.
#
# Example 1:
# The following command scans the source directory for files having
# string 'json' in the filenames and scans relevant stats from the files
# to generate comparison graphs in the destination folder.
# python3 fio-plot-stats.py -s ~/cbt_logs/json_logs -f json -o write -d ~/cbt_logs/json_logs
import os
import argparse
from fiostatsparser import Parsejson
from fioplotter import Barplot

def dir_path(path):
  if os.path.isdir(path):
    return path
  else:
    raise argparse.ArgumentTypeError(f"{path} is not a valid directory.")

def parse_args():
  parser = argparse.ArgumentParser(description='Generate plots from fio output')
  parser.add_argument('-f', '--filetype', dest='ftype', required=True, choices=['json', 'csv'], help='type of file to parse')
  parser.add_argument('-s', '--source', dest='srcdir', required=True, type=dir_path, help='source directory containing fio output files')
  parser.add_argument('-d', '--destination', dest='destdir', required=False, type=dir_path, help='destination directory to save generated plots')
  parser.add_argument('-o', '--optype', dest='optype', required=True, choices=['read', 'write'], help='plot read or write stats')
  args = parser.parse_args()

  return args

if __name__ == '__main__':
  args = parse_args()

  # Parse data and generate plots
  if args.ftype == 'json':
    print("Parsing JSON files...")
    pj = Parsejson(args)
    pj.dump_all_stats_in_csv()
    print("Creating plots...")
    Barplot(args, pj.get_fio_latdata(), 'lat')
    Barplot(args, pj.get_fio_pctdata(), 'pct')

  if args.ftype == 'csv':
    print("Cannot parse CSV data...coming soon")

