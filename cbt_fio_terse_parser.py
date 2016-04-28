#!/usr/bin/python
import argparse
import sys
import yaml
import os
import subprocess
import re

def parse_args(args):
    parser = argparse.ArgumentParser(description='Parser for FIO terse output')
    parser.add_argument(
        '-a', '--archive',
        required=True,
        help='Directory where CBT results has been archived.',
        )

    parser.add_argument(
        '-c', '--config_file',
        required=True,
        help='YAML config file that has been used previously with CBT to generate FIO terse output. ',
        )

    return parser.parse_args(args[1:])

def getFromDict(dataDict, mapList):
    for k in mapList:
      dataDict = dataDict[k]
    return dataDict

def fio_parser(args):
    config = {}
    try:
        with file(args.config_file) as f:
            map(config.update, yaml.safe_load_all(f))
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))

    benchmarks = config.get('benchmarks', {})
    if "stdfiobench" in benchmarks:
      benchmark_mode = ["stdfiobench", "mode"]
      benchmark_mode  = getFromDict(benchmarks, benchmark_mode) 
    for i in range(len(benchmark_mode)):
      if benchmark_mode[i] in ('read','write','randread','randwrite'):
        file_header = 'echo "Hostname Pattern Block_Size_Bytes Bandwidth_KB/s IOPS Threads Queue_Depth Latency_mean Total_I/O_KB Rununtime_ms" >> %s/fio_%s_summary.out' % (args.archive,benchmark_mode[i])
      elif benchmark_mode[i] in ('readwrite','rw','randrw'):
        file_header = 'echo "Hostname Pattern Block_Size_Bytes Threads Queue_Depth Read_Bandwidth_KB/s Read_IOPS Read_latency_mean Read_Total_I/O_KB Read_runtime_ms Write_Bandwidth_KB/s Write_IOPS Write_latency_mean Write_Total_I/O_KB Write_runtime_ms" >> %s/fio_%s_summary.out' % (args.archive,benchmark_mode[i])
      subprocess.call(file_header, shell=True)

      for dirname, dirnames, filenames in os.walk(args.archive):
        for subdirname in dirnames:
          if subdirname == benchmark_mode[i]:
            path = os.path.join(dirname, subdirname)
            for dirname, dirnames, filenames in os.walk(path):
              for filename in sorted(filenames):
                if re.search('terse_output*', filename):
                  output_file_name = os.path.join(dirname, filename)
                  if benchmark_mode[i] in ('write','randwrite'): 
                    command = "cat %s | awk -F ';' '{print $131,$132,$133,$48,$49,$135,$134,$81,$47,$50}' >> %s/fio_%s_summary.out" % (output_file_name,args.archive,benchmark_mode[i])
                  elif benchmark_mode[i] in ('read','randread'):
                    command = "cat %s | awk -F ';' '{print $131,$132,$133,$7,$8,$135,$134,$40,$6,$9}' >> %s/fio_%s_summary.out" % (output_file_name,args.archive,benchmark_mode[i])
                  elif benchmark_mode[i] in ('readwrite','rw','randrw'):
                    command = "cat %s | awk -F ';' '{print $131,$132,$133,$135,$134,$7,$8,$40,$6,$9,$48,$49,$81,$47,$50}' >> %s/fio_%s_summary.out" % (output_file_name,args.archive,benchmark_mode[i])
	          subprocess.call(command, shell=True)

def main(argv):
    args = parse_args(argv)
    fio_parser(args)

if __name__ == '__main__':
    exit(main(sys.argv))
