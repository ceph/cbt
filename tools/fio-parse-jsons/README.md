# fio-parse-jsons.py - a FIO post processing tool.

## Description

This is a standalone tool to assist the post processing of JSON outout files from CBT when running the FIO benchmark.

The execution of the script produces as outp:

1. a gnuplot script,
2. a .dat file with the data to plot,
3. a summary table of FIO results in wiki format, printed to stdout.

This is especially useful to produce a response graph from a set of executions ranging the number of FIO jobs and the iodepth values.
The script was written before knowledge of CBT was gained, so in a way is independent of the script driving the tests.
A future PR would integrate the functionality of this standalone script with that of CBT.

## Usage:

The following is an example of the execution of the script:

```bash
# python3 /cbt/tools/fio-parse-jsons.py -c crimson200gb_1procs_randwrite_list -t 'Crimson 200GB RBD 4k rw' -a crimson4cores_200gb_1img_4k_1procs_randwrite_avg.json
```

the arguments are:

- `-c config_file:`a txt file containing the list of FIO output JSON file to process,
- `-t title:` the string ot use as title for the gnuplot chart,
- `-a cpu_avg.json:` a .json file containing the avg CPU utilisation, normally produced by the script parse-top.pl.

The following are the .dat and gnuplot files produced:
```bash
 crimson200gb_1procs_randwrite.dat
 crimson200gb_1procs_randwrite.plot
```

To produce the chart, simply execute

```bash
gnuplot classic200gb_1procs_randwrite.plot
```

the IOPs vs latency chart result is shown below:
