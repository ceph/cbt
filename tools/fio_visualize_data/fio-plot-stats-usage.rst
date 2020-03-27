====================
Visualize Fio Output
====================

Motivation
==========

Fio generates quite a bit of output that is sometimes hard to decipher
and understand. This problem is exacerbated further if one is running
multiple tests with different ceph options to tune ceph performance.
It would be good to have a tool that decodes the data from the log files
created by Fio and generate meaningful graphs that provide insight into
ceph performance.

The attempt here is to start with some basic scripts that parse Fio
output and generate plots like average client latencies and completion
latency percentiles.

Going further the idea is to enhance the scripts to generate more meaningful
graphs, tighter integration with cbt to generate graphs via yaml
specification as part of the test itself.

Usage
=====
.. code-block:: console


    $ ./fio-plot-stats.py -h
    usage: fio-plot-stats.py [-h] -f {json,csv} -s SRCDIR [-d DESTDIR] -o
                         {read,write}

    Generate plots from fio output

    optional arguments:
     -h, --help            show this help message and exit
     -f {json,csv}, --filetype {json,csv}
                           type of file to parse
     -s SRCDIR, --source SRCDIR
                           source directory containing fio output files
     -d DESTDIR, --destination DESTDIR
                           destination directory to save generated plots
     -o {read,write}, --optype {read,write}
                           plot read or write stats


The input file format option ``-f`` is mandatory. Depending on this,
additonal  options if preferred may be provided to override the default
behavior. The default behavior is to treat each output file in the source
directory and generate comparison graphs.

The option ``-o`` tells the script to scan read or write statistcs in the
Fio files and generate the graphs.

Assumption
==========
All fio files in the source directory having 'json'
string in filename are treated as JSON files. Otherwise, the file is
assumed to be of type CSV.

Example
=======
The following command scans the source directory for files having
string 'json' in the filenames and scans relevant stats from the files
to generate one graph per file in the destination folder

.. code-block:: console

     $python3 fio-plot-stats.py -s ~/cbt_logs/json_logs -f json -o write -d ~/cbt_logs/json_logs

