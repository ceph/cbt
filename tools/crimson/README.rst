=============
Crimson tools
=============

SeaStore rados bench profiler
=============================

We can profile SeaStore using rados bench with 1 OSD.

To start the test envionment:

.. code-block:: console

     $ MGR=0 MON=1 OSD=1 MDS=0 RGW=0 ../src/vstart.sh -n -x --without-dashboard --seastore --crimson --nodaemon --redirect-output
     $ ./bin/ceph osd pool create test-pool 5 5
     $ ./bin/ceph osd pool set test-pool size 1 --yes-i-really-mean-it

Then run ``seastore_radosbench_run.sh`` to generate workload and collect
metrics.

Finally, run ``seastore_metrics_analyze.py`` to generate diagram from the
collected metrics.

SeaStore fio bench profiler
===========================

We can profile SeaStore using fio bench with 1 OSD.

To start the test envionment:

.. code-block:: console

     $ MGR=0 MON=1 OSD=1 MDS=0 RGW=0 ../src/vstart.sh -n -x --without-dashboard --seastore --crimson --nodaemon --redirect-output

Then run ``seastore_fio_run.sh`` to generate workload and collect
metrics.

Finally, run ``seastore_metrics_analyze.py`` to generate diagram from the
collected metrics.

example rbd_write.fio:

.. code-block:: ini

  [global]
  ioengine=rbd
  clientname=admin
  pool=rbd
  rbdname=fio_test
  rw=randwrite
  bs=4K
  runtime=60
  numjobs=1
  direct=1
  group_reporting

  [rbd_iodepth32]
  iodepth=2

SeaStore metrics profiler
=========================

This is a client-independent and non-stop way to collect metrics for profiling.

Without the client providing the client IO size and number, it assumes the
information from the metrics of committing OBJ_DATA_BLOCK extents.

Since metric files are labeled with time, all the existing plots are changed to
use time as x-axis, and the non-stop way makes it possible to include seastar
metrics correctly.

To start tests, prepare the crimson cluster correctly with seastore, generate
the desired workload, and run ``seastore_metrics_run.sh`` to collect the
metrics.

Finally, run ``seastore_metrics_analyze.py`` to generate plots in png format.

Rados bench stress tool
=======================

This is a rados bench stress tool for multiple clients and multiple threads 
osd writing test to understand how to stress crimson osd. User can set the 
number of clients and threads, which processors will bench threads execute 
on(to avoid test threads influencing the result), test time, block size, etc.
, to run the test case. Then the tool will integrate IOPS, Bandwidth, Latency 
seastar reactor utilization or other targets to help user analyze crimson 
osd performance.

To use this tool, prepare the python3 environment, osd cluster and a test 
pool. Put this tool in the ceph build directory. Since we will decide which 
processors the bench threads execute on, sudo is needed.

Run ``./rados_bench_tools.py --help`` to get the detail parameter information.

Example:

.. code-block:: console
    
    sudo ./rados_bench_tools.py --thread-list 1 2 --client-list 2 4 6 --taskset=16-31 --reactor-utilization=True --time=300

The tool will run test case with the combination of 1 or 2 clients and 2, 4 or
6 threads. Meanwhile, it will collect the reactor utilization, which is the 
utilization of the cpu from seastar. The test thread will run in processors 
16~31. In consideration of SeaStore starts in processor 0 by default, please 
avoid setting --taskset to 0.

Example of result:

.. code-block:: console

   bandwidth                iops             latency reactor_utilization          thread_num          client_num
   0.5301985               135.5           0.0073715   2.370669279999993                   1                   2
   0.4831115               123.5          0.01609335  27.892383860000024                   2                   2
