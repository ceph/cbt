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

This is a rados bench stress tool for single client or multiple client crimson
osd writing test to understand how to stress crimson osd. User can set the 
number of clients and threads, which processors will bench threads execute 
on(to avoid test threads influence the result), test time, block size, etc.,
to run the test case. Then the tool will integrate IOPS, Bandwidth, Latency 
and seastar reactor utilization to help user analyze cimson osd performance.

To use this tool, prepare the python3 environment, crimson cluster and a test 
pool. Put this tool in the ceph build directory. Since we will decide which 
processors the bench threads execute on, sudo is needed.

Run ``./rados_bench_tools.py --help`` to get the detail parameter information.

Example of single client stress test:

.. code-block:: console
    
    sudo ./rados_bench_tools.py --clean-up=True --client=1 --thread-list=[4,6,8,10,12,14,16,18,20,22,24,26,28,30,32] --time=300

The tool will serially run test case with 1 client and 4 threads, 6 threads, 
8 threads, etc. In every test case, threads run parallel but different cases 
run serially. 

Example of multiple client stress test:

.. code-block:: console

    sudo ./rados_bench_tools.py --clean-up=True --taskset=16-31 --client-list=[4,6,8,10,12,14,16,18,20,22,24,26,28,30,32] --thread=2

The tool will run test case with 2 threads and 4 clients(parallel), 6 clients, 
etc. and the test thread will run in processors 16~31. In every test case, 
clients and threads in every client run parallel.
In consideration of SeaStore starts in processor 0 by default, please avoid 
setting --taskset to 0.

Example of result:

.. code-block:: console
    
    CLIENTS  THREADS  IOPS  BANDWIDTCH  LATENCY REACTOR_UTILIZATION
    3         1        2580.75    10.0838    0.0003851295    12.74903761999997
    5         1        1728.6666666666667    6.754173333333333    0.0005760875    92.36707335999996
