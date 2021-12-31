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

Crimson stress tool
=======================

This is a crimson stress tool for multiple clients and multiple threads 
osd writing test to understand how to stress crimson osd. Users can set the 
number of clients and threads, which kind of test to run (eg. rados bench or 
fio), which processors will bench threads execute on(to avoid test threads 
influencing the result), test time, block size, etc.
Meanwhile, users can set the basic test cases and the clients ratio of each 
type of test case threads running in the same time, for example, 75% of the 
write clients and 25% of the read clients.
Then the tool will integrate IOPS, Bandwidth, Latency, seastar reactor 
utilization, cpu cycle, and other targets to help user analyze 
crimson osd performance.

To use this tool, prepare the python3 environment. You don't need to start a 
ceph cluster because the tool will do that. Put this tool in the ceph build 
directory. Since we will decide which processors the bench threads execute on
, sudo is needed.

Run ``./crimson_stress_tool.py --help`` to get the detail parameter information.

Example:

.. code-block:: console
    
    sudo ./crimson_stress_tool.py \ 
        --client-list 4 8 --thread-list 2 4 6 --taskset 16-31 --time 300 \ 
        --rand-write 0.75 \
        --rand-read 0.25 \ 
        --reactor-utilization True \
        --perf True \
        --scenario crimson-seastore

The tool will run rados bench write and read test case with the combination 
of 4 or 8 clients and 2, 4 or 6 threads. In Every test case, there will be 75% of
write clients in all clients and the read clients will be 25%. Also, you can set
read clients ratio to 0 to do the write only tests, vive versa.
Meanwhile, it will collect the reactor cpu utilization, and the perf information. 
The test thread will run in processors 16~31. In consideration of SeaStore starts 
in processor 0 by default, please avoid setting --taskset to 0.
The tests will run in crimson seastore.

Example of result:

.. code-block:: console
    
    rw_bandwidth        3.10453               3.541689             3.5182199999999995   2.51662
    rw_iops             793.0                 903.0                900.0                642.0
    rw_latency          0.0037714466666666662 0.006630723333333334 0.006629473333333333 0.0186881
    rr_bandwidth        2.84725               2.68286              3.05852              1.471667
    rr_iops             728.0                 686.0                782.0                376.0
    rr_latency          0.00136141            0.002904295000000000 0.00254781           0.01060875
    reactor_utilization 51.76855957999997     63.44185818000002    62.81135658000002    57.04136848000001
    context-switches    21343                 20060                20770                18164
    cpu_cycle           4617829192            4504157482           4539431704           4732829464
    instructions        7146125116            7342745221           7228392400           7705910181
    branches            1305636128            1332912452           1316155264           1385922457
    thread_num          1                     1                    2                    2
    client_num          4                     8                    4                    8 
