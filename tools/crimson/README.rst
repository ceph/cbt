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
=============================
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
