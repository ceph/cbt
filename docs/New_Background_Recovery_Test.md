# CBT - New Recovery Test Steps and Usage

## INTRODUCTION
A new recovery test thread class is created for the new test. The original recovery thread class is retained as is and only the class name is changed in order to create the appropriate recovery test thread object based on user options. By default the original test will be executed if no options are set in the CBT yaml configuration file. The example yaml configuration further below shows how to set the new options in order to invoke the new test. The original recovery test utilized a single rbd image to run the recovery test. This test class name within CBT is renamed to `RecoveryTestThreadBlocking`.

Broadly speaking, the new recovery test uses two pools - one for running recovery load and another for running client load and captures statistics related to recovery IO and client IO. The new test class is called `RecoveryTestThreadBackground`. This is created to test QoS using mclock scheduler and its associated options. But this test can be used to verify performance when using other schedulers as well.

## TEST STEPS

The following outlines the new recovery test steps:

 1. At the start of the test, the cluster is brought up with specified number of osds.
 2. Create a pool to populate recovery data (recovery pool).
 3. Create the recovery thread and mark an osd down and out.
 4. After the cluster handles the osd down event, data is prefilled into the recovery pool using the `pool_profile` options.
 5. After the prefill stage is completed, the downed osd is brought up and in. The rebalancing and backfill phase starts at this point. During this phase, the test captures the total number of misplaced objects and the number of misplaced objects recovered.
 6. At the same point the backfill/rebalancing stage starts, the test proceeds to initiate client IO (fio) on the IO pool using client(s) as mentioned in the cbt yaml file. Statistics related to the client latency and bandwidth are captured as usual by fio during this phase.

To summarize, the steps above creates 2 pools during the test. Recovery is triggered on one pool and client IO is triggered on the other. Statistics during each of the phases is captured and is discussed below.

## USAGE

> NOTE: The recovery test will currenly work with `librbdfio` and `fio` (and therefore librbd, rbd-kernel, rbd-nbd, rbd-fuse, rbd-tcmu, cephfs-kernel and cephfs-fuse drivers) benchmarks.

The following example yaml file creates a single node cluster and uses the `fio` benchmark with `librbd` driver. The new options to take note of are within the `client_endpoints` section. Since the test requires two pools as described above, specify a recovery pool for e.g. 'rbdrecov' for the `recov_pool_profile` option and another pool for e.g. `rbd` to run client IO on.

> NOTE: The recovery pool regardless of the driver being tested is a rbd based pool and populated with objects using radosbench.

The following options must be set on the recovery pool to differentiate it from a normal pool,
1. Set `recov_pool` to `True` to designate the pool as recovery pool.
2. Set the `prefill_recov_time`, `prefill_recov_objects` and `prefill_recov_object_size` to the desired values. Prefill of objects is done using radosbench tool as usual.

In addition to the above, set the `recov_test_type` to the recovery test type to run. Until now there was only one type of recovery test and this is designated as `blocking`. By default `recov_test_type` is set to `blocking` by CBT unless overridden by the option shown in the example below i.e. set to the new recovery test type - `background`.

Statistics related to the total misplaced objects and the number of misplaced objects recovered during the test is captured in a backfill stats log file for further analysis. Existing client related statistics logs can be used to investigate client throughput, latency etc. during the time recovery is in progress.

> NOTE: If the `recov_test_type` option is not set, then the default recovery test (i.e. 'blocking') will be invoked regardless of any of the new options that may be set.

```yaml
cluster:
  user: 'sridhar'
  head: "incerta06"
  clients: ["incerta06"]
  osds: ["incerta06"]
  mons:
    incerta06:
      a: "127.0.0.1:6789"
  mgrs:
    incerta06:
      x: "127.0.0.1:1234"
  osds_per_node: 4
  fs: 'xfs'
  mkfs_opts: '-f -i size=2048'
  mount_opts: '-o inode64,noatime,logbsize=256k'
  conf_file: '/home/sridhar/cbt_tests/conf/ceph.conf.4osd'
  iterations: 1
  use_existing: False
  clusterid: "ceph"
  tmp_dir: "/tmp/cbt"
  pool_profiles:
    rbdrecov:
      recov_pool: True
      pg_size: 64
      pgp_size: 64
      replication: 3
      prefill_recov_time: 60
      prefill_recov_objects: 500000
      prefill_recov_object_size: 4194304
    rbd:
      pg_size: 64
      pgp_size: 64
      replication: 3
  recovery_test:
    osds: [0]
client_endpoints:
  incerta06:
    driver: 'librbd'
    pool_profile: 'rbd'
    recov_pool_profile: 'rbdrecov'
benchmarks:
  fio:
    time: 180
    time_based: true
    direct: 1
    prefill: False
    vol_size: 2048
    mode: ['randwrite']
    op_size: [4096]
    procs_per_volume: [1]
    iodepth: [64]
    osd_ra: [4096]
    cmd_path: '/home/sridhar/cbt_tests/fio/fio'
    volumes_per_client: [1]
    client_endpoints: 'incerta06'
    recov_test_type: 'background'
```

The associated ceph config file sample is shown below with various settings used to test mclock features,

```ini
[global]
        osd pool default size = 3
        auth cluster required = none
        auth service required = none
        auth client required = none
        keyring = /home/sridhar/cbt_tests/keyring
        osd pg bits = 8
        osd pgp bits = 8
        log to syslog = false
        log file = /home/sridhar/cbt_tests/logs/$name.log
        rbd cache = true
        osd scrub load threshold = 0.01
        osd scrub min interval = 137438953472
        osd scrub max interval = 137438953472
        osd deep scrub interval = 137438953472
        osd max scrubs = 16
        osd_crush_chooseleaf_type = 0
        osd_recovery_sleep_hdd = 0
        osd_recovery_sleep_hybrid = 0
        osd_recovery_max_active = 1000
        osd_max_backfills = 1000
        bluestore_throttle_bytes = 131072
        bluestore_throttle_deferred_bytes = 131072
        osd_pool_default_pg_autoscale_mode = off
        osd_op_num_shards = 1
        osd_op_queue = mclock_scheduler
        osd_mclock_scheduler_client_res = 1
        osd_mclock_scheduler_client_wgt = 500
        osd_mclock_scheduler_client_lim = 1000000
        osd_mclock_scheduler_background_recovery_res = 1
        osd_mclock_scheduler_background_recovery_wgt = 1
        osd_mclock_scheduler_background_recovery_lim = 1000000
        filestore merge threshold = 40
        filestore split multiple = 8
        mon pg warn max object skew = 100000
        mon pg warn min per osd = 0
        mon pg warn max per osd = 32768
        erasure code dir = /usr/local/lib64/ceph/erasure-code
        plugin dir = /usr/local/lib64/ceph
        osd class dir = /usr/local/lib64/rados-classes

[mon]
        mon data = /tmp/cbt/ceph/mon.$id

[mon.a]
        host = incerta06
        mon addr = 127.0.0.1:6789

[osd.0]
        host = incerta06
        osd data = /tmp/cbt/mnt/osd-device-0-data
        osd journal = /dev/disk/by-partlabel/osd-device-0-journal
        bluestore block path = /dev/nvme0n1p1
...
...
```

The benchmark suite can be launched with:

```bash
cbt.py --archive=<archive dir> --conf=./ceph.conf ./mytests.yaml
```

## CONCLUSION & NEXT STEPS

The new recovery test was used extensively to test the impact of mclock options on both recovery IO and client IO. The same test was also used to compare statistics across schedulers for e.g. 'WPQ'. The recovery test can currently be invoked using the `librbdfio` and `fio` benchmarks. Depending on the need other existing or upcoming  benchmarks can implement the test as the next logical step.

