# CBT - The Ceph Benchmarking Tool

## INTRODUCTION

CBT is a python tool for building a ceph cluster and running benchmarks against
it. Several benchmarks are supported, included radosbench, fio and cosbench. CBT
also can record system metrics and profiles using a number of tools including
perf, collectl, blktrace, and valgrind.  In addition various interesting ceph
configurations and tests are supported, including automated OSD outages, erasure
coded pools, and pool cache tier configurations.

## PREREQUISITES

CBT uses several libraries and tools to run:

 1. python-yaml - A YAML library for python used for reading 
    configuration files.
 2. ssh (and scp) - remote secure command executation and data 
    transfer
 3. pdsh (and pdcp) - a parallel ssh and scp implementation
 4. ceph - A scalable distributed storage system

Note that PDSH is not packaged for RHEL7 and CentOS 7 based distributations 
at this time, though the Fedora PDSH package installs and is usable.  The
RPMs for these packages are available here:

 - ftp://rpmfind.net/linux/fedora/linux/development/rawhide/i386/os/Packages/p/pdsh-2.31-4.fc23.i686.rpm
 - ftp://rpmfind.net/linux/fedora/linux/development/rawhide/x86_64/os/Packages/p/pdsh-rcmd-rsh-2.31-4.fc23.x86_64.rpm
 - ftp://rpmfind.net/linux/fedora/linux/development/rawhide/x86_64/os/Packages/p/pdsh-rcmd-ssh-2.31-4.fc23.x86_64.rpm

Optional tools and benchmarks can be used if desired:

 1. collectl - system data collection
 2. blktrace - block device io tracing
 3. seekwatcher - create graphs and movies from blktrace data
 4. perf - system and process profiling
 5. valgrind - runtime memory and cpu profiling of specific processes
 6. fio - benchmark suite with integrated posix, libaio, and librbd 
    support
 7. cosbench - object storage benchmark from Intel

## USER AND NODE SETUP

In addition to the above software, a number of nodes must be available to run
tests.  These are divided into several categories.  Multiple categories can
contain the same host if it is assuming multiple roles (running OSDs and a mon
for instance).

 1. head - node where general ceph commands are run
 2. clients - nodes that will run benchmarks or other client tools
 3. osds - nodes where OSDs will live
 4. rgws - nodes where rgw servers will live
 5. mons - nodes where mons will live

A user may also be specified to run all remote commands.  The host that is used
to run cbt must be able to issue passwordless ssh commands as the specified
user.  This can be accomplished by creating a passwordless ssh key:

```bash
sshkey-gen -t dsa
```

and copying the resulting public key in the ~/.ssh to the ~/.ssh/authorized_key
file on all remote hosts.

This user must also be able to run certain commands with sudo.  The easiest
method to enable this is to simply enable blanket passwordless sudo access for
this user, though this is only appropriate in laboratory environments.  This
may be acommplished by running visudo and adding something like:

```bash
# passwordless sudo for cbt
<user>    ALL=(ALL)       NOPASSWD:ALL
```

Where `<user>` is the user that will have password sudo access.  
Please see your OS documentation for specific details.

## DISK PARTITIONING

Currently CBT looks for specific partition labels in 
`/dev/disk/by-partlabel` for the Ceph OSD data and journal partitions.  
At some point in the future this will be made more flexible, for now 
this is the expected behavior.  Specifically on each OSD host 
partitions should be specified with the following gpt labels:

```
osd-device-<num>-data
osd-device-<num>-journal
```

where `<num>` is a device ordered starting at 0 and ending with the 
last device on the system.  Currently cbt assumes that all nodes in 
the system have the same number of devices.  A script is available 
that shows an example of how we create partition labels in our test 
lab here:

<https://github.com/ceph/cbt/blob/master/tools/mkpartmagna.sh>


## CREATING A YAML FILE

CBT yaml files have a basic structure where you define a cluster and a set of
benchmarks to run against it.  For example, the following yaml file creates a
single node cluster on a node with hostname "burnupiX". A pool profile is
defined for a 1x replication pool using 256 PGs, and that pool is used to run
RBD performance tests using fio with the librbd engine.

```yaml
cluster:
  user: 'nhm'
  head: "burnupiX"
  clients: ["burnupiX"]
  osds: ["burnupiX"]
  mons:
    burnupiX:
      a: "127.0.0.1:6789"
  osds_per_node: 1
  fs: 'xfs'
  mkfs_opts: '-f -i size=2048 -n size=64k'
  mount_opts: '-o inode64,noatime,logbsize=256k'
  conf_file: '/home/nhm/src/ceph-tools/cbt/newstore/ceph.conf.1osd'
  iterations: 1
  use_existing: False
  clusterid: "ceph"
  tmp_dir: "/tmp/cbt"
  pool_profiles:
    rbd:
      pg_size: 256
      pgp_size: 256
      replication: 1
benchmarks:
  librbdfio:
    time: 300
    vol_size: 16384
    mode: [read, write, randread, randwrite]
    op_size: [4194304, 2097152, 1048576]
    concurrent_procs: [1]
    iodepth: [64]
    osd_ra: [4096]
    cmd_path: '/home/nhm/src/fio/fio'
    pool_profile: 'rbd'
```

An associated ceph.conf.1osd file is also defined with various settings that
are to be used in this test:

```ini
[global]
        osd pool default size = 1
        auth cluster required = none
        auth service required = none
        auth client required = none
        keyring = /tmp/cbt/ceph/keyring
        osd pg bits = 8  
        osd pgp bits = 8
        log to syslog = false
        log file = /tmp/cbt/ceph/log/$name.log
        public network = 192.168.10.0/24
        cluster network = 192.168.10.0/24
        rbd cache = true
        osd scrub load threshold = 0.01
        osd scrub min interval = 137438953472
        osd scrub max interval = 137438953472
        osd deep scrub interval = 137438953472
        osd max scrubs = 16
        filestore merge threshold = 40
        filestore split multiple = 8
        osd op threads = 8
        mon pg warn max object skew = 100000
        mon pg warn min per osd = 0
        mon pg warn max per osd = 32768

[mon]
        mon data = /tmp/cbt/ceph/mon.$id
        
[mon.a]
        host = burnupiX 
        mon addr = 127.0.0.1:6789

[osd.0]
        host = burnupiX
        osd data = /tmp/cbt/mnt/osd-device-0-data
        osd journal = /dev/disk/by-partlabel/osd-device-0-journal
```

To run this benchmark suite, cbt is launched with an output archive 
directory to store the results and the yaml configuration file to use:

```bash
cbt.py --archive=<archive dir> ./mytests.yaml
```

You can also specify the ceph.conf file to use by specifying it on the
commandline:

```bash
cbt.py --archive=<archive dir> --conf=./ceph.conf.1osd ./mytests.yaml
```

In this way you can mix and match ceph.conf files and yaml test configuration
files to create parametric sweeps of tests.  A script in the tools directory
called mkcephconf.py lets you automatically generate hundreds or thousands of
ceph.conf files from defined ranges of different options that can then be used
with cbt in this way.

## CONCLUSION

There are many additional and powerful ways you can use cbt that are not yet
covered in this document. As time goes on we will try to provide better examples
and documentation for these features. For now, it's best to look at the
examples, look at the code, and ask questions!
