# Introduction

The stdfiobench CBT module is the simplest way to benchmark any block device using FIO. 
This module does not uses Ceph or its components, so it can be used for non Ceph block device benchmarking. 
There are scenarios such as measuring baseline or measuring Ceph block device vs. other block device performance, you can use this module in such cases

## How to use this module

If you are familiar with CBT and its YAML configuration files, then using stdfiobench is pretty straightforward. Just call the module from
YAML file and provide values to its arguments as shown below.

**Step:1  Design your test case in CBT YAML file**
```
cluster:
  head: "ceph@rhclient0"
  clients: ["ceph@rhclient0","ceph@rhclient1","ceph@rhclient2","ceph@rhclient3","ceph@rhclient4","ceph@rhclient5","ceph@rhclient6","ceph@rhclient7","ceph@rhclient8","ceph@rhclient9"]
  iterations: 1
  rebuild_every_test: False
  use_existing: True
  tmp_dir: "/tmp/cbt"
benchmarks:
  stdfiobench:
    concurrent_procs: [1]
    run_time: 5
    ramp_time: 5
    iodepth: [32]
    numjobs: 1
    mode: ['write','read']
    rwmixread: 70 # Default 50-50 for read and write
    ioengine: 'libaio'
    op_size: [40960,10240] # Block size in Bytes
    vol_size: 106444 # Volume size in MB
    fio_path: '/usr/local/bin/fio' # Path to fio command on the remote machine
    block_device: 'scinia' # Just provide block device name ex: 'sdc' , do not prefix '/dev/'
    mount_point_name: '/mnt/stdfiobench' # Directory where block device must be mounted
    filesystem: 'xfs'
    use_existing: True
    client_ra: 1024
    output_format: 'terse' # 'normal' , 'json', 'terse'(default, supports parsing using tools/cbt_fio_terse_parser.py)
```
**Step:2  Using CBT fire you test**
```
[ceph@rhclient0 cbt]$ ./cbt.py --archive=test1_results example/stdfiobench.yaml
```
Wait for CBT to finish your tests and collect the results under test1_results archive directory

**Step:3  Parse your test results**

stdfiobench module comes with a parser located at  ``tools/cbt_fio_terse_parser.py`` , which currently supports parsing FIO output generated **only in 'terse' format.**

If you are using output format other then terse, then you need to parse output in your own ways ( looking forward for your contribution ) 

To parse your CBT results which are in 'terse' format , execute the following command
```
[ceph@rhclient0 cbt]$ ./tools/cbt_fio_terse_parser.py --archive=test1_results --config_file=example/stdfiobench.yaml
[ceph@rhclient0 cbt]$
```
Based on the test modes, the parser will generate as many *.out files under the archive directory

```
[ceph@rhclient0 cbt]$ ls -l test1_results/
total 12
drwxrwxr-x 3 ceph ceph   24 Apr 26 14:19 00000000
-rw-rw-r-- 1 ceph ceph 1403 May  2 05:31 fio_read_summary.out
-rw-rw-r-- 1 ceph ceph 1383 May  2 05:31 fio_write_summary.out
[ceph@rhclient0 cbt]$
```

**Step:4  Read the results**

The *.out are your final result files, just copy-paste in spreadsheet and make nice benchmarking graphs
```
[ceph@rhclient0 cbt]$
[ceph@rhclient0 cbt]$ cat test1_results/fio_write_summary.out
Hostname Pattern Block_Size_Bytes Bandwidth_KB/s IOPS Threads Queue_Depth Latency_mean Total_I/O_KB Rununtime_ms
rhclient0 write 40960 308596 7708 1 32 4157.868334 1543600 5002
rhclient1 write 40960 299172 7473 1 32 4284.611236 1497360 5005
rhclient2 write 40960 294820 7364 1 32 4341.939347 1475280 5004
rhclient3 write 40960 291675 7285 1 32 4393.145830 1458960 5002
rhclient4 write 40960 298809 7464 1 32 4283.484891 1495840 5006
rhclient5 write 40960 295709 7386 1 32 4329.048401 1481800 5011
rhclient6 write 40960 359184 8973 1 32 3562.948526 1796640 5002
rhclient7 write 40960 305515 7631 1 32 4190.137938 1528800 5004
rhclient8 write 40960 301791 7538 1 32 4251.131456 1509560 5002
rhclient9 write 40960 298698 7461 1 32 4284.651876 1496480 5010
[ceph@rhclient0 cbt]$
[ceph@rhclient0 cbt]$ cat test1_results/fio_read_summary.out
Hostname Pattern Block_Size_Bytes Bandwidth_KB/s IOPS Threads Queue_Depth Latency_mean Total_I/O_KB Rununtime_ms
rhclient0 read 40960 5197360 129927 1 32 245.921331 25992000 5001
rhclient1 read 40960 5141347 128527 1 32 248.530895 25711880 5001
rhclient2 read 40960 5371773 134288 1 32 237.890541 26864240 5001
rhclient3 read 40960 5304819 132614 1 32 240.992396 26529400 5001
rhclient4 read 40960 5027018 125669 1 32 254.232817 25140120 5001
rhclient5 read 40960 5464427 136604 1 32 233.821755 27327600 5001
rhclient6 read 40960 5444967 136117 1 32 234.638216 27230280 5001
rhclient7 read 40960 5336956 133417 1 32 239.377187 26690120 5001
rhclient8 read 40960 5631865 140790 1 32 226.878658 28164960 5001
rhclient9 read 40960 4945658 123635 1 32 258.412485 24733240 5001
[ceph@rhclient0 cbt]$
```
## Additional information
To get to know all the fields of FIO terse output check out [FIO minimal output explained](http://andypeace.com/fio_minimal.html)

In addition to standard terse FIO output , i have introduced some new fields , which decodes as below
```
131 - Client hostname
132 - Pattern / mode
133 - Block Size
134 - Queue Depth
135 - Number of threads
136 - Client Read Ahead value
137 - Concurrent procs
```




