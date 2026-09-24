# Elbencho S3 — running with CBT

This guide covers running the Elbencho S3 benchmark end-to-end via CBT; writing the test plan
YAML, executing the run, and verifying results. A ready-to-edit example YAML lives at
[`example/wip-elbencho/elbencho_ex.yaml`](../../example/wip-elbencho/elbencho_ex.yaml).

## Prerequisites

- `elbencho` installed on all client nodes
- A running Ceph RGW endpoint and an S3 user with read/write access
- An existing S3 bucket (or set `mkdirs: True` on the first write workload to create one)

## Test plan YAML

```yaml
cluster:
  user: 'cbt'
  head: 'mon1'
  clients: ['client1']
  osds: ['osd1', 'osd2', 'osd3']
  rgws: ['osd1', 'osd2', 'osd3']
  osds_per_node: 1
  conf_file: '/etc/ceph/ceph.conf'
  iterations: 1
  use_existing: True
  clusterid: 'ceph'
  tmp_dir: '/tmp/cbt'

benchmarks:
  elbencho:
    cmd_path: '/usr/local/bin/elbencho'
    auth:
      config: access_key=<your-access-key>;secret_key=<your-secret-key>;url=http://192.168.110.51:8000;retry=9

    workloads:
      write_small:
        s3_bucket: 'cbt-benchmark'
        mode: 'write'
        mkdirs: True
        threads: [1, 4]
        iodepth: [1, 4]
        blocksize: ['5m', '16m']
        size: '5g'
        num_objects: 100
        duration: 30

      read_small:
        s3_bucket: 'cbt-benchmark'
        mode: 'read'
        threads: [1, 4]
        iodepth: [1, 4]
        blocksize: ['5m', '16m']
        size: '5g'
        num_objects: 100
        duration: 30
```

Replace `<your-access-key>`, `<your-secret-key>`, and the RGW URL with your cluster's values.
Your RGW user should be setup already as a pre-requisite to this process. To get your username,
simply run `radosgw-admin user list`, and to see your access/secret key, run

```bash
radosgw-admin user info --uid="<username>"
```

### How `blocksize` and `size` control S3 uploads

In S3 mode, `blocksize` is the **multipart upload part size** and `size` is the **per-object
size**. Elbencho decides between single-PUT and multipart upload based on their relationship:

- `blocksize >= size` → **single PutObject** (one HTTP PUT per object, no part-size constraints)
- `blocksize < size` → **multipart upload** (each part = `blocksize` bytes, parts per object = `ceil(size / blocksize)`)

When using multipart upload, S3 requires each part (except the last) to be **at least 5 MB**.
If `blocksize` is smaller than 5 MB in a multipart scenario, RGW rejects the upload.

With the example config above (`blocksize: ['5m', '16m']`, `size: '5g'`):
- `5m` part size → `ceil(5g / 5m)` = **1024 parts per object** (multipart)
- `16m` part size → `ceil(5g / 16m)` = **320 parts per object** (multipart)

To test single-PUT performance, set `blocksize >= size` (e.g. `blocksize: '64m'`, `size: '64m'`).

**Read workloads must match write parameters.** Elbencho locates objects by the same
`blocksize` and `size` used at write time. A mismatch causes HTTP 416 (Range Not Satisfiable).

```yaml
# Single-PUT: one HTTP request per object, no multipart overhead
blocksize: ['64m']
size: '64m'

# Multipart: 320 parts per object
blocksize: ['16m']
size: '5g'

# Wrong — blocksize too small for multipart (< 5 MB minimum part size)
blocksize: ['4k']
size: '1m'

# Wrong — read doesn't match write
write_small:
  blocksize: ['5m']
  size: '5g'
read_small:
  blocksize: ['8m']   # ← 416, doesn't match write
  size: '10g'         # ← 416, doesn't match write

# Correct — read matches write exactly
write_small:
  blocksize: ['5m', '16m']
  size: '5g'
read_small:
  blocksize: ['5m', '16m']   # identical to write
  size: '5g'                 # identical to write
```

## Running

```bash
PYTHONPATH=/cbt python3 cbt.py --archive /tmp/cbt-results example/wip-elbencho/elbencho_ex.yaml
```

CBT will:

1. Verify the elbencho binary is executable on all client nodes via parallel SSH
2. Expand each workload's list-valued params (`blocksize`, `threads`, `iodepth`) into the cartesian product of run cells through the shared Workloads pipeline. `blocksize` sets the multipart part size, `threads` sets the number of independent S3 client workers, and `iodepth` sets the number of concurrent async HTTP requests per thread.
3. Fan out one elbencho process per client node via parallel SSH for each run cell
4. Sync results back to the archive directory via `scp -r` when complete

The expected log output for each run cell looks like:

```
INFO  - Elbencho: running 1 command(s) → /tmp/cbt/00000000/Elbencho/write_131072/threads-004/iodepth-004
DEBUG - ssh [client1] exit=0
DEBUG - ssh [client1] stdout: OPERATION   RESULT TYPE  ...
=========== ================    ==========   =========
MKBUCKETS   Elapsed time     :        26ms        26ms
            Buckets/s        :          38          38
            Buckets total    :           1           1
---
WRITE       Elapsed time     :     16.248s     16.501s
            Objects/s        :          24          24
            Throughput MiB/s :         122         121
            Total MiB        :        1985        2000
            Objects total    :         395         400
---
INFO  - Elbencho: all workloads complete.
```

With 2 blocksizes × 2 thread values × 2 iodepth values × 2 workloads (write + read), this
configuration produces **16 run cells**.

## Expected result files

Each run cell produces a `result.csv`. Results are pulled from each client node via `scp -r`
into a per-hostname subdirectory under the archive, mirroring the remote path:

```
/tmp/cbt-results/<hostname>/tmp/cbt/00000000/Elbencho/
  write_4096/threads-001/iodepth-001/result.csv
  write_4096/threads-001/iodepth-004/result.csv
  ...
  write_131072/threads-004/iodepth-004/result.csv
  read_4096/threads-001/iodepth-001/result.csv
  ...
  read_131072/threads-004/iodepth-004/result.csv
```

A populated `result.csv` for the highest-load write cell (`128k`, 4 threads, iodepth 4)
looks like:

```
ISO DATE: 2026-08-12T16:27:22+0100
COMMAND LINE: "/usr/local/bin/elbencho" "--write" "--threads" "4" "--block" "128k" ...

OPERATION   RESULT TYPE         FIRST DONE   LAST DONE
=========== ================    ==========   =========
WRITE       Elapsed time     :     30.140s     30.196s
            IOPS             :         291         291
            Throughput MiB/s :          36          36
            Total MiB        :        1101        1101
```

> **Note**: `result.csv` is elbencho's native CSV result format. Parsing and plotting these
> files into CBT's standard report pipeline is the work of Stories 4 and 5.
