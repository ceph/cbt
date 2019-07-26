=========================
BlueStore Throttle Tuning
=========================

Motivation
==========

BlueStore has a throttling mechanism in order to ensure that queued IO doesn't
increase without bound.  If this throttle is set too low, osd throughput will
suffer.  If it's set too high, we'll see unnecessary increases in latency at
the objectstore level preventing the OSD queues from performing QoS.  If
latency at the objectstore level is 1s due to the current queue length, the
best possible latency for an incoming high priority IO would be 1s.

Generally, we'd expect the relationship between latency and throttle value (or
queue depth) to have two behavior types.  When the store is sub-saturated, we'd
expect increases in queued IO to increase throughput with little corresponding
increase in latency.  As the store saturates, we'd expect throughput to become
relatively insensitive to throttle, but latency would begin to increase
linearly.

In choosing these throttle limits, a user would want first to understand the
latency/throughput/throttle relationships for their hardware as well as their
workload/application's preference for latency vs throughput.  One could choose
to deliberately sacrafice some amount of max throughput in exchange for better
qos, or one might choose to capture as much throughput as possible at the
expense of higher average and especially tail latency.

Usage
=====

There is a backend for fio (src/test/fio/fio_ceph_objectstore.cc) which backs
fio with a single objectstore instance.  This instance has an option which will
at configurable intervals alter the throttle values among the configured
options online as the fio test runs.  By capturing a trace of ios performed via
lttng, we can get an idea of the throttle/latency/throughput relationship for a
particular workload and device.

First, ceph needs to be built with fio and lttng:

::
   ./do_cmake.sh --verbose -DWITH_FIO=on -DWITH_LTTNG=on -DCMAKE_BUILD_TYPE=RelWithDebInfo

Next, there are a few scripts in the cbt.git repository to ease running fio
with the right backend and graphing the results under fio_objectstore_tools/.
Create a copy of runs.json updating configs as needed (particularly device
paths).  You can then do a run by running:

::
   ./run.py --initialize runs <path_to_json>
   ./run.py --run <path_to_json>

Results will appear in dated subdirs under ~/output by default.

In order to generate graphs from these results, run:

::
   ./analyze.py --generate-graphs --output <path_for_generated_pdfs> <path_to_output_dir>

The resulting graphs will plot latency and throughput for each traced IO (with
curves for median (green) and 99pct (red)) against the kv throttle and deferred
throttle values when the IO was released from the throttle.
