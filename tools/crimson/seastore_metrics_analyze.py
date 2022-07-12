#!/usr/bin/env python3

from collections import defaultdict
from collections import OrderedDict
from enum import Enum
import json
import os
from os import path
import pandas as pd
import seaborn as sns

class BenchT(Enum):
    NULL    = 0
    RADOS   = 1
    FIO     = 2
    METRICS = 3

def load_dir(dir_name, headcut, tailcut):
    load_folder = path.join(os.getcwd(), dir_name)
    benches = []
    metrics = []
    stats = []
    radosbench = ""
    for full_file_name in os.listdir(load_folder):
        if not full_file_name.endswith(".log"):
            continue
        file_name = full_file_name.rstrip(".log")
        names = file_name.split("_")
        if names[0] != "result":
            continue

        index = int(names[1])
        file_type = names[2]
        file_dir = path.join(load_folder, full_file_name)

        if file_type.startswith("bench"):
            assert(len(names) == 3)
            benches.append((index, file_dir))
        elif file_type.startswith("metrics"):
            time = -1
            if len(names) == 4:
                # Mode: METRICS
                time = int(names[3]) / 1000 # as seconds
            else:
                # Mode with bench files
                assert(len(names) == 3)
            metrics.append((index, file_dir, time))
        elif file_type.startswith("stats"):
            assert(len(names) == 4)
            time = int(names[3]) / 1000 # as seconds
            stats.append((index, file_dir, time))
        else:
            assert(file_type.startswith("radosbench"))
            assert(len(names) == 3)
            radosbench = file_dir

    benches.sort()
    metrics.sort()
    stats.sort()
    if (len(metrics) and metrics[0][2] != -1) or len(stats):
        # Mode: METRICS
        assert(len(benches) == 0)
    else:
        # Mode with bench files
        assert(len(benches))
        if len(metrics) == len(benches):
            # no matching matric file to the last bench result
            benches.pop()
        assert(len(metrics) == len(benches) + 1)
    if len(stats) and len(metrics):
        assert(len(stats) == len(metrics))

    index = 0
    ctime = -1
    len_indexes = 0
    if len(metrics):
        assert(metrics[index][0] == index)
        ctime = metrics[index][2]
        len_indexes = len(metrics)
    if len(stats):
        assert(stats[index][0] == index)
        _ctime = stats[index][2]
        if ctime != -1:
            assert(ctime == _ctime)
        else:
            ctime = _ctime
        len_indexes = len(stats)
    assert(len_indexes)

    ret_times = []
    index += 1
    while index < len_indexes:
        if len(benches):
            assert(benches[index - 1][0] == index)
        else:
            time = -1
            if len(metrics):
                assert(metrics[index][0] == index)
                time = metrics[index][2]
            if len(stats):
                assert(stats[index][0] == index)
                _time = stats[index][2]
                if time != -1:
                    assert(_time == time)
                else:
                    time = _time
            difftime = time - ctime
            assert(difftime > 0)
            ret_times.append(difftime)
            ctime = time
        index += 1

    if tailcut == 0:
        tailcut = len_indexes
        print(tailcut)
    diff_tailcut = tailcut
    if diff_tailcut > 0:
        diff_tailcut += 1

    benches = benches[headcut:tailcut]
    metrics = metrics[headcut:diff_tailcut]
    stats = stats[headcut:diff_tailcut]
    bench_start = ret_times[0]
    bench_skip = 0
    for t in ret_times[0:headcut]:
        bench_skip += t
    ret_times = ret_times[headcut:tailcut]

    return [item[1] for item in benches], [item[1] for item in metrics], \
            [item[1] for item in stats], ret_times, \
            (bench_start, bench_skip, radosbench)

def parse_bench_file(bench_file):
    btype = BenchT.NULL
    writes = 0
    obj_size = 0
    with open(bench_file, 'r') as reader:
        for line in reader:
            if line.startswith("Total writes made:"):
                assert(btype == BenchT.NULL)
                btype = BenchT.RADOS
                writes = int([x for x in line.split(' ') if x][3])
            elif line.startswith("Object size:"):
                assert(btype == BenchT.RADOS)
                obj_size = int([x for x in line.split(' ') if x][2])
            elif line.startswith("rbd_iodepth32") and line.find('rw=') >= 0:
                assert(btype == BenchT.NULL)
                btype = BenchT.FIO
                obj_size = int([x for x in line.split(',') if x][2].split('-')[1][:-1])
            elif line.startswith("     issued"):
                assert(btype == BenchT.FIO)
                writes = int([x for x in line.split(',') if x][1])

    assert(btype != BenchT.NULL)
    assert(writes)
    assert(obj_size)
    return (writes * obj_size / 4096), btype

def _load_json(file):
    def parse_object_pairs(pairs):
        return pairs
    with open(file, 'r') as reader:
        return json.load(reader, object_pairs_hook=parse_object_pairs)

def _process_json_item(json_item):
    name = json_item[0]
    value = -1
    labels = {}
    for k, v in json_item[1]:
        if k == "value":
            if isinstance(v, int) or isinstance(v, float):
                value = v
            elif isinstance(v, list):
                if (len(v) == 3 and
                    isinstance(v[2], tuple) and
                    v[2][0] == "buckets"):
                    bucket = v[2][1]
                    # valid bucket type, [[('le', 0.1), ('count', 9)], ...]
                    tmp = []
                    for item in bucket:
                        key = item[0][1]
                        val = item[1][1]
                        if isinstance(key, int) or isinstance(key, float):
                            tmp.append((key, val))
                    if (len(tmp)):
                        tmp = sorted(tmp)
                        value = []
                        accumulated = 0
                        for item in tmp:
                            accumulated += item[1]
                            value.append((item[0], accumulated))
                        value = OrderedDict(value)
        else:
            assert(isinstance(v, str))
            labels[k] = v
    if "shard" in labels:
        assert(labels["shard"] == "0")
    return name, labels, value

def parse_metric_file(metric_file):
    data = {}
    # blocks
    data["segment_read_4KB"] = 0
    data["segment_write_4KB"] = 0
    data["segment_write_meta_4KB"] = 0
    data["cached_4KB"] = 0
    data["dirty_4KB"] = 0
    data["reactor_aio_read_4KB"] = 0
    data["reactor_aio_write_4KB"] = 0
    data["memory_allocate_KB"] = 0
    data["memory_free_KB"] = 0
    data["memory_total_KB"] = 0
    data["available_KB"] = 0
    data["projected_used_sum_KB"] = 0
    data["unavail_reclaimable_KB"] = 0
    data["unavail_unreclaimable_KB"] = 0
    data["unavail_used_KB"] = 0
    data["unavail_unused_KB"] = 0
    data["reclaimed_KB"] = 0
    data["reclaimed_segment_KB"] = 0
    data["closed_journal_total_KB"] = 0
    data["closed_journal_used_KB"] = 0
    data["closed_ool_total_KB"] = 0
    data["closed_ool_used_KB"] = 0
    data["alloc_journal_KB"] = 0
    data["dirty_journal_KB"] = 0
    # count
    data["segment_reads"] = 0
    data["segment_writes"] = 0
    data["segment_meta_writes"] = 0
    data["reactor_aio_reads"] = 0
    data["reactor_aio_writes"] = 0
    data["object_data_writes"] = 0
    data["reactor_polls_M"] = 0
    data["reactor_tasks_pending"] = 0
    data["reactor_tasks_processed_M"] = 0
    data["memory_frees"] = 0
    data["memory_mallocs"] = 0
    data["memory_reclaims"] = 0
    data["memory_live_objs"] = 0
    data["segments_open"] = 0
    data["segments_closed"] = 0
    data["segments_empty"] = 0
    data["segments_in_journal"] = 0
    data["segments_type_journal"] = 0
    data["segments_type_ool"] = 0
    data["segments_count_open_journal"] = 0
    data["segments_count_close_journal"] = 0
    data["segments_count_release_journal"] = 0
    data["segments_count_open_ool"] = 0
    data["segments_count_close_ool"] = 0
    data["segments_count_release_ool"] = 0
    data["projected_count"] = 0
    data["io_count"] = 0
    data["io_blocked_count"] = 0
    data["io_blocked_count_trim"] = 0
    data["io_blocked_count_reclaim"] = 0
    data["io_blocked_sum"] = 0
    data["version_count_dirty"] = 0
    data["version_sum_dirty"] = 0
    data["version_count_reclaim"] = 0
    data["version_sum_reclaim"] = 0
    # ratio
    data["reactor_util"] = 0
    data["unavailiable_total"] = 0
    data["alive_unavailable"] = 0
    # time
    data["reactor_busytime_sec"] = 0
    data["reactor_stealtime_sec"] = 0
    # submitter -> blocks/count
    data["journal_padding_4KB"] = defaultdict(lambda: 0)
    data["journal_metadata_4KB"] = defaultdict(lambda: 0) # without padding
    data["journal_data_4KB"] = defaultdict(lambda: 0)
    data["journal_record_num"] = defaultdict(lambda: 0)
    data["journal_record_batch_num"] = defaultdict(lambda: 0)
    data["journal_io_num"] = defaultdict(lambda: 0)
    data["journal_io_depth_num"] = defaultdict(lambda: 0)
    # util -> count
    data["segment_util_distribution"] = defaultdict(lambda: 0)
    # srcs -> count
    data["trans_srcs_invalidated"] = defaultdict(lambda: 0)
    # scheduler-group -> time
    data["scheduler_runtime_sec"] = defaultdict(lambda: 0)
    data["scheduler_waittime_sec"] = defaultdict(lambda: 0)
    data["scheduler_starvetime_sec"] = defaultdict(lambda: 0)
    # scheduler-group -> count
    data["scheduler_queue_length"] = defaultdict(lambda: 0)
    data["scheduler_tasks_processed_M"] = defaultdict(lambda: 0)
    # tree-type -> depth
    data["tree_depth"] = defaultdict(lambda: 0)
    # src -> count
    data["cache_access"] = defaultdict(lambda: 0)
    data["cache_hit"] = defaultdict(lambda: 0)
    data["created_trans"] = defaultdict(lambda: 0)
    data["committed_trans"] = defaultdict(lambda: 0)
    data["invalidated_ool_records"] = defaultdict(lambda: 0)
    data["committed_ool_records"] = defaultdict(lambda: 0)
    # src -> blocks
    data["invalidated_ool_record_4KB"] = defaultdict(lambda: 0)
    data["committed_ool_record_metadata_4KB"] = defaultdict(lambda: 0) # without padding
    data["committed_ool_record_data_4KB"] = defaultdict(lambda: 0)
    data["committed_inline_record_metadata_4KB"] = defaultdict(lambda: 0) # without delta buffer
    # src -> tree-type -> count
    data["tree_erases_committed"] = defaultdict(lambda: defaultdict(lambda: 0))
    data["tree_inserts_committed"] = defaultdict(lambda: defaultdict(lambda: 0))
    data["tree_updates_committed"] = defaultdict(lambda: defaultdict(lambda: 0))
    # src-> extent-type -> count
    data["invalidated_trans"] = defaultdict(lambda: defaultdict(lambda: 0))
    # src-> effort-type -> blocks
    data["invalidated_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: 0))
    data["committed_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: 0))
    data["committed_trans_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: 0))
    # src-> extent-type -> effort-type -> blocks
    data["committed_disk_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0)))

    expected_data = data.keys()
    found_data = set()
    def set_value(name, value, labels=[], data=data, found_data=found_data):
        found_data.add(name)
        setter = data
        _labels=labels[:]
        _labels.insert(0, name)
        while len(_labels) > 1:
            setter = setter[_labels[0]]
            _labels.pop(0)
        if isinstance(value, OrderedDict):
            setter[_labels[0]] = value
        else:
            setter[_labels[0]] += value

    found_metrics = set()
    expected_metrics = {
        # 4KB
        "segment_manager_data_read_bytes",
        "segment_manager_data_write_bytes",
        "segment_manager_metadata_write_bytes",
        "cache_cached_extent_bytes",
        "cache_dirty_extent_bytes",
        "reactor_aio_bytes_read",
        "reactor_aio_bytes_write",
        "memory_allocated_memory",
        "memory_free_memory",
        "memory_total_memory",
        "async_cleaner_available_bytes",
        "async_cleaner_projected_used_bytes_sum",
        "async_cleaner_unavailable_reclaimable_bytes",
        "async_cleaner_unavailable_unreclaimable_bytes",
        "async_cleaner_unavailable_unused_bytes",
        "async_cleaner_used_bytes",
        "async_cleaner_reclaimed_bytes",
        "async_cleaner_reclaimed_segment_bytes",
        "async_cleaner_closed_journal_total_bytes",
        "async_cleaner_closed_journal_used_bytes",
        "async_cleaner_closed_ool_total_bytes",
        "async_cleaner_closed_ool_used_bytes",
        "async_cleaner_alloc_journal_bytes",
        "async_cleaner_dirty_journal_bytes",
        # count
        "segment_manager_data_read_num",
        "segment_manager_data_write_num",
        "segment_manager_metadata_write_num",
        "reactor_aio_reads",
        "reactor_aio_writes",
        "reactor_polls",
        "reactor_tasks_pending",
        "reactor_tasks_processed",
        "memory_free_operations",
        "memory_malloc_operations",
        "memory_reclaims_operations",
        "memory_malloc_live_objects",
        "async_cleaner_segments_open",
        "async_cleaner_segments_closed",
        "async_cleaner_segments_empty",
        "async_cleaner_segments_in_journal",
        "async_cleaner_segments_type_journal",
        "async_cleaner_segments_type_ool",
        "async_cleaner_segments_count_open_journal",
        "async_cleaner_segments_count_close_journal",
        "async_cleaner_segments_count_release_journal",
        "async_cleaner_segments_count_open_ool",
        "async_cleaner_segments_count_close_ool",
        "async_cleaner_segments_count_release_ool",
        "async_cleaner_projected_count",
        "async_cleaner_io_count",
        "async_cleaner_io_blocked_count",
        "async_cleaner_io_blocked_count_trim",
        "async_cleaner_io_blocked_count_reclaim",
        "async_cleaner_io_blocked_sum",
        "cache_version_count_dirty",
        "cache_version_count_reclaim",
        "cache_version_sum_dirty",
        "cache_version_sum_reclaim",
        # ratio
        "reactor_utilization",
        "async_cleaner_available_ratio",
        "async_cleaner_reclaim_ratio",
        # time
        "reactor_cpu_busy_ms",
        "reactor_cpu_steal_time_ms",
        # submitter -> blocks/count
        "journal_record_group_padding_bytes",
        "journal_record_group_metadata_bytes",
        "journal_record_group_data_bytes",
        "journal_record_num",
        "journal_record_batch_num",
        "journal_io_num",
        "journal_io_depth_num",
        # util -> count
        "async_cleaner_segment_utilization_distribution",
        # srcs -> count
        "cache_trans_srcs_invalidated",
        # scheduler-group -> time
        "scheduler_runtime_ms",
        "scheduler_waittime_ms",
        "scheduler_starvetime_ms",
        # scheduler-group -> count
        "scheduler_queue_length",
        "scheduler_tasks_processed",
        # tree-type -> depth
        "cache_tree_depth",
        # src -> count
        "cache_cache_access",
        "cache_cache_hit",
        "cache_trans_created",
        "cache_trans_committed",
        "cache_trans_read_successful",
        "cache_invalidated_ool_records",
        "cache_committed_ool_records",
        # src -> blocks
        "cache_invalidated_ool_record_bytes",
        "cache_committed_ool_record_metadata_bytes",
        "cache_committed_ool_record_data_bytes",
        "cache_committed_inline_record_metadata_bytes",
        # src -> tree-type -> count
        "cache_tree_erases_committed",
        "cache_tree_inserts_committed",
        "cache_tree_updates_committed",
        # src -> extent-type -> count
        "cache_trans_invalidated",
        # src -> effort-type -> blocks
        "cache_invalidated_extent_bytes",
        "cache_invalidated_delta_bytes",
        # src -> effort-type -> blocks
        # src -> extent-type -> effort-type -> blocks
        "cache_committed_extent_bytes",
        "cache_committed_delta_bytes",
        "cache_successful_read_extent_bytes",
        # others
        "cache_committed_extents"
    }

    illegal_metrics = set()
    ignored_metrics = set()
    json_items = _load_json(metric_file)
    for json_item in json_items:
        name, labels, value = _process_json_item(json_item)
        if value == -1:
            illegal_metrics.add(name)
            continue

        # 4KB
        if   name == "segment_manager_data_read_bytes":
            set_value("segment_read_4KB", value/4096)
        elif name == "segment_manager_data_write_bytes":
            set_value("segment_write_4KB", value/4096)
        elif name == "segment_manager_metadata_write_bytes":
            set_value("segment_write_meta_4KB", value/4096)
        elif name == "cache_cached_extent_bytes":
            set_value("cached_4KB", value/4096)
        elif name == "cache_dirty_extent_bytes":
            set_value("dirty_4KB", value/4096)
        elif name == "reactor_aio_bytes_read":
            set_value("reactor_aio_read_4KB", value/4096)
        elif name == "reactor_aio_bytes_write":
            set_value("reactor_aio_write_4KB", value/4096)
        elif name == "memory_allocated_memory":
            set_value("memory_allocate_KB", value/1024)
        elif name == "memory_free_memory":
            set_value("memory_free_KB", value/1024)
        elif name == "memory_total_memory":
            set_value("memory_total_KB", value/1024)
        elif name == "async_cleaner_available_bytes":
            set_value("available_KB", value/1024)
        elif name == "async_cleaner_projected_used_bytes_sum":
            set_value("projected_used_sum_KB", value/1024)
        elif name == "async_cleaner_unavailable_reclaimable_bytes":
            set_value("unavail_reclaimable_KB", value/1024)
        elif name == "async_cleaner_unavailable_unreclaimable_bytes":
            set_value("unavail_unreclaimable_KB", value/1024)
        elif name == "async_cleaner_unavailable_unused_bytes":
            set_value("unavail_unused_KB", value/1024)
        elif name == "async_cleaner_used_bytes":
            set_value("unavail_used_KB", value/1024)
        elif name == "async_cleaner_reclaimed_bytes":
            set_value("reclaimed_KB", value/1024)
        elif name == "async_cleaner_reclaimed_segment_bytes":
            set_value("reclaimed_segment_KB", value/1024)
        elif name == "async_cleaner_closed_journal_total_bytes":
            set_value("closed_journal_total_KB", value/1024)
        elif name == "async_cleaner_closed_journal_used_bytes":
            set_value("closed_journal_used_KB", value/1024)
        elif name == "async_cleaner_closed_ool_total_bytes":
            set_value("closed_ool_total_KB", value/1024)
        elif name == "async_cleaner_closed_ool_used_bytes":
            set_value("closed_ool_used_KB", value/1024)
        elif name == "async_cleaner_alloc_journal_bytes":
            set_value("alloc_journal_KB", value/1024)
        elif name == "async_cleaner_dirty_journal_bytes":
            set_value("dirty_journal_KB", value/1024)

        # count
        elif name == "segment_manager_data_read_num":
            set_value("segment_reads", value)
        elif name == "segment_manager_data_write_num":
            set_value("segment_writes", value)
        elif name == "segment_manager_metadata_write_num":
            set_value("segment_meta_writes", value)
        elif name == "reactor_aio_reads":
            set_value("reactor_aio_reads", value)
        elif name == "reactor_aio_writes":
            set_value("reactor_aio_writes", value)
        elif name == "reactor_polls":
            set_value("reactor_polls_M", value/1000000)
        elif name == "reactor_tasks_pending":
            set_value("reactor_tasks_pending", value)
        elif name == "reactor_tasks_processed":
            set_value("reactor_tasks_processed_M", value/1000000)
        elif name == "memory_free_operations":
            set_value("memory_frees", value)
        elif name == "memory_malloc_operations":
            set_value("memory_mallocs", value)
        elif name == "memory_reclaims_operations":
            set_value("memory_reclaims", value)
        elif name == "memory_malloc_live_objects":
            set_value("memory_live_objs", value)
        elif name == "async_cleaner_segments_open":
            set_value("segments_open", value)
        elif name == "async_cleaner_segments_closed":
            set_value("segments_closed", value)
        elif name == "async_cleaner_segments_empty":
            set_value("segments_empty", value)
        elif name == "async_cleaner_segments_in_journal":
            set_value("segments_in_journal", value)
        elif name == "async_cleaner_segments_type_journal":
            set_value("segments_type_journal", value)
        elif name == "async_cleaner_segments_type_ool":
            set_value("segments_type_ool", value)
        elif name == "async_cleaner_segments_count_open_journal":
            set_value("segments_count_open_journal", value)
        elif name == "async_cleaner_segments_count_close_journal":
            set_value("segments_count_close_journal", value)
        elif name == "async_cleaner_segments_count_release_journal":
            set_value("segments_count_release_journal", value)
        elif name == "async_cleaner_segments_count_open_ool":
            set_value("segments_count_open_ool", value)
        elif name == "async_cleaner_segments_count_close_ool":
            set_value("segments_count_close_ool", value)
        elif name == "async_cleaner_segments_count_release_ool":
            set_value("segments_count_release_ool", value)
        elif name == "async_cleaner_projected_count":
            set_value("projected_count", value)
        elif name == "async_cleaner_io_count":
            set_value("io_count", value)
        elif name == "async_cleaner_io_blocked_count":
            set_value("io_blocked_count", value)
        elif name == "async_cleaner_io_blocked_count_trim":
            set_value("io_blocked_count_trim", value)
        elif name == "async_cleaner_io_blocked_count_reclaim":
            set_value("io_blocked_count_reclaim", value)
        elif name == "async_cleaner_io_blocked_sum":
            set_value("io_blocked_sum", value)
        elif name == "cache_version_count_dirty":
            set_value("version_count_dirty", value)
        elif name == "cache_version_count_reclaim":
            set_value("version_count_reclaim", value)
        elif name == "cache_version_sum_dirty":
            set_value("version_sum_dirty", value)
        elif name == "cache_version_sum_reclaim":
            set_value("version_sum_reclaim", value)

        # ratio
        elif name == "reactor_utilization":
            set_value("reactor_util", value/100)
        elif name == "async_cleaner_available_ratio":
            set_value("unavailiable_total", 1 - value)
        elif name == "async_cleaner_reclaim_ratio":
            set_value("alive_unavailable", 1 - value)

        # time
        elif name == "reactor_cpu_busy_ms":
            set_value("reactor_busytime_sec", value/1000)
        elif name == "reactor_cpu_steal_time_ms":
            set_value("reactor_stealtime_sec", value/1000)

        # submitter -> blocks/count
        elif name == "journal_record_group_padding_bytes":
            set_value("journal_padding_4KB", value/4096, [labels["submitter"]])
        elif name == "journal_record_group_metadata_bytes":
            set_value("journal_metadata_4KB", value/4096, [labels["submitter"]])
        elif name == "journal_record_group_data_bytes":
            set_value("journal_data_4KB", value/4096, [labels["submitter"]])
        elif name == "journal_record_num":
            set_value("journal_record_num", value, [labels["submitter"]])
        elif name == "journal_record_batch_num":
            set_value("journal_record_batch_num", value, [labels["submitter"]])
        elif name == "journal_io_num":
            set_value("journal_io_num", value, [labels["submitter"]])
        elif name == "journal_io_depth_num":
            set_value("journal_io_depth_num", value, [labels["submitter"]])

        # util -> count
        elif name == "async_cleaner_segment_utilization_distribution":
            set_value("segment_util_distribution", value)

        # srcs -> count
        elif name == "cache_trans_srcs_invalidated":
            set_value("trans_srcs_invalidated", value, [labels["srcs"]])

        # scheduler-group -> time
        elif name == "scheduler_runtime_ms":
            set_value("scheduler_runtime_sec", value/1000, [labels["group"]])
        elif name == "scheduler_waittime_ms":
            set_value("scheduler_waittime_sec", value/1000, [labels["group"]])
        elif name == "scheduler_starvetime_ms":
            set_value("scheduler_starvetime_sec", value/1000, [labels["group"]])

        # scheduler-group -> count
        elif name == "scheduler_queue_length":
            set_value("scheduler_queue_length", value, [labels["group"]])
        elif name == "scheduler_tasks_processed":
            set_value("scheduler_tasks_processed_M", value/1000000, [labels["group"]])

        # tree-type -> depth
        elif name == "cache_tree_depth":
            set_value("tree_depth", value, [labels["tree"]])

        # src -> count
        elif name == "cache_cache_access":
            set_value("cache_access", value, [labels["src"]])
        elif name == "cache_cache_hit":
            set_value("cache_hit", value, [labels["src"]])
        elif name == "cache_trans_created":
            set_value("created_trans", value, [labels["src"]])
        elif name == "cache_trans_committed":
            assert(labels["src"] != "READ")
            set_value("committed_trans", value, [labels["src"]])
        elif name == "cache_trans_read_successful":
            set_value("committed_trans", value, ["READ"])
        elif name == "cache_invalidated_ool_records":
            assert(labels["src"] != "READ")
            set_value("invalidated_ool_records", value, [labels["src"]])
        elif name == "cache_committed_ool_records":
            assert(labels["src"] != "READ")
            set_value("committed_ool_records", value, [labels["src"]])

        # src -> blocks
        elif name == "cache_invalidated_ool_record_bytes":
            assert(labels["src"] != "READ")
            set_value("invalidated_ool_record_4KB", value/4096, [labels["src"]])
        elif name == "cache_committed_ool_record_metadata_bytes":
            assert(labels["src"] != "READ")
            set_value("committed_ool_record_metadata_4KB", value/4096, [labels["src"]])
        elif name == "cache_committed_ool_record_data_bytes":
            assert(labels["src"] != "READ")
            set_value("committed_ool_record_data_4KB", value/4096, [labels["src"]])
        elif name == "cache_committed_inline_record_metadata_bytes":
            assert(labels["src"] != "READ")
            set_value("committed_inline_record_metadata_4KB", value/4096, [labels["src"]])

        # src -> tree-type -> count
        elif name == "cache_tree_erases_committed":
            assert(labels["src"] != "READ")
            set_value("tree_erases_committed", value, [labels["src"], labels["tree"]])
        elif name == "cache_tree_inserts_committed":
            assert(labels["src"] != "READ")
            set_value("tree_inserts_committed", value, [labels["src"], labels["tree"]])
        elif name == "cache_tree_updates_committed":
            assert(labels["src"] != "READ")
            set_value("tree_updates_committed", value, [labels["src"], labels["tree"]])

        # src -> extent-type -> count
        elif name == "cache_trans_invalidated":
            set_value("invalidated_trans", value, [labels["src"], labels["ext"]])

        # src -> effort-type -> blocks
        elif name == "cache_invalidated_extent_bytes":
            if labels["src"] == "READ":
                assert(labels["effort"] == "READ")
            set_value("invalidated_efforts_4KB", value/4096, [labels["src"], labels["effort"]])
        elif name == "cache_invalidated_delta_bytes":
            assert(labels["src"] != "READ")
            set_value("invalidated_efforts_4KB", value/4096, [labels["src"], "MUTATE_DELTA"])

        # src -> effort-type -> blocks
        # src -> extent-type -> effort-type -> blocks
        elif name == "cache_committed_extent_bytes":
            assert(labels["src"] != "READ")
            # READ, MUTATE, RETIRE, FRESH_INVLID/INLINE/OOL
            effort_name = labels["effort"]
            if effort_name == "FRESH_INLINE":
                set_value("committed_disk_efforts_4KB", value/4096,
                          [labels["src"], labels["ext"], effort_name])
                set_value("committed_trans_efforts_4KB", value/4096,
                          [labels["src"], effort_name])
                set_value("committed_efforts_4KB", value/4096,
                          [labels["src"], "FRESH"])
            elif effort_name == "FRESH_INVALID":
                set_value("committed_disk_efforts_4KB", value/4096,
                          [labels["src"], labels["ext"], effort_name])
                # FRESH_INLINE includes FRESH_INVLIAD
                set_value("committed_efforts_4KB", -value/4096,
                          [labels["src"], "FRESH"])
                set_value("committed_disk_efforts_4KB", -value/4096,
                          [labels["src"], labels["ext"], "FRESH_INLINE"])
                set_value("committed_trans_efforts_4KB", -value/4096,
                          [labels["src"], "FRESH_INLINE"])
            elif effort_name == "FRESH_OOL":
                set_value("committed_disk_efforts_4KB", value/4096,
                          [labels["src"], labels["ext"], effort_name])
                set_value("committed_trans_efforts_4KB", value/4096,
                          [labels["src"], effort_name])
                # match cache_invalidated_extent_bytes FRESH, FRESH_OOL_WRITTEN
                set_value("committed_efforts_4KB", value/4096,
                          [labels["src"], "FRESH"])
                set_value("committed_efforts_4KB", value/4096,
                          [labels["src"], "FRESH_OOL_WRITTEN"])
            elif effort_name == "RETIRE":
                set_value("committed_efforts_4KB", value/4096,
                          [labels["src"], effort_name])
                set_value("committed_trans_efforts_4KB", value/4096,
                          [labels["src"], effort_name])
            else:
                # READ, MUTATE
                set_value("committed_efforts_4KB", value/4096,
                          [labels["src"], effort_name])
        elif name == "cache_committed_delta_bytes":
            assert(labels["src"] != "READ")
            effort_name = "MUTATE_DELTA"
            set_value("committed_efforts_4KB", value/4096,
                      [labels["src"], effort_name])
            set_value("committed_disk_efforts_4KB", value/4096,
                      [labels["src"], labels["ext"], effort_name])
            set_value("committed_trans_efforts_4KB", value/4096,
                      [labels["src"], effort_name])
        elif name == "cache_successful_read_extent_bytes":
            set_value("committed_efforts_4KB", value/4096, ["READ", "READ"])

        # others
        elif name == "cache_committed_extents":
            if labels["ext"] == "OBJECT_DATA_BLOCK" and labels["src"] == "MUTATE":
                if labels["effort"] == "FRESH_INLINE" or labels["effort"] == "FRESH_OOL":
                    set_value("object_data_writes", value)
                if labels["effort"] == "FRESH_INVALID":
                    # FRESH_INLINE includes FRESH_INVLIAD
                    set_value("object_data_writes", -value)
        else:
            ignored_metrics.add(name)
        if name not in ignored_metrics:
            found_metrics.add(name)

    assert(found_metrics.issubset(expected_metrics))
    missing_metrics = expected_metrics - found_metrics
    assert(found_data.issubset(expected_data))
    missing_data = expected_data - found_data
    if missing_metrics or missing_data:
        print()
        print("error, missing metrics:")
        print(missing_metrics)
        print("error, missing data:")
        print(missing_data)
        assert(False)

    return data, illegal_metrics, ignored_metrics

def parse_stats_file(stats_file):
    data = {}
    block_per_sector = 8*1024 # 32MiB

    json_items = _load_json(stats_file)
    for json_item in json_items:
        name, labels, value = _process_json_item(json_item)
        assert(len(labels) == 0)
        if name == "read_kb":
            data["iostat_read_4KB"] = value/4
        elif name == "wrtn_kb":
            data["iostat_write_4KB"] = value/4
        elif name == "dscd_kb":
            pass
        elif name == "nand_sect":
            data["nvme_nand_4KB"] = value*block_per_sector
        elif name == "host_sect":
            data["nvme_host_4KB"] = value*block_per_sector
        else:
            assert(False)

    return data

def parse_radosbench_file(_radosbench, raw_dataset, times):
    bench_start, bench_skip, radosbench = _radosbench
    if radosbench == "":
        return

    writes_4KB = raw_dataset["radosbench_4KB"]
    time_index = 0
    cur_time = bench_skip - bench_start + times[time_index]
    time_index += 1
    assert(time_index < len(times))
    while cur_time <= 0:
        writes_4KB.append(0)
        if time_index >= len(times):
            time_index += 1
            break
        cur_time += times[time_index]
        time_index += 1
    assert(cur_time > 0)

    write_finish_4KB = 0
    write_4KB = 0
    prv_time_seconds = -1
    for line in open(radosbench, 'r').readlines():
        items = line.split()
        if len(items) != 8:
            continue
        time_seconds = int(items[0])
        assert(prv_time_seconds + 1 == time_seconds)
        prv_time_seconds = time_seconds
        if time_seconds > cur_time:
            writes_4KB.append(write_4KB)
            write_4KB = 0
            if time_index >= len(times):
                time_index += 1
                break
            cur_time += times[time_index]
            time_index += 1
        _write_finish_4KB = int(items[3]) # assume writes are 4KB
        assert(_write_finish_4KB >= write_finish_4KB)
        write_4KB += (_write_finish_4KB - write_finish_4KB)
        write_finish_4KB = _write_finish_4KB

    while time_index <= len(times):
        writes_4KB.append(write_4KB)
        write_4KB = 0
        time_index += 1

def prepare_raw_dataset():
    data = {}
    # radosbench
    data["radosbench_4KB"] = []
    # stats
    data["iostat_write_4KB"] = []
    data["iostat_read_4KB"] = []
    data["nvme_host_4KB"] = []
    data["nvme_nand_4KB"] = []
    # blocks
    data["segment_read_4KB"] = []
    data["segment_write_4KB"] = []
    data["segment_write_meta_4KB"] = []
    data["cached_4KB"] = []
    data["dirty_4KB"] = []
    data["reactor_aio_read_4KB"] = []
    data["reactor_aio_write_4KB"] = []
    data["memory_allocate_KB"] = []
    data["memory_free_KB"] = []
    data["memory_total_KB"] = []
    data["available_KB"] = []
    data["projected_used_sum_KB"] = []
    data["unavail_reclaimable_KB"] = []
    data["unavail_unreclaimable_KB"] = []
    data["unavail_used_KB"] = []
    data["unavail_unused_KB"] = []
    data["reclaimed_KB"] = []
    data["reclaimed_segment_KB"] = []
    data["closed_journal_total_KB"] = []
    data["closed_journal_used_KB"] = []
    data["closed_ool_total_KB"] = []
    data["closed_ool_used_KB"] = []
    data["alloc_journal_KB"] = []
    data["dirty_journal_KB"] = []
    # count
    data["segment_reads"] = []
    data["segment_writes"] = []
    data["segment_meta_writes"] = []
    data["reactor_aio_reads"] = []
    data["reactor_aio_writes"] = []
    data["object_data_writes"] = []
    data["reactor_polls_M"] = []
    data["reactor_tasks_pending"] = []
    data["reactor_tasks_processed_M"] = []
    data["memory_frees"] = []
    data["memory_mallocs"] = []
    data["memory_reclaims"] = []
    data["memory_live_objs"] = []
    data["segments_open"] = []
    data["segments_closed"] = []
    data["segments_empty"] = []
    data["segments_in_journal"] = []
    data["segments_type_journal"] = []
    data["segments_type_ool"] = []
    data["segments_count_open_journal"] = []
    data["segments_count_close_journal"] = []
    data["segments_count_release_journal"] = []
    data["segments_count_open_ool"] = []
    data["segments_count_close_ool"] = []
    data["segments_count_release_ool"] = []
    data["projected_count"] = []
    data["io_count"] = []
    data["io_blocked_count"] = []
    data["io_blocked_count_trim"] = []
    data["io_blocked_count_reclaim"] = []
    data["io_blocked_sum"] = []
    data["version_count_dirty"] = []
    data["version_sum_dirty"] = []
    data["version_count_reclaim"] = []
    data["version_sum_reclaim"] = []
    # ratio
    data["reactor_util"] = []
    data["unavailiable_total"] = []
    data["alive_unavailable"] = []
    # time
    data["reactor_busytime_sec"] = []
    data["reactor_stealtime_sec"] = []
    # submitter -> blocks/count
    data["journal_padding_4KB"] = defaultdict(lambda: [])
    data["journal_metadata_4KB"] = defaultdict(lambda: [])
    data["journal_data_4KB"] = defaultdict(lambda: [])
    data["journal_record_num"] = defaultdict(lambda: [])
    data["journal_record_batch_num"] = defaultdict(lambda: [])
    data["journal_io_num"] = defaultdict(lambda: [])
    data["journal_io_depth_num"] = defaultdict(lambda: [])
    # util -> count
    data["segment_util_distribution"] = defaultdict(lambda: [])
    # srcs -> count
    data["trans_srcs_invalidated"] = defaultdict(lambda: [])
    # scheduler-group -> time
    data["scheduler_runtime_sec"] = defaultdict(lambda: [])
    data["scheduler_waittime_sec"] = defaultdict(lambda: [])
    data["scheduler_starvetime_sec"] = defaultdict(lambda: [])
    # scheduler-group -> count
    data["scheduler_queue_length"] = defaultdict(lambda: [])
    data["scheduler_tasks_processed_M"] = defaultdict(lambda: [])
    # tree-type -> depth
    data["tree_depth"] = defaultdict(lambda: [])
    # src -> count
    data["cache_access"] = defaultdict(lambda: [])
    data["cache_hit"] = defaultdict(lambda: [])
    data["created_trans"] = defaultdict(lambda: [])
    data["committed_trans"] = defaultdict(lambda: [])
    data["invalidated_ool_records"] = defaultdict(lambda: [])
    data["committed_ool_records"] = defaultdict(lambda: [])
    # src -> blocks
    data["invalidated_ool_record_4KB"] = defaultdict(lambda: [])
    data["committed_ool_record_metadata_4KB"] = defaultdict(lambda: [])
    data["committed_ool_record_data_4KB"] = defaultdict(lambda: [])
    data["committed_inline_record_metadata_4KB"] = defaultdict(lambda: [])
    # src -> tree-type -> count
    data["tree_erases_committed"] = defaultdict(lambda: defaultdict(lambda: []))
    data["tree_inserts_committed"] = defaultdict(lambda: defaultdict(lambda: []))
    data["tree_updates_committed"] = defaultdict(lambda: defaultdict(lambda: []))
    # src -> extent-type -> count
    data["invalidated_trans"] = defaultdict(lambda: defaultdict(lambda: []))
    # src -> effort-type -> blocks
    data["invalidated_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: []))
    data["committed_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: []))
    data["committed_trans_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: []))
    # src -> extent-type -> effort-type -> blocks
    data["committed_disk_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: [])))
    return data

def append_raw_data(dataset, metrics_start, metrics_end, stats_start, stats_end):
    def get_diff(metric_name, dataset, metrics_start, metrics_end):
        value = metrics_end[metric_name] - metrics_start[metric_name]
        # the value can be negative for reactor_stealtime_sec
        dataset[metric_name].append(value)
    if len(stats_start):
        get_diff("iostat_write_4KB",       dataset, stats_start, stats_end)
        get_diff("iostat_read_4KB",        dataset, stats_start, stats_end)
        get_diff("nvme_host_4KB",          dataset, stats_start, stats_end)
        get_diff("nvme_nand_4KB",          dataset, stats_start, stats_end)

    if len(metrics_start):
        # blocks
        get_diff("segment_read_4KB",          dataset, metrics_start, metrics_end)
        get_diff("segment_write_4KB",         dataset, metrics_start, metrics_end)
        get_diff("segment_write_meta_4KB",    dataset, metrics_start, metrics_end)
        get_diff("reactor_aio_read_4KB",      dataset, metrics_start, metrics_end)
        get_diff("reactor_aio_write_4KB",     dataset, metrics_start, metrics_end)
        get_diff("projected_used_sum_KB",     dataset, metrics_start, metrics_end)
        get_diff("reclaimed_KB",              dataset, metrics_start, metrics_end)
        get_diff("reclaimed_segment_KB",      dataset, metrics_start, metrics_end)
        get_diff("closed_journal_total_KB",   dataset, metrics_start, metrics_end)
        get_diff("closed_journal_used_KB",    dataset, metrics_start, metrics_end)
        get_diff("closed_ool_total_KB",       dataset, metrics_start, metrics_end)
        get_diff("closed_ool_used_KB",        dataset, metrics_start, metrics_end)
        # count
        get_diff("segment_reads",             dataset, metrics_start, metrics_end)
        get_diff("segment_writes",            dataset, metrics_start, metrics_end)
        get_diff("segment_meta_writes",       dataset, metrics_start, metrics_end)
        get_diff("reactor_aio_reads",         dataset, metrics_start, metrics_end)
        get_diff("reactor_aio_writes",        dataset, metrics_start, metrics_end)
        get_diff("object_data_writes",        dataset, metrics_start, metrics_end)
        get_diff("reactor_polls_M",           dataset, metrics_start, metrics_end)
        get_diff("reactor_tasks_processed_M", dataset, metrics_start, metrics_end)
        get_diff("memory_frees",              dataset, metrics_start, metrics_end)
        get_diff("memory_mallocs",            dataset, metrics_start, metrics_end)
        get_diff("memory_reclaims",           dataset, metrics_start, metrics_end)
        get_diff("segments_count_open_journal",       dataset, metrics_start, metrics_end)
        get_diff("segments_count_close_journal",      dataset, metrics_start, metrics_end)
        get_diff("segments_count_release_journal",    dataset, metrics_start, metrics_end)
        get_diff("segments_count_open_ool",   dataset, metrics_start, metrics_end)
        get_diff("segments_count_close_ool",  dataset, metrics_start, metrics_end)
        get_diff("segments_count_release_ool",dataset, metrics_start, metrics_end)
        get_diff("projected_count",           dataset, metrics_start, metrics_end)
        get_diff("io_count",                  dataset, metrics_start, metrics_end)
        get_diff("io_blocked_count",          dataset, metrics_start, metrics_end)
        get_diff("io_blocked_count_trim",     dataset, metrics_start, metrics_end)
        get_diff("io_blocked_count_reclaim",  dataset, metrics_start, metrics_end)
        get_diff("io_blocked_sum",            dataset, metrics_start, metrics_end)
        get_diff("version_count_dirty",       dataset, metrics_start, metrics_end)
        get_diff("version_sum_dirty",         dataset, metrics_start, metrics_end)
        get_diff("version_count_reclaim",     dataset, metrics_start, metrics_end)
        get_diff("version_sum_reclaim",       dataset, metrics_start, metrics_end)
        # time
        get_diff("reactor_busytime_sec",      dataset, metrics_start, metrics_end)
        get_diff("reactor_stealtime_sec",     dataset, metrics_start, metrics_end)

        # these are special: no diff
        dataset["cached_4KB"].append(metrics_end["cached_4KB"])
        dataset["dirty_4KB"].append(metrics_end["dirty_4KB"])
        for name, value in metrics_end["tree_depth"].items():
            dataset["tree_depth"][name].append(value)
        dataset["reactor_util"].append(metrics_end["reactor_util"])
        dataset["unavailiable_total"].append(metrics_end["unavailiable_total"])
        dataset["alive_unavailable"].append(metrics_end["alive_unavailable"])
        dataset["reactor_tasks_pending"].append(metrics_end["reactor_tasks_pending"])
        for name, value in metrics_end["scheduler_queue_length"].items():
            dataset["scheduler_queue_length"][name].append(value)
        dataset["memory_allocate_KB"].append(metrics_end["memory_allocate_KB"])
        dataset["memory_free_KB"].append(metrics_end["memory_free_KB"])
        dataset["memory_total_KB"].append(metrics_end["memory_total_KB"])
        dataset["memory_live_objs"].append(metrics_end["memory_live_objs"])
        dataset["segments_open"].append(metrics_end["segments_open"])
        dataset["segments_closed"].append(metrics_end["segments_closed"])
        dataset["segments_empty"].append(metrics_end["segments_empty"])
        dataset["segments_in_journal"].append(metrics_end["segments_in_journal"])
        dataset["segments_type_journal"].append(metrics_end["segments_type_journal"])
        dataset["segments_type_ool"].append(metrics_end["segments_type_ool"])
        dataset["available_KB"].append(metrics_end["available_KB"])
        dataset["unavail_reclaimable_KB"].append(metrics_end["unavail_reclaimable_KB"])
        dataset["unavail_unreclaimable_KB"].append(metrics_end["unavail_unreclaimable_KB"])
        dataset["unavail_used_KB"].append(metrics_end["unavail_used_KB"])
        dataset["unavail_unused_KB"].append(metrics_end["unavail_unused_KB"])
        dataset["alloc_journal_KB"].append(metrics_end["alloc_journal_KB"])
        dataset["dirty_journal_KB"].append(metrics_end["dirty_journal_KB"])

        def get_no_diff_l1(metric_name, dataset, metrics_end):
            for name, value_end in metrics_end[metric_name].items():
                dataset[metric_name][name].append(value_end)
        get_no_diff_l1("segment_util_distribution", dataset, metrics_end)

        def get_diff_l1(metric_name, dataset, metrics_start, metrics_end):
            for name, value_end in metrics_end[metric_name].items():
                value_start = metrics_start[metric_name][name]
                value = value_end - value_start
                assert(value >= 0)
                dataset[metric_name][name].append(value)
        # submitter -> blocks/count
        get_diff_l1("journal_padding_4KB",       dataset, metrics_start, metrics_end)
        get_diff_l1("journal_metadata_4KB",      dataset, metrics_start, metrics_end)
        get_diff_l1("journal_data_4KB",          dataset, metrics_start, metrics_end)
        get_diff_l1("journal_record_num",        dataset, metrics_start, metrics_end)
        get_diff_l1("journal_record_batch_num",  dataset, metrics_start, metrics_end)
        get_diff_l1("journal_io_num",            dataset, metrics_start, metrics_end)
        get_diff_l1("journal_io_depth_num",      dataset, metrics_start, metrics_end)
        # srcs -> count
        get_diff_l1("trans_srcs_invalidated",  dataset, metrics_start, metrics_end)
        # src -> count
        get_diff_l1("cache_access",            dataset, metrics_start, metrics_end)
        get_diff_l1("cache_hit",               dataset, metrics_start, metrics_end)
        get_diff_l1("created_trans",           dataset, metrics_start, metrics_end)
        get_diff_l1("committed_trans",         dataset, metrics_start, metrics_end)
        get_diff_l1("invalidated_ool_records", dataset, metrics_start, metrics_end)
        get_diff_l1("committed_ool_records",   dataset, metrics_start, metrics_end)
        # src -> blocks
        get_diff_l1("invalidated_ool_record_4KB",           dataset, metrics_start, metrics_end)
        get_diff_l1("committed_ool_record_metadata_4KB",    dataset, metrics_start, metrics_end)
        get_diff_l1("committed_ool_record_data_4KB",        dataset, metrics_start, metrics_end)
        get_diff_l1("committed_inline_record_metadata_4KB", dataset, metrics_start, metrics_end)
        # scheduler-group -> time
        get_diff_l1("scheduler_runtime_sec",   dataset, metrics_start, metrics_end)
        get_diff_l1("scheduler_waittime_sec",  dataset, metrics_start, metrics_end)
        get_diff_l1("scheduler_starvetime_sec", dataset, metrics_start, metrics_end)
        # scheduler-group -> count
        get_diff_l1("scheduler_tasks_processed_M", dataset, metrics_start, metrics_end)

        def get_diff_l2(metric_name, dataset, metrics_start, metrics_end):
            for l2_name, l2_items_end in metrics_end[metric_name].items():
                for name, value_end in l2_items_end.items():
                    value_start = metrics_start[metric_name][l2_name][name]
                    value = value_end - value_start
                    assert(value >= 0)
                    dataset[metric_name][l2_name][name].append(value)
        # src -> tree-type -> count
        get_diff_l2("tree_erases_committed",    dataset, metrics_start, metrics_end)
        get_diff_l2("tree_inserts_committed",   dataset, metrics_start, metrics_end)
        get_diff_l2("tree_updates_committed",   dataset, metrics_start, metrics_end)
        # src -> extent-type -> count
        get_diff_l2("invalidated_trans",        dataset, metrics_start, metrics_end)
        # src -> effort-type -> blocks
        get_diff_l2("invalidated_efforts_4KB",  dataset, metrics_start, metrics_end)
        get_diff_l2("committed_efforts_4KB",    dataset, metrics_start, metrics_end)
        get_diff_l2("committed_trans_efforts_4KB", dataset, metrics_start, metrics_end)

        def get_diff_l3(metric_name, dataset, metrics_start, metrics_end):
            for l3_name, l3_items_end in metrics_end[metric_name].items():
                for l2_name, l2_items_end in l3_items_end.items():
                    for name, value_end in l2_items_end.items():
                        value_start = metrics_start[metric_name][l3_name][l2_name][name]
                        value = value_end - value_start
                        assert(value >= 0)
                        dataset[metric_name][l3_name][l2_name][name].append(value)
        # src -> extent-type -> effort-type -> blocks
        get_diff_l3("committed_disk_efforts_4KB", dataset, metrics_start, metrics_end)

def wash_dataset(dataset, writes_4KB, times_sec, absolute):
    def merge_lists(lists):
        assert(len(lists))
        length = 0
        for _list in lists:
            if length == 0:
                length = len(_list)
                assert(length)
            else:
                assert(length == len(_list))
        return [sum(values) for values in zip(*lists)]

    if len(times_sec) > 0:
        assert(len(writes_4KB) == 0)
        print("WARN: no bench file available, guess writes_4KB from committed OBJECT_DATA_BLOCK!")
        writes_by_mutate = dataset["committed_disk_efforts_4KB"]["MUTATE"]
        writes_by_odb = writes_by_mutate["OBJECT_DATA_BLOCK"]
        assert(all(blocks == 0 for blocks in writes_by_odb["MUTATE_DELTA"]))
        writes_4KB = merge_lists([writes_by_odb["FRESH_INLINE"],
                                  writes_by_odb["FRESH_OOL"]])
        assert(len(writes_4KB) == len(times_sec))

    if len(dataset["radosbench_4KB"]) > 0:
        assert(len(dataset["radosbench_4KB"]) == len(times_sec))

    INVALID_RATIO = -0.1
    dataset_size = len(writes_4KB)
    washed_dataset = {}

    # 1. from cached_4KB, dirty_4KB
    data_name = "cache_usage(MiB)"

    assert(len(dataset["cached_4KB"]) == dataset_size)
    assert(len(dataset["dirty_4KB"]) == dataset_size)

    def block_to_MB(items):
        return [item/256 for item in items]
    cached_MB = block_to_MB(dataset["cached_4KB"])
    dirty_MB = block_to_MB(dataset["dirty_4KB"])
    cached_clean_MB = [cache - dirty for cache, dirty in zip(cached_MB, dirty_MB)]
    washed_dataset[data_name] = {
        "cached": cached_MB,
        "dirty": dirty_MB,
        "cached_clean": cached_clean_MB,
    }

    # 2. from tree_depth
    data_name = "tree_depths"
    for name, values in dataset["tree_depth"].items():
        assert(len(values) == dataset_size)
    washed_dataset[data_name] = dataset["tree_depth"]

    # 3. from tree_erases_committed, tree_inserts_committed
    data_name = "tree_operations_sum"

    def merge_lists_l1_by_l2(l3_items):
        l2_ret = {}
        for l2_name, l2_items in l3_items.items():
            for name, items in l2_items.items():
                if name in l2_ret:
                    l2_ret[name] = merge_lists([l2_ret[name], items])
                else:
                    l2_ret[name] = items
        return l2_ret
    _tree_inserts_committed_by_tree = merge_lists_l1_by_l2(dataset["tree_inserts_committed"])
    _tree_erases_committed_by_tree = merge_lists_l1_by_l2(dataset["tree_erases_committed"])
    _tree_updates_committed_by_tree = merge_lists_l1_by_l2(dataset["tree_updates_committed"])

    def accumulate(values):
        out = []
        out_value = 0
        for v in values:
            out_value += v
            out.append(out_value)
        return out
    def accumulate_l2(l2_items):
        out = {}
        for name, values in l2_items.items():
            out[name] = accumulate(values)
        return out
    tree_inserts_committed_by_tree = accumulate_l2(_tree_inserts_committed_by_tree)
    tree_erases_committed_by_tree = accumulate_l2(_tree_erases_committed_by_tree)
    tree_updates_committed_by_tree = accumulate_l2(_tree_updates_committed_by_tree)

    washed_dataset[data_name] = {}
    for tree_type, values in tree_inserts_committed_by_tree.items():
        sub_name = tree_type + "_inserts"
        washed_dataset[data_name][sub_name] = values
    for tree_type, values in tree_erases_committed_by_tree.items():
        sub_name = tree_type + "_erases"
        washed_dataset[data_name][sub_name] = values
    for tree_type, values in tree_updates_committed_by_tree.items():
        sub_name = tree_type + "_updates"
        washed_dataset[data_name][sub_name] = values

    def get_IOPS(rws, ts_sec):
        assert(len(rws) == len(ts_sec))
        return [rw/t for rw, t in zip(rws, ts_sec)]
    def get_IOPS_l2(l2_rws, ts_sec):
        ret = {}
        for name, data in l2_rws.items():
            iops = get_IOPS(data, ts_sec)
            ret[name] = iops
        return ret

    data_name = "tree_operations_per_second"
    tree_inserts_PS_committed_by_tree = get_IOPS_l2(
        _tree_inserts_committed_by_tree, times_sec)
    tree_erases_PS_committed_by_tree = get_IOPS_l2(
        _tree_erases_committed_by_tree, times_sec)
    tree_updates_PS_committed_by_tree = get_IOPS_l2(
        _tree_updates_committed_by_tree, times_sec)
    washed_dataset[data_name] = {}
    for tree_type, values in tree_inserts_PS_committed_by_tree.items():
        sub_name = tree_type + "_inserts"
        washed_dataset[data_name][sub_name] = values
    for tree_type, values in tree_erases_PS_committed_by_tree.items():
        sub_name = tree_type + "_erases"
        washed_dataset[data_name][sub_name] = values
    for tree_type, values in tree_updates_PS_committed_by_tree.items():
        sub_name = tree_type + "_updates"
        washed_dataset[data_name][sub_name] = values

    # 4. from cache_hit, cache_access
    data_name = "cache_hit_ratio_by_source"

    def get_ratio(numerators, denominators, invalid=INVALID_RATIO):
        assert(len(numerators) == len(denominators))
        ratios = []
        for numerator, denominator in zip(numerators, denominators):
            ratio = invalid
            if denominator != 0:
                ratio = (numerator/denominator)
            else:
                if numerator != 0:
                    # special case
                    ratio = invalid * 2
            ratios.append(ratio)
        return ratios
    def get_ratio_l2(l2_numerators, l2_denominators, expected_size):
        l2_ret = {}
        for name, denominators in l2_denominators.items():
            numerators = l2_numerators[name]
            ratios = get_ratio(numerators, denominators)
            assert(len(ratios) == expected_size)
            l2_ret[name] = ratios
        return l2_ret
    def filter_out_invalid_ratio_l2(l2_items):
        return {name:items
                for name, items in l2_items.items()
                if any(item != INVALID_RATIO for item in items)}
    cache_hit_access_ratio = get_ratio_l2(dataset["cache_hit"],
                                          dataset["cache_access"],
                                          dataset_size)
    washed_dataset[data_name] = filter_out_invalid_ratio_l2(cache_hit_access_ratio)

    # 5. from invalidated_trans, committed_trans
    data_name = "transaction_invalidated_committed_ratio_by_source"

    def merge_lists_l2(l3_items):
        l2_ret = {}
        for l2_name, l2_items in l3_items.items():
            l2_ret[l2_name] = merge_lists(l2_items.values())
        return l2_ret
    invalidated_trans_by_src = merge_lists_l2(dataset["invalidated_trans"])

    for src_name, created_list in dataset["created_trans"].items():
        index = 0
        total_diff = 0
        for created, invalidated, committed in zip(created_list,
                                                   invalidated_trans_by_src[src_name],
                                                   dataset["committed_trans"][src_name]):
            index += 1
            diff = created - invalidated - committed
            if diff != 0:
                total_diff += diff
                print("WARN: extra created transactions %d -- total %d -- %s at round %d"
                      % (diff, total_diff, src_name, index))
    print()

    washed_dataset[data_name] = get_ratio_l2(invalidated_trans_by_src,
                                             dataset["committed_trans"],
                                             dataset_size)

    # 6.x from invalidated_trans, committed_trans
    def inplace_merge_l1_from_l3(to_metric, from_metric1, from_metric2, l3_items):
        for l2_items in l3_items.values():
            from_items1 = l2_items[from_metric1]
            from_items2 = l2_items[from_metric2]
            to_items = merge_lists([from_items1, from_items2])
            del l2_items[from_metric1]
            del l2_items[from_metric2]
            l2_items[to_metric] = to_items
    inplace_merge_l1_from_l3("LADDR", "LADDR_LEAF", "LADDR_INTERNAL", dataset["invalidated_trans"])
    inplace_merge_l1_from_l3("OMAP",  "OMAP_LEAF",  "OMAP_INNER",     dataset["invalidated_trans"])

    def filter_out_empty_l2(l2_items):
        return {name:items
                for name, items in l2_items.items()
                if any(items)}
    def get_ratio_l2_by_l1(l2_numerators, denominators):
        ret = {}
        for name, numerators in l2_numerators.items():
            ratios = get_ratio(numerators, denominators)
            ret[name] = ratios
        return ret
    for src, invalidated_trans_by_extent in dataset["invalidated_trans"].items():
        data_name = "transaction_invalidated_committed_ratio_by_extent---" + src
        non_empty_invalidated_trans = filter_out_empty_l2(invalidated_trans_by_extent)
        if len(non_empty_invalidated_trans) == 0:
            print(data_name + " is emtpy!")
            continue
        washed_dataset[data_name] = get_ratio_l2_by_l1(
            non_empty_invalidated_trans, dataset["committed_trans"][src])

    # 7.x from invalidated_efforts_4KB, committed_efforts_4KB
    for src, committed_efforts_4KB in dataset["committed_efforts_4KB"].items():
        data_name = "transaction_invalidated_committed_ratio_by_actual_effort---" + src
        result_ratio = get_ratio_l2(dataset["invalidated_efforts_4KB"][src],
                                    committed_efforts_4KB,
                                    dataset_size)

        non_empty_result_ratio = filter_out_invalid_ratio_l2(result_ratio)
        if len(non_empty_result_ratio) == 0:
            print(data_name + " is empty!")
            continue
        washed_dataset[data_name] = non_empty_result_ratio

    # 8.x from writes_4KB, committed_disk_efforts_4KB
    def inplace_merge_l2_from_l3(l2_to_metric, l2_from_metric1, l2_from_metric2, l3_items):
        l2_from_items1 = l3_items[l2_from_metric1]
        l2_from_items2 = l3_items[l2_from_metric2]
        l2_to_items = {}
        for name, from_items1 in l2_from_items1.items():
            to_items = merge_lists([from_items1, l2_from_items2[name]])
            l2_to_items[name] = to_items

        del l3_items[l2_from_metric1]
        del l3_items[l2_from_metric2]
        l3_items[l2_to_metric] = l2_to_items

    def filter_out_empty_l2_from_l3(l3_items):
        return {l2_name:l2_items
                for l2_name, l2_items in l3_items.items()
                if any([any(items) for name, items in l2_items.items()])}

    commit_srcs = []
    fresh_ool_4KB = {}
    fresh_inline_4KB = {}
    fresh_invalid_4KB = {}
    mutate_delta_4KB = {}
    for src, committed_disk_efforts in dataset["committed_disk_efforts_4KB"].items():
        if absolute:
            data_name = "write_4KB_by_extent---" + src
        else:
            data_name = "write_amplification_by_extent---" + src

        inplace_merge_l2_from_l3("LADDR", "LADDR_LEAF", "LADDR_INTERNAL", committed_disk_efforts)
        inplace_merge_l2_from_l3("OMAP",  "OMAP_LEAF",  "OMAP_INNER",     committed_disk_efforts)

        non_empty_committed_disk_efforts = filter_out_empty_l2_from_l3(committed_disk_efforts)
        if len(non_empty_committed_disk_efforts) == 0:
            print(data_name + " is empty!")
            continue

        fresh_ool = []
        fresh_inline = []
        fresh_invalid = []
        mutate_delta = []
        output = {}
        for ext_name, items_by_effort in non_empty_committed_disk_efforts.items():
            assert(len(items_by_effort) == 4)

            fresh_ool.append(items_by_effort["FRESH_OOL"])
            fresh_inline.append(items_by_effort["FRESH_INLINE"])
            fresh_invalid.append(items_by_effort["FRESH_INVALID"])
            mutate_delta.append(items_by_effort["MUTATE_DELTA"])

            total_disk_writes = merge_lists([items_by_effort["FRESH_INLINE"],
                                             items_by_effort["FRESH_OOL"],
                                             items_by_effort["MUTATE_DELTA"]])
            output[ext_name] = total_disk_writes

        commit_srcs.append(src)
        fresh_ool_4KB[src] = merge_lists(fresh_ool)
        fresh_inline_4KB[src] = merge_lists(fresh_inline)
        fresh_invalid_4KB[src] = merge_lists(fresh_invalid)
        mutate_delta_4KB[src] = merge_lists(mutate_delta)
        if absolute:
            output["writes_4KB"] = writes_4KB
        else:
            output = get_ratio_l2_by_l1(output, writes_4KB)
        washed_dataset[data_name] = output

    # 9.x write_amplification_detail
    valid_data_4K = {}
    valid_metadata_4K = {}
    invalid_write_4K = {}
    for src in commit_srcs:
        if absolute:
            data_name = "write_4KB_by_category---" + src
        else:
            data_name = "write_amplification_by_category---" + src

        invalid_ool = dataset["invalidated_ool_record_4KB"][src]
        valid_ool_data = dataset["committed_ool_record_data_4KB"][src]
        assert(fresh_ool_4KB[src] == valid_ool_data)
        valid_ool_metadata = dataset["committed_ool_record_metadata_4KB"][src]
        inline_fresh_data = fresh_inline_4KB[src]
        inline_retired_data = fresh_invalid_4KB[src]
        inline_delta_data = mutate_delta_4KB[src]
        inline_metadata = dataset["committed_inline_record_metadata_4KB"][src]
        output = {
            "OOL_INVALID":            invalid_ool,
            "OOL_VALID_DATA":         valid_ool_data,
            "OOL_VALID_METADATA":     valid_ool_metadata,
            "INLINE_FRESH_DATA":      inline_fresh_data,
            "INLINE_RETIRED_DATA":    inline_retired_data,
            "INLINE_DELTA_DATA":      inline_delta_data,
            "INLINE_METADATA":        inline_metadata,
        }
        if absolute:
            output["writes_4KB"] = writes_4KB
        else:
            output = get_ratio_l2_by_l1(output, writes_4KB)
        washed_dataset[data_name] = output

        valid_data = merge_lists([valid_ool_data,
                                  inline_fresh_data,
                                  inline_delta_data])
        valid_data_4K[src] = valid_data
        valid_metadata = merge_lists([valid_ool_metadata,
                                      inline_metadata])
        valid_metadata_4K[src] = valid_metadata
        invalid_write = merge_lists([invalid_ool,
                                     inline_retired_data])
        invalid_write_4K[src] = invalid_write

    # 10. write_amplification_by_src
    if absolute:
        data_name = "write_4KiB_by_source"
    else:
        data_name = "write_amplification_by_source"
    data_10 = {}
    mutate_trans_data_write = []
    for src in commit_srcs:
        name = src + "_VALID_DATA_"
        data_10[name] = valid_data_4K[src]     # ool/inline data, inline delta
        name = src + "_VALID_METADATA"
        data_10[name] = valid_metadata_4K[src] # ool/inline metadata
        name = src + "_INVALID_WRITE"
        data_10[name] = invalid_write_4K[src]  # inline-retired, invalid-ool
        if src == "MUTATE":
            mutate_trans_data_write = valid_data_4K[src]
    if commit_srcs:
        if absolute:
            data_10["writes_4KB"] = writes_4KB
        else:
            data_10 = get_ratio_l2_by_l1(data_10, writes_4KB)
        washed_dataset[data_name] = data_10

    # 11. write_amplification_overall
    segmented_read = dataset["segment_read_4KB"]
    segmented_write = merge_lists([dataset["segment_write_4KB"],
                                   dataset["segment_write_meta_4KB"]])

    if commit_srcs:
        if absolute:
            data_name = "write_4KiB_overall"
        else:
            data_name = "write_amplification_overall"

        aw_padding =  merge_lists(dataset["journal_padding_4KB"].values())
        total_md =    merge_lists(dataset["journal_metadata_4KB"].values())
        total_data =  merge_lists(dataset["journal_data_4KB"].values())
        total_write = merge_lists([aw_padding,
                                   total_md,
                                   total_data])

        aw_valid_data = merge_lists(valid_data_4K.values())
        # note: record_group_header is not accounted
        aw_valid_metadata = merge_lists(valid_metadata_4K.values())
        aw_invalid = merge_lists(invalid_write_4K.values())

        accounted_write = merge_lists([aw_valid_data,
                                       aw_valid_metadata,
                                       aw_invalid,
                                       aw_padding])

        data_11 = {
            "SEGMENTED_READ":     segmented_read,
            "SEGMENTED_WRITE":    segmented_write,
            "TOTAL_WRITE":        total_write,
            "ACCOUNTED_WRITE":    accounted_write,
            "AW_VALID_DATA":      aw_valid_data,
            "AW_VALID_METADATA":  aw_valid_metadata,
            "AW_INVALID":         aw_invalid,
            "AW_PADDING":         aw_padding,
            "MUTATE_TRANS_DATA":  mutate_trans_data_write,
        }

        if absolute:
            data_11["writes_4KiB"] = writes_4KB
        else:
            data_11 = get_ratio_l2_by_l1(data_11, writes_4KB)
        washed_dataset[data_name] = data_11

        if len(dataset["iostat_write_4KB"]) or len(dataset["radosbench_4KB"]):
            if absolute:
                data_name = "write_4KiB_overall_2"
            else:
                data_name = "write_amplification_overall_2"
            data_11_2 = {}
            if len(dataset["iostat_write_4KB"]):
                data_11_2.update({
                    "SEGMENTED_READ":   segmented_read,
                    "SEGMENTED_WRITE":  segmented_write,
                    "IOSTAT_READ":      dataset["iostat_read_4KB"],
                    "IOSTAT_WRITE":     dataset["iostat_write_4KB"],
                    "NVME_HOST":        dataset["nvme_host_4KB"],
                    "NVME_NAND":        dataset["nvme_nand_4KB"],
                })
            if len(dataset["radosbench_4KB"]):
                data_11_2.update({
                    "RADOS_BENCH": dataset["radosbench_4KB"],
                })
            if absolute:
                data_11_2["writes_4KiB"] = writes_4KB
            else:
                data_11_2 = get_ratio_l2_by_l1(data_11_2, writes_4KB)
                print(data_11_2["RADOS_BENCH"])
                print()
            washed_dataset[data_name] = data_11_2

    # 12. record_fullness
    data_name = "record_fullness"
    data_12_ratio = {}
    submitter_writes_4KB_avg = {}
    submitter_writes_4KB_sum = {}
    for submitter, raw_md in dataset["journal_metadata_4KB"].items():
        total_md = merge_lists([dataset["journal_padding_4KB"][submitter], raw_md])
        data_12_ratio[submitter + "-md"] = get_ratio(raw_md, total_md)
        raw_all = merge_lists([dataset["journal_data_4KB"][submitter], raw_md])
        total_all = merge_lists([dataset["journal_data_4KB"][submitter], total_md])
        data_12_ratio[submitter + "-all"] = get_ratio(raw_all, total_all)
        submit_record_num = dataset["journal_record_num"][submitter]
        submitter_writes_4KB_avg[submitter] = get_ratio(total_all, submit_record_num)
        submitter_writes_4KB_sum[submitter] = accumulate(total_all)
        washed_dataset["submitters_write_4KiB_average---" + submitter] = {
            "metadata": get_ratio(raw_md, submit_record_num),
            "data":     get_ratio(dataset["journal_data_4KB"][submitter], submit_record_num),
            "padding":  get_ratio(dataset["journal_padding_4KB"][submitter], submit_record_num),
        }
    washed_dataset[data_name] = filter_out_invalid_ratio_l2(data_12_ratio)
    washed_dataset["submitters_write_4KiB_average"] = submitter_writes_4KB_avg
    washed_dataset["submitters_write_4KiB_sum"] = submitter_writes_4KB_sum

    # 13. journal io by submitter
    data_name = "journal_io_pattern_by_submitter"
    data_13 = {}
    for submitter, io_depth_num in dataset["journal_io_depth_num"].items():
        journal_io_depth = get_ratio(io_depth_num,
                                     dataset["journal_io_num"][submitter])
        data_13[submitter + "-io_depth"] = journal_io_depth
        journal_record_batching = get_ratio(dataset["journal_record_batch_num"][submitter],
                                            dataset["journal_record_num"][submitter])
        data_13[submitter + "-batched_records"] = journal_record_batching
    washed_dataset[data_name] = data_13

    # 14. trans_srcs_invalidated
    data_name = "transaction_invalidated_committed_ratio_by_sources"
    committed_trans_all = merge_lists(dataset["committed_trans"].values())
    non_empty_trans_srcs_invalidated = filter_out_empty_l2(dataset["trans_srcs_invalidated"])
    if non_empty_trans_srcs_invalidated:
        washed_dataset[data_name] = get_ratio_l2_by_l1(
            non_empty_trans_srcs_invalidated,
            committed_trans_all)

    # 15. segments state
    data_name = "segment_state"
    washed_dataset[data_name] = {
        "open": dataset["segments_open"],
        "closed": dataset["segments_closed"],
        "empty": dataset["segments_empty"],
        "in_journal": dataset["segments_in_journal"],
        "type_journal": dataset["segments_type_journal"],
        "type_ool": dataset["segments_type_ool"],
    }

    # 16. segments operations
    data_name = "segment_operation_sum"
    segments_count_open_journal = accumulate(dataset["segments_count_open_journal"])
    segments_count_close_journal = accumulate(dataset["segments_count_close_journal"])
    segments_count_release_journal = accumulate(dataset["segments_count_release_journal"])
    segments_count_open_ool = accumulate(dataset["segments_count_open_ool"])
    segments_count_close_ool = accumulate(dataset["segments_count_close_ool"])
    segments_count_release_ool = accumulate(dataset["segments_count_release_ool"])
    washed_dataset[data_name] = {
        "open_journal": segments_count_open_journal,
        "close_journal": segments_count_close_journal,
        "release_journal": segments_count_release_journal,
        "open_ool": segments_count_open_ool,
        "close_ool": segments_count_close_ool,
        "release_ool": segments_count_release_ool,
    }

    data_name = "segment_operation_per_second"
    segments_count_open_journal_PS = get_IOPS(dataset["segments_count_open_journal"], times_sec)
    segments_count_close_journal_PS = get_IOPS(dataset["segments_count_close_journal"], times_sec)
    segments_count_release_journal_PS = get_IOPS(dataset["segments_count_release_journal"], times_sec)
    segments_count_open_ool_PS = get_IOPS(dataset["segments_count_open_ool"], times_sec)
    segments_count_close_ool_PS = get_IOPS(dataset["segments_count_close_ool"], times_sec)
    segments_count_release_ool_PS = get_IOPS(dataset["segments_count_release_ool"], times_sec)
    washed_dataset[data_name] = {
        "open_journal": segments_count_open_journal_PS,
        "close_journal": segments_count_close_journal_PS,
        "release_journal": segments_count_release_journal_PS,
        "open_ool": segments_count_open_ool_PS,
        "close_ool": segments_count_close_ool_PS,
        "release_ool": segments_count_release_ool_PS,
    }

    # 17. space usage
    data_name = "space_usage_MiB"
    avg_projected_used_KB = get_ratio(dataset["projected_used_sum_KB"],
                                      dataset["projected_count"])
    def KB_to_MB(items):
        return [item/1024 for item in items]
    total_KB = merge_lists([dataset["available_KB"],
                            dataset["unavail_reclaimable_KB"],
                            dataset["unavail_unreclaimable_KB"]])
    washed_dataset[data_name] = {
        "available": KB_to_MB(dataset["available_KB"]),
        "unavail_reclaimable": KB_to_MB(dataset["unavail_reclaimable_KB"]),
        "unavail_unreclaimable": KB_to_MB(dataset["unavail_unreclaimable_KB"]),
        "unavail_used": KB_to_MB(dataset["unavail_used_KB"]),
        "unavail_unused": KB_to_MB(dataset["unavail_unused_KB"]),
        "projected_avg": KB_to_MB(avg_projected_used_KB),
        "total": KB_to_MB(total_KB),
    }

    # 18. space ratio
    data_name = "space_ratio"
    avg_reclaimed_ratio = get_ratio(dataset["reclaimed_KB"],
                                    dataset["reclaimed_segment_KB"])
    reclaimed_KB = accumulate(dataset["reclaimed_KB"])
    reclaimed_segment_KB = accumulate(dataset["reclaimed_segment_KB"])
    reclaimed_alive_total = get_ratio(reclaimed_KB, reclaimed_segment_KB)
    closed_journal_alive_total = get_ratio(dataset["closed_journal_used_KB"],
                                           dataset["closed_journal_total_KB"])
    closed_ool_alive_total = get_ratio(dataset["closed_ool_used_KB"],
                                       dataset["closed_ool_total_KB"])
    alive_total = get_ratio(dataset["unavail_used_KB"], total_KB)
    washed_dataset[data_name] = {
        "unavailable/total": dataset["unavailiable_total"],
        "alive/unavailable": dataset["alive_unavailable"],
        "reclaimed_average_alive/total": avg_reclaimed_ratio,
        "reclaimed_sum_alive/total": reclaimed_alive_total,
        "closed_journal_alive/total": closed_journal_alive_total,
        "closed_ool_alive/total": closed_ool_alive_total,
        "alive/total": alive_total,
    }

    # 19. cleaner blocked io
    data_name = "cleaner_blocked_io_by_reason"
    blocked_iops = get_ratio(dataset["io_blocked_count"],
                             dataset["io_count"],
                             -0.0001)
    blocked_iops_trim = get_ratio(dataset["io_blocked_count_trim"],
                                  dataset["io_count"],
                                  -0.0001)
    blocked_iops_reclaim = get_ratio(dataset["io_blocked_count_reclaim"],
                                     dataset["io_count"],
                                     -0.0001)
    blocking_iops = get_ratio(dataset["io_blocked_sum"],
                              dataset["io_count"],
                              -0.0001)
    washed_dataset[data_name] = {
        "blocked_iops": blocked_iops,
        "blocked_iops_by_trim": blocked_iops_trim,
        "blocked_iops_by_reclaim": blocked_iops_reclaim,
        "blocked_depth_iops": blocking_iops,
    }

    # 20. segment usage distribution
    washed_dataset["segment_usage_distribution"] = dataset["segment_util_distribution"]

    # 21.* transaction commit efforts by src
    data_name = "transaction_commit_average_efforts_4KiB_by_source"
    washed_dataset[data_name] = {}
    for src, efforts in dataset["committed_trans_efforts_4KB"].items():
        data_name_src = "transaction_commit_average_efforts_4KiB_detail---" + src
        washed_dataset[data_name_src] = get_ratio_l2_by_l1(efforts, dataset["committed_trans"][src])
        sum_efforts = merge_lists([data for effort, data in efforts.items() if effort != "RETIRE"])
        washed_dataset[data_name][src] = get_ratio(sum_efforts, dataset["committed_trans"][src])

    if len(times_sec) == 0:
        # indexes
        indexes = accumulate(writes_4KB)
        return washed_dataset, indexes

    # 22. journal sizes
    data_name = "journal_length_MiB"
    washed_dataset[data_name] = {
        "alloc_journal": KB_to_MB(dataset["alloc_journal_KB"]),
        "dirty_journal": KB_to_MB(dataset["dirty_journal_KB"]),
    }

    # 23 rewrite versions
    data_name = "rewrite_average_versions"
    rewrite_dirty_version = get_ratio(dataset["version_sum_dirty"],
                                      dataset["version_count_dirty"],
                                      -1)
    rewrite_reclaim_version = get_ratio(dataset["version_sum_reclaim"],
                                        dataset["version_count_reclaim"],
                                        -1)
    washed_dataset[data_name] = {
        "rewrite_dirty": rewrite_dirty_version,
        "rewrite_reclaim": rewrite_reclaim_version,
    }

    #
    # Metric-only specific graph
    #

    # 1. from writes_4KB
    data_name = "writes_accumulated_MiB"
    washed_dataset[data_name] = {
        "obj_data(client)":  block_to_MB(accumulate(writes_4KB)),
        "reactor_aio_write": block_to_MB(accumulate(dataset["reactor_aio_write_4KB"])),
        "reactor_aio_read":  block_to_MB(accumulate(dataset["reactor_aio_read_4KB"])),
    }
    for src, writes in valid_data_4K.items():
        washed_dataset[data_name]["trans_" + src] = block_to_MB(accumulate(writes))
    for name, data in washed_dataset[data_name].items():
        print("%s: %s MiB" % (name, str(data[-1])))
    print()

    if len(dataset["iostat_write_4KB"]) or len(dataset["radosbench_4KB"]):
        data_name = "writes_accumulated_MiB_2"
        washed_dataset[data_name] = {
            "obj_data(client)":  block_to_MB(accumulate(writes_4KB)),
        }
        if len(dataset["iostat_write_4KB"]):
            washed_dataset[data_name].update({
                "reactor_aio_write": block_to_MB(accumulate(dataset["reactor_aio_write_4KB"])),
                "reactor_aio_read":  block_to_MB(accumulate(dataset["reactor_aio_read_4KB"])),
                "iostat_read":   block_to_MB(accumulate(dataset["iostat_read_4KB"])),
                "iostat_write":  block_to_MB(accumulate(dataset["iostat_write_4KB"])),
                "nvme_host":     block_to_MB(accumulate(dataset["nvme_host_4KB"])),
                "nvme_nand":     block_to_MB(accumulate(dataset["nvme_nand_4KB"])),
            })
        if len(dataset["radosbench_4KB"]):
            washed_dataset[data_name].update({
                "rados_bench": block_to_MB(accumulate(dataset["radosbench_4KB"])),
            })
        for name, data in washed_dataset[data_name].items():
            print("%s: %s MiB" % (name, str(data[-1])))
        print()

    # 2. from reactor_util, reactor_busytime_sec, reactor_stealtime_sec
    #         scheduler_runtime_sec, scheduler_waittime_sec, scheduler_starvetime_sec
    washed_dataset["CPU_utilities_ratio"] = {
        "reactor_util": dataset["reactor_util"],
        "reactor_busy": get_ratio(dataset["reactor_busytime_sec"], times_sec),
        "reactor_steal": get_ratio(dataset["reactor_stealtime_sec"], times_sec)
    }
    scheduler_runtime_ratio = get_ratio_l2_by_l1(dataset["scheduler_runtime_sec"], times_sec)
    for group_name, ratios in scheduler_runtime_ratio.items():
        if not any(ratios):
            continue
        washed_dataset["CPU_utilities_ratio"]["sched_run_" + group_name] = ratios
    scheduler_waittime_ratio = get_ratio_l2_by_l1(dataset["scheduler_waittime_sec"], times_sec)
    for group_name, ratios in scheduler_waittime_ratio.items():
        if not any(ratios):
            continue
        washed_dataset["CPU_utilities_ratio"]["sched_wait_" + group_name] = ratios
    scheduler_starvetime_ratio = get_ratio_l2_by_l1(dataset["scheduler_starvetime_sec"], times_sec)
    for group_name, ratios in scheduler_starvetime_ratio.items():
        if not any(ratios):
            continue
        washed_dataset["CPU_utilities_ratio"]["sched_starve_" + group_name] = ratios

    # 3. from reactor_aio_read_4KB, reactor_aio_write_4KB,
    #         segment_write_4KB, segment_write_meta_4KB, segment_read_4KB,
    #         committed_disk_efforts_4KB
    #         writes_4KB
    def get_throughput_MB(rws_4KB, ts_sec):
        assert(len(rws_4KB) == len(ts_sec))
        return [rw/256/t for rw, t in zip(rws_4KB, ts_sec)]
    obj_data_throughput_MB = get_throughput_MB(writes_4KB, times_sec)
    print(obj_data_throughput_MB)
    print()
    washed_dataset["throughput_MiB"] = {
        "reactor_aio_read":   get_throughput_MB(dataset["reactor_aio_read_4KB"], times_sec),
        "reactor_aio_write":  get_throughput_MB(dataset["reactor_aio_write_4KB"], times_sec),
        "device_read":        get_throughput_MB(segmented_read, times_sec),
        "device_write":       get_throughput_MB(segmented_write, times_sec),
        "obj_data_write":     obj_data_throughput_MB,
    }
    if commit_srcs:
        washed_dataset["throughput_MiB"].update({
            "accounted_write":    get_throughput_MB(accounted_write, times_sec),
            "valid_extent_write": get_throughput_MB(aw_valid_data, times_sec),
            "commit_trans_data_write": get_throughput_MB(mutate_trans_data_write, times_sec),
        })
    if len(dataset["iostat_write_4KB"]) or len(dataset["radosbench_4KB"]):
        data_name = "throughput_MiB_2"
        washed_dataset[data_name] = {
            "obj_data_write":     obj_data_throughput_MB,
        }
        if len(dataset["iostat_write_4KB"]):
            washed_dataset[data_name].update({
                "reactor_aio_read":   get_throughput_MB(dataset["reactor_aio_read_4KB"], times_sec),
                "reactor_aio_write":  get_throughput_MB(dataset["reactor_aio_write_4KB"], times_sec),
                "iostat_read":  get_throughput_MB(dataset["iostat_read_4KB"], times_sec),
                "iostat_write": get_throughput_MB(dataset["iostat_write_4KB"], times_sec),
                "nvme_host":    get_throughput_MB(dataset["nvme_host_4KB"], times_sec),
                "nvme_nand":    get_throughput_MB(dataset["nvme_nand_4KB"], times_sec),
            })
        if len(dataset["radosbench_4KB"]):
            washed_dataset[data_name].update({
                "radosbench":   get_throughput_MB(dataset["radosbench_4KB"], times_sec),
            })

    # 4.x IOPS_by_src, IOPS_overall
    data_IOPS_detail = {}
    read_trans = []
    commit_trans = []
    for src, items in dataset["committed_trans"].items():
        if src == "READ":
            read_trans = items
        else:
            commit_trans.append(items)
        name = "committed_" + src
        data_IOPS_detail[name] = items
    ool_records = []
    for src, invalidated_ool in dataset["invalidated_ool_records"].items():
        name = "ool_records_" + src
        ool = merge_lists([invalidated_ool,
                           dataset["committed_ool_records"][src]])
        ool_records.append(ool)
        data_IOPS_detail[name] = ool
    washed_dataset["IOPS_by_source"] = get_IOPS_l2(data_IOPS_detail, times_sec)

    washed_dataset["IOPS_by_source2"] = washed_dataset["IOPS_by_source"].copy()
    del washed_dataset["IOPS_by_source2"]["committed_MUTATE"]
    del washed_dataset["IOPS_by_source2"]["ool_records_MUTATE"]
    del washed_dataset["IOPS_by_source2"]["committed_READ"]

    segmented_writes = merge_lists([dataset["segment_writes"],
                                    dataset["segment_meta_writes"]])
    data_IOPS = {
        "reactor_aio_read":  dataset["reactor_aio_reads"],
        "reactor_aio_write": dataset["reactor_aio_writes"],
        "device_read":       dataset["segment_reads"],
        "device_write":      segmented_writes,
        "obj_data_write":    dataset["object_data_writes"],
        "committed_trans":   merge_lists(commit_trans),
        "read_trans":        read_trans,
        "ool_records":       merge_lists(ool_records),
    }
    washed_dataset["IOPS"] = get_IOPS_l2(data_IOPS, times_sec)

    # 5. from reactor_polls_M, reactor_tasks_processed_M, scheduler_tasks_processed_M
    washed_dataset["tasks_and_polls_M"] = {
        "reactor_polls": dataset["reactor_polls_M"],
        "reactor_tasks": dataset["reactor_tasks_processed_M"]
    }
    for group_name, tasks in dataset["scheduler_tasks_processed_M"].items():
        if not any(tasks):
            continue
        washed_dataset["tasks_and_polls_M"]["sched_" + group_name + "_tasks"] = tasks

    # 6. from reactor_tasks_pending, scheduler_queue_length
    washed_dataset["tasks_pending"] = {
        "reactor_tasks": dataset["reactor_tasks_pending"]
    }
    for group_name, tasks in dataset["scheduler_queue_length"].items():
        if not any(tasks):
            continue
        washed_dataset["tasks_pending"]["sched_" + group_name + "_tasks"] = tasks

    # 7. from memory_allocate_KB, memory_free_KB, memory_total_KB
    washed_dataset["memory_usage_MiB"] = {
        "allocated": KB_to_MB(dataset["memory_allocate_KB"]),
        # "free": KB_to_MB(dataset["memory_free_KB"]),
        # "total": KB_to_MB(dataset["memory_total_KB"])
    }

    # 8. from memory_frees, memory_mallocs, memory_reclaims, memory_live_objs
    washed_dataset["memory_operations"] = {
        #"frees": dataset["memory_frees"],
        #"mallocs": dataset["memory_mallocs"],
        #"reclaims": dataset["memory_reclaims"],
        "live_objs": dataset["memory_live_objs"],
    }

    # indexes
    indexes = accumulate(times_sec)
    return washed_dataset, indexes

def wash_dataset_no_metrics(dataset, writes_4KB, times_sec, absolute):
    assert(len(dataset["radosbench_4KB"]) == len(times_sec))
    washed_dataset = {}

    INVALID_RATIO = -0.1
    def get_ratio(numerators, denominators, invalid=INVALID_RATIO):
        assert(len(numerators) == len(denominators))
        ratios = []
        for numerator, denominator in zip(numerators, denominators):
            ratio = invalid
            if denominator != 0:
                ratio = (numerator/denominator)
            else:
                if numerator != 0:
                    # special case
                    ratio = invalid * 2
            ratios.append(ratio)
        return ratios
    def get_ratio_l2_by_l1(l2_numerators, denominators):
        ret = {}
        for name, numerators in l2_numerators.items():
            ratios = get_ratio(numerators, denominators)
            ret[name] = ratios
        return ret
    def block_to_MB(items):
        return [item/256 for item in items]
    def get_throughput_MB(rws_4KB, ts_sec):
        assert(len(rws_4KB) == len(ts_sec))
        return [rw/256/t for rw, t in zip(rws_4KB, ts_sec)]
    def accumulate(values):
        out = []
        out_value = 0
        for v in values:
            out_value += v
            out.append(out_value)
        return out

    data_name = ""
    if absolute:
        data_name = "write_4KiB_overall_2"
    else:
        data_name = "write_amplification_overall_2"
    data = {
        "IOSTAT_READ":      dataset["iostat_read_4KB"],
        "IOSTAT_WRITE":     dataset["iostat_write_4KB"],
        "NVME_HOST":        dataset["nvme_host_4KB"],
        "NVME_NAND":        dataset["nvme_nand_4KB"],
    }
    if absolute:
        data["RADOS_BENCH"] = dataset["radosbench_4KB"]
    else:
        data = get_ratio_l2_by_l1(data, dataset["radosbench_4KB"])
    washed_dataset[data_name] = data

    data_name = "writes_accumulated_MiB_2"
    washed_dataset[data_name] = {
        "iostat_read":   block_to_MB(accumulate(dataset["iostat_read_4KB"])),
        "iostat_write":  block_to_MB(accumulate(dataset["iostat_write_4KB"])),
        "nvme_host":     block_to_MB(accumulate(dataset["nvme_host_4KB"])),
        "nvme_nand":     block_to_MB(accumulate(dataset["nvme_nand_4KB"])),
        "rados_bench":   block_to_MB(accumulate(dataset["radosbench_4KB"])),
    }
    for name, data in washed_dataset[data_name].items():
        print("%s: %s MiB" % (name, str(data[-1])))
    print()

    data_name = "throughput_MiB_2"
    washed_dataset[data_name] = {
        "iostat_read":  get_throughput_MB(dataset["iostat_read_4KB"], times_sec),
        "iostat_write": get_throughput_MB(dataset["iostat_write_4KB"], times_sec),
        "nvme_host":    get_throughput_MB(dataset["nvme_host_4KB"], times_sec),
        "nvme_nand":    get_throughput_MB(dataset["nvme_nand_4KB"], times_sec),
        "radosbench":   get_throughput_MB(dataset["radosbench_4KB"], times_sec),
    }
    return washed_dataset, accumulate(times_sec)

def relplot_data(directory, bench_type, name, data, indexes, ylim):
    sns.set_theme(style="whitegrid")
    to_draw = pd.DataFrame(data, index=indexes)
    assert(bench_type != BenchT.NULL)
    if bench_type == BenchT.METRICS:
        to_draw.index.name = "time_seconds"
    else:
        to_draw.index.name = "writes_4KB"
    to_draw.columns.name = "legend"
    g = sns.relplot(data=to_draw,
                    kind="line",
                    markers=True,
                   ).set(title=name, ylim=ylim, ylabel="")
    g.fig.set_size_inches(15,6)
    g.savefig("%s/%s.png" % (directory, name))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "-d", "--directory", type=str,
            help="result directory to evaluate", default="results")
    parser.add_argument(
            "--absolute", action='store_true',
            help="no write amplification")
    parser.add_argument(
            "--headcut", type=int,
            help="drop the first N results", default=0)
    parser.add_argument(
            "--tailcut", type=int,
            help="drop the tail -N results", default=0)
    args = parser.parse_args()

    print("loading dir %s ..." % (args.directory))
    benches, metrics, stats, times, radosbench = load_dir(args.directory, args.headcut, args.tailcut)
    print("loaded %d metrics, %d stats, %d times" % (len(metrics), len(stats), len(times)))
    print()

    print("parse results ...")
    raw_dataset = prepare_raw_dataset()
    parse_radosbench_file(radosbench, raw_dataset, times)

    bench_type = BenchT.NULL
    if len(times):
        bench_type = BenchT.METRICS

    writes_4KB = []
    illegal_metrics = set()
    ignored_metrics = set()

    index = 0
    print(index, end=" ", flush=True)
    num_indexes = 0

    metrics_start = {}
    if len(metrics):
        metric_file = metrics[index]
        metrics_start, illegal, ignored = parse_metric_file(metric_file)
        illegal_metrics |= illegal
        ignored_metrics |= ignored
        num_indexes = len(metrics) - 1

    stats_start = {}
    if len(stats):
        stats_file = stats[index]
        stats_start = parse_stats_file(stats_file)
        _num_indexes = len(stats) - 1
        if num_indexes > 0:
            assert(num_indexes == _num_indexes)
        else:
            num_indexes = _num_indexes

    while index < num_indexes:
        print(index + 1, end=" ", flush=True)

        if bench_type != BenchT.METRICS:
            # mode with bench files
            bench_file = benches[index]
            write_4KB, btype = parse_bench_file(bench_file)
            if bench_type == BenchT.NULL:
                bench_type = btype
            else:
                assert(bench_type == btype)
            writes_4KB.append(write_4KB)

        metrics_end = {}
        if len(metrics):
            metric_file = metrics[index + 1]
            metrics_end, illegal, ignored = parse_metric_file(metric_file)
            illegal_metrics |= illegal
            ignored_metrics |= ignored

        stats_end = {}
        if len(stats):
            stats_file = stats[index + 1]
            stats_end = parse_stats_file(stats_file)

        append_raw_data(raw_dataset, metrics_start, metrics_end, stats_start, stats_end)

        index += 1
        metrics_start = metrics_end
        stats_start = stats_end
    print()
    print("   bench type: %s" % (bench_type))
    print("   illegal metrics: %s" % (illegal_metrics))
    print("   ignored metrics: %s" % (ignored_metrics))
    print("parse results done")
    print()

    print("wash results (absolute=%s) ..." % (args.absolute))
    dataset = {}
    indexes = []
    if len(metrics):
        dataset, indexes = wash_dataset(raw_dataset, writes_4KB, times, args.absolute)
    else:
        dataset, indexes = wash_dataset_no_metrics(raw_dataset, writes_4KB, times, args.absolute)
    print("wash results done")
    print()

    print("generate figures ...")
    for name, data in dataset.items():
        print("figure " + name + "...")
        ylim = None
        relplot_data(args.directory, bench_type, name, data, indexes, ylim)
    print()
    print("generate figures done")
