#!/usr/bin/env python3

from collections import defaultdict
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

def load_dir(dir_name):
    load_folder = path.join(os.getcwd(), dir_name)
    benches = []
    metrics = []
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
        else:
            assert(file_type.startswith("metrics"))
            time = -1
            if len(names) == 4:
                time = int(names[3]) / 1000 # as seconds
            else:
                assert(len(names) == 3)
            metrics.append((index, file_dir, time))

    benches.sort()
    metrics.sort()
    assert(len(metrics) > 1)
    if metrics[0][2] != -1:
        # Mode: METRICS
        assert(len(benches) == 0)
    else:
        # Mode with bench files
        assert(len(benches))
        if len(metrics) == len(benches):
            # no matching matric file to the last bench result
            benches.pop()
        assert(len(metrics) == len(benches) + 1)

    index = 0
    assert(metrics[index][0] == index)
    ctime = metrics[index][2]
    ret_times = []
    index += 1
    while index < len(metrics):
        assert(metrics[index][0] == index)
        if len(benches):
            assert(benches[index - 1][0] == index)
        else:
            time = metrics[index][2]
            difftime = time - ctime
            assert(difftime > 0)
            ret_times.append(difftime)
            ctime = time
        index += 1

    return [item[1] for item in benches], [item[1] for item in metrics], ret_times

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
        else:
            assert(isinstance(v, str))
            labels[k] = v
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
    # ratio
    data["reactor_util"] = 0
    # time
    data["reactor_busytime_sec"] = 0
    data["reactor_stealtime_sec"] = 0
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
    # src -> tree-type -> count
    data["tree_erases_committed"] = defaultdict(lambda: defaultdict(lambda: 0))
    data["tree_erases_invalidated"] = defaultdict(lambda: defaultdict(lambda: 0))
    data["tree_inserts_committed"] = defaultdict(lambda: defaultdict(lambda: 0))
    data["tree_inserts_invalidated"] = defaultdict(lambda: defaultdict(lambda: 0))
    # src-> extent-type -> count
    data["invalidated_trans"] = defaultdict(lambda: defaultdict(lambda: 0))
    # src-> effort-type -> blocks
    data["invalidated_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: 0))
    data["committed_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: 0))
    # src-> extent-type -> effort-type -> blocks
    data["committed_disk_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0)))

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
            data["segment_read_4KB"] += (value/4096)
        elif name == "segment_manager_data_write_bytes":
            data["segment_write_4KB"] += (value/4096)
        elif name == "segment_manager_metadata_write_bytes":
            data["segment_write_meta_4KB"] += (value/4096)
        elif name == "cache_cached_extent_bytes":
            data["cached_4KB"] += (value/4096)
        elif name == "cache_dirty_extent_bytes":
            data["dirty_4KB"] += (value/4096)
        elif name == "reactor_aio_bytes_read":
            data["reactor_aio_read_4KB"] += (value/4096)
        elif name == "reactor_aio_bytes_write":
            data["reactor_aio_write_4KB"] += (value/4096)
        elif name == "memory_allocated_memory":
            data["memory_allocate_KB"] += (value/1024)
        elif name == "memory_free_memory":
            data["memory_free_KB"] += (value/1024)
        elif name == "memory_total_memory":
            data["memory_total_KB"] += (value/1024)

        # count
        elif name == "segment_manager_data_read_num":
            data["segment_reads"] += value
        elif name == "segment_manager_data_write_num":
            data["segment_writes"] += value
        elif name == "segment_manager_metadata_write_num":
            data["segment_meta_writes"] += value
        elif name == "reactor_aio_reads":
            data["reactor_aio_reads"] += value
        elif name == "reactor_aio_writes":
            data["reactor_aio_writes"] += value
        elif name == "reactor_polls":
            data["reactor_polls_M"] += (value/1000000)
        elif name == "reactor_tasks_pending":
            data["reactor_tasks_pending"] += value
        elif name == "reactor_tasks_processed":
            data["reactor_tasks_processed_M"] += (value/1000000)
        elif name == "memory_free_operations":
            data["memory_frees"] += value
        elif name == "memory_malloc_operations":
            data["memory_mallocs"] += value
        elif name == "memory_reclaims_operations":
            data["memory_reclaims"] += value
        elif name == "memory_malloc_live_objects":
            data["memory_live_objs"] += value

        # ratio
        elif name == "reactor_utilization":
            data["reactor_util"] += (value/100)

        # time
        elif name == "reactor_cpu_busy_ms":
            data["reactor_busytime_sec"] += (value/1000)
        elif name == "reactor_cpu_steal_time_ms":
            data["reactor_stealtime_sec"] += (value/1000)

        # scheduler-group -> time
        elif name == "scheduler_runtime_ms":
            data["scheduler_runtime_sec"][labels["group"]] += (value/1000)
        elif name == "scheduler_waittime_ms":
            data["scheduler_waittime_sec"][labels["group"]] += (value/1000)
        elif name == "scheduler_starvetime_ms":
            data["scheduler_starvetime_sec"][labels["group"]] += (value/1000)

        # scheduler-group -> count
        elif name == "scheduler_queue_length":
            data["scheduler_queue_length"][labels["group"]] += value
        elif name == "scheduler_tasks_processed":
            data["scheduler_tasks_processed_M"][labels["group"]] += (value/1000000)

        # tree-type -> depth
        elif name == "cache_tree_depth":
            data["tree_depth"][labels["tree"]] += value

        # src -> count
        elif name == "cache_cache_access":
            data["cache_access"][labels["src"]] += value
        elif name == "cache_cache_hit":
            data["cache_hit"][labels["src"]] += value
        elif name == "cache_trans_created":
            data["created_trans"][labels["src"]] += value
        elif name == "cache_trans_committed":
            assert(labels["src"] != "READ")
            data["committed_trans"][labels["src"]] += value
        elif name == "cache_trans_read_successful":
            data["committed_trans"]["READ"] += value

        # src -> tree-type -> count
        elif name == "cache_tree_erases_committed":
            assert(labels["src"] != "READ")
            if labels["src"] == "CLEANER":
                assert(labels["tree"] != "ONODE")
            data["tree_erases_committed"][labels["src"]][labels["tree"]] += value
        elif name == "cache_tree_erases_invalidated":
            assert(labels["src"] != "READ")
            if labels["src"] == "CLEANER":
                assert(labels["tree"] != "ONODE")
            data["tree_erases_invalidated"][labels["src"]][labels["tree"]] += value
        elif name == "cache_tree_inserts_committed":
            assert(labels["src"] != "READ")
            if labels["src"] == "CLEANER":
                assert(labels["tree"] != "ONODE")
            data["tree_inserts_committed"][labels["src"]][labels["tree"]] += value
        elif name == "cache_tree_inserts_invalidated":
            assert(labels["src"] != "READ")
            if labels["src"] == "CLEANER":
                assert(labels["tree"] != "ONODE")
            data["tree_inserts_invalidated"][labels["src"]][labels["tree"]] += value

        # src -> extent-type -> count
        elif name == "cache_trans_invalidated":
            data["invalidated_trans"][labels["src"]][labels["ext"]] += value

        # src -> effort-type -> blocks
        elif name == "cache_invalidated_extent_bytes":
            if labels["src"] == "READ":
                assert(labels["effort"] == "READ")
            data["invalidated_efforts_4KB"][labels["src"]][labels["effort"]] += (value/4096)
        elif name == "cache_invalidated_delta_bytes":
            assert(labels["src"] != "READ")
            data["invalidated_efforts_4KB"][labels["src"]]["MUTATE_DELTA"] += (value/4096)

        # src -> effort-type -> blocks
        # src -> extent-type -> effort-type -> blocks
        elif name == "cache_committed_extent_bytes":
            assert(labels["src"] != "READ")
            data["committed_efforts_4KB"][labels["src"]][labels["effort"]] += (value/4096)
            if labels["effort"] == "FRESH":
                data["committed_disk_efforts_4KB"][labels["src"]][labels["ext"]]["FRESH"] += (value/4096)
        elif name == "cache_committed_delta_bytes":
            assert(labels["src"] != "READ")
            data["committed_efforts_4KB"][labels["src"]]["MUTATE_DELTA"] += (value/4096)
            data["committed_disk_efforts_4KB"][labels["src"]][labels["ext"]]["MUTATE_DELTA"] += (value/4096)
        elif name == "cache_successful_read_extent_bytes":
            data["committed_efforts_4KB"]["READ"]["READ"] += (value/4096)

        # others
        elif name == "cache_committed_extents":
            if (labels["effort"] == "FRESH" and
                labels["ext"] == "OBJECT_DATA_BLOCK" and
                labels["src"] == "MUTATE"):
                data["object_data_writes"] += value
        else:
            ignored_metrics.add(name)

    return data, illegal_metrics, ignored_metrics

def prepare_raw_dataset():
    data = {}
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
    # ratio
    data["reactor_util"] = []
    # time
    data["reactor_busytime_sec"] = []
    data["reactor_stealtime_sec"] = []
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
    # src -> tree-type -> count
    data["tree_erases_committed"] = defaultdict(lambda: defaultdict(lambda: []))
    data["tree_erases_invalidated"] = defaultdict(lambda: defaultdict(lambda: []))
    data["tree_inserts_committed"] = defaultdict(lambda: defaultdict(lambda: []))
    data["tree_inserts_invalidated"] = defaultdict(lambda: defaultdict(lambda: []))
    # src -> extent-type -> count
    data["invalidated_trans"] = defaultdict(lambda: defaultdict(lambda: []))
    # src -> effort-type -> blocks
    data["invalidated_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: []))
    data["committed_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: []))
    # src -> extent-type -> effort-type -> blocks
    data["committed_disk_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: [])))
    return data

def append_raw_data(dataset, metrics_start, metrics_end):
    def get_diff(metric_name, dataset, metrics_start, metrics_end):
        value = metrics_end[metric_name] - metrics_start[metric_name]
        # the value can be negative for reactor_stealtime_sec
        dataset[metric_name].append(value)
    # blocks
    get_diff("segment_read_4KB",          dataset, metrics_start, metrics_end)
    get_diff("segment_write_4KB",         dataset, metrics_start, metrics_end)
    get_diff("segment_write_meta_4KB",    dataset, metrics_start, metrics_end)
    get_diff("reactor_aio_read_4KB",      dataset, metrics_start, metrics_end)
    get_diff("reactor_aio_write_4KB",     dataset, metrics_start, metrics_end)
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
    # time
    get_diff("reactor_busytime_sec",      dataset, metrics_start, metrics_end)
    get_diff("reactor_stealtime_sec",     dataset, metrics_start, metrics_end)

    # these are special: no diff
    dataset["cached_4KB"].append(metrics_end["cached_4KB"])
    dataset["dirty_4KB"].append(metrics_end["dirty_4KB"])
    for name, value in metrics_end["tree_depth"].items():
        dataset["tree_depth"][name].append(value)
    dataset["reactor_util"].append(metrics_end["reactor_util"])
    dataset["reactor_tasks_pending"].append(metrics_end["reactor_tasks_pending"])
    for name, value in metrics_end["scheduler_queue_length"].items():
        dataset["scheduler_queue_length"][name].append(value)
    dataset["memory_allocate_KB"].append(metrics_end["memory_allocate_KB"])
    dataset["memory_free_KB"].append(metrics_end["memory_free_KB"])
    dataset["memory_total_KB"].append(metrics_end["memory_total_KB"])
    dataset["memory_live_objs"].append(metrics_end["memory_live_objs"])

    # src -> count
    def get_diff_l1(metric_name, dataset, metrics_start, metrics_end):
        for name, value_end in metrics_end[metric_name].items():
            value_start = metrics_start[metric_name][name]
            value = value_end - value_start
            assert(value >= 0)
            dataset[metric_name][name].append(value)
    # src -> count
    get_diff_l1("cache_access",            dataset, metrics_start, metrics_end)
    get_diff_l1("cache_hit",               dataset, metrics_start, metrics_end)
    get_diff_l1("created_trans",           dataset, metrics_start, metrics_end)
    get_diff_l1("committed_trans",         dataset, metrics_start, metrics_end)
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
    get_diff_l2("tree_erases_invalidated",  dataset, metrics_start, metrics_end)
    get_diff_l2("tree_inserts_committed",   dataset, metrics_start, metrics_end)
    get_diff_l2("tree_inserts_invalidated", dataset, metrics_start, metrics_end)
    # src -> extent-type -> count
    get_diff_l2("invalidated_trans",        dataset, metrics_start, metrics_end)
    # src -> effort-type -> blocks
    get_diff_l2("invalidated_efforts_4KB",  dataset, metrics_start, metrics_end)
    get_diff_l2("committed_efforts_4KB",    dataset, metrics_start, metrics_end)

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

def wash_dataset(dataset, writes_4KB, times_sec):
    if len(times_sec) > 0:
        assert(len(writes_4KB) == 0)
        print("WARN: no bench file available, guess writes_4KB from committed OBJECT_DATA_BLOCK!")
        writes_by_mutate = dataset["committed_disk_efforts_4KB"]["MUTATE"]
        writes_by_odb = writes_by_mutate["OBJECT_DATA_BLOCK"]
        assert(all(blocks == 0 for blocks in writes_by_odb["MUTATE_DELTA"]))
        writes_4KB = writes_by_odb["FRESH"]
        assert(len(writes_4KB) == len(times_sec))

    INVALID_RATIO = -0.1
    dataset_size = len(writes_4KB)
    washed_dataset = {}

    # 1. from cached_4KB, dirty_4KB
    data_name = "cache_usage_MB"

    assert(len(dataset["cached_4KB"]) == dataset_size)
    assert(len(dataset["dirty_4KB"]) == dataset_size)

    def block_to_MB(items):
        return [item/256 for item in items]
    washed_dataset[data_name] = {
        "cached": block_to_MB(dataset["cached_4KB"]),
        "dirty": block_to_MB(dataset["dirty_4KB"])
    }

    # 2. from tree_depth
    data_name = "tree_depth"
    for name, values in dataset["tree_depth"].items():
        assert(len(values) == dataset_size)
    washed_dataset[data_name] = dataset["tree_depth"]

    # 3. from tree_erases_committed, tree_inserts_committed
    data_name = "tree_operations"

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

    washed_dataset[data_name] = {}
    for tree_type, values in tree_inserts_committed_by_tree.items():
        sub_name = tree_type + "_inserts"
        washed_dataset[data_name][sub_name] = values
    for tree_type, values in tree_erases_committed_by_tree.items():
        sub_name = tree_type + "_erases"
        washed_dataset[data_name][sub_name] = values

    # 4. from cache_hit, cache_access
    data_name = "cache_hit_access_ratio_by_src"

    def get_ratio(numerators, denominators):
        assert(len(numerators) == len(denominators))
        ratios = []
        for numerator, denominator in zip(numerators, denominators):
            ratio = INVALID_RATIO
            if denominator != 0:
                ratio = (numerator/denominator)
            else:
                if numerator != 0:
                    # special case
                    ratio = INVALID_RATIO - 0.1
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
    data_name = "trans_invalidate_committed_ratio_by_src---inaccurate"

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
        data_name = "trans_invalidate_committed_ratio_by_extent---" + src
        non_empty_invalidated_trans = filter_out_empty_l2(invalidated_trans_by_extent)
        if len(non_empty_invalidated_trans) == 0:
            print(data_name + " is emtpy!")
            continue
        washed_dataset[data_name] = get_ratio_l2_by_l1(
            non_empty_invalidated_trans, dataset["committed_trans"][src])

    # 7.x from invalidated_efforts_4KB, committed_efforts_4KB
    for src, committed_efforts_4KB in dataset["committed_efforts_4KB"].items():
        data_name = "trans_invalidate_committed_ratio_by_effort---accurate---" + src
        result_ratio = get_ratio_l2(dataset["invalidated_efforts_4KB"][src],
                                    committed_efforts_4KB,
                                    dataset_size)

        tree_erases_committed = dataset["tree_erases_committed"][src]
        tree_erases_invalidated = dataset["tree_erases_invalidated"][src]
        for tree_type, cmt_items in tree_erases_committed.items():
            sub_name = tree_type + "_ERASES"
            result_ratio[sub_name] = get_ratio(tree_erases_invalidated[tree_type], cmt_items)

        tree_inserts_committed = dataset["tree_inserts_committed"][src]
        tree_inserts_invalidated = dataset["tree_inserts_invalidated"][src]
        for tree_type, cmt_items in tree_inserts_committed.items():
            sub_name = tree_type + "_INSERTS"
            result_ratio[sub_name] = get_ratio(tree_inserts_invalidated[tree_type], cmt_items)

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

    extent_writes_list = []
    for src, committed_disk_efforts in dataset["committed_disk_efforts_4KB"].items():
        data_name = "write_amplification_by_extent---" + src

        inplace_merge_l2_from_l3("LADDR", "LADDR_LEAF", "LADDR_INTERNAL", committed_disk_efforts)
        inplace_merge_l2_from_l3("OMAP",  "OMAP_LEAF",  "OMAP_INNER",     committed_disk_efforts)

        non_empty_committed_disk_efforts = filter_out_empty_l2_from_l3(committed_disk_efforts)
        if len(non_empty_committed_disk_efforts) == 0:
            print(data_name + " is empty!")
            continue

        committed_disk_efforts_merged = {}
        for ext_name, items_by_effort in non_empty_committed_disk_efforts.items():
            assert(len(items_by_effort) == 2)
            disk_writes = merge_lists([items_by_effort["FRESH"],
                                       items_by_effort["MUTATE_DELTA"]])
            extent_writes_list.append(disk_writes)
            committed_disk_efforts_merged[ext_name] = disk_writes

        data = get_ratio_l2_by_l1(committed_disk_efforts_merged, writes_4KB)
        washed_dataset[data_name] = data

    # 9. from writes_4KB, committed_disk_efforts_4KB,
    #         segment_read_4KB, segment_write_4KB, segment_write_meta_4KB
    data_name = "write_amplification_overall"

    segment_read_amp = get_ratio(dataset["segment_read_4KB"], writes_4KB)
    segment_total_write_4KB = merge_lists([dataset["segment_write_4KB"],
                                           dataset["segment_write_meta_4KB"]])
    segment_write_amp = get_ratio(segment_total_write_4KB, writes_4KB)
    extent_writes_4KB = merge_lists(extent_writes_list)
    extent_level_amp = get_ratio(extent_writes_4KB, writes_4KB)

    washed_dataset[data_name] = {
        "FRESH_EXTENTS+DELTA": extent_level_amp,
        "SEGMENTED_WRITE": segment_write_amp,
        "SEGMENTED_READ": segment_read_amp
    }

    if len(times_sec) == 0:
        # indexes
        indexes = []
        current = writes_4KB[0]
        indexes.append(current)
        for data in writes_4KB[1:]:
            assert(data > 0)
            current += data
            indexes.append(current)
        return washed_dataset, indexes

    #
    # Metric-only specific graph
    #

    # 1. from writes_4KB
    washed_dataset["writes_accumulated_MB"] = {
        "obj_data(client)":  accumulate([write/256 for write in writes_4KB]),
        "reactor_aio_write": accumulate([write/256 for write in dataset["reactor_aio_write_4KB"]])
    }

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
    washed_dataset["throughput_MB"] = {
        "reactor_aio_read":  get_throughput_MB(dataset["reactor_aio_read_4KB"], times_sec),
        "reactor_aio_write": get_throughput_MB(dataset["reactor_aio_write_4KB"], times_sec),
        "device_read":       get_throughput_MB(dataset["segment_read_4KB"], times_sec),
        "device_write":      get_throughput_MB(segment_total_write_4KB, times_sec),
        "extent_write":      get_throughput_MB(extent_writes_4KB, times_sec),
        "obj_data_write":    get_throughput_MB(writes_4KB, times_sec)
    }

    # 4. from reactor_aio_reads, reactor_aio_writes,
    #         segment_writes, segment_meta_writes, segment_reads,
    #         object_data_writes
    segment_total_writes = merge_lists([dataset["segment_writes"],
                                        dataset["segment_meta_writes"]])
    def get_IOPS(rws, ts_sec):
        assert(len(rws) == len(ts_sec))
        return [rw/t for rw, t in zip(rws, ts_sec)]
    washed_dataset["IOPS"] = {
        "reactor_aio_read":  get_IOPS(dataset["reactor_aio_reads"], times_sec),
        "reactor_aio_write": get_IOPS(dataset["reactor_aio_writes"], times_sec),
        "device_read":       get_IOPS(dataset["segment_reads"], times_sec),
        "device_write":      get_IOPS(segment_total_writes, times_sec),
        "obj_data_write":    get_IOPS(dataset["object_data_writes"], times_sec)
    }

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
    def KB_to_MB(items):
        return [item/1024 for item in items]
    washed_dataset["memory_usage_MB"] = {
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
    indexes = []
    current = times_sec[0]
    indexes.append(current)
    for data in times_sec[1:]:
        assert(data > 0)
        current += data
        indexes.append(current)

    return washed_dataset, indexes

def relplot_data(directory, bench_type, name, data, indexes, ylim):
    sns.set_theme(style="whitegrid")
    to_draw = pd.DataFrame(data, index=indexes)
    assert(bench_type != BenchT.NULL)
    if bench_type == BenchT.METRICS:
        to_draw.index.name = "time_seconds"
    else:
        to_draw.index.name = "writes_4KB"
    g = sns.relplot(data=to_draw,
                    kind="line",
                    markers=True,
                   ).set(title=name, ylim=ylim)
    g.fig.set_size_inches(15,6)
    g.savefig("%s/%s.png" % (directory, name))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "-d", "--directory", type=str,
            help="result directory to evaluate", default="results")
    args = parser.parse_args()

    print("loading dir %s ..." % (args.directory))
    benches, metrics, times = load_dir(args.directory)
    print("loaded %d rounds" % (len(benches)))
    print()

    print("parse results ...")
    writes_4KB = []
    illegal_metrics = set()
    ignored_metrics = set()
    raw_dataset = prepare_raw_dataset()

    bench_type = BenchT.NULL
    if len(times):
        bench_type = BenchT.METRICS

    index = 0
    metric_file = metrics[index]
    metrics_start, illegal, ignored = parse_metric_file(metric_file)
    illegal_metrics |= illegal
    ignored_metrics |= ignored
    while index < (len(metrics) - 1):
        print(".", end="", flush=True)
        metric_file = metrics[index + 1]

        if bench_type != BenchT.METRICS:
            # mode with bench files
            bench_file = benches[index]
            write_4KB, btype = parse_bench_file(bench_file)
            if bench_type == BenchT.NULL:
                bench_type = btype
            else:
                assert(bench_type == btype)
            writes_4KB.append(write_4KB)

        metrics_end, illegal, ignored = parse_metric_file(metric_file)
        illegal_metrics |= illegal
        ignored_metrics |= ignored

        append_raw_data(raw_dataset, metrics_start, metrics_end)
        index += 1
        metrics_start = metrics_end
    print()
    print("   bench type: %s" % (bench_type))
    print("   illegal metrics: %s" % (illegal_metrics))
    print("   ignored metrics: %s" % (ignored_metrics))
    print("parse results done")
    print()

    print("wash results ...")
    dataset, indexes = wash_dataset(raw_dataset, writes_4KB, times)
    print("wash results done")
    print()

    print("generate figures ...")
    for name, data in dataset.items():
        print(".", end="", flush=True)
        ylim = None
        relplot_data(args.directory, bench_type, name, data, indexes, ylim)
    print()
    print("generate figures done")
