#!/usr/bin/env python3

from collections import defaultdict
import json
import os
from os import path
import pandas as pd
import seaborn as sns

def load_dir(dir_name):
    load_folder = path.join(os.getcwd(), dir_name)
    benches = []
    metrics_start = []
    metrics_end = []
    for file_name in os.listdir(load_folder):
        if not file_name.endswith(".log"):
            continue
        names = file_name.split("_")
        index = int(names[1])
        file_type = names[2]
        file_dir = path.join(load_folder, file_name)

        if file_type.startswith("bench"):
            benches.append((index, file_dir))
        else:
            assert(file_type.startswith("metrics"))
            if names[3].startswith("start"):
                metrics_start.append((index, file_dir))
            else:
                assert(names[3].startswith("end"))
                metrics_end.append((index, file_dir))
    benches.sort()
    metrics_start.sort()
    metrics_end.sort()
    return [item[1] for item in benches], [item[1] for item in metrics_start], [item[1] for item in metrics_end]

def parse_bench_file(bench_file):
    writes = 0
    obj_size = 0
    with open(bench_file, 'r') as reader:
        for line in reader:
            if line.startswith("Total writes made:"):
                writes = int([x for x in line.split(' ') if x][3])
            elif line.startswith("Object size:"):
                obj_size = int([x for x in line.split(' ') if x][2])
    assert(writes)
    assert(obj_size)
    return (writes * obj_size / 4096)

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
            if isinstance(v, int):
                value = v
        else:
            assert(isinstance(v, str))
            labels[k] = v
    return name, labels, value

def parse_metric_file(metric_file):
    data = {}
    # blocks
    data["segment_read_4KB"] = 0
    data["segment_write_4KB"] = 0
    data["segment_write_meta_4KB"] = 0
    data["cached_4KB"] = 0
    data["dirty_4KB"] = 0
    # src -> count
    data["cache_access"] = defaultdict(lambda: 0)
    data["cache_hit"] = defaultdict(lambda: 0)
    data["created_trans"] = defaultdict(lambda: 0)
    data["committed_trans"] = defaultdict(lambda: 0)
    data["invalidated_trans"] = defaultdict(lambda: 0)
    # extent-type -> count
    data["invalidated_reason"] = defaultdict(lambda: 0)
    # effort-type -> blocks
    data["invalidated_efforts_4KB"] = defaultdict(lambda: 0)
    data["committed_efforts_4KB"] = defaultdict(lambda: 0)
    # extent-type -> effort-type -> blocks
    data["committed_disk_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: 0))

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

        # src -> count
        # extent-type -> count
        elif name == "cache_trans_invalidated":
            data["invalidated_trans"][labels["src"]] += value
            data["invalidated_reason"][labels["ext"]] += value

        # effort-type -> blocks
        elif name == "cache_invalidated_extent_bytes":
            if labels["src"] == "READ":
                assert(labels["effort"] == "READ")
                data["invalidated_efforts_4KB"]["READ_ONLY"] += (value/4096)
            else:
                data["invalidated_efforts_4KB"][labels["effort"]] += (value/4096) 
        elif name == "cache_invalidated_delta_bytes":
            assert(labels["src"] != "READ")
            data["invalidated_efforts_4KB"]["MUTATE_DELTA"] += (value/4096)

        # effort-type -> blocks
        # extent-type -> effort-type -> blocks
        elif name == "cache_committed_extent_bytes":
            assert(labels["src"] != "READ")
            data["committed_efforts_4KB"][labels["effort"]] += (value/4096)
            if labels["effort"] == "FRESH":
                data["committed_disk_efforts_4KB"][labels["ext"]]["FRESH"] += (value/4096)
        elif name == "cache_committed_delta_bytes":
            assert(labels["src"] != "READ")
            data["committed_efforts_4KB"]["MUTATE_DELTA"] += (value/4096)
            data["committed_disk_efforts_4KB"][labels["ext"]]["MUTATE_DELTA"] += (value/4096)
        elif name == "cache_successful_read_extent_bytes":
            data["committed_efforts_4KB"]["READ_ONLY"] += (value/4096)

        # others
        else:
            ignored_metrics.add(name)

    return data, illegal_metrics, ignored_metrics;

def prepare_raw_dataset():
    data = {}
    # blocks
    data["segment_read_4KB"] = []
    data["segment_write_4KB"] = []
    data["segment_write_meta_4KB"] = []
    data["cached_4KB"] = []
    data["dirty_4KB"] = []
    # src -> count
    data["cache_access"] = defaultdict(lambda: [])
    data["cache_hit"] = defaultdict(lambda: [])
    data["created_trans"] = defaultdict(lambda: [])
    data["committed_trans"] = defaultdict(lambda: [])
    data["invalidated_trans"] = defaultdict(lambda: [])
    # extent-type -> count
    data["invalidated_reason"] = defaultdict(lambda: [])
    # effort-type -> blocks
    data["invalidated_efforts_4KB"] = defaultdict(lambda: [])
    data["committed_efforts_4KB"] = defaultdict(lambda: [])
    # extent-type -> effort-type -> blocks
    data["committed_disk_efforts_4KB"] = defaultdict(lambda: defaultdict(lambda: []))
    return data

def append_raw_data(dataset, metrics_start, metrics_end):
    # blocks
    def get_diff_l1(metric_name, dataset, metrics_start, metrics_end):
        value = metrics_end[metric_name] - metrics_start[metric_name]
        assert(value >= 0)
        dataset[metric_name].append(value)
    get_diff_l1("segment_read_4KB",       dataset, metrics_start, metrics_end)
    get_diff_l1("segment_write_4KB",      dataset, metrics_start, metrics_end)
    get_diff_l1("segment_write_meta_4KB", dataset, metrics_start, metrics_end)

    # these are special: no diff
    dataset["cached_4KB"].append(metrics_end["cached_4KB"])
    dataset["dirty_4KB"].append(metrics_end["dirty_4KB"])

    # src -> count
    def get_diff_l2(metric_name, dataset, metrics_start, metrics_end):
        for l1_name, value_end in metrics_end[metric_name].items():
            value_start = metrics_start[metric_name][l1_name]
            value = value_end - value_start
            assert(value >= 0)
            dataset[metric_name][l1_name].append(value)
    get_diff_l2("cache_access",            dataset, metrics_start, metrics_end)
    get_diff_l2("cache_hit",               dataset, metrics_start, metrics_end)
    get_diff_l2("created_trans",           dataset, metrics_start, metrics_end)
    get_diff_l2("committed_trans",         dataset, metrics_start, metrics_end)
    get_diff_l2("invalidated_trans",       dataset, metrics_start, metrics_end)

    # extent-type -> count
    get_diff_l2("invalidated_reason",      dataset, metrics_start, metrics_end)

    # effort-type -> blocks
    get_diff_l2("invalidated_efforts_4KB", dataset, metrics_start, metrics_end)
    get_diff_l2("committed_efforts_4KB",   dataset, metrics_start, metrics_end)

    # extent-type -> effort-type -> blocks
    def get_diff_l3(metric_name, dataset, metrics_start, metrics_end):
        for l1_name, l1_items_end in metrics_end[metric_name].items():
            for l2_name, value_end in l1_items_end.items():
                value_start = metrics_start[metric_name][l1_name][l2_name]
                value = value_end - value_start
                assert(value >= 0)
                dataset[metric_name][l1_name][l2_name].append(value)
    get_diff_l3("committed_disk_efforts_4KB", dataset, metrics_start, metrics_end)

def wash_dataset(dataset, writes_4KB):
    dataset_size = len(writes_4KB)
    washed_dataset = {}

    # 1. from cached_4KB, dirty_4KB
    data_name = "cache_usage"

    assert(len(dataset["cached_4KB"]) == dataset_size)
    assert(len(dataset["dirty_4KB"]) == dataset_size)

    washed_dataset[data_name] = {
        "cached_4KB": dataset["cached_4KB"],
        "dirty_4KB": dataset["dirty_4KB"]
    }

    # 2. from cache_hit, cache_access
    data_name = "cache_hit_access_ratio_by_src"

    def get_ratio(numerators, denominators):
        assert(len(numerators) == len(denominators))
        ratios = []
        for numerator, denominator in zip(numerators, denominators):
            ratio = -0.1
            if denominator != 0:
                ratio = (numerator/denominator)
            else:
                assert(numerator == 0)
            ratios.append(ratio)
        return ratios
    def assign_ratio_l2(l2_out, l2_numerators, l2_denominators, expected_size):
        for name, denominators in l2_denominators.items():
            numerators = l2_numerators[name]
            ratios = get_ratio(numerators, denominators)
            assert(len(ratios) == expected_size)
            l2_out[name] = ratios

    washed_dataset[data_name] = {}
    assign_ratio_l2(washed_dataset[data_name],
                    dataset["cache_hit"],
                    dataset["cache_access"],
                    dataset_size)

    # 3. from invalidated_trans, committed_trans
    data_name = "trans_invalidate_committed_ratio_by_src---inaccurate"

    for src_name, created_list in dataset["created_trans"].items():
        for created, invalidated, committed in zip(created_list,
                                                   dataset["invalidated_trans"][src_name],
                                                   dataset["committed_trans"][src_name]):
            assert(created == invalidated + committed)

    washed_dataset[data_name] = {}
    assign_ratio_l2(washed_dataset[data_name],
                    dataset["invalidated_trans"],
                    dataset["committed_trans"],
                    dataset_size)

    # 4. from invalidated_reason, committed_trans
    data_name = "trans_invalidate_committed_ratio_by_extent"

    def merge_lists(lists):
        assert(len(lists))
        length = len(lists[0])
        for _list in lists:
            assert(length == len(_list))
        return [sum(values) for values in zip(*lists)]
    def inplace_merge_l2(to_metric, from_metric1, from_metric2, l2_items):
        from_items1 = l2_items[from_metric1]
        from_items2 = l2_items[from_metric2]
        to_items = merge_lists([from_items1, from_items2])
        del l2_items[from_metric1]
        del l2_items[from_metric2]
        l2_items[to_metric] = to_items
    inplace_merge_l2("LADDR", "LADDR_LEAF", "LADDR_INTERNAL", dataset["invalidated_reason"])
    inplace_merge_l2("OMAP",  "OMAP_LEAF",  "OMAP_INNER",     dataset["invalidated_reason"])

    merged_committed_trans = merge_lists([items for name, items in dataset["committed_trans"].items()])

    def filter_out_empty_l2(l2_items):
        return {name:items
                for name, items in l2_items.items()
                if any(items)}
    non_empty_invalidated_reasons = filter_out_empty_l2(dataset["invalidated_reason"])

    def assign_ratio_l2_l1(l2_numerators, denominators):
        ret = {}
        for name, numerators in l2_numerators.items():
            ratios = get_ratio(numerators, denominators)
            ret[name] = ratios
        return ret
    washed_dataset[data_name] = assign_ratio_l2_l1(
            non_empty_invalidated_reasons, merged_committed_trans)

    # 5. from invalidated_efforts_4KB, committed_efforts_4KB
    data_name = "trans_invalidate_committed_ratio_by_effort---accurate"

    washed_dataset[data_name] = {}
    assign_ratio_l2(washed_dataset[data_name],
                    dataset["invalidated_efforts_4KB"],
                    dataset["committed_efforts_4KB"],
                    dataset_size)

    # 6. from writes_4KB, committed_disk_efforts_4KB
    data_name = "write_amplification_by_extent"

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
    inplace_merge_l2_from_l3("LADDR", "LADDR_LEAF", "LADDR_INTERNAL", dataset["committed_disk_efforts_4KB"])
    inplace_merge_l2_from_l3("OMAP",  "OMAP_LEAF",  "OMAP_INNER",     dataset["committed_disk_efforts_4KB"])

    def filter_out_empty_l2_from_l3(l3_items):
        return {l2_name:l2_items
                for l2_name, l2_items in l3_items.items()
                if any([any(items) for name, items in l2_items.items()])}
    committed_disk_efforts_4KB = filter_out_empty_l2_from_l3(dataset["committed_disk_efforts_4KB"])

    committed_disk_efforts_4KB_merged = {}
    for ext_name, items_by_effort in committed_disk_efforts_4KB.items():
        assert(len(items_by_effort) == 2)
        disk_writes = merge_lists([items_by_effort["FRESH"],
                                   items_by_effort["MUTATE_DELTA"]])
        committed_disk_efforts_4KB_merged[ext_name] = disk_writes

    washed_dataset[data_name] = assign_ratio_l2_l1(
            committed_disk_efforts_4KB_merged, writes_4KB)

    # 7. from writes_4KB, committed_disk_efforts_4KB,
    #         segment_read_4KB, segment_write_4KB, segment_write_meta_4KB
    data_name = "write_amplification_overall"

    segment_read_amp = get_ratio(dataset["segment_read_4KB"], writes_4KB)

    segment_write_4KB = merge_lists([dataset["segment_write_4KB"],
                                     dataset["segment_write_meta_4KB"]])
    segment_write_amp = get_ratio(segment_write_4KB, writes_4KB)

    extent_level_amp = merge_lists([ext_ratios for ext_name, ext_ratios
                                    in washed_dataset["write_amplification_by_extent"].items()])
    assert(len(extent_level_amp) == dataset_size)

    washed_dataset[data_name] = {
        "FRESH_EXTENTS+DELTA": extent_level_amp,
        "SEGMENTED_WRITE": segment_write_amp,
        "SEGMENTED_READ": segment_read_amp
    }

    # indexes
    indexes = []
    current = 0
    for write in writes_4KB:
        current += write
        indexes.append(current)

    return washed_dataset, indexes

def relplot_data(directory, name, data, indexes, ylim):
    sns.set_theme(style="whitegrid")
    to_draw = pd.DataFrame(data, index=indexes)
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
    benches, metrics_start, metrics_end = load_dir(args.directory)
    assert(len(benches) == len(metrics_start))
    assert(len(metrics_start) == len(metrics_end))
    print("loaded %d rounds" % (len(benches)))
    print()

    print("parse results ...")
    writes_4KB = []
    illegal_metrics = set()
    ignored_metrics = set()
    raw_dataset = prepare_raw_dataset()
    for bench_file, metric_start_file, metric_end_file in zip(benches, metrics_start, metrics_end):
        print(".", end="", flush=True)
        write_4KB = parse_bench_file(bench_file)

        metrics_start, illegal, ignored = parse_metric_file(metric_start_file)
        illegal_metrics |= illegal
        ignored_metrics |= ignored
        metrics_end, illegal, ignored = parse_metric_file(metric_end_file)
        illegal_metrics |= illegal
        ignored_metrics |= ignored

        append_raw_data(raw_dataset, metrics_start, metrics_end)
        writes_4KB.append(write_4KB)
    print()
    print("   illegal metrics: %s" % (illegal_metrics))
    print("   ignored metrics: %s" % (ignored_metrics))
    print("parse results done")
    print()

    print("wash results ...")
    dataset, indexes = wash_dataset(raw_dataset, writes_4KB)
    print("wash results done")
    print()

    print("generate figures ...")
    for name, data in dataset.items():
        print(".", end="", flush=True)
        ylim = None
        if name == "cache_hit_access_ratio_by_src":
            ylim = (0.93, 1.0)
        relplot_data(args.directory, name, data, indexes, ylim)
    print()
    print("generate figures done")
