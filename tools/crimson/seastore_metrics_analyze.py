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
    metrics = []
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
            metrics.append((index, file_dir))

    benches.sort()
    metrics.sort()

    assert(len(metrics) > 1)
    if len(metrics) == len(benches):
        # no matching matric file to the last bench result
        benches.pop()
    assert(len(metrics) == len(benches) + 1)

    index = 0
    while index < len(benches):
        assert(metrics[index][0] == index)
        assert(benches[index][0] == index + 1)
        index += 1
    assert(metrics[index][0] == index)

    return [item[1] for item in benches], [item[1] for item in metrics]

def parse_bench_file(bench_file):
    writes = 0
    obj_size = 0
    with open(bench_file, 'r') as reader:
        for line in reader:
            if line.startswith("Total writes made:"):
                writes = int([x for x in line.split(' ') if x][3])
            elif line.startswith("Object size:"):
                obj_size = int([x for x in line.split(' ') if x][2])
            elif line.startswith("     issued"):
                writes = int([x for x in line.split(',') if x][1])
            elif line.startswith("rbd_iodepth32") and line.find('rw=') >= 0:
                obj_size = int([x for x in line.split(',') if x][2].split('-')[1][:-1])

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
    # blocks
    def get_diff(metric_name, dataset, metrics_start, metrics_end):
        value = metrics_end[metric_name] - metrics_start[metric_name]
        assert(value >= 0)
        dataset[metric_name].append(value)
    get_diff("segment_read_4KB",       dataset, metrics_start, metrics_end)
    get_diff("segment_write_4KB",      dataset, metrics_start, metrics_end)
    get_diff("segment_write_meta_4KB", dataset, metrics_start, metrics_end)

    # these are special: no diff
    dataset["cached_4KB"].append(metrics_end["cached_4KB"])
    dataset["dirty_4KB"].append(metrics_end["dirty_4KB"])
    for name, value in metrics_end["tree_depth"].items():
        dataset["tree_depth"][name].append(value)

    # src -> count
    def get_diff_l1(metric_name, dataset, metrics_start, metrics_end):
        for name, value_end in metrics_end[metric_name].items():
            value_start = metrics_start[metric_name][name]
            value = value_end - value_start
            assert(value >= 0)
            dataset[metric_name][name].append(value)
    get_diff_l1("cache_access",            dataset, metrics_start, metrics_end)
    get_diff_l1("cache_hit",               dataset, metrics_start, metrics_end)
    get_diff_l1("created_trans",           dataset, metrics_start, metrics_end)
    get_diff_l1("committed_trans",         dataset, metrics_start, metrics_end)

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

def wash_dataset(dataset, writes_4KB):
    INVALID_RATIO = -0.1
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
                assert(numerator == 0)
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
        for created, invalidated, committed in zip(created_list,
                                                   invalidated_trans_by_src[src_name],
                                                   dataset["committed_trans"][src_name]):
            index += 1
            if (created != invalidated + committed):
                print("WARN: extra created transactions %d -- %s at round %d"
                      % (created - invalidated - committed, src_name, index))

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

    write_amplification_dataset = []
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
            committed_disk_efforts_merged[ext_name] = disk_writes

        data = get_ratio_l2_by_l1(committed_disk_efforts_merged, writes_4KB)
        washed_dataset[data_name] = data
        write_amplification_dataset.append(data)

    # 9. from writes_4KB, committed_disk_efforts_4KB,
    #         segment_read_4KB, segment_write_4KB, segment_write_meta_4KB
    data_name = "write_amplification_overall"

    segment_read_amp = get_ratio(dataset["segment_read_4KB"], writes_4KB)

    segment_write_4KB = merge_lists([dataset["segment_write_4KB"],
                                     dataset["segment_write_meta_4KB"]])
    segment_write_amp = get_ratio(segment_write_4KB, writes_4KB)

    ratio_list_to_merge = []
    for data in write_amplification_dataset:
        for ext_name, ext_ratios in data.items():
            ratio_list_to_merge.append(ext_ratios)
    extent_level_amp = merge_lists(ratio_list_to_merge)
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
    benches, metrics = load_dir(args.directory)
    print("loaded %d rounds" % (len(benches)))
    print()

    print("parse results ...")
    writes_4KB = []
    illegal_metrics = set()
    ignored_metrics = set()
    raw_dataset = prepare_raw_dataset()

    index = 0
    metric_file = metrics[index]
    metrics_start, illegal, ignored = parse_metric_file(metric_file)
    illegal_metrics |= illegal
    ignored_metrics |= ignored
    while index < len(benches):
        print(".", end="", flush=True)
        bench_file = benches[index]
        metric_file = metrics[index + 1]

        write_4KB = parse_bench_file(bench_file)
        metrics_end, illegal, ignored = parse_metric_file(metric_file)
        illegal_metrics |= illegal
        ignored_metrics |= ignored

        append_raw_data(raw_dataset, metrics_start, metrics_end)
        writes_4KB.append(write_4KB)
        index += 1
        metrics_start = metrics_end
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
        relplot_data(args.directory, name, data, indexes, ylim)
    print()
    print("generate figures done")
