"""
Refactoring of the standalone tool fio-parse-jsons.py into modules that can be reused.
"""

import os
import math
import logging
import re
import json
import functools
from operator import add
from abc import ABC, abstractmethod

__author__ = "Jose J Palacios-Perez"
logger = logging.getLogger(__name__)


# These generic functions do not need to be part of a class
# Might be better placed on an common/utils module?
def combined_mean(a, b):
    """
    Calculates the combined mean of two groups:
    X_c = frac{ n_1*mean(X_1)+n_2*mean(X_2)) }{ (n_1+n_2) }
    FIO already provides the (mean,stdev) of completion latency per sample
    Expects two tuples: (mx_1, n_1) and (mx_2,n_2), and returns a tuple.
    """
    mx_1, n_1 = a
    mx_2, n_2 = b
    n_c = n_1 + n_2
    return ((n_1 * mx_1 + n_2 * mx_2) / n_c, n_c)


def combined_std_dev(a, b):
    """
    Calculats the combined std dev, normally for the completion latency
    Expects a,b to be tuples (s_i,x_i) std dev and mean, respectively,
    and returns a tuple.
    """
    y_1, n_1 = a
    y_2, n_2 = b
    s_1, mx_1 = y_1
    s_2, mx_2 = y_2
    mx_c, _nc = combined_mean((mx_1, n_1), (mx_2, n_2))
    v_1 = s_1 * s_1
    v_2 = s_2 * s_2
    q_1 = (n_1 - 1.0) * v_1 + n_1 * (mx_1 * mx_1)
    q_2 = (n_2 - 1.0) * v_2 + n_2 * (mx_2 * mx_2)
    q_c = q_1 + q_2
    n_c = n_1 + n_2
    return ((math.sqrt((q_c - n_c * mx_c * mx_c) / (n_c - 1.0)), mx_c), n_c)


class JsonXtractor(ABC):
    # This is a generic JSON xtractor used by several modules and scripts
    # each subclass MUST provide its own dictionary for paths and leaf-node
    # accessor

    def __init__(self):
        """
        Constructor: should really check whether the instance has provided this attribute
        """
        # self.predef_dict = path_dict
        pass

    def filter_json_node(self, next_branch, jnode_list_in):
        """
        Traverse the JSON jnode_list_in according to the next_branch:
        jnode_list_in: [dict]
        Assumption: json output of non-leaf nodes consists of either
        - dictionary - key field selects sub-value
        - sequence - key field syntax is name=value, where
                  name is a dictionary key of sequence elements, and
                  value is the desired value to select a sequence element
        """
        next_node_list = []
        # Nothing to do if the input next_branch is empty
        if not next_branch:
            return next_node_list
        for n in jnode_list_in:
            dotlist = next_branch.split("=")
            if len(dotlist) > 2:
                print(f"unrecognized syntax at {next_branch}")
                # logger.debug(f"unrecognized syntax at {next_branch}")
                return []
            if len(dotlist) == 1:
                assert isinstance(n, dict)
                next_node_list.append(n[next_branch])
            else:  # must be a sequence, take any element with key matching value
                select_key = dotlist[0]
                select_value = dotlist[1]
                assert isinstance(n, list)
                for e in n:
                    # n is a list
                    # print 'select with key %s value %s sequence
                    # element %s'%(select_key, select_value, e)
                    if select_value == "*":
                        next_node_list.append(e)
                    else:
                        v = e[select_key]
                        if v == select_value:
                            next_node_list.append(e)
                            # print('selecting: %s'%str(e))
                if len(next_node_list) == 0:
                    print(f"{select_key}={select_value} not found")
                    # logger.debug(f"{select_key}={select_value} not found")
                    return []
        return next_node_list

    def load_json_file(self, json_file):
        """
        Generic wrapper for json.load()
        Returns a dictionary
        """
        node = {}
        with open(json_file, "r") as json_data:
            # check for empty file
            f_info = os.fstat(json_data.fileno())
            if f_info.st_size == 0:
                print(f"JSON input file {json_file} is empty")
            else:
                # parse the JSON object
                node = json.load(json_data)
            return node

    @abstractmethod
    def process_leaf_item(self, key, next_node_list):
        """
        Abstract method that the subclass MUST implement
        """
        pass

    @abstractmethod
    def apply_reductor(self, result_dict, metric):
        """
        Applies the particular reduction to the list of values.
        Returns a value (scalar numeric)
        """
        pass


class JsonFioXtractor(JsonXtractor):
    """
    Subclass for FIO .json output
    This is normally to produce a single TestRunResult
    The structure is <jobtype> : {dict of paths to .json fields}
    """

    # This dict specifies the paths that the key is associated with:
    # 'jobs/jobname=*/read/iops'
    predef_dict = {
        "randwrite": {
            "iops": "write/iops",
            "total_ios": "write/total_ios",
            "clat_ms": "write/clat_ns",
            "clat_stdev": "write/clat_ns",
            "usr_cpu": "usr_cpu",
            "sys_cpu": "sys_cpu",
        },
        "randread": {
            "iops": "read/iops",
            "total_ios": "read/total_ios",
            "clat_ms": "read/clat_ns",
            "clat_stdev": "read/clat_ns",
            "usr_cpu": "usr_cpu",
            "sys_cpu": "sys_cpu",
        },
        "write": {  # aka seqwrite
            "bw": "write/bw",
            "total_ios": "write/total_ios",
            "clat_ms": "write/clat_ns",
            "clat_stdev": "write/clat_ns",
            "usr_cpu": "usr_cpu",
            "sys_cpu": "sys_cpu",
        },
        "read": {  # seqread
            "bw": "read/bw",
            "total_ios": "read/total_ios",
            "clat_ms": "read/clat_ns",
            "clat_stdev": "read/clat_ns",
            "usr_cpu": "usr_cpu",
            "sys_cpu": "sys_cpu",
        },
    }

    def __init__(self):
        """
        Constructor: use the class attribute predef_dict to specify the paths
        """
        # self.pathd = predef_dict
        pass

    def process_leaf_item(self, k, next_node_list):
        """
        The subclass must provide this method to process leaf node items.
        Dict of results: shall we use a generic class to encapsulate a TestRunResult?
        file: { /path/: value, ...}
        For default (empty paths) queries:
        file: { /workload-type/: wrte: iops, latency_ms: (sort list by value, get top) }
        To coalesce the results of several files:
        use the timestamp to groups json files -- pending
        For IOPs or BW: sum the values together from all the json files for the
        same timestamp
        For latency: multiply this value by IOPs, sum these values from all the
        json files for the same timestamp and then divide by total IOPs to get
        an average latency
        """
        #    match k: # Python version on the SV1 node does not support 'match'
        #    case 'iops' | 'usr_cpu' | 'sys_cpu':
        # For consistency, these are normally the "columns" for the TestRunResult table
        # So we might enfoce its type...
        if re.search("iops|usr_cpu|sys_cpu|iodepth|total_ios", k):
            return next_node_list[0]
        if k == "bw":
            return next_node_list[0] / 1000
        if k == "latency_ms":
            #    case 'latency_ms':
            unsorted_dict = next_node_list[0]
            sorted_dict = dict(
                sorted(unsorted_dict.items(), key=lambda x: x[1], reverse=True)
            )
            firstk = list(sorted_dict.keys())[0]
            return firstk
        if k == "clat_ms":
            #    case 'clat_ns':
            unsorted_dict = next_node_list[0]
            clat_ms = unsorted_dict["mean"] / 1e6
            return clat_ms
        if k == "clat_stdev":
            #    case 'clat_ns':
            unsorted_dict = next_node_list[0]
            clat_stdev = unsorted_dict["stddev"] / 1e6
            return clat_stdev

    def apply_reductor(self, result_dict, metric):
        """
        Applies the particular reduction to the list of values.
        Returns a value (scalar numeric)
        """
        if re.search("iops|usr_cpu|sys_cpu|bw|total_ios", metric):
            return functools.reduce(add, result_dict[metric])
        if metric == "clat_ms":
            z = zip(result_dict["clat_ms"], result_dict["total_ios"])
            mx, _ = functools.reduce(lambda x, y: combined_mean(x, y), z)
            return mx
        if metric == "clat_stdev":
            z = zip(result_dict["clat_stdev"], result_dict["clat_ms"])
            zz = zip(z, result_dict["total_ios"])
            zc, _ = functools.reduce(lambda x, y: combined_std_dev(x, y), zz)
            sc, _ = zc
            return sc

    def reduce_result_list(self, result_dict, jobname):
        """
        Applies a reduction to each of the lists of the result_dict:
        - IOPS/BW is the cummulative (sum)
        - avg (completion) latency is the combined avg
        - clat std dev is the combined std dev -- for the last two, we need
        the number of samples from FIO, which is "total_ios"
        """
        _res = {}
        for metric in self.predef_dict[jobname].keys():
            _res[metric] = self.apply_reductor(result_dict, metric)
        return _res

    def process_fio_json_file(self, json_file, json_tree_path):
        """
        Collect metrics from an individual JSON file, which might
        contain several entries, one per job
        Returns a dictionary with the filtered data as specified by predef_dict
        """
        node = self.load_json_file(json_file)
        result_dict = {}
        # Extract the json timestamp: useful for matching same workloads from
        # different FIO processes
        result_dict["timestamp"] = str(node["timestamp"])
        result_dict["iodepth"] = node["global options"]["iodepth"]
        result_dict["jobname"] = node["global options"]["rw"]
        # Use the jobname to index the predef_dict for the json query
        jobs_list = node["jobs"]
        print(f"Num jobs: {len(jobs_list)}")
        job_result = {}
        for _i, job in enumerate(jobs_list):
            jobname = result_dict["jobname"]
            query_dict = self.predef_dict[jobname]
            # These keys are metrics (columns of the TestRunTable) -- they should
            # have a fixed key order
            for k in query_dict.keys():
                json_tree_path = query_dict[k].split("/")
                next_node_list = [job]

                for next_branch in json_tree_path:
                    next_node_list = self.filter_json_node(next_branch, next_node_list)
                item = self.process_leaf_item(k, next_node_list)
                if k not in job_result:
                    job_result[k] = []
                job_result[k].append(item)

        reduced = self.reduce_result_list(job_result, result_dict["jobname"])
        merged = {**result_dict, **reduced}
        return merged


class JsonFioSetXtractor(JsonXtractor):
    """
    Subclass to compare against several TestRunResult tables
    The input list refers to TestRunResults archives or subdirectories, which have been produced
    by the above sibling class.
    The TestRunResult .json table is a flat dict: so we don't really need to generic xtractor above ...
    We only probably require that the order of the keys (measurements names, or columns in the tables) are
    fixed.
    """

    predef_dict = {
        "randwrite": {
            "iops": "iops",
            "total_ios": "total_ios",
            "clat_ms": "clat_ns",
            "clat_stdev": "clat_stdev",
            "usr_cpu": "usr_cpu",
            "sys_cpu": "sys_cpu",
            # aggregated CPU.MEM, etc
        },
        "randread": {
            "iops": "iops",
            "total_ios": "total_ios",
            "clat_ms": "clat_ns",
            "clat_stdev": "clat_stdev",
            "usr_cpu": "usr_cpu",
            "sys_cpu": "sys_cpu",
        },
        "write": {  # aka seqwrite
            "bw": "bw",
            "total_ios": "total_ios",
            "clat_ms": "clat_ns",
            "clat_stdev": "clat_stdev",
            "usr_cpu": "usr_cpu",
            "sys_cpu": "sys_cpu",
        },
        "read": {  # seqread
            "bw": "bw",
            "total_ios": "total_ios",
            "clat_stdev": "clat_stdev",
            "usr_cpu": "usr_cpu",
            "sys_cpu": "sys_cpu",
        },
    }

    def __init__(self):
        """
        Constructor: use the class attribute predef_dict to specify the paths
        """
        # self.pathd = predef_dict
        pass
