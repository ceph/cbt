#!/usr/bin/python
"""
This script extracts average utilisation (CPU and MEM) from a list of process id (PIDs) from the .json output
from the Linux command top. Produces a gnuplot script and corresponding .dat

Arguments are:
- (input) a _top.json file name, 
- (input) a _pid.json file name,
- (input/output) and a _cpu_avg.json file name, average over a range (typically for Response latency curves).

Example of usage:
    cat ${TEST_RESULT}_top.out | jc --top --pretty > ${TEST_RESULT}_top.json
    python3 /root/bin/parse-top.py --config=${TEST_RESULT}_top.json --cpu="${OSD_CORES}" --avg=${OSD_CPU_AVG} \
          --pids=${TOP_PID_JSON} 2>&1 > /dev/null

"""

import argparse
import logging
import os
import sys
import re
import json
import tempfile
from pprint import pformat

from gnuplot_plate import GnuplotTemplate

__author__ = "Jose J Palacios-Perez"

logger = logging.getLogger(__name__)


def serialize_sets(obj):
    """
    Serialise sets as lists
    """
    if isinstance(obj, set):
        return list(obj)

    return obj


DEFAULT_NUM_SAMPLES = 30


class TopEntry(object):
    """
    Filter the .json to a dictionary with keys the threads command names, and values
    array of avg measurement samples (normally 30),
    produce as output a gnuplot (script an d.dat files) and .json

    The following is an example of the structure of the input .json:

    cat _top.out | python3 ~/Work/cephdev/jc/jc --top --pretty > _top.json
    [
    {
    "time": "23:47:35",
    "uptime": 211235,
    "users": 0,
    "load_1m": 6.06,
    "load_5m": 7.09,
    "load_15m": 8.13,
    "mem_total": 385053.0,
    "mem_free": 340771.4,
    "mem_used": 38974.0,
    "mem_buff_cache": 7839.5,
    "swap_total": 0.0,
    "swap_free": 0.0,
    "swap_used": 0.0,
    "mem_available": 346079.1,
    "processes": [
      {
        "parent_pid": 1073313,
        "pid": 1073378,
        "last_used_processor": 41,
        "priority": 20,
        "nice": 0,
        "virtual_mem": 16.0,
        "resident_mem": 3.4,
        "shared_mem": 54656.0,
        "status": "sleeping",
        "percent_cpu": 25.0,
        "percent_mem": 0.9,
        "time_hundredths": "0:25.50",
        "command": "reactor-1"
      },

    "pid" and "parent_pid" are used to filter those processes specified in the _pid.json
    We are only interested in the following measurements:
    "percent_cpu" "percent_mem"
    """

    # Define some regex for threads that can be agglutinated
    # "control" dict, the proc_groups should be containing the _data to plot
    # These are intended for Ceph and Crimson OSD, so you might need to extend for your own
    # needs.
    PROC_INFO = {
        "OSD": {
            "tname": re.compile(
                r"^(crimson-osd|alien-store-tp|reactor|bstore|log|cfin|rocksdb|syscall-0).*$"
                # re.DEBUG,
            ),
            "regex": {
                "reactor": re.compile(r"reactor-\d+"),
            },
            "pids": set([]),
            "threads": {},
            "sorted": {},
            "num_samples": 0,
        },
        "FIO": {
            "tname": re.compile(
                r"^(fio|msgr-worker|io_context_pool|log|ceph_timer|safe_timer|taskfin_librbd|ms_dispatch).*$"
            ),
            "regex": {
                "msgr-worker": re.compile(r"msgr-worker-\d+"),
            },
            "pids": set([]),
            "threads": {},
            "sorted": {},
            "num_samples": 0,
        },
    }
    METRICS = ["cpu", "mem"]
    CPU_RANGE = {
        "regex": re.compile(r"^(\d+)-(\d+)$"),
        "min": 0,
        "max": 0,
    }

    def __init__(self, options):
        """
        This class expects the required options
        Filters the .json into a dict: keys are thread names (commands) and values are arrays of
        metrics (cpu/mem), coalesced into an avg every DEFAULT_NUM_SAMPLES, which amounts to a single data point.
        """
        self.options = options
        self.measurements = [
            "percent_cpu",
            "percent_mem",
        ]

        # This would be the result dictionary
        self.proc_groups = {}
        self.num_samples = 0
        self.avg_cpu = {}

    def init_avg_cpu(self):
        """
        Initialises the avg_cpu dictionary
        """
        for pg in self.PROC_INFO:
            if pg not in self.avg_cpu:
                self.avg_cpu.update({pg: {}})
            if pg not in self.proc_groups:
                self.proc_groups.update({pg: {}})
            for m in self.METRICS:
                if m not in self.avg_cpu[pg]:
                    self.avg_cpu[pg].update({m: {"total": 0.0, "index": 0, "data": []}})

    def _get_pname(self, pg, p):
        """
        Return the name to use as key in the dictionary for this sample
        """
        pgroup = self.PROC_INFO[pg]["regex"]
        for pname in pgroup:
            if pgroup[pname].search(p["command"]):
                return pname
        return p["command"]

    def _is_p_in_pgroup(self, pg, p):
        """
        Returns True if the given p is a member of pgroup
        """
        a = set([p["parent_pid"], p["pid"]])
        pdict = self.PROC_INFO[pg]
        b = pdict["pids"]  # already a set set(pdict['pids'])
        intersect = list(a & b)
        return pdict["tname"].search(p["command"]) and intersect

    def create_cpu_range(self):
        """
        Create the corresponding CPU range of interest.
        At the moment ignored since jc does not support the CPU core view yet (PR in progress)
        """
        regex = self.CPU_RANGE["regex"]
        m = regex.search(self.options.cpu)
        if m:
            self.CPU_RANGE["min"] = min([int(m.group(1)), int(m.group(2))])
            self.CPU_RANGE["max"] = max([int(m.group(1)), int(m.group(2))])
        logger.debug(f"CPU range: {self.CPU_RANGE}")

    def update_pids(self, pg, p):
        """
        Update the self.proc_groups[pg]["pids"] with the PIDs of the sample
        This is an array, we might want to use sets instead to avoid dupes
        """
        pid_set = self.PROC_INFO[pg]["pids"]
        if p["parent_pid"] not in pid_set:
            pid_set.add(p["parent_pid"])

    def update_avg(self, num_samples: int):
        """
        Update the avg_cpu array
        """
        if ((num_samples + 1) % DEFAULT_NUM_SAMPLES) == 0:
            if num_samples > 0:
                for pg in self.PROC_INFO:  # proc_groups:
                    for m in self.METRICS:
                        avg_d = self.avg_cpu[pg][m]
                        val = avg_d["total"] / DEFAULT_NUM_SAMPLES
                        avg_d["data"].append(val)
                        avg_d["index"] += 1  # prob redundant
                        avg_d["total"] = 0.0

    def aggregate_proc(self, index, pg, procs):
        """
        Aggregate the procs onto the corresponding pg under pdict
        """
        pdict = self.proc_groups[pg]
        for p in procs:
            if self._is_p_in_pgroup(pg, p):
                # Find the corresp thread name to insert this sample, it can be "pure"
                # or in a group, in which case it needs to be agglutinated
                pname = self._get_pname(pg, p)
                if pname not in pdict:
                    pdict.update(
                        {
                            pname: {
                                "cpu": [0.0] * self.num_samples,
                                "mem": [0.0] * self.num_samples,
                            }
                        }
                    )
                    self.update_pids(pg, p)
                for m in self.METRICS:
                    # Agglutinate up to num samples
                    pdict[pname][m][index] += p[f"percent_{m}"]
                    self.avg_cpu[pg][m]["total"] += p[f"percent_{m}"]

    def filter_metrics(self, samples):
        """
        Filter the (array of dicts) to the measurements we want,
        of those threads names using the PID and PPID
        """
        self.num_samples = len(samples)
        logger.debug(f"Got {self.num_samples}")
        for _i, item in enumerate(samples):
            self.update_avg(_i)
            procs = item["processes"]  # list of dicts jobs
            # Filter those PIDs we are interested
            for pg in self.PROC_INFO:
                self.aggregate_proc(_i, pg, procs)

        logger.info(f"Parsed {self.num_samples} entries from {self.options.config}")
        logger.debug(f"avg_cpu: {json.dumps(self.avg_cpu, indent=4)}")

    def load_json(self, json_fname):
        """
        Load a .json file
        Returns a dict
        """
        try:
            with open(json_fname, "r") as json_data:
                # Check for empty file
                f_info = os.fstat(json_data.fileno())
                if f_info.st_size == 0:
                    logger.error(f"JSON input file {json_fname} is empty")
                    # bail out
                    sys.exit(1)
                return json.load(json_data)
        except IOError as e:
            raise argparse.ArgumentTypeError(str(e))

    def load_top_json(self, json_fname):
        """
        Load a .json file containing top metrics
        Returns a dict with keys only those interested thread names
        """
        samples = self.load_json(json_fname)
        self.filter_metrics(samples)
        logger.debug(f"JSON {json_fname} top loaded")

    def load_pid_json(self, json_fname):
        """
        Load a _pid.json file containing the PIDs for the processes
        that need to be filtered
        Returns a dict with keys only those interested thread names
        """
        pids_list = self.load_json(json_fname)
        for pg in pids_list:
            if pg in self.PROC_INFO:
                self.PROC_INFO[pg]["pids"] = set(pids_list[pg])
        logger.debug(f"JSON pid loaded: {pformat(self.PROC_INFO)}")

    def get_job_stats(self, pg: str, metric: str):
        """
        Calculate the min, max and median of the metric (cpu,mem)
        """
        pdict = self.proc_groups[pg]
        for pname in pdict:
            _data = pdict[pname][metric]
            nentries = len(_data)
            sum_metric = sum(_data)
            if nentries > 0:
                avg_metric = sum_metric / nentries
            else:
                avg_metric = 0
            pg_control = self.PROC_INFO[pg]["threads"]
            if pname not in pg_control:
                pg_control.update({pname: {metric: {}}})
            else:
                if metric not in pg_control[pname]:
                    pg_control[pname].update({metric: {}})
            pg_control[pname][metric].update(
                {"avg": avg_metric, "min": min(_data), "max": max(_data)}
            )

    def sort_jobs(self, pg: str, metric: str):
        """
        Sort the list of threads by metric utilisation
        """
        d = {}
        pg_control = self.PROC_INFO[pg]["threads"]
        for comm, job in pg_control.items():
            d[comm] = job[metric]["avg"]
        # Alt: dsorted = {k: v for k, v in sorted(d.items(), key=lambda item: item[1])}
        self.PROC_INFO[pg]["sorted"][metric] = sorted(d, key=d.get, reverse=True)

    def get_top_procs_util(self):
        """
        Sort the list of threads from top (metric) utilisation
        """
        for pg in self.proc_groups:
            for metric in self.METRICS:
                self.get_job_stats(pg, metric)
                self.sort_jobs(pg, metric)
        logger.debug(f"Process group: {json.dumps(self.proc_groups, indent=4)}")

    def gen_plots(self):
        """
        Generate the .dat, .plot files for pg at metric m
        """
        comm_sorted = {}
        for pg in self.PROC_INFO:
            for m in self.METRICS:
                if pg not in comm_sorted:
                    comm_sorted.update({pg: {m: {}}})
                comm_sorted[pg][m] = self.PROC_INFO[pg]["sorted"][m]

        plot = GnuplotTemplate(
            self.options.config, self.proc_groups, comm_sorted, self.num_samples
        )
        for metric in self.METRICS:
            for pg in self.proc_groups:
                logger.debug(
                    f"Generating output for Process group: {pg}, metric: {metric}"
                )
                plot.genPlot(metric, pg)

    def save_json(self, json_out: str, pobject):
        """
        Save the given struct in a .json fname file
        """
        logger.debug(f"Writing: {json_out}")
        with open(json_out, "w", encoding="utf-8") as f:
            json.dump(pobject, f, indent=4, sort_keys=True, default=serialize_sets)
            f.close()

    def run(self):
        """
        Entry point
        """
        os.chdir(self.options.directory)
        self.load_pid_json(self.options.pids)
        self.init_avg_cpu()
        self.load_top_json(self.options.config)
        self.get_top_procs_util()
        # Check whether the flag to skip plot gneration is on -- atm ignored
        self.gen_plots()
        self.save_json(self.options.avg, [self.avg_cpu])


def main(argv):
    examples = """
    Examples:
    # Produce _top.json from a _top.out:
    # cat _top.out | jc --pretty --top  > _top.json
    # Use that to produce the gnuplot charts:
    # parse-top.py -c _top.json -p _pids.json -u "0-111" -a _avg.json 
    # Use the _avg.json to combine in a FIO results table:
    # fio-parse-jsons.py -c test_list -t test_title -a _avg.json 

    """
    parser = argparse.ArgumentParser(
        description="""This tool is used to filter a _top.out into _top.json""",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        help="Input _top.out file",
        default=None,
    )
    parser.add_argument(
        "-p",
        "--pids",
        type=str,
        required=True,
        help="Input _pids.json file",
        default=None,
    )
    parser.add_argument(
        "-u",
        "--cpu",
        type=str,
        required=False,
        help="Range of CPUs id to filter",
        default="0-111",
    )
    parser.add_argument(
        "-a",
        "--avg",
        type=str,
        required=False,
        help=".json output file of CPU avg to produce (cummulative if it already exists)",
        default="",
    )
    parser.add_argument(
        "-n",
        "--num",
        type=int,
        required=False,
        help=f"number of samples to use for a period (default {DEFAULT_NUM_SAMPLES})",
        default=DEFAULT_NUM_SAMPLES,
    )
    parser.add_argument(
        "-d", "--directory", type=str, help="Directory to examine", default="./"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="True to enable verbose logging mode",
        default=False,
    )

    options = parser.parse_args(argv)

    if options.verbose:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO

    with tempfile.NamedTemporaryFile(dir="/tmp", delete=False) as tmpfile:
        logging.basicConfig(filename=tmpfile.name, encoding="utf-8", level=logLevel)

    logger.debug(f"Got options: {options}")

    top_meter = TopEntry(options)
    top_meter.run()


if __name__ == "__main__":
    main(sys.argv[1:])
