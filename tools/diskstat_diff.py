#!/usr/bin/python
"""
This script expect an input .json file name as argument, and a .json stream
from stdin, and
calculates its difference, (producing a gnuplot .plot and dat for it)
Might generalise later for a whole set of samples (like we do with top).
It could also be extended to process .json from ceph conf osd tell dump_metrics.
"""

import argparse
import logging
import os
import sys
import re
import json
import tempfile

__author__ = "Jose J Palacios-Perez"

logger = logging.getLogger(__name__)


def serialize_sets(obj):
    """
    Serialise sets as lists
    """
    if isinstance(obj, set):
        return list(obj)

    return obj


class DiskStatEntry(object):
    """
    Calculate the difference between an diskstat .json file and
    a .json stream from stdin, and
    produce a gnuplot and .JSON of the difference
    jc --pretty /proc/diskstats
    {
    "maj": 8,
    "min": 1,
    "device": "sda1",
    "reads_completed": 43291,
    "reads_merged": 34899,
    "sectors_read": 4570338,
    "read_time_ms": 20007,
    "writes_completed": 6562480,
    "writes_merged": 9555760,
    "sectors_written": 1681486816,
    "write_time_ms": 10427489,
    "io_in_progress": 0,
    "io_time_ms": 2062151,
    "weighted_io_time_ms": 10447497,
    "discards_completed_successfully": 0,
    "discards_merged": 0,
    "sectors_discarded": 0,
    "discarding_time_ms": 0,
    "flush_requests_completed_successfully": 0,
    "flushing_time_ms": 0
    }

    Only interested in the following measurements:
    "device" "reads_completed" "read_time_ms" "writes_completed" "write_time_ms"
    """

    def __init__(self, aname: str, regex: str, directory: str):
        """
        This class expects two input .json files
        Calculates the difference b - a and replaces b with this
        The result is a dict with keys the device names, values the measurements above
        """
        self.aname = aname
        self.regex = re.compile(regex)  # , re.DEBUG)
        self.time_re = re.compile(r"_time_ms$")
        self.measurements = [
            "reads_completed",
            "read_time_ms",
            "writes_completed",
            "write_time_ms",
        ]

        self.directory = directory
        self._diff = {}

    def filter_metrics(self, ds):
        """
        Filter the (array of dicts) to the measurements we want, of those device names
        """
        result = {}
        for item in ds:
            dv = item["device"]
            # Can we use list comprehension here?
            if self.regex.search(dv):
                if dv not in result:
                    result.update({dv: {}})
                for m in self.measurements:
                    result[dv].update({m: item[m]})
        return result

    def get_diff(self, a_data, b_data):
        """
        Calculate the difference of b_data - a_data
        Assigns the result to self._diff
        """
        for dev in b_data:
            for m in b_data[dev]:
                if self.time_re.search(m):
                    _max = max([b_data[dev][m], a_data[dev][m]])
                    b_data[dev][m] = _max
                else:
                    b_data[dev][m] -= a_data[dev][m]
        self._diff = b_data

    def load_json(self, json_fname):
        """
        Load a .json file containing diskstat metrics
        Returns a dict with keys only those interested device names
        """
        try:
            with open(json_fname, "r") as json_data:
                ds_list = []
                # check for empty file
                f_info = os.fstat(json_data.fileno())
                if f_info.st_size == 0:
                    logger.error(f"JSON input file {json_fname} is empty")
                    return ds_list
                ds_list = json.load(json_data)
                return self.filter_metrics(ds_list)
        except IOError as e:
            raise argparse.ArgumentTypeError(str(e))

    def save_json(self):
        """
        Save the difference
        """
        if self.aname:
            with open(self.aname, "w", encoding="utf-8") as f:
                json.dump(
                    self._diff, f, indent=4, sort_keys=True, default=serialize_sets
                )
                f.close()

    def run(self):
        """
        Entry point: processes the input files, then produces the diff
        and saves it back to -a
        """
        os.chdir(self.directory)
        a_data = self.load_json(self.aname)
        b_data = self.filter_metrics(json.load(sys.stdin))
        self.get_diff(a_data, b_data)
        self.save_json()


def main(argv):
    examples = """
    Examples:
    # Calculate the difference in diskstats between the start/end of a performance run:
    # jc --pretty /proc/diskstats  > _start.json
    < .. run test.. >
    # jc --pretty /proc/diskstats | %prog -a _start.json

    """
    parser = argparse.ArgumentParser(
        description="""This tool is used to calculate the difference in diskstat measurements""",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-a",
        type=str,
        required=True,
        help="Input .json file",
        default=None,
    )
    parser.add_argument(
        "-r",
        "--regex",
        type=str,
        required=False,
        help="Regex to describe the device names",
        default=r"nvme\d+n1p2",
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

    dsDiff = DiskStatEntry(options.a, options.regex, options.directory)
    dsDiff.run()


if __name__ == "__main__":
    main(sys.argv[1:])
