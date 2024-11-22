#!/usr/bin/python3
#
# serialise_benchmark.py - generate a bechserial.json from all the benchmark classes,
# and automated creation of unit tests for them.
#
import argparse
import hashlib
import json
import os
import pprint
import sys
from json import JSONEncoder

import yaml

import benchmarkfactory
import settings
from cluster.ceph import Ceph
from log_support import setup_loggers

log_fname = "/tmp/cbt-utest.log"


class BenchGenerator(object):
    """
    Class used for the serialisation of the benchmark classes
    and automated generation or unit tests
    """

    all_benchmarks = [
        "nullbench",
        "fio",
        "hsbench",
        "radosbench",
        "kvmrbdfio",
        "rawfio",
        "librbdfio",
        "cephtestrados",
        "rbdfio",
        "getput",
    ]
    archive_dir = "/tmp"
    iteration = {"acceptable": [1, 2, 3], "iteration": 0}
    cluster = {}
    bl_name = "tools/baseline.json"
    bl_md5 = None
    cl_name = "tools/invariant.yaml"
    ut_name = "tests/test_bm.py"
    djson = {}
    current = {}

    def __init__(self):
        """Init using mock constructors for a fixed cluster"""
        settings.mock_initialize(config_file=BenchGenerator.cl_name)
        BenchGenerator.cluster = Ceph.mockinit(settings.cluster)

    def get_md5_bl(self):
        """Calculate the MD5sum from baseline contents"""
        with open(self.bl_name, "rb") as f:
            data = f.read()
            f.close()
        return hashlib.md5(data).hexdigest()
        # bl_md5 = hashlib.md5(data.encode("utf-8")).hexdigest()

    def gen_json(self):
        """Serialise the object into a json file"""
        result = {}
        for bm in self.all_benchmarks:
            b = benchmarkfactory.get_object(self.archive_dir, self.cluster, bm, self.iteration)
            result[bm] = b.__dict__
        with open(self.bl_name, "w", encoding="utf-8") as f:
            json.dump(result, f, sort_keys=True, indent=4, cls=BenchJSONEncoder)
            f.close()
        # data from json.dump() does not support buffer API
        self.bl_md5 = self.get_md5_bl()

    def verify_md5(self):
        """Verify the MD5SUM of the baseline.json is correct"""
        md5_returned = self.get_md5_bl()
        if self.bl_md5 == md5_returned:
            print("MD5 verified.")
            return True
        else:
            print(f"MD5 verification failed! {self.bl_md5} vs. {md5_returned}")
            return False

    def verify_json(self):
        """Verify the baseline json against the current benchmark classes"""
        with open(self.bl_name, "r") as f:
            self.djson = json.load(f)
            f.close()
        for bm in self.all_benchmarks:
            b = benchmarkfactory.get_object(self.archive_dir, self.cluster, bm, self.iteration)
            self.current[bm] = b.__dict__
        # This loop verifies that the active classes have the same attributes
        # as the baseline: no complains would happen if new attributes have been
        # added, but a difference will show for each old attribute removed
        for bm in self.djson.keys():
            if isinstance(self.djson[bm], dict):
                for k in self.djson[bm].keys():
                    print(f"looking at key {k} for benchmark {bm}")
                    # Skip Cluster since its a Ceph object, and acceptable was removed
                    if k == "cluster" or k == "acceptable" or k == "_cluster":
                        continue
                    if not self.djson[bm][k] == self.current[bm][k]:
                        if isinstance(self.djson[bm][k], dict):
                            set1 = dict(self.djson[bm][k].items())
                            set2 = dict(self.current[bm][k].items())
                            print(set2 ^ set1)
                        else:
                            print(f"{bm}[{k}]: diff type {type(self.djson[bm][k])}")
                            print(f"{bm}[{k}]: {self.djson[bm][k]} vs {self.current[bm][k]}")

    def gen_utests(self):
        """Generate the unit tests from baseline json against the self.current benchmark classes"""
        djson = self.djson
        for bm in djson.keys():
            if isinstance(djson[bm], dict):
                subst = f"sed -e 's/BenchmarkX/Benchmark{bm}/g' -e 's/MD5SUMNone/{self.bl_md5}/g' "
                input = "tools/test_bm_template.py"
                out = f"tests/test_bm_{bm}.py"
                cmd = f"{subst} {input} > {out}"
                # print(cmd)
                os.system(cmd)
                with open(out, "a") as f:
                    for k in djson[bm].keys():
                        # Skip Cluster since its a Ceph object, and acceptable is removed
                        if k == "cluster" or k == "acceptable" or k == "_cluster":
                            continue
                        ut = f"""
    def test_valid_{k}(self):
        \"\"\" Basic sanity attribute identity {k} check\"\"\"
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, '{bm}', self.iteration)
        self.assertEqual(self.bl_json['{bm}']['{k}'], b.__dict__['{k}'])
"""
                        f.write(ut)
                    tail = f"""
if __name__ == '__main__':
    unittest.main()
"""
                    f.write(tail)
                    f.close()


class BenchJSONEncoder(JSONEncoder):
    def default(self, obj):
        return obj.__dict__


def main(argv):
    setup_loggers(log_fname="/tmp/cbt-utest.log")
    bg = BenchGenerator()
    bg.gen_json()
    bg.verify_json()
    bg.verify_md5()
    bg.gen_utests()
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
