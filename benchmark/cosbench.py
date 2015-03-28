import subprocess
import common
import settings
import monitoring
import os
import time
import threading
import lxml.etree as ET
import re
import time

from cluster.ceph import Ceph
from benchmark import Benchmark

class Cosbench(Benchmark):

    def __init__(self, cluster, config):
        super(Cosbench, self).__init__(cluster, config)

    def exists(self):
        return False

    def choose_template(self, temp_name, conf):
        template = {
            "default":{
                "description": conf["mode"],
                "name": "%s_%scon_%sobj_%s_%dw" % (conf["mode"], conf["containers_max"], conf["objects_max"], conf["obj_size"], conf["workers"]),
                "storage": {"type":"swift", "config":"timeout=300000" },
                "auth": {"type":"swauth", "config":"%s" % (conf["auth"]["config"])},
                "workflow": {
                    "workstage": [{
                        "name": "init",
                        "work": {"type":"init", "workers":conf["workers"], "config":"containers=r(1,%s);cprefix=%s-%s" % (conf["containers_max"], conf["obj_size"], conf["mode"])}
                    },{
                        "name": "main",
                        "work": {"rampup":conf["rampup"], "rampdown":conf["rampdown"], "name":conf["obj_size"], "workers":conf["workers"], "runtime":conf["runtime"],
                            "operation": {
                                "config":"containers=%s;objects=%s;cprefix=%s-%s;sizes=c(%s)%s" % (conf["containers"], conf["objects"], conf["obj_size"], conf["mode"], conf["obj_size_num"], conf["obj_size_unit"]),
                                "ratio":conf["ratio"],
                                "type":conf["mode"]
                            }
                        }
                    }]
                }
            }
        } 
        if temp_name in template:
            return template[temp_name]

    def parse_conf(self, conf):
        if "containers" in conf:
            m = re.findall("(\w{1})\((\d+),(\d+)\)", conf["containers"])
            if m:
                conf["containers_method"] = m[0][0]
                conf["containers_min"] = m[0][1]
                conf["containers_max"] = m[0][2]
        if "objects" in conf:
            m = re.findall("(\w{1})\((\d+),(\d+)\)", conf["objects"])
            if m:
                conf["objects_method"] = m[0][0]
                conf["objects_min"] = m[0][1]
                conf["objects_max"] = m[0][2]
        if "obj_size" in conf:
            m = re.findall("(\d+)(\w+)", conf["obj_size"])
            if m:
                conf["obj_size_num"] = m[0][0]
                conf["obj_size_unit"] = m[0][1]
        return conf

    def initialize(self): 
        #super(Cosbench, self).initialize()
        conf = self.config
        if not self.config["template"]:
            self.config["template"] = "default"
        conf = self.parse_conf(conf)
        self.config["workload"] = self.choose_template("default", conf)
        self.prepare_xml(self.config["workload"])
        return True

    #function use_template, set_leaf and run_content, add_leaf_to_tree all used for generate a cosbench xml.
    def prepare_xml(self, leaves):
        conf = self.config
        root = ET.Element("workload")
        parent = root
        self.add_leaf_to_tree(leaves, parent)
        self.config["xml_name"] = leaves["name"]
        tree = ET.ElementTree(root)
        tree.write("%s/%s.xml" % (conf["cosbench_xml_dir"], leaves["name"]),pretty_print=True)
        print "Write xml conf to %s/%s.xml" % (conf["cosbench_xml_dir"], leaves["name"])

    def add_leaf_to_tree(self, leaves, parent):
        for leaf, leaf_content in leaves.iteritems():
            if isinstance(leaf_content, str) or isinstance(leaf_content, int):
                parent.set(leaf, str(leaf_content))
            elif isinstance(leaf_content, list):
                leaves = leaf_content
                for leaf_content in leaves:
                    self.add_leaf_to_tree(leaf_content, ET.SubElement(parent, leaf))
            else:
                self.add_leaf_to_tree(leaf_content, ET.SubElement(parent, leaf))

    def run(self):
        super(Cosbench, self).run()
        
        # Run write test
        self._run('write', '%s/write' % self.run_dir, '%s/write' % self.out_dir)

    def _run(self):
        conf = self.config
        res = common.pdsh(conf["controller"],'sh %s/cli.sh submit %s/%s.xml' % (conf["cosbench_dir"], conf["cosbench_xml_dir"], conf["xml_name"]), True) 
        print res[0]
        wait_time = conf["rampup"]+conf["rampdown"]+conf["runtime"] 
        print "====== cosbench job: %s started ======" % (conf["xml_name"])
        print "wait %d secs to finish the test" % (wait_time)
        print "You can monitor the runtime status and results on http://localhost:19088/controller"
        time.sleep(wait_time)
        
    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Cosbench, self).__str__())
