import subprocess
import common
import settings
import monitoring
import os, sys
import time
import threading
import lxml.etree as ET
import re
import time
import logging

from cluster.ceph import Ceph
from benchmark import Benchmark

logger = logging.getLogger("cbt")

class Cosbench(Benchmark):

    def __init__(self, cluster, config):
        super(Cosbench, self).__init__(cluster, config)

        config = self.parse_conf(config)

        self.op_size = config["obj_size"]
        self.total_procs = config["workers"]
        self.containers = config["containers_max"]
        self.objects = config["objects_max"]
        self.mode = config["mode"]
        self.user = settings.cluster.get('user')
        self.rgw = settings.cluster.get('rgws')[0]
        self.use_existing = settings.cluster.get('use_existing')

        self.run_dir = '%s/osd_ra-%08d/op_size-%s/concurrent_procs-%03d/containers-%05d/objects-%05d/%s' % (self.run_dir, int(self.osd_ra), self.op_size, int(self.total_procs), int(self.containers),int(self.objects), self.mode)
        self.out_dir = '%s/osd_ra-%08d/op_size-%s/concurrent_procs-%03d/containers-%05d/objects-%05d/%s' % (self.archive_dir, int(self.osd_ra), self.op_size, int(self.total_procs),  int(self.containers),int(self.objects), self.mode)

    def prerun_check(self):
        #1. check cosbench
        if not self.check_workload_status():
            sys.exit()
        #2. check rgw
        cosconf = {}
        for param in self.config["auth"]["config"].split(';'):
            try:
                key, value = param.split('=')
                cosconf[key] = value
            except:
                pass
        logger.debug("%s", cosconf)
        if "username" in cosconf and "password" in cosconf and "url" in cosconf:

	    if not self.use_existing:
	        user, subuser = cosconf["username"].split(':')
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin user create --uid='%s' --display-name='%s'" % (user, user)).communicate()
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin subuser create --uid=%s --subuser=%s --access=full" % (user, cosconf["username"])).communicate()
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin key create --uid=%s --subuser=%s --key-type=swift" % (user, cosconf["username"])).communicate()
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin user modify --uid=%s --max-buckets=100000" % (user)).communicate()
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin subuser modify --uid=%s --subuser=%s --secret=%s --key-type=swift" % (user, cosconf["username"], cosconf["password"])).communicate()

            stdout, stderr = common.pdsh("%s@%s" % (self.user, self.config["controller"]),"curl -D - -H 'X-Auth-User: %s' -H 'X-Auth-Key: %s' %s" % (cosconf["username"], cosconf["password"], cosconf["url"])).communicate()

        else:
            logger.error("Auth Configuration in Yaml file is not in correct format")
            sys.exit()
        if re.search('(refused|error)', stderr):
            logger.error("Cosbench connect to Radosgw Connection Failed\n%s", stderr)
            sys.exit()
        if re.search("AccessDenied", stdout):
            logger.error("Cosbench connect to Radosgw Auth Failed\n%s", stdout)
            sys.exit()

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.debug('Skipping existing test in %s.', self.out_dir)
            return True
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
                            "operation":[ {
                                "config":"containers=%s;objects=%s;cprefix=%s-%s;sizes=c(%s)%s" % (conf["containers"], conf["objects"], conf["obj_size"], conf["mode"], conf["obj_size_num"], conf["obj_size_unit"]),
                                "ratio":conf["ratio"],
                                "type":("read" if conf["mode"] == "mix" else conf["mode"])
                            }]
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
        super(Cosbench, self).initialize()

        logger.debug('Running cosbench and radosgw check.')
        self.prerun_check()

        logger.debug('Running scrub monitoring.')
        monitoring.start("%s/scrub_monitoring" % self.run_dir)
        self.cluster.check_scrub()
        monitoring.stop()

        logger.debug('Pausing for 60s for idle monitoring.')
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        time.sleep(60)
        monitoring.stop()

        common.sync_files('%s' % self.run_dir, self.out_dir)

        # Create the run directory
        common.make_remote_dir(self.run_dir)

        conf = self.config
        if not self.config["template"]:
            self.config["template"] = "default"
        self.config["workload"] = self.choose_template("default", conf)

        # add a "prepare" stage if mode is read or mix
        if (self.mode != "write"):
            workstage_prepare= { "name":"prepare",
                                 "work": {
                "type":"prepare",
                "workers":conf["workers"],
                "config":"containers=r(1,%s);objects=r(1,%s);cprefix=%s-%s;sizes=c(%s)%s" % 
                (conf["containers_max"], conf["objects_max"], conf["obj_size"], conf["mode"], conf["obj_size_num"], conf["obj_size_unit"])
             }}
            self.config["workload"]["workflow"]["workstage"].insert(1, workstage_prepare)

        # add a second (write)operation if mode is "mix"
        # parameters same as for read except ratio = 100 - read_ratio
        if (self.mode == "mix"):
            operation_write = {
               "config":"containers=%s;objects=%s;cprefix=%s-%s;sizes=c(%s)%s"
                %(conf["containers"], conf["objects"], conf["obj_size"], conf["mode"], conf["obj_size_num"], conf["obj_size_unit"]),
                "ratio":(100 - conf["ratio"]),
                "type":"write"
            }
            self.config["workload"]["workflow"]["workstage"][2]["work"]["operation"].append(operation_write)

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
        logger.info("Write xml conf to %s/%s.xml", conf["cosbench_xml_dir"], leaves["name"])

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
        self.dropcaches()
        self.cluster.dump_config(self.run_dir)
        monitoring.start(self.run_dir)

        # Run cosbench test
        try:
            self._run()
        except KeyboardInterrupt:
            logger.warning("accept keyboard interrupt, cancel this run")
            conf = self.config
            stdout, stderr = common.pdsh("%s@%s" % (self.user, conf["controller"]),'sh %s/cli.sh cancel %s' % (conf["cosbench_dir"], self.runid)).communicate()
            logger.info("%s", stdout)

        self.check_workload_status()
        self.check_cosbench_res_dir()

        monitoring.stop(self.run_dir)
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)
        common.sync_files('%s/archive/%s*' % (self.config["cosbench_dir"], self.runid), self.out_dir)

    def check_workload_status(self):
        wait = True
        try:
            self.runid
        except:
            wait = False
        while wait:
            stdout, stderr = common.pdsh("%s@%s" % (self.user, self.config["controller"]),"sh %s/cli.sh info | grep %s | awk '{print $8}'" % (self.config["cosbench_dir"], self.runid)).communicate()
            if stderr:
                logger.info("Cosbench Deamon is not running on %s", self.config["controller"])
                return False
            try:
                status = stdout.split(':')[1]
                if status.strip() != 'PROCESSING':
                    wait = False
            except:
                wait = False
            time.sleep(1)
        stdout, stderr = common.pdsh("%s@%s" % (self.user, self.config["controller"]),"sh %s/cli.sh info " % (self.config["cosbench_dir"])).communicate()
        logger.debug(stdout)
        return True

    def check_cosbench_res_dir(self):
        #check res dir
        check_time = 0
        while True:
            stdout, stderr = common.pdsh("%s@%s" % (self.user, self.config["controller"]), "find %s/archive -maxdepth 1 -name '%s-*'" % (self.config["cosbench_dir"], self.runid)).communicate() 
            if stdout:
                return True
            if check_time == 300:
                return False
            check_time += 1
            time.sleep(1)

    def _run(self):
        conf = self.config
        stdout, stderr = common.pdsh("%s@%s" % (self.user, conf["controller"]),'sh %s/cli.sh submit %s/%s.xml' % (conf["cosbench_dir"], conf["cosbench_xml_dir"], conf["xml_name"])).communicate()
        m = re.findall('Accepted with ID:\s*(\w+)', stdout )
        if not m:
            logger.error("cosbench start failing with error: %s", stderr)
            sys.exit()
        self.runid = m[0]
        logger.info("cosbench job start, job number %s", self.runid)
        wait_time = conf["rampup"]+conf["rampdown"]+conf["runtime"] 
        logger.info("====== cosbench job: %s started ======", conf["xml_name"])
        logger.info("wait %d secs to finish the test", wait_time)
        logger.info("You can monitor the runtime status and results on http://localhost:19088/controller")
        time.sleep(wait_time)

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Cosbench, self).__str__())
