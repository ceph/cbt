"""This module implements the COSBench benchmark"""

# standard imports
import subprocess
import os, sys
import time
import threading
import lxml.etree as ET
import re
import time
import logging

# custom imports
import common
import settings
import monitoring

from cluster.ceph import Ceph
from benchmark import Benchmark

# get the logger handle to log to for this module
logger = logging.getLogger("cbt")

# the cosbench benchmark class
class Cosbench(Benchmark):
    """"""
    def __init__(self, cluster, config):
        # setup the stuff common to all benchmarks, dirs etc
        super(Cosbench, self).__init__(cluster, config)

        # parse the options in the workload-profile XML file of cosbench to get info out of it
        config = self.parse_conf(config)

        # chunks in which to perform the R/W operation, in our case the 'object size' to use
        self.op_size = config["obj_size"]
        # total number of worker processes to deploy
        self.total_procs = config["workers"]
        # maximum number of containers to create for the user
        self.containers = config["containers_max"]
        # maximum number of objects to read/write
        self.objects = config["objects_max"]
        # mode of operation i.e. read/write/mix
        # this will also be used by 'choose_template()' to assign a worklaod template to COSBench
        # can be 'read' 'write' 'mix'
        self.mode = config["mode"]
        # ceph-user on the cluster nodes
        self.user = settings.cluster.get('user')
        # hostname of a RADOSGW daemon - choosing the first?
        self.rgw = settings.cluster.get('rgws').keys()[0]
        # executable for radosgw-admin
        self.radosgw_admin_cmd = settings.cluster.get('radosgw-admin_cmd', '/usr/bin/radosgw-admin')
        # whether to use the existing cluster
        self.use_existing = settings.cluster.get('use_existing')
        # whether to use teuthology
        self.is_teuthology = settings.cluster.get('is_teuthology', False)

        # create the 'run-dir' specific to this particular test profile
        self.run_dir = '%s/osd_ra-%08d/op_size-%s/concurrent_procs-%03d/containers-%05d/objects-%05d/%s' % (self.run_dir, int(self.osd_ra), self.op_size, int(self.total_procs), int(self.containers),int(self.objects), self.mode)
        
        # output directory is the archive directory given in YAML
        self.out_dir = self.archive_dir


    # ensure some prereqs for the COSBench running
    def prerun_check(self):
        """Checking for a stable COSBench environment before starting:
        Parse the XML to get auth info
        Create users if new cluster
        Attempt connection with the RADOSGW
        Check for containers and set the 'container_prepared' bool
        """
        #1. check cosbench to see if it's running workload
        if not self.check_workload_status():
            sys.exit()
        #2. check rgw
        cosconf = {}
        # simply parse the 'config' entry of 'auth' tag of the XML, each item is separated by ;
        for param in self.config["auth"].split(';'):
            try:
                key, value = param.split('=')
                cosconf[key] = value
            except:
                pass
        # log the stuff for looking on it
        logger.debug("%s", cosconf)

        # FIXME: is the XML entry of authentication url called 'url' or 'authurl'? fix this if condition if it is
        if "username" in cosconf and "password" in cosconf and "url" in cosconf:
            # if not using an existing cluster, need to create users for the benchmarking
            # also true if using the teuthology framework, need users for testing
            if not self.use_existing or self.is_teuthology:
                # test:test format is being split
                user, subuser = cosconf["username"].split(':')
                # setup user, subuser, create key, add quota etc and apply key to the user, basic setup, just like before
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin user create --uid='%s' --display-name='%s'" % (user, user)).communicate()
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin subuser create --uid=%s --subuser=%s --access=full" % (user, cosconf["username"])).communicate()
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin key create --uid=%s --subuser=%s --key-type=swift" % (user, cosconf["username"])).communicate()
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin user modify --uid=%s --max-buckets=100000" % (user)).communicate()
                stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"radosgw-admin subuser modify --uid=%s --subuser=%s --secret=%s --key-type=swift" % (user, cosconf["username"], cosconf["password"])).communicate()

            # curl is called just to test out the RADOSGW using the parameters:
            # -D - -> dump header information on STDOUT
            # -H -> mention header entries to add, we're adding Auth User/Key entries for the swift authentication
            # 
            stdout, stderr = common.pdsh("%s@%s" % (self.user, self.config["controller"]),"curl -D - -H 'X-Auth-User: %s' -H 'X-Auth-Key: %s' %s" % (cosconf["username"], cosconf["password"], cosconf["url"])).communicate()

        else:
            # log about error
            logger.error("Auth Configuration in Yaml file is not in correct format")
            sys.exit()

        # can't connect to the RADOSGW
        if re.search('(refused|error)', stderr):
            logger.error("Cosbench connect to Radosgw Connection Failed\n%s", stderr)
            sys.exit()

        # authentication credentials are invalid
        if re.search("AccessDenied", stdout):
            logger.error("Cosbench connect to Radosgw Auth Failed\n%s", stdout)
            sys.exit()

        #3. check if container and obj created
        # the target that we need to have, it could exist, or we'll create it, this is probably SWIFT convention
        target_name = "%s-%s-%s" % (self.config["obj_size"], self.config["mode"], self.config["objects_max"])
        
        # track number of containers existing/just created
        container_count = 0
        # perform an API operation, this is the python-swiftclient which has the following arguments:
        # -A -> authentication URL
        # -U -> username
        # -K -> api_key
        # list -> list the containers or the objects for the container for that user
        stdout, stderr = common.pdsh("%s@%s" % (self.user, self.rgw),"swift -A %s -U %s -K %s list" % (cosconf["url"], cosconf["username"], cosconf["password"])).communicate()
        # if an error was returned
        if stderr != "":
            # containers aren't prepared
            self.container_prepared = False
            return

        # parse to determine if a target is prepared, simple code
        for container_name in stdout.split('\n'):
            if target_name in container_name:
                container_count += 1
        if container_count >= int(self.config["containers_max"]):
            self.container_prepared = True
        else:
            self.container_prepared = False

    # skip if such a test profile already exists
    def exists(self):
        """Determine whether such a test profile already exists, checking if a dir exists."""
        if os.path.exists(self.out_dir):
            logger.debug('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    # use some nice templates for the workload definition
    def choose_template(self, temp_name, conf):
        """Given a configuration file supplied by the user, along with a template name,
        return a template of workload profile as a dictionary."""

        # default R/W ratio
        ratio = { "read": 0, "write": 0 }
        # if it's read or write only, ratio is gonna be 100%
        if conf["mode"] == "read" or conf["mode"] == "write":
            mode = [conf["mode"]]
            ratio[conf["mode"]] = 100
        # in case of mix operation, ratio needs to be divided
        elif conf["mode"]  == "mix":
            mode = ["read", "write"]
            ratio["read"] = conf["ratio"]
            ratio["write"] = 100 - conf["ratio"]
        # otherwise, log an error as unknown benchmarking mode, and bail!
        else:
            logger.error("Unknown benchmark mode: %s", conf["mode"])
            sys.exit()

        # 
        operation = []
        for tmp_mode in mode:
            operation.append({
                "config":"containers=%s;objects=%s;cprefix=%s-%s-%s;sizes=c(%s)%s"
                %(conf["containers"], conf["objects"], conf["obj_size"], conf["mode"], conf["objects_max"], conf["obj_size_num"], conf["obj_size_unit"]),
                "ratio":ratio[tmp_mode],
                "type":tmp_mode
            })

        template = {
            "default":{
                "description": conf["mode"],
                "name": "%s_%scon_%sobj_%s_%dw" % (conf["mode"], conf["containers_max"], conf["objects_max"], conf["obj_size"], conf["workers"]),
                "storage": {"type":"swift", "config":"timeout=300000" },
                "auth": {"type":"swauth", "config":"%s" % (conf["auth"])},
                "workflow": {
                    "workstage": [{
                        "name": "main",
                        "work": {"rampup":conf["rampup"], "rampdown":conf["rampdown"], "name":conf["obj_size"], "workers":conf["workers"], "runtime":conf["runtime"],
                            "operation":operation
                        }
                    }]
                }
            }
        }
        if temp_name in template:
            return template[temp_name]

    # parse the XML config of cosbench to get the stuff out of it
    def parse_conf(self, conf):
        """Parse the COSBench workload XML and extract information including:
         - Number of containers
         - Number of objects/container
         - Size of an object"""
        # parse the 'containers' tag
        if "containers" in conf:
            # find the pattern "r(1,32)"
            # start with one alphanum char, then ( then one or more digits, then comma, then more digits, then )
            m = re.findall("(\w{1})\((\d+),(\d+)\)", conf["containers"])
            if m:
                # the first char is the method r-> range?
                conf["containers_method"] = m[0][0]
                # mininum number of containers is first integer
                conf["containers_min"] = m[0][1]
                # maximum number of containers is second integer
                conf["containers_max"] = m[0][2]
        # parse the 'objects' tag
        if "objects" in conf:
            # same idea this time for objects
            m = re.findall("(\w{1})\((\d+),(\d+)\)", conf["objects"])
            if m:
                # method r -> range?
                conf["objects_method"] = m[0][0]
                # minimum number of objects/container?
                conf["objects_min"] = m[0][1]
                # maximum number of objects/container?
                conf["objects_max"] = m[0][2]
        # parse object sizes
        if "obj_size" in conf:
            # same idea for object size
            # c(64)KB
            m = re.findall("(\d+)(\w+)", conf["obj_size"])
            if m:
                # first set of integers is the object size
                conf["obj_size_num"] = m[0][0]
                # second set of integers is the size unit
                conf["obj_size_unit"] = m[0][1]
        # return the populated dictionary
        return conf

    # initialize the sutff needed for cosbench
    def initialize(self):
        """"""

        # setup the stuff common to all benchmarks
        super(Cosbench, self).initialize()

        # log some stuff
        logger.debug('Running cosbench and radosgw check.')
        
        # handle pre-run stuff, before starting off
        self.prerun_check()

        # Idle monitoring to determine the running status of the node before starting 
        logger.debug('Pausing for 60s for idle monitoring.')
        monitoring.start("%s/idle_monitoring" % self.run_dir)
        time.sleep(60)
        monitoring.stop()

        # synchronize files between rundir of COSBench and out_dir of CBT
        common.sync_files('%s' % self.run_dir, self.out_dir)

        # Create the run directory
        common.make_remote_dir(self.run_dir)

        conf = self.config
        if not self.config["template"]:
            self.config["template"] = "default"
        self.config["workload"] = self.choose_template("default", conf)

        # add a "prepare" stage if mode is read or mix
        if not self.container_prepare_check():
            workstage_init = {
                "name": "init",
                "work": {"type":"init", "workers":conf["workers"], "config":"containers=r(1,%s);cprefix=%s-%s-%s" % (conf["containers_max"], conf["obj_size"], conf["mode"], conf["objects_max"])}
            }
            workstage_prepare = {
                "name":"prepare",
                "work": {
                    "type":"prepare",
                    "workers":conf["workers"],
                    "config":"containers=r(1,%s);objects=r(1,%s);cprefix=%s-%s-%s;sizes=c(%s)%s" %
                    (conf["containers_max"], conf["objects_max"], conf["obj_size"], conf["mode"], conf["objects_max"], conf["obj_size_num"], conf["obj_size_unit"])
                }
            }
            self.config["workload"]["workflow"]["workstage"].insert(0, workstage_prepare)
            self.config["workload"]["workflow"]["workstage"].insert(0, workstage_init)

        self.prepare_xml(self.config["workload"])
        return True

    def container_prepare_check(self):
        return self.container_prepared

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

    # is Cosbench already running a benchmark?
    def check_workload_status(self):
        """Check the runid of COSBench (if it exists) to determine if COSBench is running a workload already."""
        # log stuff
        logger.info("Checking workload status")
        # do we have to wait?
        wait = True
        try:
            # try to access an attribute
            self.runid
        except:
            # if we dont' have a runid yet, means no COSBench is running, no need to wait!
            wait = False

        # as long as we have to wait, 
        while wait:
            # run the cli.sh script of COSBench with 'info' 
            # to ensure that a runid has already been setup which refers to an existing instance of COSBench
            stdout, stderr = common.pdsh("%s@%s" % (self.user, self.config["controller"]),"sh %s/cli.sh info | grep %s | awk '{print $8}'" % (self.config["cosbench_dir"], self.runid)).communicate()
            # if anything was put in the stderr PIPE, daemon wasn't running, since the command returned an error
            if stderr:
                # log that it's so
                logger.info("Cosbench Deamon is not running on %s", self.config["controller"])
                # we're not running any COSBench, no need to check for workload status, return False
                return False
            try:
                # parse the output from the shell in case COSBench is running, see if a workload is also running
                status = stdout.split(':')[1]
                # if we're not longer running a workload
                if status.strip() != 'PROCESSING':
                    # stop waiting, and end!
                    wait = False
            except:
                # if there was nothing in the stdout, COSBench isn't running now, you can stop
                wait = False
            
            # try again after a second
            time.sleep(1)

        # log the COSBench status to the log, and end
        stdout, stderr = common.pdsh("%s@%s" % (self.user, self.config["controller"]),"sh %s/cli.sh info " % (self.config["cosbench_dir"])).communicate()
        # log the output for reference
        logger.debug(stdout)
        # wait for some time to let things settle
        time.sleep(15)
        # a workload is running
        return True

    def check_cosbench_res_dir(self):
        #check res dir
        check_time = 0
        while True:
            stdout, stderr = common.pdsh("%s@%s" % (self.user, self.config["controller"]), "find %s/archive -maxdepth 1 -name '%s-*'" % (self.config["cosbench_dir"], self.runid)).communicate() 
            if stdout:
                return True
            if check_time == 3000:
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
