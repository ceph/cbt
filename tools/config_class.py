#!/usr/bin/env python2
import os
import sys

MODES = ["randwrite", "randread", "write", "read"]


class Config:
    def __init__(self, out_file, hosts, user, tmp_dir, osds_per_node=1):
        self.benchmarks = ""
        self.ceph_conf = "/etc/ceph/ceph.conf"
        self.hosts = hosts
        self.iterations = 1
        self.mkfs_opts = "-f -i size=2048 -n size=64k"
        self.mount_opts = "-o inode64,noatime,logbsize=256k"
        self.osds_per_node = osds_per_node
        self.out_file = "%s/%s" % (os.path.dirname(os.path.realpath(__file__)),
                                   out_file)
        self.tmp_dir = tmp_dir
        self.user = user

    def add_benchmark_settings(self, settings):
        self.benchmarks += settings

    def get_pgs(self):
        while True:
            pgs = 0
            pgs = get_input("Enter the number of placement groups (PGs): ")
            try:
                pgs = int(pgs)
            except ValueError:
                print "PGs can only be integer values greater than 0."
                continue
            if pgs <= 0:
                print "PGs must be greater than 0."
                continue
            return pgs

    def get_mode(self):
        modes = ""
        while True:
            print "Which of the following modes do you wish to run?"
            modes = get_input("randwrite, randread, write, read: ")
            modes = modes.replace(" ", "").split(",")
            valid = True
            for mode in modes:
                if mode.lower() not in MODES:
                    valid = False
                    break
            if valid:
                break
        return str(modes).replace("'", "")

    def get_integer_list(self, prompt, example):
        int_list = ""
        while True:
            print prompt
            int_list = get_input("e.g. %s " % (example)).replace(" ", "")
            int_list = int_list.replace("[", "").replace("]", "").split(",")
            valid = True
            for num in int_list:
                try:
                    num = int(num)
                except ValueError:
                    print "Only integer values greater than 0 are allowed."
                    valid = False
                    break
                if num <= 0:
                    print "Value must be greater than 0."
                    valid = False
                    break
            if valid:
                break
        return str(int_list).replace("'", "")

    def get_integer(self, prompt, example):
        int_out = ""
        while True:
            print prompt
            int_out = get_input("e.g. %s: " % (example))
            valid = True
            if "," in int_out:
                print "Only single integer value allowed."
                valid = False
            try:
                int_out = int(int_out)
            except ValueError:
                print "Only valid integer values greater than 0 are allowed."
                valid = False
            if valid:
                break
        return int_out

    def get_time(self):
        while True:
            time = 0
            time = get_input("Enter the desired timeout in seconds: ")
            try:
                time = int(time)
            except ValueError:
                print "Time must be a valid integer number greater than 0."
                continue
            if time <= 0:
                print "Time must be greater than 0."
                continue
            return time

    def get_volume(self):
        while True:
            volume = 0
            volume = get_input("Enter the total number of I/O bytes to operate"
                               " on: ")
            try:
                volume = int(volume)
            except ValueError:
                print "Volume must be a valid integer number greater than 0."
                continue
            if volume <= 0:
                print "Volume must be greater than 0."
                continue
            return volume

    def get_pgs_per_pool(self):
        while True:
            pgs = 0
            pgs = get_input("Enter the number of placement groups per pool: ")
            try:
                pgs = int(pgs)
            except ValueError:
                print "Placement groups must be a valid integer number "\
                      "greater than 0."
                continue
            if pgs <= 0:
                print "Placement groups must be greater than 0."
                continue
            return pgs

    def true_or_false(self, prompt):
        while True:
            response = get_input("%s [y/n]?: " % (prompt))
            if response.lower() == "y":
                return True
            elif response.lower() == "n":
                return False
            else:
                continue

    def save_file(self):
        f = open(self.out_file, "w")
        out_message = ("cluster:\n"
                       "  head: '%s'\n"
                       "  clients: %s\n"
                       "  servers: %s\n"
                       "  mons: ['%s']\n"
                       "  user: %s\n"
                       "  osds_per_node: %s\n"
                       "  fs: xfs\n"
                       "  mkfs_opts %s\n"
                       "  mount_opts: %s\n"
                       "  ceph.conf: %s\n"
                       "  iterations: %s\n"
                       "  tmp_dir: '%s'\n"
                       "benchmarks: %s\n") % (self.hosts[0], self.hosts[1],
                                              self.hosts[1], self.hosts[0],
                                              self.user, self.osds_per_node,
                                              self.mkfs_opts, self.mount_opts,
                                              self.ceph_conf, self.iterations,
                                              self.tmp_dir, self.benchmarks)
        f.write(out_message)


class KvmRbdFio:
    def __init__(self, default, config):
        self.iodepth = "[1, 2, 4, 8, 16]"
        self.mode = "[randwrite, randread, write, read]"
        self.op_size = "[4096, 131072, 4194304]"
        self.osd_ra = "128"
        self.output = ""
        self.pgs = 8192
        self.time = 60
        self.vol_size = 65536

        if not default:
            self.get_settings(config)

        self.generate_output()

    def generate_output(self):
        self.output = ("\n  kvmrbdfio:\n"
                       "    time: %s\n"
                       "    pgs: %s\n"
                       "    vol_size: %s\n"
                       "    mode: %s\n"
                       "    op_size: %s\n"
                       "    iodepth: %s\n"
                       "    osd_ra: %s\n") % (self.time, self.pgs,
                                              self.vol_size, self.mode,
                                              self.op_size, self.iodepth,
                                              self.osd_ra)

    def get_settings(self, config):
        self.pgs = config.get_pgs()
        self.mode = config.get_mode()
        self.iodepth = config.get_integer_list("Enter the desired IO depth",
                                               "[1, 2, 4, 8, 16]")
        self.op_size = config.get_integer_list("Enter the desired object"
                                               " size(s) in bytes",
                                               "[4096, 131072, 4194304]")
        self.osd_ra = config.get_integer("Enter the desired OSD read-ahead"
                                         " in bytes", "128")
        self.time = config.get_time()
        self.vol_size = config.get_volume()


class Radosbench:
    def __init__(self, default, config):
        self.concurrent_ops = "[32]"
        self.concurrent_procs = 2
        self.op_size = "[4096, 131072, 4194304]"
        self.osd_ra = "128"
        self.output = ""
        self.pgs_per_pool = 1024
        self.time = 300
        self.write_only = False

        if not default:
            self.get_settings(config)

        self.generate_output()

    def generate_output(self):
        self.output = ("\n  radosbench:\n"
                       "    op_size: %s\n"
                       "    write_only: %s\n"
                       "    time: %s\n"
                       "    concurrent_ops: %s\n"
                       "    concurrent_procs: %s\n"
                       "    pgs_per_pool: %s\n"
                       "    osd_ra: %s\n") % (self.op_size, self.write_only,
                                              self.time, self.concurrent_ops,
                                              self.concurrent_procs,
                                              self.pgs_per_pool, self.osd_ra)

    def get_settings(self, config):
        self.concurrent_ops = config.get_integer_list("Enter the number of"
                                                      " concurrent operations"
                                                      " to run", "32")
        self.concurrent_procs = config.get_integer_list("Enter the number of"
                                                        " concurrent processes"
                                                        " to run", "32")
        self.op_size = config.get_integer_list("Enter the desired object"
                                               " size(s) in bytes",
                                               "[4096, 131072, 4194304]")
        self.osd_ra = config.get_integer("Enter the desired OSD read-ahead"
                                         " in bytes", "128")
        self.pgs_per_pool = config.get_pgs_per_pool()
        self.time = config.get_time()
        self.write_only = config.true_or_false("Would you like to only perform"
                                               " write-tests")


class RbdFio:
    def __init__(self, default, config):
        self.concurrent_procs = "[1, 2, 4, 8, 16]"
        self.iodepth = "[1, 2, 4, 8, 16]"
        self.mode = "[randwrite, randread, write, read]"
        self.op_size = "[4096, 131072, 4194304]"
        self.osd_ra = "128"
        self.output = ""
        self.pgs = 8192
        self.rbdadd_options = "'noshare'"
        self.time = 60
        self.vol_size = 65536

        if not default:
            self.get_settings(config)

        self.generate_output()

    def generate_output(self):
        self.output = ("\n  rbdfio:\n"
                       "    rbdadd_options: %s\n"
                       "    time: %s\n"
                       "    pgs: %s\n"
                       "    vol_size: %s\n"
                       "    mode: %s\n"
                       "    op_size: %s\n"
                       "    concurrent_procs: %s\n"
                       "    iodepth: %s\n"
                       "    osd_ra: %s\n") % (self.rbdadd_options, self.time,
                                              self.pgs, self.vol_size,
                                              self.mode, self.op_size,
                                              self.concurrent_procs,
                                              self.iodepth, self.osd_ra)

    def get_settings(self, config):
        self.pgs = config.get_pgs()
        self.mode = config.get_mode()
        self.concurrent_procs = config.get_integer_list("Enter the number of"
                                                        " concurrent processes"
                                                        " you desire to run",
                                                        "[1, 2, 4, 8, 16]")
        self.iodepth = config.get_integer_list("Enter the desired IO depth",
                                               "[1, 2, 4, 8, 16]")
        self.time = config.get_time()
        self.op_size = config.get_integer_list("Enter the desired object"
                                               " size(s) in bytes",
                                               "[4096, 131072, 4194304]")
        self.osd_ra = config.get_integer("Enter the desired OSD read-ahead"
                                         " in bytes", "128")
        self.vol_size = config.get_volume()


def keyboard_input(func):
    def wrapper(prompt):
        try:
            return func(prompt)
        except KeyboardInterrupt:
            print "Aborting script. No data will be saved."
            sys.exit(1)
    return wrapper


@keyboard_input
def get_input(prompt):
    return raw_input(prompt)
