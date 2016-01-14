#!/usr/bin/env python2
# NOTE: Be sure to run script on the main ceph monitor as the desired
# CBT user if running the script automatically (-a).

import argparse
import os
import socket
import sys

from config_class import Config, KvmRbdFio, Radosbench, RbdFio

BENCHMARKS = ["radosbench", "kvmrbdfio", "rbdfio"]
TMP_DIR = "/dev/null"


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--automate", help="Automatically create a config"
                                                 " file with default values for"
                                                 " Radosbench, RBDFIO and "
                                                 "KVMRBDFIO.",
                                                 action="store_true")
    parser.add_argument("-o", "--output_file", help="Specify filename for "
                                                    "output config file. "
                                                    "Defaults to 'cbt_config"
                                                    ".xfs.yaml'", type=str,
                                                    nargs="?",
                                                    default="cbt_config"
                                                    ".xfs.yaml")
    return parser.parse_args()


def get_hosts(auto):
    if auto:
        clients = []
        monitor = os.popen("hostname -s").read().rstrip()
        hosts = os.popen("ceph osd tree | grep host").read().split("\n")
        for host in hosts:
            if host != "":
                clients.append(host.rstrip().split(" ")[-1])
        return (monitor, clients)

    try:
        monitor = raw_input("Enter the hostname of the monitor: ")
        clients = raw_input("Enter the hostname(s) of the OSD(s) seperated by"
                            " comma: ").replace(" ", "").split(",")
    except KeyboardInterrupt:
        print "Aborting script. No data will be saved."
        sys.exit(1)
    return (monitor, clients)


def get_user(auto):
    if auto:
        return os.getlogin()

    try:
        user = raw_input("Enter the username for CBT: ")
    except KeyboardInterrupt:
        print "Aborting script. No data will be saved."
        sys.exit(1)
    return user


def get_tmp_dir(auto):
    if auto:
        return TMP_DIR

    try:
        directory = raw_input("Enter the temporary directory for CBT results: ")
    except KeyboardInterrupt:
        print "Aborting script. No data will be saved."
        sys.exit(1)
    return directory


def select_tests():
    while True:
        valid = True
        print "Which of the following tests would you like to run?\nradosbench"\
              ", kvmrbdfio, rbdfio"
        try:
            tests = raw_input("Enter the test names seperated by comma: ")
            tests = tests.replace(" ", "").split(",")
        except KeyboardInterrupt:
            print "Aborting script. No data will be saved."
            sys.exit(1)
        for test in tests:
            if test.lower() not in BENCHMARKS:
                print "Unknown test: %s" % (test)
                print "Please specify only valid tests from the list above\n"
                valid = False
                break
        if valid:
            return [x.lower() for x in tests]


def generate_test_values(test, default, config):
    if test == "rbdfio":
        rbdfio = RbdFio(default, config)
        config.add_benchmark_settings(rbdfio.output)
    elif test == "kvmrbdfio":
        kvmrbdfio = KvmRbdFio(default, config)
        config.add_benchmark_settings(kvmrbdfio.output)
    else:
        radosbench = Radosbench(default, config)
        config.add_benchmark_settings(radosbench.output)


def main():
    args = parse_arguments()
    hosts = get_hosts(args.automate)
    user = get_user(args.automate)
    tmp_dir = get_tmp_dir(args.automate)
    conf = Config(args.output_file, hosts, user, tmp_dir)
    if args.automate:
        rbdfio = RbdFio(True, conf)
        kvmrbdfio = KvmRbdFio(True, conf)
        radosbench = Radosbench(True, conf)
        conf.add_benchmark_settings(rbdfio.output)
        conf.add_benchmark_settings(kvmrbdfio.output)
        conf.add_benchmark_settings(radosbench.output)
    else:
        tests = select_tests()
        for test in tests:
            use_default = False
            print "\nEntering settings for %s:" % (test)
            while True:
                try:
                    default = raw_input("Would you like to use default"
                                        " settings for %s [y/n]? " % (test))
                except KeyboardInterrupt:
                    print "Aborting script. No data will be saved."
                    sys.exit(1)
                if default.lower() == "y":
                    print "Using default values for %s" % (test)
                    use_default = True
                    break
                elif default.lower() == "n":
                    use_default = False
                    break
            generate_test_values(test, use_default, conf)
    conf.save_file()
    print "Output saved to: %s" % (conf.out_file)

if __name__ == "__main__":
    main()
