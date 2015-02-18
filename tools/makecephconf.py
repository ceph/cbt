#!/usr/bin/python
import argparse
import os
import subprocess
import sys
import yaml
import time
import copy

def read_config(config_file):
    config = {}
    try:
        with file(config_file) as f:
            g = yaml.safe_load_all(f)
            for new in g:
                config.update(new)
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))
    return config

def parse_args():
    parser = argparse.ArgumentParser(description='Continuously run ceph tests.')
    parser.add_argument(
        '--target',
        required = True,
        help = 'Directory where the config files should go.',
        )
    parser.add_argument(
        'config_file',
        help = 'YAML config file.',
        )
    args = parser.parse_args()
    return args

def populate(l, name, value):
    name = name.replace("_", " ")
    l.append("        %s = %s" % (name, value))

def mkosds(lists, yaml):
    i = 0
    for server in yaml.get('osd_servers', []):
        for j in xrange(0, yaml.get('osds_per_server', 0)):
            name = "osd.%d" % i
            lists[name] = []
            lists[name].append("        host = %s" % server)
            lists[name].append("        osd data = /srv/osd-device-%d-data" % j)
            lists[name].append("        osd journal = /srv/osd-device-%d-data/journal" % j)
#            lists[name].append("        osd journal = /dev/disk/by-partlabel/osd-device-%d-journal" % j)
            i += 1

def writescript(f, param, value, conf):
    for fs,rtconf in sorted(runtests_conf.iteritems()):
        pdir = param
        if value:
            pdir = "%s_%s"  % (param, value)
        f.write("%s --conf %s --archive %s/%s/%s %s\n" % (runtests_exec, conf, outdir, fs, pdir, rtconf))

def parametric(lists, yaml):
    if "global" not in lists:
        lists["global"] = []
    scriptname = "%s/runme.sh" % target
    f = open(scriptname,'w')
    f.write("#!/bin/bash\n")

    # the default
    filename = "%s/default.ceph.conf" % target
    writefile(lists, filename)
    writescript(f, "default", "", filename)

    for param,value in sorted(yaml.iteritems()):
        if (isinstance(value, dict)):
            lc = copy.deepcopy(lists)
            for k,v in sorted(value.iteritems()):
                populate(lc.get("global"), k, v)
            filename = "%s/%s.ceph.conf" % (target, param)
            writefile(lc, filename)
            writescript(f, param, "", filename) 
        elif (isinstance(value, list)):
            for vi in value:
                lc = copy.deepcopy(lists)
                populate(lc.get("global"), param, vi)
                filename = "%s/%s_%s.ceph.conf" % (target, param, vi)
                writefile(lc, filename)
                writescript(f, param, vi, filename)
        else:
            lc = copy.deepcopy(lists)
            populate(lc.get("global"), param, value)
            filename = "%s/%s_%s.ceph.conf" % (target, param, value)
            writefile(lc, filename)
            writescript(f, param, value, filename)
    f.close()
    os.chmod(scriptname, 0755)
    
def writefile(lists, out):
    f = open(out,'w')
#    print out
    for k,v in sorted(lists.iteritems()):
        f.write("[%s]\n" % k)
        for line in v: f.write("%s\n" % line)
        f.write("\n")
    f.close()

target = ""
outdir = ""
runtests_exec = ""
runtests_conf = {} 

if __name__ == '__main__':
    ctx = parse_args()
    config = read_config(ctx.config_file)

    target = os.path.abspath(ctx.target)
    os.system("mkdir -p -m0755 -- %s" % target)

    settings = config.get("settings", {})
    runtests_exec = settings.get("runtests_exec", "")
    runtests_conf = settings.get("runtests_conf", {})
    outdir = settings.get("outdir", "")

    default = config.get("default", {})
    lists = {}
    for section in default:
        lists[section] = []
        for k,v in default.get(section).iteritems():
            populate(lists.get(section), k, v) 
    mkosds(lists, config.get("settings", {}))
    parametric(lists, config.get("parametric", {}))
