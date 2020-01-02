"""
This module does the initialization and setup work for CBT given the clusters from the given config files in the CLI.
Also does the node_names -> string conversion
"""
import argparse
import yaml
import sys
import os
import socket
import logging

# using the same-old same-old logger we've been using from the very beginning
logger = logging.getLogger("cbt")

# dictionaries to hold the 'cluster' and 'benchmarks' entries from the YAML file given in config
cluster = {}
client_endpoints = {}
benchmarks = {}
monitoring_profiles = {}

def _handle_monitoring_legacy():
    """
    Inject collectl even if the config says nothing about it to preserve
    compatibility with current CBT's configuration files.
    """
    global monitoring_profiles
    if 'collectl' not in monitoring_profiles:
        monitoring_profiles['collectl'] = {}

def initialize(ctx):
    """Operate on the YAML file(s) to determine the 'cluster' and 'benchmark' parts.
    Verifying if they exist, creating result directories, and such.
    Basically, initializing the benchmarking process from the CBT end. """

    # referencing the global dictionaries
    global cluster, client_endpoints, benchmarks, monitoring_profiles

    # array to hold the YAML file objects given after parsing, each YAML tag is converted to a python object
    # for easier manipulation
    config = {}
    try:
        # going to open the given 'config-file' argument to CBT as a file
        with open(ctx.config_file) as f:
        # get all the parameters from the file
            config = yaml.safe_load(f)
    except IOError as e:
        raise argparse.ArgumentTypeError(str(e))

    # retrieve the object named 'cluster' from the dictionary, otherwise return an empty dictionary!
    cluster = config.get('cluster', {})
    client_endpoints = config.get('client_endpoints', {})
    # retrieve the object named 'benchmarks' from the dictionary, otherwise return an empty dictionary!
    benchmarks = config.get('benchmarks', {})
    monitoring_profiles = config.get('monitoring_profiles', dict(collectl={}))

    # if no cluster tag in YAML config file, exit with error message!
    if not cluster:
        shutdown('No cluster section found in config file, bailing.')

    # if no benchmarks tag in YAML config file, exit with the error message!
    if not benchmarks:
        shutdown('No benchmarks section found in config file, bailing.')

    # set the archive_dir from the commandline if present
    if ctx.archive:
        cluster['archive_dir'] = ctx.archive
    if 'archive_dir' not in cluster:
        shutdown('No archive dir has been set.')

    _handle_monitoring_legacy()

    # store cbt configuration in the archive directory
    # create corresponding path objects with desired dir/file names
    cbt_results = os.path.join(cluster['archive_dir'], 'results')
    config_file = os.path.join(cbt_results, 'cbt_config.yaml')

    # create directories if they don't exist already, inside the archive directory provided as the CLI argument
    if not os.path.exists(cluster['archive_dir']):
        os.makedirs(cluster['archive_dir'])
    if not os.path.exists(cbt_results):
        os.makedirs(cbt_results)
    # dump the python object of the config file into archive dir through serialization
    if not os.path.exists(config_file):
        # create a new dictionary with the given objects in fields
        config_dict = dict(cluster=cluster, benchmarks=benchmarks)
        with open(config_file, 'w') as fd:
            # this is the actual action, writing YAML-type object config_dict, into a stream given by descriptor fd
            yaml.dump(config_dict, fd, default_flow_style=False)
        print '>>>>>>>>>>'
        conf_loc = config_dict["cluster"]["conf_file"]
        conf_cp = os.path.join(cbt_results, 'ceph.conf')
        command = 'cp ' + conf_loc + ' ' + conf_cp
      #  print command
        os.system(command)
    # set the tmp_dir if not set.
    if 'tmp_dir' not in cluster:
        # create a temp directory with PID name for temporal uniqueness
        cluster['tmp_dir'] = '/tmp/cbt.%s' % os.getpid()

    # set the ceph.conf file from the commandline if present
    if ctx.conf:
        cluster['conf_file'] = ctx.conf
    # If no conf file is set, default to /etc/ceph/ceph.conf
    # FIXME: We shouldn't have cluster specific defaults in settings.
    # Eventually make a base class with specific cluster implementations.
    if 'conf_file' not in cluster:
        cluster['conf_file'] = '/etc/ceph/ceph.conf'
    try:
        f = open(cluster['conf_file'])
        f.close()
    except IOError as e:
        shutdown('Was not able to access conf file: %s' % cluster['conf_file'])

def host_info(host):
    ret = {}
    user = cluster.get('user')

    if '@' in host:
        user, host = host.split('@')
        ret['user'] = user
    if user:
        ret['user'] = user
    ret['host'] = host
    ret['addr'] = socket.gethostbyname(host)
    return ret

def getnodes(*nodelists):
    """Convert the given node list (probably in YAML) to a string with names appended"""
    nodes = []

    # convert the object into a list of node names
    for nodelist in nodelists:
        # bascially, iterate over each node list given and add them up to get one big list
        cur = cluster.get(nodelist, [])
        if isinstance(cur, str):
            nodes.append(cur)
        elif isinstance(cur, dict):
            nodes.extend(cur.keys())
        elif isinstance(cur, list):
            nodes.extend(cur)
        else:
            raise ValueError("Can't process nodes of type %s - unknown set type: %r",
                             nodelist, cur)

    # join all the node names (after applying uniqueness) in comma separated values
    str_nodes = ','.join(uniquenodes(nodes))
    # add the info in the 'cbt' logger
    logger.debug("Nodes : %s", str_nodes)

    # return the nodes_string to the caller function
    return str_nodes


def uniquenodes(nodes):
    """Filter out for empty strings in nodes, also use list comprehension to perform uniqueness check.
    Give output as a set of user@node strings."""
    # rule out empty strings
    unique = [node for node in nodes if node]
    ret = []

    for host in unique:
        info = host_info(host)
        host_str = info['host']
        if 'user' in info:
            # return a string of format user@node for each node
            host_str = "%s@%s" % (info['user'], host_str)
        ret.append(host_str)
    return set(ret)

def shutdown(message):
    """exit the program with a custom message"""
    sys.exit(message)
