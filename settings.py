"""
This module does the initialization and setup work for CBT given the clusters from the given config files in the CLI.
Also does the node_names -> string conversion
"""
import argparse
import yaml
import sys
import os
import logging

# using the same-old same-old logger we've been using from the very beginning
logger = logging.getLogger("cbt")

# dictionaries to hold the 'cluster' and 'benchmarks' entries from the YAML file given in config
cluster = {}
benchmarks = {}


def initialize(ctx):
    """Operate on the YAML file(s) to determine the 'cluster' and 'benchmark' parts.
    Verifying if they exist, creating result directories, and such.
    Basically, initializing the benchmarking process from the CBT end. """

    # referencing the global dictionaries
    global cluster, benchmarks

    # array to hold the YAML file objects given after parsing, each YAML tag is converted to a python object
    # for easier manipulation
    config = {}
    try:
        # going to open the given 'config-file' argument to CBT as a file
        with file(ctx.config_file) as f:
            # update the config dictionary with all the objects returned by the safe_load_all function operating on the file
            map(config.update, yaml.safe_load_all(f))

    except IOError, e:
        # if not able to open the config file, throw an exception!
        raise argparse.ArgumentTypeError(str(e))

    # retrieve the object named 'cluster' from the dictionary, otherwise return an empty dictionary!
    cluster = config.get('cluster', {})
    # retrieve the object named 'benchmarks' from the dictionary, otherwise return an empty dictionary!
    benchmarks = config.get('benchmarks', {})

    # if no cluster tag in YAML config file, exit with error message!
    if not cluster:
        shutdown('No cluster section found in config file, bailing.')

    # if no benchmarks tag in YAML config file, exit with the error message!
    if not benchmarks:
        shutdown('No benchmarks section found in config file, bailing.')

    # store cbt configuration in the archive directory
    # create corresponding path objects with desired dir/file names
    cbt_results = os.path.join(ctx.archive, 'results')
    config_file = os.path.join(cbt_results, 'cbt_config.yaml')
    # create directories if they don't exist already, inside the archive directory provided as the CLI argument
    if not os.path.exists(ctx.archive):
        os.makedirs(ctx.archive)
    if not os.path.exists(cbt_results):
        os.makedirs(cbt_results)
    # dump the python object of the config file into archive dir through serialization
    if not os.path.exists(config_file):
        # create a new dictionary with the given objects in fields
        config_dict = dict(cluster=cluster, benchmarks=benchmarks)
        with open(config_file, 'w') as fd:
            # this is the actual action, writing YAML-type object config_dict, into a stream given by descriptor fd
            yaml.dump(config_dict, fd, default_flow_style=False)

    # set the tmp_dir if not set.
    if 'tmp_dir' not in cluster:
        # create a temp directory with PID name for temporal uniqueness
        cluster['tmp_dir'] = '/tmp/cbt.%s' % os.getpid()

    # set the ceph.conf file from the commandline, yaml, or default
    if ctx.conf:
        cluster['conf_file'] = ctx.conf
    elif 'conf_file' not in cluster:
        cluster['conf_file'] = "%s/ceph.conf" % (cluster.get('conf_file'),)

    if ctx.archive:
        cluster['archive_dir'] = ctx.archive


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
    ret = [node for node in nodes if node]

    # determine the Ceph user created during installation
    user = cluster.get('user')
    if user is not None:
        # return a string of format user@node for each node
        ret = ['%s@%s' % (user, node) for node in ret]

    # return this set of strings
    return set(ret)


def shutdown(message):
    """exit the program with a custom message"""
    sys.exit(message)
