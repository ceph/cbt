"""
This module contains common utilities that have been tailor made to handle the given ceph cluster.
Most importantly, it implements the 'return code checking' on the subprocess.Popen.communicate() function
by overriding the function and performing error handling. It provides a 'CheckedPopen' class as a wrapper
to the actual subprocess.Popen with all the nice error handling embedded.
"""
import errno
import logging
import os
import subprocess

import settings

# acquire the pointer to the logging object for logging
logger = logging.getLogger("cbt")

# this class overrides the communicate() method to check the return code and
# throw an exception if return code is not OK

# The idea is to create this class with all the error checking done on the return code of subprocess.Popen
#  and then create functions that can take inputs as arguments of Popen, and return a CheckedPopen object
#  which can then be operated on to use all the error checking performed.
class CheckedPopen:
    """A wrapper around subprocess.Popen() for return code processing and resiliency."""
    # uninitialized return value integer
    UNINIT=-720
    # a correct return value interger
    OK=0

    def __init__(self, args, continue_if_error=False):
        """Initialize the popen object with given data, and log the stuff."""
        # arguments to pass to popen
        self.args = args[:]
        # the return value from popen
        self.myrtncode = self.UNINIT
        # continue if error parameter
        self.continue_if_error = continue_if_error
        # the process STDOUT and STDERR stream data is going to be processed upon return of the function
        # therefore, there's a need to initialize the stdout= and stderr= parameters with a PIPE which will 
        # allow communication with the process
        # also, close fd's upon process termination, for safekeeping
        self.popen_obj = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        # log some debugging stuff in CBT debugger
        logger.debug('CheckedPopen continue_if_error=%s args=%s'%(str(continue_if_error), ' '.join(args)))

    def __str__(self):
       """Good old __str___ for object printing"""
       return 'checked_Popen args=%s continue_if_error=%s rtncode=%d'%(str(self.args), str(self.continue_if_error), self.myrtncode)

    # we transparently check return codes for this method now so callers don't have to

    # input is the data to send to the STDIN of the process
    def communicate(self, input=None, continue_if_error=True):
        """Wrapper of the subprocess.Popen.communicate() for fancier error handling"""

        # let the process run, give 'input' data to STDIN, read data from STDOUT STDERR and place it in a tuple, wait till finish
        (stdoutdata, stderrdata) = self.popen_obj.communicate(input=input)

        self.myrtncode = self.popen_obj.returncode  # THIS is the thing we couldn't do before

        # if something went wrong when creating the process
        if self.myrtncode != self.OK:
            # if we weren't supposed to continue in case of an error
            if not self.continue_if_error:
                # throw and exception and bail out!
                raise Exception(str(self)+'\nstdout:\n'+stdoutdata+'\nstderr\n'+stderrdata)
            else:
                # otherwise, log a warning and continue with the program
                logger.warning(' '.join(self.args))
                logger.warning('error %d seen, continuing anyway...'%self.myrtncode)
        # return the STDstream data of the process taken from the subprocess.PIPEs given to it
        return (stdoutdata, stderrdata)

    def wait(self):
        """Simply wait till the process ends, and give it's return code upon return."""
        self.communicate(continue_if_error=True)
        return self.myrtncode

# by default, do NOT abort if pdsh returns error status
# this policy results in minimal code change to CBT while allowing
# us to strengthen error checking where it's needed.
#
# we set fanout based on number of nodes in list so that
# pdsh() calls that require full parallelism (workload generation)
# work correctly.

def expanded_node_list(nodes):
    """"implement some pdsh syntax for converting a 'nodes' list into a string.
    Basically checks if the given nodes are in a file, or just a simple CLI arg."""
    # nodes is a comma-separated list for pdsh "-w" parameter
    # nodes may have some entries with '^' prefix, pdsh syntax meaning
    # we should read list of actual hostnames/ips from a file 
    node_list = []
    for h in nodes.split(','):
        if h.startswith('^'):  # pdsh syntax for file containing list of nodes
            with open(h[1:], 'r') as nodefile:
                list_from_file = [ h.strip() for h in nodefile.readlines() ]
                node_list.extend(list_from_file)
        else:
            node_list.append(h)
    #logger.info("full list of hosts: %s" % str(full_node_list))
    return node_list

# return the PDSH object with error checking
def pdsh(nodes, command, continue_if_error=True):
    """PDSH using the error handled popen implemented as CheckedPopen"""
    args = ['pdsh', '-f', str(len(expanded_node_list(nodes))), '-R', 'ssh', '-w', nodes, command]
    # -S means pdsh fails if any host fails 
    if not continue_if_error: args.insert(1, '-S')
    return CheckedPopen(args,continue_if_error=continue_if_error)
 
# return the PDCP object with error checking
def pdcp(nodes, flags, localfile, remotefile):
    """PDCP using the error handled popen implemented as CheckedPopen"""
    args = ['pdcp', '-f', '10', '-R', 'ssh', '-w', nodes]
    if flags:
        args += [flags]
    return CheckedPopen(args + [localfile, remotefile], 
                        continue_if_error=False)

# return the reverse PDCP object with error checking
def rpdcp(nodes, flags, remotefile, localfile):
    """RPDCP using the error handled popen implemented as CheckedPopen"""
    args = ['rpdcp', '-f', '10', '-R', 'ssh', '-w', nodes]
    if flags:
        args += [flags]
    return CheckedPopen(args + [remotefile, localfile], 
                        continue_if_error=False)

# return the SCP object with error checking
def scp(node, localfile, remotefile):
    """SCP using the error handled popen implemented as CheckedPopen"""
    return CheckedPopen(['scp', localfile, '%s:%s' % (node, remotefile)], 
                        continue_if_error=False)

# return the reverse SCP object with error checking
def rscp(node, remotefile, localfile):
    """RSCP using the error handled popen implemented as CheckedPopen"""
    return CheckedPopen(['scp', '%s:%s' % (node, remotefile), localfile],
                        continue_if_error=False)

# simple enough to understand
def get_fqdn_cmd():
    """return a fqdn of a host with 'hostname' -f"""
    return 'hostname -f'

# same thing for a list of nodes
def get_fqdn_list(nodes):
    """Get fqdn of each node in the cluster and return as list"""

    # run the checkedpopen process and get the data
    stdout, stderr = pdsh(settings.getnodes(nodes), '%s' % get_fqdn_cmd()).communicate()
    print stdout
    # only do one split, and return the second element from the split 
    ret = [i.split(' ', 1)[1] for i in stdout.splitlines()]
    print ret
    # return the fqdn list
    return ret

# clean up a given directory on each cluster node
def clean_remote_dir (remote_dir):
    """Do a simple rm -rf on a given remote_directory for all cluster nodes."""
    # printing debug message
    print "cleaning remote dir %s" % remote_dir

    # if it's / or absolute directory, don't delete it!
    if remote_dir == "/" or not os.path.isabs(remote_dir):
       raise SystemExit("Cleaning the remote dir doesn't seem safe, bailing.")

    # get the list of all nodes given in the YAML file
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
    # run the subprocess to get rid of the directory data
    pdsh(nodes, 'if [ -d "%s" ]; then rm -rf %s; fi' % (remote_dir, remote_dir),
         continue_if_error=False).communicate()

# create a given directory on each cluster node
def make_remote_dir(remote_dir):
    """Do a simple mkdir -p on each cluster node"""

    # get the node list
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
    # run the mkdir process on each
    pdsh(nodes, 'sudo mkdir -p -m0755 -- %s' % remote_dir,
         continue_if_error=False).communicate()

# sync up the contents of a remote dir with local dir
def sync_files(remote_dir, local_dir):
    """Sync files between a remote directory and local directory.
    Create dirs if they don't exist, and then copy files over here with reverse PDCP"""
    
    # get all the node hostnames
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')

    # if a directory doesn't exist locally, create it
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # if a user was mentioned in YAML, change ownership of remote files to that user before copying over - DON'T KNOW WHY
    if 'user' in settings.cluster:
        try:
            pdsh(nodes, 
                'sudo chown -R {0}.{0} {1}'.format(settings.cluster['user'], remote_dir),
                continue_if_error=False).communicate()
        except OSError as e:
            # log it as a warning, if remote files don't exist
            logger.warning("Exception in common.py @sync_files %s" % e.message)
        except Exception as e:
            logger.warning("Exception in common.py @sync_files %s" % e.message)

    # copy files from remote host
    try:
        rpdcp(nodes, '-r', remote_dir, local_dir).communicate()
    except OSError as e:
        logger.warning("Exception in common.py @sync_files %s" % e.message)
    except Exception as e:
        logger.warning("Exception in common.py @sync_files %s" % e.message)

# mkdir -p for the current node
def mkdir_p(path):
    """Same old mkdir -p for this node"""
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

# setup valgrind with given log filename, in the given tmp_dir
def setup_valgrind(mode, name, tmp_dir):
    """Setup the valgrind dir on each cluster node at tmp_dir.
    Return the string with command to run a specific 'mode' of valgrind.
    Return empty string if mode isn't supported, after logging a warning."""

    # mode simply referes to the tool which is to be used with valgrind. 
    # Two supported ones are 'massif' and 'memcheck'

    # directory in which to do valgrind stuff
    valdir = '%s/valgrind' % tmp_dir
    # new file to create
    logfile = '%s/%s.log' % (valdir, name)
    # create the directory on each node
    pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % valdir).communicate()

    # if the tool being used is massif, create output log file accordingly
    if mode == 'massif':
        outfile = '%s/%s.massif.out' % (valdir, name)
        # call valgrind with corresponding params
        return ('valgrind --tool=massif --soname-synonyms=somalloc=*tcmalloc* ' +
                '--massif-out-file=%s --log-file=%s ') % (outfile, logfile)

    # if the tool being used is memcheck, so extra out file is needed, simply call valgrind with params
    if mode == 'memcheck':
        return 'valgrind --tool=memcheck --soname-synonyms=somalloc=*tcmalloc* --log-file=%s ' % (logfile)

    # if mode wasn't 'memcheck' or 'massif' then it's not supported, log a warning, and bail out!
    logger.warning('valgrind mode: %s is not supported.', mode)
    return ''

# get the size of OSD ReadAhead cache size in kilobytes
def get_osd_ra():
    """Read size of read_ahead_kb in /sys, return as an integer if it exists.
    Otherwise, raise an exception."""
    # go through the /sys file system
    for root, directories, files in os.walk('/sys'):
        # for all the files available
        for filename in files:
            # if there is a file name with 'read_ahead_kb' in block dir
            if 'block' in root and 'read_ahead_kb' in filename:
                # get it's filename, this is what we need
                filename = os.path.join(root, filename)
                try:
                    # try to read from that file, the RA size of OSD
                    osd_ra = int(open(filename, 'r').read())
                    # return if found
                    return osd_ra
                    #Otherwise raise an exception, and continue to look in other files
                except ValueError:
                    continue
