"""
Common classes to wrap around pdsh (parallel shell)
"""
import errno
import logging
import os
import signal
import socket
import subprocess

import settings

logger = logging.getLogger("cbt")

class Localhost(object):
    """
    This class encapsulates a single dictionary with the information of the localhost
    """ 
    def __init__(self):
        self.local_fqdn = get_fqdn_local()
        self.local_hostname = socket.gethostname()
        self.local_short_hostname = self.local_hostname.split('.')[0]
        self.local_list = ('localhost', self.local_fqdn, self.local_hostname, self.local_short_hostname)

    def is_localhost(self, node):
        """ Returns true if the name refers to the local host """
        if node in self.local_list:
            return node
        return None

#global 
SINGLETON_LOCALHOST = None
def getLocalhost(node):
    global SINGLETON_LOCALHOST 
    if SINGLETON_LOCALHOST is None:
        SINGLETON_LOCALHOST = Localhost()
    return SINGLETON_LOCALHOST.is_localhost(node)

def join_nostr(command):
    if isinstance(command, list):
        return ' '.join(command)
    return command

class CheckedPopen(object):
    """
    This class overrides the communicate() method to check the return code and
    throw an exception if return code is not OK
    """ 
    UNINIT = -720
    OK = 0

    def __init__(self, args, continue_if_error=False, shell=False, env_vars={}):
        logger.debug('CheckedPopen continue_if_error=%s, shell=%s args=%s'
                     % (str(continue_if_error), str(shell), join_nostr(args)))
        env = dict(os.environ)
        env.update(env_vars)
        env['LC_ALL'] = 'C'
        self.args = args[:]
        self.myrtncode = self.UNINIT
        self.continue_if_error = continue_if_error
        self.shell = shell
        self.popen_obj = subprocess.Popen(args, shell=shell,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE,
                                          preexec_fn=os.setsid,
                                          close_fds=True,
                                          env=env)

    def __str__(self):
        return 'checked_Popen args=%s continue_if_error=%s rtncode=%d' % (join_nostr(self.args), str(self.continue_if_error), self.myrtncode)

    # we transparently check return codes for this method now so callers don't have to

    def communicate(self, input=None):
        stdoutdata, stderrdata = self.popen_obj.communicate(input=input)
        stdoutdata = stdoutdata.decode(errors='ignore')
        stderrdata = stderrdata.decode(errors='ignore')
        self.myrtncode = self.popen_obj.returncode  # THIS is the thing we couldn't do before
        if self.myrtncode != self.OK:
            if not self.continue_if_error:
                raise Exception('\n'.join([str(self),
                                           'stdout:', stdoutdata,
                                           'stderr:', stderrdata]))
            else:
                logger.warning(join_nostr(self.args))
                logger.warning('error %d seen, continuing anyway...' % self.myrtncode)
        return stdoutdata, stderrdata

    def wait(self):
        self.communicate()
        return self.myrtncode

    def kill(self, sig=signal.SIGINT):
        if self.shell:
            os.killpg(os.getpgid(self.popen_obj.pid), sig)
        else:
            self.popen_obj.send_signal(sig)


class CheckedPopenLocal(CheckedPopen):
    def __init__(self, host, args, continue_if_error=False, shell=False):
        super(CheckedPopenLocal, self).__init__(args, continue_if_error, shell)
        self.host = host

    def communicate(self, input=None):
        stdout, stderr = super(CheckedPopenLocal, self).communicate()
        stdout = "%s: %s" % (self.host, stdout)
        stderr = "%s: %s" % (self.host, stderr)
        return (stdout, stderr)

# by default, do NOT abort if pdsh returns error status
# this policy results in minimal code change to CBT while allowing
# us to strengthen error checking where it's needed.
#
# we set fanout based on number of nodes in list so that
# pdsh() calls that require full parallelism (workload generation)
# work correctly.


def expanded_node_list(nodes):
    # nodes is a comma-separated list for pdsh "-w" parameter
    # nodes may have some entries with '^' prefix, pdsh syntax meaning
    # we should read list of actual hostnames/ips from a file
    node_list = []
    for h in nodes.split(','):
        if h.startswith('^'):  # pdsh syntax for file containing list of nodes
            with open(h[1:], 'r') as nodefile:
                list_from_file = [h.strip() for h in nodefile.readlines()]
                node_list.extend(list_from_file)
        else:
            node_list.append(h)
    # logger.info("full list of hosts: %s" % str(full_node_list))
    return node_list

# Define an auxiliar method to sanitize the list of nodes, once
def get_localnode(nodes):
    # Similarly to `expanded_node_list(nodes)` we assume the passed nodes
    # param is always string. This is justified as the callers use `nodes`
    # to supply the `-w ...` parameter of ssh during CheckedPopen() call.

    # if more than one node is listed, fallback to pdsh
    nodes_list = expanded_node_list(nodes)
    if len(nodes_list) < 1:
        return None
    return getLocalhost(nodes_list[0])

def sh(local_node, command, continue_if_error=True):
    return CheckedPopenLocal(local_node, join_nostr(command),
                             continue_if_error=continue_if_error, shell=True)

# Follow-up: implement recognise port option
def pdsh(nodes, command, continue_if_error=True):
    local_node = get_localnode(nodes)
    if local_node:
        return sh(local_node, command, continue_if_error=continue_if_error)
    else:
        pdsh_cmd = settings.common.get("pdsh_cmd", "pdsh")
        pdsh_ssh_args = settings.common.get("pdsh_ssh_args", None)
        env = {}
        if pdsh_ssh_args:
            env = {'PDSH_SSH_ARGS':pdsh_ssh_args}
        # -f: fan out n nodes, -R rcmd name (ssh by default), -w target node list
        args = [pdsh_cmd, '-f', str(len(expanded_node_list(nodes))), '-R', 'ssh', '-w', nodes, join_nostr(command)]
        # -S means pdsh fails if any host fails
        if not continue_if_error:
            args.insert(1, '-S')
        return CheckedPopen(args, continue_if_error=continue_if_error, env_vars=env)


def pdcp(nodes, flags, localfile, remotefile):
    local_node = get_localnode(nodes)
    if local_node:
        return sh(local_node, ['cp', flags, localfile, remotefile], continue_if_error=False)
    else:
        pdcp_cmd = settings.common.get("pdcp_cmd", "pdcp")
        pdsh_ssh_args = settings.common.get("pdsh_ssh_args", None)
        env = {}
        if pdsh_ssh_args:
            env = {'PDSH_SSH_ARGS':pdsh_ssh_args}
        args = [pdcp_cmd, '-f', '10', '-R', 'ssh', '-w', nodes]
        if flags:
            args += [flags]
        return CheckedPopen(args + [localfile, remotefile],
                            continue_if_error=False, env_vars=env)


def rpdcp(nodes, flags, remotefile, localdir):
    local_node = get_localnode(nodes)
    if local_node:
        assert len(expanded_node_list(nodes)) == 1
        return sh(local_node, ['for', 'i', 'in', remotefile, ';',
                               'do', 'cp', flags, '${i}', "%s/$(basename ${i}).%s" % (localdir, local_node), ';',
                               'done'],
                  continue_if_error=False)
    else:
        rpdcp_cmd = settings.common.get("rpdcp_cmd", "rpdcp")
        pdsh_ssh_args = settings.common.get("pdsh_ssh_args", None)
        env = {}
        if pdsh_ssh_args:
            env = {'PDSH_SSH_ARGS':pdsh_ssh_args}
        args = [rpdcp_cmd, '-f', '10', '-R', 'ssh', '-w', nodes]
        if flags:
            args += [flags]
        return CheckedPopen(args + [remotefile, localdir],
                            continue_if_error=False, env_vars=env)


def scp(node, localfile, remotefile):
    local_node = get_localnode(node)
    if local_node:
        return sh(local_node, ['cp', localfile, remotefile], continue_if_error=False)
    else:
        return CheckedPopen(['scp', localfile, '%s:%s' % (node, remotefile)],
                            continue_if_error=False)


def rscp(node, remotefile, localfile):
    local_node = get_localnode(node)
    if local_node:
        return sh(local_node, ['cp', remotefile, localfile], continue_if_error=False)
    else:
        return CheckedPopen(['scp', '%s:%s' % (node, remotefile), localfile],
                            continue_if_error=False)


def get_fqdn_cmd():
    return 'hostname -f'


def get_fqdn_list(nodes):
    stdout, stderr = pdsh(settings.getnodes(nodes), '%s' % get_fqdn_cmd()).communicate()
    print(stdout)
    ret = [i.split(' ', 1)[1] for i in stdout.splitlines()]
    print(ret)
    return ret


def get_fqdn_local():
    local_fqdn = socket.getfqdn()
    #logger.debug('get_fqdn_local()=%s' % local_fqdn)
    return local_fqdn


def clean_remote_dir(remote_dir):
    print("cleaning remote dir {}".format(remote_dir))
    if remote_dir == "/" or not os.path.isabs(remote_dir):
        raise SystemExit("Cleaning the remote dir doesn't seem safe, bailing.")

    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
    pdsh(nodes, 'if [ -d "%s" ]; then rm -rf %s; fi' % (remote_dir, remote_dir),
         continue_if_error=False).communicate()


def make_remote_dir(remote_dir):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
    pdsh(nodes, 'mkdir -p -m0755 -- %s' % remote_dir,
         continue_if_error=False).communicate()


def sync_files(remote_dir, local_dir):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    if 'user' in settings.cluster:
        pdsh(nodes,
             'sudo chown -R {0}.{0} {1}'.format(settings.cluster['user'], remote_dir),
             continue_if_error=False).communicate()
    rpdcp(nodes, '-r', remote_dir, local_dir).communicate()


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def setup_valgrind(mode, name, tmp_dir):
    valdir = '%s/valgrind' % tmp_dir
    logfile = '%s/%s.log' % (valdir, name)

    pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % valdir).communicate()

    if mode == 'massif':
        outfile = '%s/%s.massif.out' % (valdir, name)
        return ('valgrind --tool=massif --soname-synonyms=somalloc=*tcmalloc* ' +
                '--massif-out-file=%s --log-file=%s ') % (outfile, logfile)

    if mode == 'memcheck':
        return 'valgrind --tool=memcheck --soname-synonyms=somalloc=*tcmalloc* --log-file=%s ' % (logfile)

    logger.warning('valgrind mode: %s is not supported.', mode)
    return ''


def get_osd_ra():
    for root, directories, files in os.walk('/sys'):
        for filename in files:
            if 'block' in root and 'read_ahead_kb' in filename:
                filename = os.path.join(root, filename)
                try:
                    osd_ra = int(open(filename, 'r').read())
                    return osd_ra
                except ValueError:
                    continue
