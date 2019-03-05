import errno
import logging
import os
import subprocess
import socket

import settings

logger = logging.getLogger("cbt")

# this class overrides the communicate() method to check the return code and
# throw an exception if return code is not OK

class CheckedPopen:
    UNINIT=-720
    OK=0
    def __init__(self, args, continue_if_error=False):
        self.args = args[:]
        self.myrtncode = self.UNINIT
        self.continue_if_error = continue_if_error
        self.popen_obj = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        logger.debug('CheckedPopen continue_if_error=%s args=%s'%(str(continue_if_error), ' '.join(args)))

    def __str__(self):
       return 'checked_Popen args=%s continue_if_error=%s rtncode=%d'%(str(self.args), str(self.continue_if_error), self.myrtncode)

    # we transparently check return codes for this method now so callers don't have to

    def communicate(self, input=None, continue_if_error=True):
        (stdoutdata, stderrdata) = self.popen_obj.communicate(input=input)
        self.myrtncode = self.popen_obj.returncode  # THIS is the thing we couldn't do before
        if self.myrtncode != self.OK:
            if not self.continue_if_error:
                raise Exception(str(self)+'\nstdout:\n'+stdoutdata+'\nstderr\n'+stderrdata)
            else:
                logger.warning(' '.join(self.args))
                logger.warning('error %d seen, continuing anyway...'%self.myrtncode)
        return (stdoutdata, stderrdata)

    def wait(self):
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

def pdsh(nodes, command, continue_if_error=True):
    #args = ['pdsh', '-f', str(len(expanded_node_list(nodes))), '-R', 'ssh', '-w', nodes, command]
    args = ['ansible', '-f', str(len(expanded_node_list(nodes))), '-m', 'shell', '-a', command, '-i', nodes, 'all']
    # -S means pdsh fails if any host fails 
    #if not continue_if_error: args.insert(1, '-S')
    return CheckedPopen(args,continue_if_error=continue_if_error)
 

def pdcp(nodes, flags, localfile, remotefile):
    args = ['pdcp', '-f', '10', '-R', 'ssh', '-w', nodes]
    if flags:
        args += [flags]
    return CheckedPopen(args + [localfile, remotefile], 
                        continue_if_error=False)


def rpdcp(nodes, flags, remotefile, localfile):
    #args = ['rpdcp', '-f', '10', '-R', 'ssh', '-w', nodes]
    #args = ['ansible', '-f', '10', '-m', 'fetch', '-a', "flat==yes src=%s dest=%s" % (remotefile, localfile), '-i', nodes, 'all']
    lhost = socket.gethostname()
    args = ['ansible', '-f', str(len(expanded_node_list(nodes))), '-m', 'shell', '-a', "scp -r %s %s:%s" % (remotefile, lhost, localfiles), '-i', nodes, 'all']
#     if flags:
#         args += [flags]
    #return CheckedPopen(args + [remotefile, localfile], 
    #                    continue_if_error=False)
    return CheckedPopen(args,continue_if_error=False)


def scp(node, localfile, remotefile):
    return CheckedPopen(['scp', localfile, '%s:%s' % (node, remotefile)], 
                        continue_if_error=False)


def rscp(node, remotefile, localfile):
    return CheckedPopen(['scp', '%s:%s' % (node, remotefile), localfile],
                        continue_if_error=False)

def get_fqdn_cmd():
    return 'hostname -f'

def get_fqdn_list(nodes):
    stdout, stderr = pdsh(settings.getnodes(nodes), '%s' % get_fqdn_cmd()).communicate()
    print stdout
    ret = [i.split(' ', 1)[1] for i in stdout.splitlines()]
    print ret
    return ret

def clean_remote_dir (remote_dir):
    print "cleaning remote dir %s" % remote_dir
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
