import settings
import subprocess
import time
import os
import errno
import logging

logger = logging.getLogger("cbt")

# this class overrides the communicate() method to check the return code and
# throw an exception if return code is not OK

class CheckedPopen:
    UNINIT=-720
    OK=0
    # if you really don't care and want old unchecked behavior, set this to True, 
    # but consider setting "continue_if_error=True" in individual common.pdsh call instead
    turn_off_checking=False  
    def __init__(self, args, continue_if_error=False):
        self.args = args[:]
        self.myrtncode = self.UNINIT
        self.continue_if_error = continue_if_error
        self.popen_obj = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        logger.debug('CheckedPopen continue_if_error=%s args=%s'%(str(continue_if_error), ' '.join(args)))

    def __str__(self):
       return 'checked_Popen args=%s continue_if_error=%s rtncode=%d'%(str(self.args), str(self.continue_if_error), self.myrtncode)

    # we transparently check return codes for this method now so callers don't have to

    def communicate(self):
       (stdoutdata, stderrdata) = self.popen_obj.communicate()
       self.myrtncode = self.popen_obj.returncode  # THIS is the thing we couldn't do before
       if (self.myrtncode != self.OK) and (not self.turn_off_checking):
           if not self.continue_if_error:
               raise Exception(str(self)+'\nstdout:\n'+stdoutdata+'\nstderr\n'+stderrdata)
           else:
               logger.warning(' '.join(self.args))
               logger.warning('error %d seen, continuing anyway...'%self.myrtncode)
       return (stdoutdata, stderrdata)

def pdsh(nodes, command, continue_if_error=False):
    args = ['pdsh', '-R', 'ssh', '-w', nodes, command]
    # -S means pdsh fails if any host fails 
    if not continue_if_error: args.insert(1, '-S')
    return CheckedPopen(args,continue_if_error=continue_if_error)

def pdcp(nodes, flags, localfile, remotefile):
    args = ['pdcp', '-f', '1', '-R', 'ssh', '-w', nodes, localfile, remotefile]
    if flags:
        args = ['pdcp', '-f', '1', '-R', 'ssh', '-w', nodes, flags, localfile, remotefile]
    return CheckedPopen(args)

def rpdcp(nodes, flags, remotefile, localfile):
    args = ['rpdcp', '-f', '1', '-R', 'ssh', '-w', nodes, remotefile, localfile]
    if flags:
        args = ['rpdcp', '-f', '1', '-R', 'ssh', '-w', nodes, flags, remotefile, localfile]
    return CheckedPopen(args)

def scp(node, localfile, remotefile):
    args = ['scp', localfile, '%s:%s' % (node, remotefile)]
    return CheckedPopen(args)

def rscp(node, remotefile, localfile):
    args = ['scp', '%s:%s' % (node, remotefile), localfile]
    return CheckedPopen(args)

# we don't want to stop the run if no such process exists, hence continue_if_error
def killall(nodelist, signalstr, process_name_pattern):
    pdsh(nodelist, 'sudo killall -s %s -q %s'%(signalstr, process_name_pattern), continue_if_error=True).communicate()

# this is used in some places instead of killall for looser matching
# again we don't care if there aren't any such processes
def pkill(nodelist, signalstr, process_name_pattern):
    pdsh(nodelist, 'sudo pkill --signal %s -f %s'%(signalstr, process_name_pattern), continue_if_error=True).communicate()

def make_remote_dir(remote_dir):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
    pdsh(nodes, 'mkdir -p -m0755 -- %s' % remote_dir).communicate()

def sync_files(remote_dir, local_dir):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    pdsh(nodes, 'sudo chown -R %s.%s %s' % (settings.cluster.get('user'), settings.cluster.get('user'), remote_dir))
    rpdcp(nodes, '-r', remote_dir, local_dir).communicate()

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def setup_valgrind(mode, name, tmp_dir):
    valdir = '%s/valgrind' % tmp_dir
    logfile = '%s/%s.log' % (valdir, name)

    pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % valdir).communicate()
    if mode == 'massif':
        outfile = '%s/%s.massif.out' % (valdir, name)
        return 'valgrind --tool=massif --soname-synonyms=somalloc=*tcmalloc* --massif-out-file=%s --log-file=%s ' % (outfile, logfile)
    if mode == 'memcheck':
        return 'valgrind --tool=memcheck --soname-synonyms=somalloc=*tcmalloc* --log-file=%s ' % (logfile)

    logger.warn('valgrind mode: %s is not supported.' % mode)
    return ''
