import settings
import subprocess
import time
import os
import errno
import sys

def pdsh(nodes, command, force=False):
    args = ['pdsh', '-R', 'ssh', '-w', nodes, command]
    print('pdsh: %s' % args)
    stdout, stderr = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True).communicate()
    if force:
        return [stdout, stderr]
    if stderr:
        print "[ERROR]:"+stderr+"\n"
        sys.exit()

def pdcp(nodes, flags, localfile, remotefile):
    args = ['pdcp', '-f', '1', '-R', 'ssh', '-w', nodes, localfile, remotefile]
    if flags:
        args = ['pdcp', '-f', '1', '-R', 'ssh', '-w', nodes, flags, localfile, remotefile]
    print('pdcp: %s' % args)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True).communicate()

def rpdcp(nodes, flags, remotefile, localfile):
    args = ['rpdcp', '-f', '1', '-R', 'ssh', '-w', nodes, remotefile, localfile]
    if flags:
        args = ['rpdcp', '-f', '1', '-R', 'ssh', '-w', nodes, flags, remotefile, localfile]
    print('rpdcp: %s'  % args)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True).communicate()

def scp(node, localfile, remotefile):
    args = ['scp', localfile, '%s:%s' % (node, remotefile)]
    print('scp: %s' % args)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True).communicate()

def rscp(node, remotefile, localfile):
    args = ['scp', '%s:%s' % (node, remotefile), localfile]
    print('rscp: %s' % args)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True).communicate()

def make_remote_dir(remote_dir):
    print 'Making remote directory: %s' % remote_dir
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
    pdsh(nodes, 'mkdir -p -m0755 -- %s' % remote_dir)

def sync_files(remote_dir, local_dir):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    pdsh(nodes, 'sudo chown -R %s.%s %s' % (settings.cluster.get('user'), settings.cluster.get('user'), remote_dir))
    rpdcp(nodes, '-r', remote_dir, local_dir)

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

    pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % valdir)
    if mode == 'massif':
        outfile = '%s/%s.massif.out' % (valdir, name)
        return 'valgrind --tool=massif --soname-synonyms=somalloc=*tcmalloc* --massif-out-file=%s --log-file=%s ' % (outfile, logfile)
    if mode == 'memcheck':
        return 'valgrind --tool=memcheck --soname-synonyms=somalloc=*tcmalloc* --log-file=%s ' % (logfile)

    print 'valgrind mode: %s is not supported.' % mode
    return ''
