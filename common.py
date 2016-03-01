import errno
import logging
import os
import subprocess

import settings

logger = logging.getLogger("cbt")


def popen(args):
    logger.debug('%s', " ".join(args))
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)


def pdsh(nodes, command):
    return popen(['pdsh', '-R', 'ssh', '-w', nodes, command])


def pdcp(nodes, flags, localfile, remotefile):
    args = ['pdcp', '-f', '1', '-R', 'ssh', '-w', nodes]
    if flags:
        args += [flags]
    return popen(args + [localfile, remotefile])


def rpdcp(nodes, flags, remotefile, localfile):
    args = ['rpdcp', '-f', '1', '-R', 'ssh', '-w', nodes]
    if flags:
        args += [flags]
    return popen(args + [remotefile, localfile])


def scp(node, localfile, remotefile):
    return popen(['scp', localfile, '%s:%s' % (node, remotefile)])


def rscp(node, remotefile, localfile):
    return popen(['scp', '%s:%s' % (node, remotefile), localfile])


def make_remote_dir(remote_dir):
    logger.info('Making remote directory: %s', remote_dir)
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
    pdsh(nodes, 'mkdir -p -m0755 -- %s' % remote_dir).communicate()


def sync_files(remote_dir, local_dir):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    if 'user' in settings.cluster:
        pdsh(nodes, 'sudo chown -R {0}.{0} {1}'.format(settings.cluster['user'], remote_dir)).communicate()
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
