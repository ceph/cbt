import time
import socket
import shutil
import logging
import os.path
import getpass
import StringIO
import threading
import subprocess


import paramiko
from concurrent.futures import ThreadPoolExecutor


logger = logging.getLogger("cbt.ssh")


__doc__ = "SSH utils"


class Local(object):
    """
    simulate ssh connection to local node
    """

    @classmethod
    def open_sftp(cls):
        return cls()

    @classmethod
    def mkdir(cls, remotepath, mode=None):
        os.mkdir(remotepath)
        if mode is not None:
            os.chmod(remotepath, mode)

    @classmethod
    def put(cls, localfile, remfile):
        dirname = os.path.dirname(remfile)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        shutil.copyfile(localfile, remfile)

    @classmethod
    def get(cls, remfile, localfile):
        dirname = os.path.dirname(localfile)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        shutil.copyfile(remfile, localfile)

    @classmethod
    def chmod(cls, path, mode):
        os.chmod(path, mode)

    @classmethod
    def copytree(cls, src, dst):
        shutil.copytree(src, dst)

    @classmethod
    def remove(cls, path):
        os.unlink(path)

    @classmethod
    def close(cls):
        pass

    @classmethod
    def open(cls, *args, **kwarhgs):
        return open(*args, **kwarhgs)

    @classmethod
    def stat(cls, path):
        return os.stat(path)

    def __enter__(self):
        return self

    def __exit__(self, x, y, z):
        return False


# map node str to ssh connection
NODES_LOCK = threading.Lock()
NODES = {}

# map node str to ssh key for node
NODE_KEYS_LOCK = threading.Lock()
NODE_KEYS = {}


def set_key_for_node(conn_url, key):
    "set ssh key, which would be used to connect to node"
    sio = StringIO.StringIO(key)
    with NODE_KEYS_LOCK:
        NODE_KEYS[conn_url] = paramiko.RSAKey.from_private_key(sio)


SSH_TCP_TIMEOUT = 15
SSH_BANNER_TIMEOUT = 30


def ssh_connect(conn_url, conn_timeout=60):
    """
    connect to node, with paramiko, return opened ssh connection object
    store object in cache for future usage

    conn_url:str - [user@]node[:port]
    conn_timeout:int - connect timeout in seconds
    """

    with NODES_LOCK:
        if conn_url in NODES:
            return NODES[conn_url]

    if conn_url == 'local':
        return Local()

    etime = time.time() + conn_timeout

    if '@' in conn_url:
        user, node = conn_url.split("@")
    else:
        user = getpass.getuser()
        node = conn_url

    if ':' in node:
        node, port = node.split(':')
        port = int(port)
    else:
        port = 22  # SSH default port

    with NODE_KEYS_LOCK:
        if conn_url in NODE_KEYS:
            ssh_params = {'pkey': NODE_KEYS[conn_url]}
        else:
            ssh_params = {}

    if 'pkey' not in ssh_params:
        ssh_params['key_filename'] = os.path.expanduser('~/.ssh/id_rsa')

    ssh = paramiko.SSHClient()
    ssh.load_host_keys('/dev/null')
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.known_hosts = None

    while True:
        try:
            tleft = etime - time.time()
            c_tcp_timeout = min(SSH_TCP_TIMEOUT, tleft)

            if paramiko.__version_info__ >= (1, 15, 2):
                ssh_params['banner_timeout'] = min(SSH_BANNER_TIMEOUT, tleft)

            logger.debug("Connecting to %s@%s:%s %r", user, node, port, ssh_params)
            ssh.connect(node,
                        username=user,
                        timeout=c_tcp_timeout,
                        look_for_keys=False,
                        port=port,
                        **ssh_params)
            break
        except paramiko.PasswordRequiredException:
            raise
        except (socket.error, paramiko.SSHException):
            if time.time() > etime:
                raise
            time.sleep(1)

    with NODES_LOCK:
        if conn_url in NODES:
            NODES[conn_url] = ssh

    return ssh


class BGSSHTask(object):
    """
    run program in romote node in background
    """
    def __init__(self, node, use_sudo):
        self.node = node
        self.pid = None
        self.use_sudo = use_sudo

    def start(self, orig_cmd, **params):
        uniq_name = 'test'
        cmd = "screen -S {0} -d -m {1}".format(uniq_name, orig_cmd)
        run_over_ssh(self.node.connection, cmd,
                     timeout=10, node=self.node.get_conn_id(),
                     **params)
        processes = run_over_ssh(self.node.connection, "ps aux", nolog=True)

        for proc in processes.split("\n"):
            if orig_cmd in proc and "SCREEN" not in proc:
                self.pid = proc.split()[1]
                break
        else:
            self.pid = -1

    def check_running(self):
        assert self.pid is not None
        try:
            run_over_ssh(self.node.connection,
                         "ls /proc/{0}".format(self.pid),
                         timeout=10, nolog=True)
            return True
        except OSError:
            return False

    def kill(self, soft=True, use_sudo=True):
        assert self.pid is not None
        try:
            if soft:
                cmd = "kill {0}"
            else:
                cmd = "kill -9 {0}"

            if self.use_sudo:
                cmd = "sudo " + cmd

            run_over_ssh(self.node.connection,
                         cmd.format(self.pid), nolog=True)
            return True
        except OSError:
            return False

    def wait(self, soft_timeout, timeout):
        end_of_wait_time = timeout + time.time()
        soft_end_of_wait_time = soft_timeout + time.time()

        time_till_check = 2
        time_till_first_check = 2

        time.sleep(time_till_first_check)
        if not self.check_running():
            return True

        while self.check_running() and time.time() < soft_end_of_wait_time:
            time.sleep(soft_end_of_wait_time - time.time())

        while end_of_wait_time > time.time():
            time.sleep(time_till_check)
            if not self.check_running():
                break
        else:
            self.kill()
            time.sleep(1)
            if self.check_running():
                self.kill(soft=False)
            return False
        return True


class SSHCmdFailed(OSError):
    "Command failed"
    def __init__(self, cmd, res, output):
        OSError.__init__(self,
                         "Cmd {0!r} failed with code {1}".format(cmd, res))
        self.cmd = cmd
        self.res = res
        self.output = output


class PSSHCmdFailed(OSError):
    "Parrallel command failed"
    MAX_MSG_NODES = 3

    def __init__(self, failed_nodes, cmd, values):
        if len(failed_nodes) > self.MAX_MSG_NODES:
            nodes_str = ",".join(failed_nodes[:self.MAX_MSG_NODES]) + ',....'
        else:
            nodes_str = ",".join(failed_nodes)

        OSError.__init__(self,
                         "Cmd {0!r} failed on node(s) {1}".format(cmd, nodes_str))
        self.cmd = cmd
        self.res = values


class SSHExecutionClass(object):
    def __init__(self, command, nodes, pool_iter):
        self.pool_iter = pool_iter
        self.nodes = nodes
        self.command = command

    def communicate(self):
        vals = dict(zip(self.nodes, self.pool_iter))

        failed_nodes = [node for node, res in vals.items()
                        if isinstance(res, Exception)]

        if failed_nodes != []:
            raise PSSHCmdFailed(failed_nodes, self.command, vals)

        out = ""
        for node, output in vals.items():
            pref = node + ": "
            out += pref + output.replace("\n", "\n" + pref)
        return out, ""


def get_nodes_lists(nodes):
    nodes_list = nodes.split(",")
    no_user_list = [(node.split('@')[1] if '@' in node else node)
                    for node in nodes_list]
    return nodes_list, no_user_list


def pdsh(nodes, command, stdin_data=None, MAX_THREADS=32, silent=False):
    def worker(node):
        try:
            conn = ssh_connect(node)
            return run_over_ssh(conn, command, stdin_data=stdin_data)
        except Exception as exc:
            logger.exception("x")
            if not silent:
                return exc

    nodes_list, no_user_list = get_nodes_lists(nodes)
    return SSHExecutionClass(command, no_user_list,
                             map(worker, nodes_list))

    with ThreadPoolExecutor(MAX_THREADS) as pool:
        return SSHExecutionClass(command, no_user_list,
                                 pool.map(worker, nodes_list))


def pdcp(nodes, flags, localfile, remotefile, MAX_THREADS=32, silent=False):
    assert flags is None or flags == ''

    def worker(node):
        try:
            conn = ssh_connect(node)
            with conn.open_sftp() as sftp:
                # if preserve_perm:
                #     sftp.chmod(remotefile,
                #                os.stat(localfile).st_mode & ALL_RWX_MODE)
                return sftp.put(localfile, remotefile)
        except Exception as exc:
            logger.exception("x")
            if not silent:
                return exc

    nodes_list, no_user_list = get_nodes_lists(nodes)
    return SSHExecutionClass("<copy {0}>".format(localfile),
                             no_user_list,
                             map(worker, nodes_list))

    with ThreadPoolExecutor(MAX_THREADS) as pool:
        return SSHExecutionClass("<copy {0}>".format(localfile),
                                 no_user_list,
                                 pool.map(worker, nodes_list))


def rpdcp(nodes, flags, remotefile, localfile, MAX_THREADS=32, silent=False):
    assert flags is None or flags == ''

    def worker(node):
        try:
            conn = ssh_connect(node)
            with conn.open_sftp() as sftp:
                sftp.get(localfile, remotefile)
        except:
            if not silent:
                raise

    return list(map(worker, nodes.split(",")))

    nodes_list, _ = get_nodes_lists(nodes)
    with ThreadPoolExecutor(MAX_THREADS) as pool:
        list(pool.map(worker, nodes_list))


def scp(node, localfile, remotefile):
    return pdcp([node], None, localfile, remotefile)


def rscp(node, remotefile, localfile):
    return rpdcp([node], None, remotefile, localfile)


ALL_SESSIONS_LOCK = threading.Lock()
ALL_SESSIONS = {}


def run_over_ssh(conn, cmd, stdin_data=None, timeout=60,
                 nolog=False, node=None):
    "should be replaces by normal implementation, with select"

    if isinstance(conn, Local):
        if not nolog:
            logger.debug("SSH:local Exec {0!r}".format(cmd))
        proc = subprocess.Popen(cmd, shell=True,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)

        stdoutdata, _ = proc.communicate(input=stdin_data)
        if proc.returncode != 0:
            templ = "SSH:{0} Cmd {1!r} failed with code {2}. Output: {3}"
            raise OSError(templ.format(node, cmd, proc.returncode, stdoutdata))

        return stdoutdata

    transport = conn.get_transport()
    session = transport.open_session()

    if node is None:
        node = ""

    with ALL_SESSIONS_LOCK:
        ALL_SESSIONS[id(session)] = session

    try:
        session.set_combine_stderr(True)

        stime = time.time()

        if not nolog:
            logger.debug("SSH:{0} Exec {1!r}".format(node, cmd))

        session.exec_command(cmd)

        if stdin_data is not None:
            session.sendall(stdin_data)

        session.settimeout(1)
        session.shutdown_write()
        output = ""

        while True:
            try:
                ndata = session.recv(1024)
                output += ndata
                if "" == ndata:
                    break
            except socket.timeout:
                pass

            if time.time() - stime > timeout:
                raise OSError(output + "\nExecution timeout")

        code = session.recv_exit_status()
    finally:
        found = False
        with ALL_SESSIONS_LOCK:
            if id(session) in ALL_SESSIONS:
                found = True
                del ALL_SESSIONS[id(session)]

        if found:
            session.close()

    if code != 0:
        templ = "SSH:{0} Cmd {1!r} failed with code {2}. Output: {3}"
        raise OSError(templ.format(node, cmd, code, output))

    return output


def close_all_sessions():
    with NODES_LOCK:
        for conn in NODES.values():
            try:
                conn.sendall('\x03')
                conn.close()
            except:
                pass
        NODES.clear()


def normalize_dirpath(dirpath):
    while dirpath.endswith("/"):
        dirpath = dirpath[:-1]
    return dirpath


ALL_RWX_MODE = ((1 << 9) - 1)


def ssh_mkdir(sftp, remotepath, mode=ALL_RWX_MODE, intermediate=False):
    remotepath = normalize_dirpath(remotepath)
    if intermediate:
        try:
            sftp.mkdir(remotepath, mode=mode)
        except (IOError, OSError):
            upper_dir = remotepath.rsplit("/", 1)[0]

            if upper_dir == '' or upper_dir == '/':
                raise

            ssh_mkdir(sftp, upper_dir, mode=mode, intermediate=True)
            return sftp.mkdir(remotepath, mode=mode)
    else:
        sftp.mkdir(remotepath, mode=mode)


def ssh_copy_file(sftp, localfile, remfile, preserve_perm=True):
    sftp.put(localfile, remfile)
    if preserve_perm:
        sftp.chmod(remfile, os.stat(localfile).st_mode & ALL_RWX_MODE)


def put_dir_recursively(sftp, localpath, remotepath, preserve_perm=True):
    "upload local directory to remote recursively"

    # hack for localhost connection
    if hasattr(sftp, "copytree"):
        sftp.copytree(localpath, remotepath)
        return

    assert remotepath.startswith("/"), "%s must be absolute path" % remotepath

    # normalize
    localpath = normalize_dirpath(localpath)
    remotepath = normalize_dirpath(remotepath)

    try:
        sftp.chdir(remotepath)
        localsuffix = localpath.rsplit("/", 1)[1]
        remotesuffix = remotepath.rsplit("/", 1)[1]
        if localsuffix != remotesuffix:
            remotepath = os.path.join(remotepath, localsuffix)
    except IOError:
        pass

    for root, dirs, fls in os.walk(localpath):
        prefix = os.path.commonprefix([localpath, root])
        suffix = root.split(prefix, 1)[1]
        if suffix.startswith("/"):
            suffix = suffix[1:]

        remroot = os.path.join(remotepath, suffix)

        try:
            sftp.chdir(remroot)
        except IOError:
            if preserve_perm:
                mode = os.stat(root).st_mode & ALL_RWX_MODE
            else:
                mode = ALL_RWX_MODE
            ssh_mkdir(sftp, remroot, mode=mode, intermediate=True)
            sftp.chdir(remroot)

        for f in fls:
            remfile = os.path.join(remroot, f)
            localfile = os.path.join(root, f)
            ssh_copy_file(sftp, localfile, remfile, preserve_perm)


def delete_file(conn, path):
    sftp = conn.open_sftp()
    sftp.remove(path)
    sftp.close()


def copy_paths(conn, paths):
    sftp = conn.open_sftp()
    try:
        for src, dst in paths.items():
            try:
                if os.path.isfile(src):
                    ssh_copy_file(sftp, src, dst)
                elif os.path.isdir(src):
                    put_dir_recursively(sftp, src, dst)
                else:
                    templ = "Can't copy {0!r} - " + \
                            "it neither a file not a directory"
                    raise OSError(templ.format(src))
            except Exception as exc:
                tmpl = "Scp {0!r} => {1!r} failed - {2!r}"
                raise OSError(tmpl.format(src, dst, exc))
    finally:
        sftp.close()


# for compatibility only, should be moved to other file and fixed

import errno
import settings


def make_remote_dir(remote_dir):
    logger.info('Making remote directory: %s', remote_dir)
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
        return ('valgrind --tool=massif --soname-synonyms=somalloc=*tcmalloc*' +
                ' --massif-out-file=%s --log-file=%s ') % (outfile, logfile)
    if mode == 'memcheck':
        return 'valgrind --tool=memcheck --soname-synonyms=somalloc=*tcmalloc* --log-file=%s ' % (logfile)

    logger.debug('valgrind mode: %s is not supported.', mode)
    return ''
