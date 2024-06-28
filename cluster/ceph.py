import common
import settings
import monitoring
import os
import time
import uuid
import threading
import logging
import json

from .cluster import Cluster


logger = logging.getLogger("cbt")


def sshtarget(user, host):
    h = host
    if user:
        h = '%s@%s' % (user, host)
    return h

# to bring an OSD up, this sequence of steps must be performed in this order
# but there are no cross-OSD dependencies so we can bring up multiple OSDs
# in parallel.


class OsdThread(threading.Thread):
    def __init__(self, cl_obj, devnumstr, osdnum, clusterid, host, osduuid, osddir, tmp_dir, crimson_cpuset):
        threading.Thread.__init__(self, name='OsdThread-%d' % osdnum)
        self.start_time = time.time()
        self.response_time = -1.0
        self.cl_obj = cl_obj
        self.devnumstr = devnumstr
        self.osdnum = osdnum
        self.clusterid = clusterid
        self.host = host
        self.osduuid = osduuid
        self.osddir = osddir
        self.tmp_dir = tmp_dir
        self.crimson_cpuset = crimson_cpuset;
        self.exc = None

    def run(self):
        try:
            ceph_conf = self.cl_obj.tmp_conf
            ceph_osd_cmd = self.cl_obj.ceph_osd_cmd
            # if crimson is being used, optionally set a per-osd cpuset
            if self.crimson_cpuset:
                ceph_osd_cmd = "%s --cpuset %s" % (ceph_osd_cmd, self.crimson_cpuset)
            phost = sshtarget(settings.cluster.get('user'), self.host)

            # Setup the keyring directory
            data_dir = '%s/osd.%s' % (self.tmp_dir, self.osdnum)
            common.pdsh(phost, 'sudo rm -rf %s' % data_dir).communicate()
            common.pdsh(phost, 'mkdir -p %s' % data_dir).communicate()
            key_fn = '%s/keyring' % data_dir

            # Setup crush and the keyring
            common.pdsh(phost, 'sudo %s auth get-or-create osd.%s mon \'allow rwx\' osd \'allow *\' -o %s' % (self.cl_obj.ceph_cmd, self.osdnum, key_fn)).communicate()

            common.pdsh(phost, 'sudo %s -c %s osd crush add osd.%d 1.0 host=%s rack=localrack root=default' % (self.cl_obj.ceph_cmd, ceph_conf, self.osdnum, self.host)).communicate()
            cmd = 'ulimit -n 16384 && ulimit -c unlimited && exec %s -c %s -i %d --mkfs --osd-uuid %s' % (ceph_osd_cmd, ceph_conf, self.osdnum, self.osduuid)
            common.pdsh(phost, 'sudo sh -c "%s"' % cmd).communicate()

            # Start the OSD
            pidfile = "%s/ceph-osd.%d.pid" % (self.cl_obj.pid_dir, self.osdnum)
            cmd = '%s -c %s -i %d --pid-file=%s' % (ceph_osd_cmd, ceph_conf, self.osdnum, pidfile)
            if self.cl_obj.osd_valgrind:
                cmd = common.setup_valgrind(self.cl_obj.osd_valgrind, 'osd.%d' % self.osdnum, self.cl_obj.tmp_dir) + ' ' + cmd
            else:
                cmd = '%s %s' % (self.cl_obj.ceph_run_cmd, cmd)
            stdout_file = "%s/osd.%d.stdout" % (self.cl_obj.tmp_dir, self.osdnum)
            stderr_file = "%s/osd.%d.stderr" % (self.cl_obj.tmp_dir, self.osdnum)
            common.pdsh(phost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s > %s 2> %s < /dev/null &"' % (cmd, stdout_file, stderr_file)).communicate()

        except Exception as e:
            self.exc = e
        finally:
            self.response_time = time.time() - self.start_time

    def __str__(self):
        return 'osd thrd %d %s %s' % (self.osdnum, self.host, self.osduuid)

    # this is intended to be called by parent thread after join()
    def postprocess(self):
        if not (self.exc is None):
            logger.error('thread %s: %s' % (self.name, str(self.exc)))
            raise Exception('OSD %s creation did not complete' % self.osdnum)
        logger.info('thread %s completed creation of OSD %d elapsed time %f' % (self.name, self.osdnum, self.response_time))


class Ceph(Cluster):
    def __init__(self, config):
        super(Ceph, self).__init__(config)
        self.health_wait = config.get('health_wait', 5)
        self.ceph_osd_cmd = config.get('ceph-osd_cmd', '/usr/bin/ceph-osd')
        self.ceph_mon_cmd = config.get('ceph-mon_cmd', '/usr/bin/ceph-mon')
        self.ceph_run_cmd = config.get('ceph-run_cmd', '/usr/bin/ceph-run')
        self.ceph_rgw_cmd = config.get('ceph-rgw_cmd', '/usr/bin/radosgw')
        self.ceph_mgr_cmd = config.get('ceph-mgr_cmd', '/usr/bin/ceph-mgr')
        self.ceph_mds_cmd = config.get('ceph-mds_cmd', '/usr/bin/ceph-mds')
        self.ceph_authtool_cmd = config.get('ceph-authtool_cmd', '/usr/bin/ceph-authtool')
        self.radosgw_admin_cmd = config.get('radosgw-admin_cmd', '/usr/bin/radosgw-admin')
        self.ceph_cmd = config.get('ceph_cmd', '/usr/bin/ceph')
        self.ceph_fuse_cmd = config.get('ceph-fuse_cmd', '/usr/bin/ceph-fuse')
        self.rados_cmd = config.get('rados_cmd', '/usr/bin/rados')
        self.rbd_cmd = config.get('rbd_cmd', '/usr/bin/rbd')
        self.rbd_nbd_cmd = config.get('rbd-nbd_cmd', '/usr/bin/rbd-nbd')
        self.rbd_fuse_cmd = config.get('rbd-fuse_cmd', '/usr/bin/rbd-fuse')
        self.mount_cmd = config.get('mount_cmd', '/usr/sbin/ceph.mount')
        self.log_dir = config.get('log_dir', "%s/log" % self.tmp_dir)
        self.pid_dir = config.get('pid_dir', "%s/pid" % self.tmp_dir)
        self.core_dir = config.get('core_dir', "%s/core" % self.tmp_dir)
        self.monitoring_dir = "%s/monitoring" % self.tmp_dir
        self.osdmap_fn = "%s/osdmap" % self.tmp_dir
        self.monmap_fn = "%s/monmap" % self.tmp_dir
        self.use_existing = config.get('use_existing', True)
        self.newstore_block = config.get('newstore_block', False)
        self.version_compat = config.get('version_compat', '')
        # these parameters control parallel OSD build
        self.ceph_osd_online_rate = config.get('osd_online_rate', 10)
        self.ceph_osd_online_tmo = config.get('osd_online_timeout', 120)
        self.ceph_osd_parallel_creates = config.get('osd_parallel_creates')
        self.disable_bal = config.get('disable_balancer', False)

        self.client_keyring = '/etc/ceph/ceph.keyring'
        self.client_secret = '/etc/ceph/ceph.secret'
        # If making the cluster, use the ceph.conf file distributed by initialize to the tmp_dir
        self.tmp_conf = '%s/ceph.conf' % self.tmp_dir
        # If using an existing cluster, defualt to /etc/ceph/ceph.conf
        if self.use_existing:
            self.tmp_conf = self.config.get('conf_file')

        self.osd_valgrind = config.get('osd_valgrind', None)
        self.mon_valgrind = config.get('mon_valgrind', None)
        self.rgw_valgrind = config.get('rgw_valgrind', None)
        self.mgr_valgrind = config.get('mgr_valgrind', None)
        self.tiering = config.get('tiering', False)
        self.ruleset_map = {}
        self.cur_ruleset = 1
        self.idle_duration = config.get('idle_duration', 0)
        self.use_existing = config.get('use_existing', True)
        self.stoprequest = threading.Event()
        self.haltrequest = threading.Event()
        self.startiorequest = threading.Event()

        self.urls = []
        self.auth_urls = []
        self.crimson_cpusets = config.get('crimson_cpusets', [])

        # Recovery objects prefill info
        self.prefill_recov_objects = 0
        self.prefill_recov_object_size = 0
        self.prefill_recov_time = 0
        self.recov_pool_name = ''

    def initialize(self):
        # Reset the rulesets
        self.ruleset_map = {}
        self.cur_ruleset = 1

        # safety check to make sure we don't blow away an existing cluster!
        if self.use_existing:
            raise RuntimeError('initialize was called on an existing cluster! Avoiding touching anything.')

        super(Ceph, self).initialize()

        # unmount any rbd volumes
        self.unmount_all()

        # shutdown any old processes
        self.shutdown()

        # Cleanup old junk and create new junk
        self.cleanup()
        common.mkdir_p(self.tmp_dir)
        common.pdsh(settings.getnodes('head', 'clients', 'mons', 'osds', 'rgws', 'mdss', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.tmp_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mdss', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.pid_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mdss', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.log_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mdss', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.monitoring_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mdss', 'mgrs'), 'mkdir -p -m0755 -- %s' % self.core_dir).communicate()
        self.distribute_conf()

        # Set the core directory
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mdss', 'mgrs'), 'echo "%s/core.%%e.%%p.%%h.%%t" | sudo tee /proc/sys/kernel/core_pattern' % self.tmp_dir).communicate()

        # Create the filesystems
        self.setup_fs()

        # Build the cluster
        monitoring.start('%s/creation' % self.monitoring_dir)
        self.make_mons()
        self.start_mgrs()
        self.make_osds()
        monitoring.stop()

        # Check Health
        monitoring.start('%s/initial_health_check' % self.monitoring_dir)
        self.check_health()
        monitoring.stop()

        # Disable scrub and wait for any scrubbing to complete
        self.disable_scrub()
        if self.disable_bal:
            self.disable_balancer()

        # FIXME with no PGs, osd pg dump appears to hang now.
        # Disable this since it wa a workaround for an old problem from the cuttlefish era.
#        self.check_scrub()

        # Make the crush and erasure profiles
        self.make_profiles()

        # Start any higher level daemons
        self.start_rgw()
        self.start_mds()
        # Peform Idle Monitoring
        if self.idle_duration > 0:
            monitoring.start("%s/idle_monitoring" % self.monitoring_dir)
            time.sleep(self.idle_duration)
            monitoring.stop()

        return True

    def shutdown(self):
        nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mdss', 'mgrs')

        common.pdsh(nodes, 'sudo killall -9 massif-amd64-li').communicate()
        common.pdsh(nodes, 'sudo killall -9 memcheck-amd64-').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-osd').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-mon').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-mds').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-mgr').communicate()
        common.pdsh(nodes, 'sudo killall -9 rados').communicate()
        common.pdsh(nodes, 'sudo killall -9 rest-bench').communicate()
        common.pdsh(nodes, 'sudo killall -9 radosgw').communicate()
        common.pdsh(nodes, 'sudo killall -9 radosgw-admin').communicate()
        common.pdsh(nodes, 'sudo /etc/init.d/apache2 stop').communicate()
        common.pdsh(nodes, 'sudo killall -9 pdsh').communicate()
        monitoring.stop()

    def cleanup(self):
        nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mdss', 'mgrs')
        logger.info('Deleting %s', self.tmp_dir)
        common.pdsh(nodes, 'sudo rm -rf %s' % self.tmp_dir).communicate()

    def setup_fs(self):
        use_existing = settings.cluster.get('use_existing', True)
        if use_existing:
            return None
        sc = settings.cluster
        fs = sc.get('fs')
        mkfs_opts = sc.get('mkfs_opts', '')
        mount_opts = sc.get('mount_opts', '')

        if fs == '':
            settings.shutdown("No OSD filesystem specified.  Exiting.")

        mkfs_threads = []
        for device in range(0, sc.get('osds_per_node')):
            osds = settings.getnodes('osds')
            common.pdsh(osds, 'sudo umount /dev/disk/by-partlabel/osd-device-%s-data' % device).communicate()
            common.pdsh(osds, 'sudo rm -rf %s/osd-device-%s-data' % (self.mnt_dir, device)).communicate()
            common.pdsh(osds, 'sudo mkdir -p -m0755 -- %s/osd-device-%s-data' % (self.mnt_dir, device)).communicate()

            if fs == 'tmpfs':
                logger.info('using tmpfs osds, not creating a file system.')
            elif fs == 'zfs':
                logger.info('ruhoh, zfs detected.  No mkfs for you!')
                common.pdsh(osds, 'sudo zpool destroy osd-device-%s-data' % device).communicate()
                common.pdsh(osds, 'sudo zpool create -f -O xattr=sa -m legacy osd-device-%s-data /dev/disk/by-partlabel/osd-device-%s-data' % (device, device)).communicate()
                common.pdsh(osds, 'sudo zpool add osd-device-%s-data log /dev/disk/by-partlabel/osd-device-%s-zil' % (device, device)).communicate()
                common.pdsh(osds, 'sudo mount %s -t zfs osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, device, self.mnt_dir, device)).communicate()
            else:
                # do mkfs and mount in 1 long command
                # alternative is to wait until make_osds to mount it
                mkfs_cmd = 'sudo sh -c "mkfs.%s %s /dev/disk/by-partlabel/osd-device-%s-data' % (fs, mkfs_opts, device)
                mkfs_cmd += '; mount %s -t %s /dev/disk/by-partlabel/osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, fs, device, self.mnt_dir, device)

                # make a symlink for block if using newstore+block
                if self.newstore_block:
                    mkfs_cmd += ' ; sudo ln -s /dev/disk/by-partlabel/osd-device-%s-block %s/osd-device-%s-data/block' % (device, self.mnt_dir, device)
                mkfs_cmd += '"'

                mkfs_threads.append((device, common.pdsh(osds, mkfs_cmd)))
        for device, t in mkfs_threads:  # for tmpfs and zfs cases, thread list is empty
            logger.info('for device %d on all hosts awaiting mkfs and mount' % device)
            t.communicate()

    def distribute_conf(self):
        nodes = settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws', 'mgrs')
        conf_file = self.config.get("conf_file")
        logger.info("Distributing %s.", conf_file)
        common.pdsh(nodes, 'mkdir -p -m0755 /etc/ceph').communicate()
        common.pdcp(nodes, '', conf_file, self.tmp_conf).communicate()
        common.pdsh(nodes, 'sudo mv /etc/ceph/ceph.conf /etc/ceph/ceph.conf.cbt.bak').communicate()
        common.pdsh(nodes, 'sudo ln -s %s /etc/ceph/ceph.conf' % self.tmp_conf).communicate()

    def get_mon_hosts(self):
        # get the list of mons
        mon_hosts = {}
        mons_config = settings.cluster.get('mons')

        # For mons specified using the string/list representation, we'll
        # assume mon ids range from a-z in order and default ports are used.
        if isinstance(mons_config, str):
            host = mons_config
            info = settings.host_info(host)
            mon_str = '%s:6789' % info['addr']
            mon_hosts[host] = {'a': mon_str}
        elif isinstance(mons_config, list):
            mon_id = 'a'
            for host in mons_config:
                info = settings.host_info(host)
                mon_str = '%s:6789' % info['addr']
                mon_hosts[host] = {mon_id: mon_str}
                if ord(mon_id) < ord('z'):
                    mon_id = chr(ord(mon_id) + 1)
                else:
                    raise ValueError("CBT does not support 27+ monitors")
        # dict representation contains hostnames with mon_id / ip:port pair:
        #
        # localhost:
        #   a: "127.0.0.1:6789"
        elif isinstance(mon_hosts, dict):
            for host, mon_config in mons_config.items():
                mon_hosts[host] = {}
                for mon_id, addr in mon_config.items():
                    mon_hosts[host][mon_id] = addr
        else:
            raise ValueError("Failed to parse monitor syntax: %r" % mon_hosts)
        return mon_hosts

    def make_mons(self):
        # Build and distribute the client keyring
        client_admin_dir = "%s/client.admin" % self.tmp_dir
        common.pdsh(settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws', 'mgrs'), 'rm -rf %s' % client_admin_dir).communicate()
        common.pdsh(settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws', 'mgrs'), 'mkdir -p %s' % client_admin_dir).communicate()

        keyring_fn = os.path.join(client_admin_dir, "keyring")
        common.pdsh(settings.getnodes('head'), '%s --create-keyring --gen-key --name=mon. %s --cap mon \'allow *\'' % (self.ceph_authtool_cmd, keyring_fn)).communicate()
        common.pdsh(settings.getnodes('head'), '%s --gen-key --name=client.admin --cap mon \'allow *\' --cap osd \'allow *\' --cap mds \'allow *\' --cap mgr \'allow *\' %s' % (self.ceph_authtool_cmd, keyring_fn)).communicate()
        common.rscp(settings.getnodes('head'), keyring_fn, '%s.tmp' % keyring_fn).communicate()
        common.pdcp(settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws', 'mgrs'), '', '%s.tmp' % keyring_fn, keyring_fn).communicate()
        common.pdsh(settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws', 'mgrs'), 'sudo mv %s %s.cbt.bak' % (self.client_keyring, self.client_keyring)).communicate()
        common.pdsh(settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws', 'mgrs'), 'sudo ln -s %s %s' % (keyring_fn, self.client_keyring)).communicate()
        common.pdsh(settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws', 'mgrs'), 'sudo mv %s %s.cbt.bak' % (self.client_secret, self.client_secret)).communicate()
        common.pdsh(settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws', 'mgrs'), 'sudo sh -c \'%s --print-key %s > %s\'' % (self.ceph_authtool_cmd, self.client_keyring, self.client_secret)).communicate()
        # Build the monmap, retrieve it, and distribute it
        mons = settings.getnodes('mons').split(',')
        cmd = 'monmaptool --create --clobber'
        monhosts = self.get_mon_hosts()
        logger.info(monhosts)
        for monhost, mons in monhosts.items():
            for mon, addr in mons.items():
                cmd = cmd + ' --add %s %s' % (mon, addr)
        cmd = cmd + ' --print %s' % self.monmap_fn
        common.pdsh(settings.getnodes('head'), cmd).communicate()
        common.rscp(settings.getnodes('head'), self.monmap_fn, '%s.tmp' % self.monmap_fn).communicate()
        common.pdcp(settings.getnodes('mons'), '', '%s.tmp' % self.monmap_fn, self.monmap_fn).communicate()

        # Build the ceph-mons
        user = settings.cluster.get('user')
        for monhost, mons in monhosts.items():
            if user:
                monhost = '%s@%s' % (user, monhost)
            for mon, addr in mons.items():
                common.pdsh(monhost, 'sudo rm -rf %s/mon.%s' % (self.tmp_dir, mon)).communicate()
                common.pdsh(monhost, 'mkdir -p %s/mon.%s' % (self.tmp_dir, mon)).communicate()
                common.pdsh(monhost, 'sudo sh -c "ulimit -c unlimited && exec %s --mkfs -c %s -i %s --monmap=%s --keyring=%s"' % (self.ceph_mon_cmd, self.tmp_conf, mon, self.monmap_fn, keyring_fn)).communicate()
                common.pdsh(monhost, 'cp %s %s/mon.%s/keyring' % (keyring_fn, self.tmp_dir, mon)).communicate()

        # Start the mons
        for monhost, mons in monhosts.items():
            if user:
                monhost = '%s@%s' % (user, monhost)
            for mon, addr in mons.items():
                pidfile = "%s/%s.pid" % (self.pid_dir, monhost)
                cmd = 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s -c %s -i %s --keyring=%s --pid-file=%s"' % (self.ceph_mon_cmd, self.tmp_conf, mon, keyring_fn, pidfile)
                if self.mon_valgrind:
                    cmd = "%s %s" % (common.setup_valgrind(self.mon_valgrind, 'mon.%s' % monhost, self.tmp_dir), cmd)
                else:
                    cmd = '%s %s' % (self.ceph_run_cmd, cmd)
                common.pdsh(monhost, 'sudo %s' % cmd).communicate()

    def make_osds(self):
        osdnum = 0
        osdhosts = settings.cluster.get('osds')
        clusterid = self.config.get('clusterid')
        user = settings.cluster.get('user')
        thread_list = []

        # set up degree of OSD creation parallelism

        logger.info('OSD creation rate: < %d OSDs/sec , join timeout %d, parallel creates < %s' % (
            self.ceph_osd_online_rate,
            self.ceph_osd_online_tmo,
            str(self.ceph_osd_parallel_creates)))
        osd_online_interval = 1.0 / self.ceph_osd_online_rate
        max_parallel_creates = settings.cluster.get('osds_per_node') * len(osdhosts)
        if self.ceph_osd_parallel_creates:
            max_parallel_creates = int(self.ceph_osd_parallel_creates)

        # build OSDs in parallel, except for "ceph osd create" command
        # which must be 1 at a time

        threads_finished = 0
        for host in osdhosts:
            for devnumstr in range(0, settings.cluster.get('osds_per_node')):
                pdshhost = sshtarget(user, host)
                crimson_cpuset = self.crimson_cpusets[devnumstr] if devnumstr < len(self.crimson_cpusets) else None
                # Build the OSD
                osduuid = str(uuid.uuid4())
#                osddir='/var/lib/ceph/osd/%s-%d'%(clusterid, osdnum)
                osddir = '%s/osd-device-%s-data' % (self.mnt_dir, devnumstr)
                # create the OSD first, so we know what number it has been assigned.
                common.pdsh(pdshhost, 'sudo %s -c %s osd create %s' % (self.ceph_cmd, self.tmp_conf, osduuid)).communicate()
                # bring the OSD online in background while continuing to create OSDs in foreground
                thrd = OsdThread(self, devnumstr, osdnum, clusterid, host, osduuid, osddir, self.tmp_dir, crimson_cpuset)
                logger.info('starting creation of OSD %d ' % osdnum)
                thrd.start()
                thread_list.append(thrd)

                # only allow up to max_parallel_creates threads to be active
                active_thread_count = len(thread_list) - threads_finished
                if active_thread_count >= max_parallel_creates:
                    thrd = thread_list[threads_finished]
                    thrd.join(self.ceph_osd_online_tmo)
                    thrd.postprocess()
                    threads_finished += 1
                time.sleep(osd_online_interval)  # don't flood Ceph with OSD commands
                osdnum += 1

        # wait for rest of them to finish
        for thrd in thread_list[threads_finished:]:
            # an exception is thrown if the thread failed, hopefully
            thrd.join(self.ceph_osd_online_tmo)
            thrd.postprocess()

    def start_mgrs(self):
        user = settings.cluster.get('user')
        mgrhosts = settings.cluster.get('mgrs')

        if not mgrhosts:
            return

        for mgrhost, manager in mgrhosts.items():
            for mgrname, mgrsettings in manager.items():
                cmd = '%s -i %s' % (self.ceph_mgr_cmd, mgrname)
                if self.mgr_valgrind:
                    cmd = "%s %s" % (common.setup_valgrind(self.mgr_valgrind, mgrname, self.tmp_dir), cmd)
                else:
                    cmd = "%s %s" % (self.ceph_run_cmd, cmd)
                pdshhost = sshtarget(user, mgrhost)
                data_dir = "%s/mgr.%s" % (self.tmp_dir, mgrname)
                common.pdsh(pdshhost, 'sudo mkdir -p %s' % data_dir).communicate()
                common.pdsh(pdshhost, 'sudo %s auth get-or-create mgr.%s mon \'allow profile mgr\' mds \'allow *\' osd \'allow *\' -o %s/keyring' % (self.ceph_cmd, mgrname, data_dir)).communicate()
                common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s"' % cmd).communicate()

    def start_mds(self):
        user = settings.cluster.get('user')
        mdshosts = settings.cluster.get('mdss')

        if not mdshosts:
            return

        for mdshost, mds in mdshosts.items():
            for mdsname, mdssettings in mds.items():
                cmd = '%s -i %s' % (self.ceph_mds_cmd, mdsname)
                if self.mgr_valgrind:
                    cmd = "%s %s" % (common.setup_valgrind(self.mds_valgrind, mdsname, self.tmp_dir), cmd)
                else:
                    cmd = "%s %s" % (self.ceph_run_cmd, cmd)
                pdshhost = sshtarget(user, mdshost)
                data_dir = "%s/mds.%s" % (self.tmp_dir, mdsname)
                common.pdsh(pdshhost, 'sudo mkdir -p %s' % data_dir).communicate()
                common.pdsh(pdshhost, 'sudo %s auth get-or-create mds.%s mon \'allow profile mds\' osd \'allow rw tag cephfs *=*\' mds \'allow\' mgr \'allow profile mds\' -o %s/keyring' % (self.ceph_cmd, mdsname, data_dir)).communicate()
                common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s"' % cmd).communicate()

    def start_rgw(self):
        user = settings.cluster.get('user')
        rgwhosts = settings.cluster.get('rgws')

        if not rgwhosts:
            return

        # If we are starting rGW, make the RGW pools
        self.make_rgw_pools()

        for rgwhost, gateways in rgwhosts.items():
            for rgwname, rgwsettings in gateways.items():
                host = rgwsettings.get('host', rgwhost)
                port = rgwsettings.get('port', None)
                ssl_certificate = rgwsettings.get('ssl_certificate', None)

                # Build the urls (s3)
                url = "http://" if ssl_certificate is None else "https://"
                url += host
                url += ":7480" if port is None else ":%s" % port
                self.urls.append(url)

                # Build the auth urls (swift)
                auth_url = url + "/auth/v1.0"
                self.auth_urls.append(auth_url)

                # set the rgw_frontends
                rgw_frontends = None
                if ssl_certificate is not None:
                    rgw_frontends = "beast ssl_certificate=%s" % ssl_certificate
                if port is not None:
                    if rgw_frontends is None:
                        rgw_frontends = "beast"
                    rgw_frontends += " ssl_port=%s" % port

                cmd = '%s -c %s -n %s --log-file=%s/rgw.log' % (self.ceph_rgw_cmd, self.tmp_conf, rgwname, self.log_dir)
                if rgw_frontends is not None:
                    cmd += " --rgw-frontends='%s'" % rgw_frontends
                if self.rgw_valgrind:
                    cmd = "%s %s" % (common.setup_valgrind(self.rgw_valgrind, 'rgw.%s' % host, self.tmp_dir), cmd)
                else:
                    cmd = '%s %s' % (self.ceph_run_cmd, cmd)

                pdshhost = sshtarget(user, rgwhost)
                common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s"' % cmd).communicate()

                # set min_size of pools to 1, when there is only one osd
                num_osds = len(settings.cluster.get('osds'))
                rgw_default_pools = ['.rgw.root', 'default.rgw.control', 'default.rgw.meta', 'default.rgw.log']
                pool_min_repl_size = 1

                if num_osds == 1:
                    time.sleep(5)
                    for pool in rgw_default_pools:
                        common.pdsh(settings.getnodes('head'),
                                    'sudo %s -c %s osd pool set %s min_size %d' % (self.ceph_cmd, self.tmp_conf, pool, pool_min_repl_size),
                                    continue_if_error=False).communicate()
                        time.sleep(5)

    def disable_scrub(self):
        common.pdsh(settings.getnodes('head'), "ceph osd set noscrub; ceph osd set nodeep-scrub").communicate()

    def disable_balancer(self):
        common.pdsh(settings.getnodes('head'), "ceph balancer off").communicate()

    def check_health(self, check_list=None, logfile=None, recstatsfile=None):
        # Wait for a defined amount of time in case ceph health is delayed
        time.sleep(self.health_wait)
        logline = ""
        if logfile:
            logline = "| tee -a %s" % logfile
        ret = 0

        # Match any of these things to continue checking health
        check_list = ["degraded", "peering", "recovery_wait", "stuck", "inactive", "unclean", "recovery", "stale"]
        if recstatsfile:
            header = "Time, Num Deg Objs, Total Deg Objs"
            stdout, stderr = common.pdsh(settings.getnodes('head'), 'echo %s >> %s' % (header, recstatsfile)).communicate()

        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), '%s -c %s health %s' % (self.ceph_cmd, self.tmp_conf, logline)).communicate()
            self.log_recovery_stats(recstatsfile)
            if check_list and not any(x in stdout for x in check_list):
                break
            if "HEALTH_OK" in stdout:
                break
            else:
                ret = ret + 1
            logger.info("%s", stdout)
            time.sleep(1)

        return ret

    def log_recovery_stats(self, recstatsfile=None):
        if not recstatsfile:
            return
        PGMAP = "pgmap"
        NUM_DEG = "degraded_objects"
        NUM_DEG_TOT = "degraded_total"
        NUM_MISP = "misplaced_objects"
        NUM_MISP_TOT = "misplaced_total"
        fmtjson = "--format=json"
        separator = ","
        stdout, stderr = common.pdsh(settings.getnodes('head'), '%s -c %s -s %s' % (self.ceph_cmd, self.tmp_conf, fmtjson)).communicate()
        stdout = stdout.split(':', 1)[1]
        stdout = stdout.strip()
        try:
            jsondata = json.loads(stdout)
        except ValueError as e:
            logger.error(str(e))
            return
        degstats = []
        degstats.append(str(time.time()))
        if NUM_DEG in jsondata[PGMAP]:
            degstats.append(str(jsondata[PGMAP][NUM_DEG]))
        if NUM_DEG_TOT in jsondata[PGMAP]:
            degstats.append(str(jsondata[PGMAP][NUM_DEG_TOT]))
        if NUM_MISP in jsondata[PGMAP]:
            degstats.append(str(jsondata[PGMAP][NUM_MISP]))
        if NUM_MISP_TOT in jsondata[PGMAP]:
            degstats.append(str(jsondata[PGMAP][NUM_MISP_TOT]))

        if len(degstats):
            message = separator.join(degstats)
            stdout, stderr = common.pdsh(settings.getnodes('head'), 'echo %s >> %s' % (message, recstatsfile)).communicate()

    def check_backfill(self, check_list=None, logfile=None, recstatsfile=None):
        # Wait for a defined amount of time in case ceph health is delayed
        time.sleep(self.health_wait)
        logline = ""
        if logfile:
            logline = "| tee -a %s" % logfile
        ret = 0

        if recstatsfile:
            header = "Time, Num Misplaced Objs, Total Misplaced Objs"
            stdout, stderr = common.pdsh(settings.getnodes('head'), 'echo %s >> %s' % (header, recstatsfile)).communicate()

        # Match any of these things to continue checking backfill
        check_list = ["backfill", "misplaced"]
        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), '%s -c %s -s %s' % (self.ceph_cmd, self.tmp_conf, logline)).communicate()
            if check_list and not any(x in stdout for x in check_list):
                break
            else:
                ret = ret + 1
            for line in stdout.splitlines():
                if 'misplaced' in line:
                    self.log_recovery_stats(recstatsfile)
                    logger.info("%s", line)
            time.sleep(1)
        return ret

    def check_scrub(self):
        logger.info('Waiting until Scrubbing completes...')
        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), '%s -c %s pg dump | cut -f 16 | grep "0.000000" | wc -l' % (self.ceph_cmd, self.tmp_conf)).communicate()
            if " 0\n" in stdout:
                break
            else:
                logger.info(stdout)
            time.sleep(1)

    def dump_config(self, run_dir):
        common.pdsh(settings.getnodes('osds'), 'sudo %s -c %s daemon osd.0 config show > %s/ceph_settings.out' % (self.ceph_cmd, self.tmp_conf, run_dir)).communicate()

    def dump_historic_ops(self, run_dir):
        common.pdsh(settings.getnodes('osds'), 'find /var/run/ceph/ceph-osd*.asok -maxdepth 1 -exec sudo %s --admin-daemon {} dump_historic_ops \; > %s/historic_ops.out' % (self.ceph_cmd, run_dir)).communicate()

    def set_osd_param(self, param, value):
        common.pdsh(settings.getnodes('osds'), 'find /dev/disk/by-partlabel/osd-device-*data -exec readlink {} \; | cut -d"/" -f 3 | sed "s/[0-9]$//" | xargs -I{} sudo sh -c "echo %s > /sys/block/\'{}\'/queue/%s"' % (value, param))

    def __str__(self):
        return "foo"

    def create_recovery_test(self, run_dir, callback, test_type='blocking'):
        rt_config = self.config.get("recovery_test", {})
        rt_config['run_dir'] = run_dir
        if test_type == 'blocking':
            self.rt = RecoveryTestThreadBlocking(rt_config, self, callback, self.stoprequest, self.haltrequest)
        elif test_type == 'background':
            self.rt = RecoveryTestThreadBackground(rt_config, self, callback, self.stoprequest, self.haltrequest, self.startiorequest)
        self.rt.start()

    def wait_start_io(self):
        logger.info("Waiting for signal to start client io...")
        self.startiorequest.wait()

    def maybe_populate_recovery_pool(self):
        if self.prefill_recov_objects > 0 or self.prefill_recov_time > 0:
            logger.info('prefilling %s %sbyte objects into recovery pool %s' % (self.prefill_recov_objects, self.prefill_recov_object_size, self.recov_pool_name))
            common.pdsh(settings.getnodes('head'), 'sudo %s -p %s bench %s write -b %s --max-objects %s --no-cleanup' % (self.rados_cmd, self.recov_pool_name, self.prefill_recov_time, self.prefill_recov_object_size, self.prefill_recov_objects)).communicate()
            self.check_health()

    def wait_recovery_done(self):
        self.stoprequest.set()
        while True:
            threads = threading.enumerate()
            if len(threads) == 1:
                break
            self.rt.join(1)

    def check_pg_autoscaler(self, timeout=-1, logfile=None):
        ret = 0
        if not timeout:
            return ret
        logline = ""
        if logfile:
            logdate = 'echo "[`date`] ceph progress" >> %s' % (logfile)
            logline = "| tee -a %s" % logfile
        pg_autoscaler_states = ["Complete", "Nothing in progress"]
        time.sleep(20)
        start_time = time.time()
        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), '%s;%s -c %s progress %s' % (logdate, self.ceph_cmd, self.tmp_conf, logline)).communicate()
            output = (stdout, stderr)
            for i in range(len(output)):
                if any(pg_state in output[i] for pg_state in pg_autoscaler_states):
                    return ret
            if timeout > 0:
                cur_time = time.time()
                if cur_time - start_time > timeout:
                    logger.info("check_pg_autoscaler() state timeout exceeded...")
                    ret = ret + 1
                    return ret
            logger.info("%s", stdout)
            time.sleep(10)
        return ret

    # FIXME: This is a total hack that assumes there is only 1 existing ruleset!
    # Will change pending a fix for http://tracker.ceph.com/issues/8060
    def set_ruleset(self, name):
        name = str(name)
        if name in self.ruleset_map:
            raise Exception('A rule named %s already exists!' % name)
        self.ruleset_map[name] = self.cur_ruleset
        self.cur_ruleset += 1

    def get_ruleset(self, name):
        name = str(name)
        logger.info("%s", self.ruleset_map)
        return self.ruleset_map[name]

    def make_profiles(self):
        crush_profiles = self.config.get('crush_profiles', {})
        for name, profile in list(crush_profiles.items()):
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush add-bucket %s-root root' % (self.ceph_cmd, self.tmp_conf, name)).communicate()
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush add-bucket %s-rack rack' % (self.ceph_cmd, self.tmp_conf, name)).communicate()
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush move %s-rack root=%s-root' % (self.ceph_cmd, self.tmp_conf, name, name)).communicate()
            # FIXME: We need to build a dict mapping OSDs to hosts and create a proper hierarchy!
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush add-bucket %s-host host' % (self.ceph_cmd, self.tmp_conf, name)).communicate()
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush move %s-host rack=%s-rack' % (self.ceph_cmd, self.tmp_conf, name, name)).communicate()

            osds = profile.get('osds', None)
            if not osds:
                raise Exception("No OSDs defined for crush profile, bailing!")
            for i in osds:
                common.pdsh(settings.getnodes('head'), '%s -c %s osd crush set %s 1.0 host=%s-host' % (self.ceph_cmd, self.tmp_conf, i, name)).communicate()
            common.pdsh(settings.getnodes('head'), '%s -c %s osd crush rule create-simple %s %s-root osd' % (self.ceph_cmd, self.tmp_conf, name, name)).communicate()
            self.set_ruleset(name)

        erasure_profiles = self.config.get('erasure_profiles', {})
        for name, profile in list(erasure_profiles.items()):
            k = profile.get('erasure_k', 6)
            m = profile.get('erasure_m', 2)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd erasure-code-profile set %s crush-failure-domain=osd k=%s m=%s' % (self.ceph_cmd, self.tmp_conf, name, k, m)).communicate()
            self.set_ruleset(name)

    def mkpool(self, name, profile_name, application, base_name=None):
        pool_profiles = self.config.get('pool_profiles', {'default': {}})
        profile = pool_profiles.get(profile_name, {})

        pg_size = profile.get('pg_size', 1024)
        pgp_size = profile.get('pgp_size', 1024)
        erasure_profile = profile.get('erasure_profile', '')
        replication = str(profile.get('replication', None))
        ec_overwrites = profile.get('ec_overwrites', False)
        cache_profile = profile.get('cache_profile', None)

        # Options for cache tiering
        crush_profile = profile.get('crush_profile', None)
        cache_mode = profile.get('cache_mode', None)
        hit_set_type = profile.get('hit_set_type', None)
        hit_set_count = profile.get('hit_set_count', None)
        hit_set_period = profile.get('hit_set_period', None)
        target_max_objects = profile.get('target_max_objects', None)
        target_max_bytes = profile.get('target_max_bytes', None)
        min_read_recency_for_promote = profile.get('min_read_recency_for_promote', None)
        min_write_recency_for_promote = profile.get('min_write_recency_for_promote', None)
        # Options for prefilling objects
        prefill_objects = profile.get('prefill_objects', 0)
        prefill_object_size = profile.get('prefill_object_size', 0)
        prefill_time = profile.get('prefill_time', 0)
        # Options for prefilling recovery objects
        recov_pool = profile.get('recov_pool', False)
        if recov_pool:
            self.prefill_recov_objects = profile.get('prefill_recov_objects', 0)
            self.prefill_recov_object_size = profile.get('prefill_recov_object_size', 0)
            self.prefill_recov_time = profile.get('prefill_recov_time', 0)
            if self.prefill_recov_objects > 0:
                self.recov_pool_name = name

        if replication and replication == 'erasure':
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool create %s %d %d erasure %s' % (self.ceph_cmd, self.tmp_conf, name, pg_size, pgp_size, erasure_profile),
                        continue_if_error=False).communicate()
            if ec_overwrites is True:
                common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s allow_ec_overwrites true' % (self.ceph_cmd, self.tmp_conf, name), continue_if_error=False).communicate()
        else:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool create %s %d %d' % (self.ceph_cmd, self.tmp_conf, name, pg_size, pgp_size),
                        continue_if_error=False).communicate()
        if self.version_compat not in ['argonaut', 'bobcat', 'cuttlefish', 'dumpling', 'emperor', 'firefly', 'giant', 'hammer', 'infernalis', 'jewel']:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool application enable %s %s' % (self.ceph_cmd, self.tmp_conf, name, application), continue_if_error=False).communicate()

        if replication and replication.isdigit():
            pool_repl_size = int(replication)
            pool_min_repl_size = 1
            if (pool_repl_size > 2):
                pool_min_repl_size = pool_repl_size - 1

            # Add mandatory UI option to actually create a pool of size 1 (Sigh).
            yes_flag = ""
            if int(replication) == 1:
                yes_flag = "--yes-i-really-mean-it"

            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s size %s %s' % (self.ceph_cmd, self.tmp_conf, name, replication, yes_flag),
                        continue_if_error=False).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s min_size %d %s' % (self.ceph_cmd, self.tmp_conf, name, pool_min_repl_size, yes_flag),
                        continue_if_error=False).communicate()

        if crush_profile:
            try:
                rule_index = int(crush_profile)
                # set crush profile using the integer 0-based index of crush rule
                # displayed by: ceph osd crush rule ls
                ruleset = crush_profile
            except ValueError:
                ruleset = self.get_ruleset(crush_profile)
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s crush_rule %s' % (self.ceph_cmd, self.tmp_conf, name, crush_profile),
                        continue_if_error=False).communicate()

        logger.info('Checking Health after pool creation.')
        self.check_health()

        if prefill_objects > 0 or prefill_time > 0:
            logger.info('prefilling %s %sbyte objects into pool %s' % (prefill_objects, prefill_object_size, name))
            common.pdsh(settings.getnodes('head'), 'sudo %s -p %s bench %s write -b %s --max-objects %s --no-cleanup' % (self.rados_cmd, name, prefill_time, prefill_object_size, prefill_objects)).communicate()
            self.check_health()

        if base_name and cache_mode:
            logger.info("Adding %s as cache tier for %s.", name, base_name)
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier add %s %s' % (self.ceph_cmd, self.tmp_conf, base_name, name)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier cache-mode %s %s' % (self.ceph_cmd, self.tmp_conf, name, cache_mode)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier set-overlay %s %s' % (self.ceph_cmd, self.tmp_conf, base_name, name)).communicate()

        if hit_set_type:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s hit_set_type %s' % (self.ceph_cmd, self.tmp_conf, name, hit_set_type)).communicate()
        if hit_set_count:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s hit_set_count %s' % (self.ceph_cmd, self.tmp_conf, name, hit_set_count)).communicate()
        if hit_set_period:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s hit_set_period %s' % (self.ceph_cmd, self.tmp_conf, name, hit_set_period)).communicate()
        if target_max_objects:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s target_max_objects %s' % (self.ceph_cmd, self.tmp_conf, name, target_max_objects)).communicate()
        if target_max_bytes:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s target_max_bytes %s' % (self.ceph_cmd, self.tmp_conf, name, target_max_bytes)).communicate()
        if min_read_recency_for_promote:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s min_read_recency_for_promote %s' % (self.ceph_cmd, self.tmp_conf, name, min_read_recency_for_promote)).communicate()
        if min_write_recency_for_promote:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s min_write_recency_for_promote %s' % (self.ceph_cmd, self.tmp_conf, name, min_write_recency_for_promote)).communicate()

        logger.info('Final Pool Health Check.')
        self.check_health()

        # If there is a cache profile assigned, make a cache pool
        if cache_profile:
            cache_name = '%s-cache' % name
            self.mkpool(cache_name, cache_profile, name, application)

    def rmpool(self, name, profile_name):
        pool_profiles = self.config.get('pool_profiles', {'default': {}})
        profile = pool_profiles.get(profile_name, {})
        cache_profile = profile.get('cache_profile', None)
        if cache_profile:
            cache_name = '%s-cache' % name

            # flush and remove the overlay and such
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier cache-mode %s forward' % (self.ceph_cmd, self.tmp_conf, cache_name)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s -p %s cache-flush-evict-all' % (self.rados_cmd, self.tmp_conf, cache_name)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier remove-overlay %s' % (self.ceph_cmd, self.tmp_conf, name)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd tier remove %s %s' % (self.ceph_cmd, self.tmp_conf, name, cache_name)).communicate()

            # delete the cache pool
            self.rmpool(cache_name, cache_profile)
        common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool delete %s %s --yes-i-really-really-mean-it' % (self.ceph_cmd, self.tmp_conf, name, name),
                    continue_if_error=False).communicate()

    def mkimage(self, name, size, pool, data_pool, order):
        dp_option = ''
        if data_pool:
            dp_option = "--data-pool %s" % data_pool
        try:
            common.pdsh(settings.getnodes('head'), '%s -c %s create %s --size %s --pool %s %s --order %s' % (self.rbd_cmd, self.tmp_conf, name, size, pool, dp_option, order), continue_if_error=False).communicate()
        except Exception as e:
            logger.error(str(e))

    def unmount_all(self):
        # Should take care of pretty much everything so long as wierd mnt_dirs aren't used.
        common.pdsh(settings.getnodes('clients'), 'sudo umount $(grep %s /proc/mounts | cut -f2 -d" " | sort -r)' % self.mnt_dir).communicate()

        # Kill the fuse processes for good measure
        common.pdsh(settings.getnodes('clients'), 'sudo killall -SIGKILL %s' % self.rbd_fuse_cmd).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo killall -SIGKILL %s' % self.ceph_fuse_cmd).communicate()

        # Unmount RBD and NBD for good measure
        common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec umount \'{}\' \\;').communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo find /dev/nbd* -maxdepth 0 -type b -exec umount \'{}\' \\;').communicate()

        # Unmap rbd, nbd, and clear the targetcli config
        common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec %s unmap \'{}\' \\;' % self.rbd_cmd).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo find /dev/nbd* -maxdepth 0 -type b -exec %s unmap \'{}\' \\;' % self.rbd_nbd_cmd).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo targetcli clearconfig confirm=True', continue_if_error=False).communicate()

    def get_urls(self):
        return self.urls

    def get_auth_urls(self):
        return self.auth_urls

    def add_s3_user(self, user, access_key, secret_key):
        if self.urls:
            cmd = "%s" % self.radosgw_admin_cmd
            node = settings.getnodes('head')
            common.pdsh(node, '%s -c %s user create --uid=%s --display-name=%s' % (cmd, self.tmp_conf, user, user)).communicate()
            common.pdsh(node, '%s -c %s key create --uid=%s --key-type=s3 --access_key=%s --secret_key=%s' % (cmd, self.tmp_conf, user, access_key, secret_key)).communicate()
            common.pdsh(node, '%s -c %s user modify --uid=%s --max-buckets=0' % (cmd, self.tmp_conf, user)).communicate()

    def add_swift_user(self, user, subuser, key):
        if self.auth_urls:
            cmd = "%s" % self.radosgw_admin_cmd
            node = settings.getnodes('head')
            common.pdsh(node, '%s -c %s user create --uid=%s --display-name=%s' % (cmd, self.tmp_conf, user, user)).communicate()
            common.pdsh(node, '%s -c %s subuser create --uid=%s --subuser=%s --access=full' % (cmd, self.tmp_conf, user, subuser)).communicate()
            common.pdsh(node, '%s -c %s key create --subuser=%s --key-type=swift --secret=%s' % (cmd, self.tmp_conf, subuser, key)).communicate()
            common.pdsh(node, '%s -c %s user modify --uid=%s --max-buckets=0' % (cmd, self.tmp_conf, user)).communicate()

    def make_rgw_pools(self):
        rgw_pools = self.config.get('rgw_pools', {})
        self.mkpool('.rgw.root', rgw_pools.get('root', 'default'), 'rgw')
        self.mkpool('default.rgw.control', rgw_pools.get('control', 'default'), 'rgw')
        self.mkpool('default.rgw.meta', rgw_pools.get('meta', 'default'), 'rgw')
        self.mkpool('default.rgw.log', rgw_pools.get('log', 'default'), 'rgw')
        self.mkpool('default.rgw.buckets', rgw_pools.get('buckets', 'default'), 'rgw')
        self.mkpool('default.rgw.buckets.index', rgw_pools.get('buckets_index', 'default'), 'rgw')
        self.mkpool('default.rgw.buckets.data', rgw_pools.get('buckets_data', 'default'), 'rgw')

class RecoveryTestThreadBlocking(threading.Thread):
    def __init__(self, config, cluster, callback, stoprequest, haltrequest):
        threading.Thread.__init__(self)
        self.config = config
        self.cluster = cluster
        self.callback = callback
        self.state = 'pre'
        self.states = {'pre': self.pre, 'markdown': self.markdown, 'osdout': self.osdout, 'osdin': self.osdin, 'post': self.post, 'done': self.done}
        self.stoprequest = stoprequest
        self.haltrequest = haltrequest
        self.outhealthtries = 0
        self.inhealthtries = 0
        self.maxhealthtries = 60
        self.health_checklist = ["degraded", "peering", "recovery_wait", "stuck", "inactive", "unclean", "recovery"]
        self.ceph_cmd = self.cluster.ceph_cmd
        self.lasttime = time.time()

    def logcmd(self, message):
        return 'echo "[`date`] %s" >> %s/recovery.log' % (message, self.config.get('run_dir'))

    def pre(self):
        pre_time = self.config.get("pre_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Starting Recovery Test Thread, waiting %s seconds.' % pre_time)).communicate()
        time.sleep(pre_time)
        lcmd = self.logcmd("Setting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), '%s -c %s osd set noup;%s' % (self.ceph_cmd, self.cluster.tmp_conf, lcmd)).communicate()
        self.state = 'markdown'

    def markdown(self):
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s down." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd down %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s out." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd out %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
        common.pdsh(settings.getnodes('head'), self.logcmd('Waiting for the cluster to break and heal')).communicate()
        self.lasttime = time.time()
        self.state = 'osdout'

    def osdout(self):
        reclog = "%s/recovery.log" % self.config.get('run_dir')
        recstatslog = "%s/recovery_stats.log" % self.config.get('run_dir')
        ret = self.cluster.check_health(self.health_checklist, reclog, recstatslog)

        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()

        if self.outhealthtries < self.maxhealthtries and ret == 0:
            self.outhealthtries = self.outhealthtries + 1
            return  # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()
            rectime = str(time.time() - self.lasttime)
            common.pdsh(settings.getnodes('head'), 'echo Time: %s >> %s' % (rectime, recstatslog)).communicate()
            common.pdsh(settings.getnodes('head'), self.logcmd('Time: %s' % rectime)).communicate()
        lcmd = self.logcmd("Unsetting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), '%s -c %s osd unset noup;%s' % (self.ceph_cmd, self.cluster.tmp_conf, lcmd)).communicate()
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s up." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd up %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s in." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd in %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
        self.lasttime = time.time()
        self.state = "osdin"

    def osdin(self):
        # Wait until the cluster is done backfilling.
        ret = self.cluster.check_backfill(self.health_checklist, "%s/recovery.log" % self.config.get('run_dir'))
        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()

        if self.inhealthtries < self.maxhealthtries and ret == 0:
            self.inhealthtries = self.inhealthtries + 1
            return  # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went into backfill.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()
            common.pdsh(settings.getnodes('head'), self.logcmd('Time: %s' % str(time.time() - self.lasttime))).communicate()
        self.state = "post"

    def post(self):
        if self.stoprequest.isSet():
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but stoprequest is set, finishing now.')).communicate()
            self.haltrequest.set()
            return

        if self.config.get("repeat", False):
            # reset counters
            self.outhealthtries = 0
            self.inhealthtries = 0

            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but repeat is set.  Moving to "markdown" state.')).communicate()
            self.state = "markdown"
            return

        post_time = self.config.get("post_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, completion in %s seconds.' % post_time)).communicate()
        time.sleep(post_time)
        self.state = "done"

    def done(self):
        common.pdsh(settings.getnodes('head'), self.logcmd("Done.  Calling parent callback function.")).communicate()
        self.callback()
        self.haltrequest.set()

    def join(self, timeout=None):
        common.pdsh(settings.getnodes('head'), self.logcmd('Received notification that parent is finished and waiting.')).communicate()
        super(RecoveryTestThreadBlocking, self).join(timeout)

    def run(self):
        self.haltrequest.clear()
        self.stoprequest.clear()
        while not self.haltrequest.isSet():
            self.states[self.state]()
        common.pdsh(settings.getnodes('head'), self.logcmd('Exiting recovery test thread.  Last state was: %s' % self.state)).communicate()

class RecoveryTestThreadBackground(threading.Thread):
    def __init__(self, config, cluster, callback, stoprequest, haltrequest, startiorequest):
        threading.Thread.__init__(self)
        self.config = config
        self.cluster = cluster
        self.callback = callback
        self.state = 'pre'
        self.states = {'pre': self.pre, 'markdown': self.markdown, 'osdout': self.osdout, 'osdin':self.osdin, 'post':self.post, 'done':self.done}
        self.startiorequest = startiorequest
        self.stoprequest = stoprequest
        self.haltrequest = haltrequest
        self.outhealthtries = 0
        self.inhealthtries = 0
        self.maxhealthtries = 60
        self.health_checklist = ["degraded", "peering", "recovery_wait", "stuck", "inactive", "unclean", "recovery"]
        self.ceph_cmd = self.cluster.ceph_cmd
        self.lasttime = time.time()

    def logcmd(self, message):
        return 'echo "[`date`] %s" >> %s/recovery.log' % (message, self.config.get('run_dir'))

    def pre(self):
        pre_time = self.config.get("pre_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Starting Recovery Test Thread, waiting %s seconds.' % pre_time)).communicate()
        time.sleep(pre_time)
        lcmd = self.logcmd("Setting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), '%s -c %s osd set noup;%s' % (self.ceph_cmd, self.cluster.tmp_conf, lcmd)).communicate()
        self.state = 'markdown'

    def markdown(self):
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s down." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd down %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s out." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd out %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
        common.pdsh(settings.getnodes('head'), self.logcmd('Waiting for the cluster to break and heal')).communicate()
        self.lasttime = time.time()
        self.state = 'osdout'

    def osdout(self):
        reclog = "%s/recovery.log" % self.config.get('run_dir')
        recstatslog = "%s/recovery_stats.log" % self.config.get('run_dir')
        ret = self.cluster.check_health(self.health_checklist, reclog, recstatslog)

        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()
            rectime = str(time.time() - self.lasttime)
            common.pdsh(settings.getnodes('head'), 'echo Time: %s >> %s' % (rectime, recstatslog)).communicate()
            common.pdsh(settings.getnodes('head'), self.logcmd('Time: %s' % rectime)).communicate()

        # Populate the recovery pool
        self.cluster.maybe_populate_recovery_pool()

        common.pdsh(settings.getnodes('head'), self.logcmd("osdout state - Sleeping for 10 secs after populating recovery pool.")).communicate()
        time.sleep(10)
        lcmd = self.logcmd("Unsetting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), '%s -c %s osd unset noup;%s' % (self.ceph_cmd, self.cluster.tmp_conf, lcmd)).communicate()
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s up." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd up %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s in." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd in %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
        self.lasttime = time.time()
        self.state = "osdin"

    def osdin(self):
        # Set startiorequest event to initiate client IO on another pool
        self.startiorequest.set()
        # Make recovery thread Wait until the cluster is done backfilling.
        recstatslog = "%s/recovery_backfill_stats.log" % self.config.get('run_dir')
        ret = self.cluster.check_backfill(self.health_checklist, "%s/recovery.log" % self.config.get('run_dir'), recstatslog)
        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()

        if self.inhealthtries < self.maxhealthtries and ret == 0:
            self.inhealthtries = self.inhealthtries + 1
            return # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went into backfill.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()
            rectime = str(time.time() - self.lasttime)
            common.pdsh(settings.getnodes('head'), 'echo Time: %s >> %s' % (rectime, recstatslog)).communicate()
            common.pdsh(settings.getnodes('head'), self.logcmd('Time: %s' % rectime)).communicate()
        self.state = "post"

    def post(self):
        if self.stoprequest.isSet():
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but stoprequest is set, finishing now.')).communicate()
            self.haltrequest.set()
            return

        if self.config.get("repeat", False):
            # reset counters
            self.outhealthtries = 0
            self.inhealthtries = 0

            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but repeat is set.  Moving to "markdown" state.')).communicate()
            self.state = "markdown"
            return

        common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, finishing up...')).communicate()
        self.state = "done"

    def done(self):
        common.pdsh(settings.getnodes('head'), self.logcmd("Done.  Calling parent callback function.")).communicate()
        self.callback()
        self.haltrequest.set()

    def join(self, timeout=None):
        common.pdsh(settings.getnodes('head'), self.logcmd('Received notification that parent is finished and waiting.')).communicate()
        super(RecoveryTestThreadBackground, self).join(timeout)

    def run(self):
        self.haltrequest.clear()
        self.stoprequest.clear()
        self.startiorequest.clear()
        while not self.haltrequest.isSet():
          self.states[self.state]()
        common.pdsh(settings.getnodes('head'), self.logcmd('Exiting recovery test thread.  Last state was: %s' % self.state)).communicate()

