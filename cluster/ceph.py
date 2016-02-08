import subprocess
import common
import settings
import monitoring
import os
import time
import uuid
import threading
import logging

from cluster import Cluster


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
    def __init__(self, cl_obj, devnumstr, osdnum, clusterid, host, osduuid, osddir):
        threading.Thread.__init__(self, name='OsdThread-%d'%osdnum)
        self.start_time = time.time()
        self.response_time = -1.0
        self.cl_obj = cl_obj
        self.devnumstr = devnumstr
        self.osdnum = osdnum
        self.clusterid = clusterid
        self.host = host
        self.osduuid = osduuid
        self.osddir = osddir
        self.exc = None

    def run(self):
        try:
            key_fn = '%s/keyring'%self.osddir
            ceph_conf = self.cl_obj.tmp_conf
            phost = sshtarget(settings.cluster.get('user'), self.host)
            common.pdsh(phost, 'sudo %s -c %s osd crush add osd.%d 1.0 host=%s rack=localrack root=default' % (self.cl_obj.ceph_cmd, ceph_conf, self.osdnum, self.host)).communicate()
            cmd='ulimit -n 16384 && ulimit -c unlimited && exec %s -c %s -i %d --mkfs --mkkey --osd-uuid %s' % (self.cl_obj.ceph_osd_cmd, ceph_conf, self.osdnum, self.osduuid)
            common.pdsh(phost, 'sudo sh -c "%s"' % cmd).communicate()
            common.pdsh(phost, 'sudo %s -c %s -i %s auth add osd.%d osd "allow *" mon "allow profile osd"' % (self.cl_obj.ceph_cmd, ceph_conf, key_fn, self.osdnum)).communicate()

            # Start the OSD
            pidfile="%s/ceph-osd.%d.pid" % (self.cl_obj.pid_dir, self.osdnum)
            cmd = '%s -c %s -i %d --pid-file=%s' % (self.cl_obj.ceph_osd_cmd, ceph_conf, self.osdnum, pidfile)
            if self.cl_obj.osd_valgrind:
                cmd = common.setup_valgrind(self.cl_obj.osd_valgrind, 'osd.%d' % self.osdnum, self.cl_obj.tmp_dir) + ' ' + cmd
            else:
                cmd = '%s %s' % (self.cl_obj.ceph_run_cmd, cmd)
            stderr_file = "%s/osd.%d.stderr" % (self.cl_obj.tmp_dir, self.osdnum)
            common.pdsh(phost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s 2> %s"' % (cmd, stderr_file)).communicate()
        except Exception as e:
            self.exc = e
        finally:
            self.response_time = time.time() - self.start_time

    def __str__(self):
        return 'osd thrd %d %s %s'%(self.osdnum, self.host, self.osduuid)

    # this is intended to be called by parent thread after join()
    def postprocess(self):
        if not (self.exc is None):
            logger.error('thread %s: %s' % (self.name, str(self.exc)))
            raise Exception('OSD %s creation did not complete' % self.osdnum)
        logger.info('thread %s completed creation of OSD %d elapsed time %f'%(self.name, self.osdnum, self.response_time))



class Ceph(Cluster):
    def __init__(self, config):
        super(Ceph, self).__init__(config)
        self.ceph_osd_cmd = config.get('ceph-osd_cmd', '/usr/bin/ceph-osd')
        self.ceph_mon_cmd = config.get('ceph-mon_cmd', '/usr/bin/ceph-mon')
        self.ceph_run_cmd = config.get('ceph-run_cmd', '/usr/bin/ceph-run')
        self.ceph_rgw_cmd = config.get('ceph-rgw_cmd', '/usr/bin/radosgw')
        self.ceph_cmd = config.get('ceph_cmd', '/usr/bin/ceph')
        self.rados_cmd = config.get('rados_cmd', '/usr/bin/rados')
        self.rbd_cmd = config.get('rbd_cmd', '/usr/bin/rbd')
        self.log_dir = config.get('log_dir', "%s/log" % self.tmp_dir)
        self.pid_dir = config.get('pid_dir', "%s/pid" % self.tmp_dir)
        self.core_dir = config.get('core_dir', "%s/core" % self.tmp_dir)
        self.monitoring_dir = "%s/monitoring" % self.tmp_dir
        self.keyring_fn = "%s/keyring" % self.tmp_dir
        self.osdmap_fn = "%s/osdmap" % self.tmp_dir
        self.monmap_fn = "%s/monmap" % self.tmp_dir
        self.use_existing = config.get('use_existing', True)
        self.newstore_block = config.get('newstore_block', False)

        # these parameters control parallel OSD build 
        self.ceph_osd_online_rate = config.get('osd_online_rate', 10)
        self.ceph_osd_online_tmo = config.get('osd_online_timeout', 120)
        self.ceph_osd_parallel_creates = config.get('osd_parallel_creates')

        # If making the cluster, use the ceph.conf file distributed by initialize to the tmp_dir
        self.tmp_conf = '%s/ceph.conf' % self.tmp_dir
        # If using an existing cluster, defualt to /etc/ceph/ceph.conf
        if self.use_existing:
            self.tmp_conf = '/etc/ceph/ceph.conf'

        self.osd_valgrind = config.get('osd_valgrind', None)
        self.mon_valgrind = config.get('mon_valgrind', None)
        self.rgw_valgrind = config.get('rgw_valgrind', None)
        self.tiering = config.get('tiering', False)
        self.ruleset_map = {}
        self.cur_ruleset = 1
        self.idle_duration = config.get('idle_duration', 0)
        self.use_existing = config.get('use_existing', True)
        self.stoprequest = threading.Event()
        self.haltrequest = threading.Event()


    def initialize(self): 
        # safety check to make sure we don't blow away an existing cluster!
        if self.use_existing:
             raise RuntimeError('initialize was called on an existing cluster! Avoiding touching anything.') 

        super(Ceph, self).initialize()

        # unmount any kernel rbd volumes
        self.rbd_unmount()

        # shutdown any old processes
        self.shutdown()

        # Cleanup old junk and create new junk
        self.cleanup()
        common.mkdir_p(self.tmp_dir)
        common.pdsh(settings.getnodes('head', 'clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.tmp_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.pid_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.log_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.monitoring_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.core_dir).communicate()
        self.distribute_conf()

        # Set the core directory
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'echo "%s/core.%%e.%%p.%%h.%%t" | sudo tee /proc/sys/kernel/core_pattern' % self.tmp_dir).communicate()

        # Create the filesystems
        self.setup_fs()

        # Build the cluster
        monitoring.start('%s/creation' % self.monitoring_dir)
        self.make_mons()
        self.make_osds()
        self.start_rgw()
        monitoring.stop()

        # Check Health
        monitoring.start('%s/initial_health_check' % self.monitoring_dir)
        self.check_health()
        monitoring.stop()

        # Disable scrub and wait for any scrubbing to complete 
        self.disable_scrub()
        self.check_scrub()

        # Make the crush and erasure profiles
        self.make_profiles()

        # Peform Idle Monitoring
        if self.idle_duration > 0:
            monitoring.start("%s/idle_monitoring" % self.monitoring_dir)
            time.sleep(self.idle_duration)
            monitoring.stop()

        return True

    def shutdown(self):
        nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')

        common.pdsh(nodes, 'sudo killall -9 massif-amd64-li').communicate()
        common.pdsh(nodes, 'sudo killall -9 memcheck-amd64-').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-osd').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-mon').communicate()
        common.pdsh(nodes, 'sudo killall -9 ceph-mds').communicate()
        common.pdsh(nodes, 'sudo killall -9 rados').communicate()
        common.pdsh(nodes, 'sudo killall -9 rest-bench').communicate()
        common.pdsh(nodes, 'sudo killall -9 radosgw').communicate()
        common.pdsh(nodes, 'sudo killall -9 radosgw-admin').communicate()
        common.pdsh(nodes, 'sudo /etc/init.d/apache2 stop').communicate()
        common.pdsh(nodes, 'sudo killall -9 pdsh').communicate()
        monitoring.stop()

    def cleanup(self):
        nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
        logger.info('Deleting %s', self.tmp_dir)
        common.pdsh(nodes, 'sudo rm -rf %s' % self.tmp_dir).communicate()

    def setup_fs(self):
        sc = settings.cluster
        fs = sc.get('fs')
        mkfs_opts = sc.get('mkfs_opts', '')
        mount_opts = sc.get('mount_opts', '')

        if fs == '':
             settings.shutdown("No OSD filesystem specified.  Exiting.")

        mkfs_threads = []
        for device in xrange (0,sc.get('osds_per_node')):
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
                mkfs_cmd='sudo sh -c "mkfs.%s %s /dev/disk/by-partlabel/osd-device-%s-data' % (fs, mkfs_opts, device)
                mkfs_cmd += '; mount %s -t %s /dev/disk/by-partlabel/osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, fs, device, self.mnt_dir, device)
                
                # make a symlink for block if using newstore+block
                if self.newstore_block:
                    mkfs_cmd += ' ; sudo ln -s /dev/disk/by-partlabel/osd-device-%s-block %s/osd-device-%s-data/block' % (device, self.mnt_dir, device)
                mkfs_cmd += '"'

                mkfs_threads.append((device, common.pdsh(osds, mkfs_cmd)))
        for device, t in mkfs_threads:  # for tmpfs and zfs cases, thread list is empty
            logger.info('for device %d on all hosts awaiting mkfs and mount'%device)
            t.communicate()

    def distribute_conf(self):
        nodes = settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws')
        conf_file = self.config.get("conf_file")
        logger.info("Distributing %s.", conf_file)
        common.pdsh(nodes, 'mkdir -p -m0755 /etc/ceph').communicate()
        common.pdcp(nodes, '', conf_file, self.tmp_conf).communicate()
        common.pdsh(nodes, 'sudo mv /etc/ceph/ceph.conf /etc/ceph/ceph.conf.cbt.bak').communicate()
        common.pdsh(nodes, 'sudo ln -s %s /etc/ceph/ceph.conf' % self.tmp_conf).communicate()

    def make_mons(self):
        # Build and distribute the keyring
        common.pdsh(settings.getnodes('head'), 'ceph-authtool --create-keyring --gen-key --name=mon. %s --cap mon \'allow *\'' % self.keyring_fn).communicate()
        common.pdsh(settings.getnodes('head'), 'ceph-authtool --gen-key --name=client.admin --set-uid=0 --cap mon \'allow *\' --cap osd \'allow *\' --cap mds allow %s' % self.keyring_fn).communicate()
        common.rscp(settings.getnodes('head'), self.keyring_fn, '%s.tmp' % self.keyring_fn).communicate()
        common.pdcp(settings.getnodes('mons', 'osds', 'rgws', 'mds'), '', '%s.tmp' % self.keyring_fn, self.keyring_fn).communicate()

        # Build the monmap, retrieve it, and distribute it
        mons = settings.getnodes('mons').split(',')
        cmd = 'monmaptool --create --clobber'
        monhosts = settings.cluster.get('mons')
        logger.info(monhosts)
        for monhost, mons in monhosts.iteritems():
           for mon, addr in mons.iteritems():
                cmd = cmd + ' --add %s %s' % (mon, addr)
        cmd = cmd + ' --print %s' % self.monmap_fn
        common.pdsh(settings.getnodes('head'), cmd).communicate()
        common.rscp(settings.getnodes('head'), self.monmap_fn, '%s.tmp' % self.monmap_fn).communicate()
        common.pdcp(settings.getnodes('mons'), '', '%s.tmp' % self.monmap_fn, self.monmap_fn).communicate()

        # Build the ceph-mons
        user = settings.cluster.get('user')
        for monhost, mons in monhosts.iteritems():
            if user:
                monhost = '%s@%s' % (user, monhost)
            for mon, addr in mons.iteritems():
                common.pdsh(monhost, 'sudo rm -rf %s/mon.%s' % (self.tmp_dir, mon)).communicate()
                common.pdsh(monhost, 'mkdir -p %s/mon.%s' % (self.tmp_dir, mon)).communicate()
                common.pdsh(monhost, 'sudo sh -c "ulimit -c unlimited && exec %s --mkfs -c %s -i %s --monmap=%s --keyring=%s"' % (self.ceph_mon_cmd, self.tmp_conf, mon, self.monmap_fn, self.keyring_fn)).communicate()
                common.pdsh(monhost, 'cp %s %s/mon.%s/keyring' % (self.keyring_fn, self.tmp_dir, mon)).communicate()
            
        # Start the mons
        for monhost, mons in monhosts.iteritems():
            if user:
                monhost = '%s@%s' % (user, monhost)
            for mon, addr in mons.iteritems():
                pidfile="%s/%s.pid" % (self.pid_dir, monhost)
                cmd = 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s -c %s -i %s --keyring=%s --pid-file=%s"' % (self.ceph_mon_cmd, self.tmp_conf, mon, self.keyring_fn, pidfile)
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

        logger.info('OSD creation rate: < %d OSDs/sec , join timeout %d, parallel creates < %s'%(
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
            for devnumstr in xrange(0, settings.cluster.get('osds_per_node')):            
                pdshhost = sshtarget(user, host)
                # Build the OSD
                osduuid = str(uuid.uuid4())
#                osddir='/var/lib/ceph/osd/%s-%d'%(clusterid, osdnum)
                osddir='%s/osd-device-%s-data' % (self.mnt_dir, devnumstr)
                # create the OSD first, so we know what number it has been assigned.
                common.pdsh(pdshhost, 'sudo %s -c %s osd create %s' % (self.ceph_cmd, self.tmp_conf, osduuid)).communicate()
                # bring the OSD online in background while continuing to create OSDs in foreground
                thrd = OsdThread(self, devnumstr, osdnum, clusterid, host, osduuid, osddir)
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
                time.sleep(osd_online_interval) # don't flood Ceph with OSD commands
                osdnum += 1

        # wait for rest of them to finish
        for thrd in thread_list[threads_finished:]:
            # an exception is thrown if the thread failed, hopefully
            thrd.join(self.ceph_osd_online_tmo)
            thrd.postprocess()

    def start_rgw(self):
        rgwhosts = settings.cluster.get('rgws', [])

        for host in rgwhosts:
            pdshhost = host
            user = settings.cluster.get('user')
            if user:
                pdshhost = '%s@%s' % (user, host)
            cmd = '%s -c %s -n client.radosgw.gateway --log-file=%s/rgw.log' % (self.ceph_rgw_cmd, self.tmp_conf, self.log_dir)
            if self.rgw_valgrind:
                cmd = "%s %s" % (common.setup_valgrind(self.rgw_valgrind, 'rgw.%s' % host, self.tmp_dir), cmd)
            else:
                cmd = '%s %s' % (self.ceph_run_cmd, cmd)

            common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s"' % cmd).communicate()


    def disable_scrub(self):
        common.pdsh(settings.getnodes('head'), "ceph osd set noscrub; ceph osd set nodeep-scrub").communicate()

    def check_health(self, check_list=None, logfile=None):
        logline = ""
        if logfile:
            logline = "| tee -a %s" % logfile
        ret = 0

        # Match any of these things to continue checking health
        check_list = ["degraded", "peering", "recovery_wait", "stuck", "inactive", "unclean", "recovery", "stale"]
        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), '%s -c %s health %s' % (self.ceph_cmd, self.tmp_conf, logline)).communicate()
            if check_list and not set(check_list).intersection(stdout.split()):
                break
            if "HEALTH_OK" in stdout:
                break
            else:
                ret = ret + 1
            logger.info("%s", stdout)
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
        common.pdsh(settings.getnodes('osds'), 'sudo %s -c %s --admin-daemon /var/run/ceph/ceph-osd.0.asok config show > %s/ceph_settings.out' % (self.ceph_cmd, self.tmp_conf, run_dir)).communicate()

    def dump_historic_ops(self, run_dir):
        common.pdsh(settings.getnodes('osds'), 'find /var/run/ceph/*.asok -maxdepth 1 -exec sudo %s --admin-daemon {} dump_historic_ops \; > %s/historic_ops.out' % (self.ceph_cmd, run_dir)).communicate()

    def set_osd_param(self, param, value):
        common.pdsh(settings.getnodes('osds'), 'find /dev/disk/by-partlabel/osd-device-*data -exec readlink {} \; | cut -d"/" -f 3 | sed "s/[0-9]$//" | xargs -I{} sudo sh -c "echo %s > /sys/block/\'{}\'/queue/%s"' % (value, param))


    def __str__(self):
        return "foo"

    def create_recovery_test(self, run_dir, callback):
        rt_config = self.config.get("recovery_test", {})
        rt_config['run_dir'] = run_dir
        self.rt = RecoveryTestThread(rt_config, self, callback, self.stoprequest, self.haltrequest)
        self.rt.start()

    def wait_recovery_done(self):
        self.stoprequest.set()
        while True:
            threads = threading.enumerate()
            if len(threads) == 1: break
            self.rt.join(1)

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
        for name,profile in crush_profiles.items():
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
        for name,profile in erasure_profiles.items():
            k = profile.get('erasure_k', 6)
            m = profile.get('erasure_m', 2)
	    common.pdsh(settings.getnodes('head'), '%s -c %s osd erasure-code-profile set %s ruleset-failure-domain=osd k=%s m=%s' % (self.ceph_cmd, self.tmp_conf, name, k, m)).communicate()
            self.set_ruleset(name)

    def mkpool(self, name, profile_name, base_name=None):
        pool_profiles = self.config.get('pool_profiles', {'default': {}})
        profile = pool_profiles.get(profile_name, {})

        pg_size = profile.get('pg_size', 1024)
        pgp_size = profile.get('pgp_size', 1024)
        erasure_profile = profile.get('erasure_profile', '')
        replication = str(profile.get('replication', None))
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

        # Options for prefilling objects
        prefill_objects = profile.get('prefill_objects', 0)
        prefill_object_size = profile.get('prefill_object_size', 0)
        prefill_time = profile.get('prefill_time', 0)

#        common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool delete %s %s --yes-i-really-really-mean-it' % (self.tmp_conf, name, name)).communicate()
        common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool create %s %d %d %s' % (self.ceph_cmd, self.tmp_conf, name, pg_size, pgp_size, erasure_profile)).communicate()

        if replication and replication == 'erasure':
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool create %s %d %d erasure %s' % (self.ceph_cmd, self.tmp_conf, name, pg_size, pgp_size, erasure_profile)).communicate()
        else:
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool create %s %d %d' % (self.ceph_cmd, self.tmp_conf, name, pg_size, pgp_size)).communicate()

        logger.info('Checking Healh after pool creation.')
        self.check_health()

        if replication and replication.isdigit():
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s size %s' % (self.ceph_cmd, self.tmp_conf, name, replication)).communicate()
            logger.info('Checking Health after setting pool replication level.')
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

        if crush_profile:
            ruleset = self.get_ruleset(crush_profile)
            common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool set %s crush_ruleset %s' % (self.ceph_cmd, self.tmp_conf, name, ruleset)).communicate()
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
        logger.info('Final Pool Health Check.')
        self.check_health()

        # If there is a cache profile assigned, make a cache pool
        if cache_profile:
            cache_name = '%s-cache' % name
            self.mkpool(cache_name, cache_profile, name)

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
        common.pdsh(settings.getnodes('head'), 'sudo %s -c %s osd pool delete %s %s --yes-i-really-really-mean-it' % (self.ceph_cmd, self.tmp_conf, name, name)).communicate()

    def rbd_unmount(self):
        common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec umount \'{}\' \;').communicate()
#        common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec rbd -c %s unmap \'{}\' \;' % self.tmp_conf).communicate()
        common.pdsh(settings.getnodes('clients'), 'sudo service rbdmap stop').communicate()

    def mkimage(self, name, size, pool, order):
        common.pdsh(settings.getnodes('head'), '%s -c %s create %s --size %s --pool %s --order %s' % (self.rbd_cmd, self.tmp_conf, name, size, pool, order)).communicate()

class RecoveryTestThread(threading.Thread):
    def __init__(self, config, cluster, callback, stoprequest, haltrequest):
        threading.Thread.__init__(self)
        self.config = config
        self.cluster = cluster
        self.callback = callback
        self.state = 'pre'
        self.states = {'pre': self.pre, 'markdown': self.markdown, 'osdout': self.osdout, 'osdin':self.osdin, 'post':self.post, 'done':self.done}
        self.stoprequest = stoprequest
        self.haltrequest = haltrequest
        self.outhealthtries = 0
        self.inhealthtries = 0
        self.maxhealthtries = 60
        self.health_checklist = ["degraded", "peering", "recovery_wait", "stuck", "inactive", "unclean", "recovery"]

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

        self.state = 'osdout'

    def osdout(self):
        ret = self.cluster.check_health(self.health_checklist, "%s/recovery.log" % self.config.get('run_dir'))
        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()

        if self.outhealthtries < self.maxhealthtries and ret == 0:
            self.outhealthtries = self.outhealthtries + 1
            return # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()

        lcmd = self.logcmd("Unsetting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), '%s -c %s osd unset noup;%s' % (self.ceph_cmd, self.cluster.tmp_conf, lcmd)).communicate()
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s up." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd up %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s in." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd in %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()

        self.state = "osdin"

    def osdin(self):
        # Wait until the cluster is healthy.
        ret = self.cluster.check_health(self.health_checklist, "%s/recovery.log" % self.config.get('run_dir'))
        if self.inhealthtries < self.maxhealthtries and ret == 0:
            self.inhealthtries = self.inhealthtries + 1
            return # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()
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
        super(RecoveryTestThread, self).join(timeout)

    def run(self):
        self.haltrequest.clear()
        self.stoprequest.clear()
        while not self.haltrequest.isSet():
          self.states[self.state]()
        common.pdsh(settings.getnodes('head'), self.logcmd('Exiting recovery test thread.  Last state was: %s' % self.state)).communicate()

