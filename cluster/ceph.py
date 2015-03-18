import subprocess
import common
import settings
import monitoring
import os
import time
import uuid
import threading

from cluster import Cluster

class Ceph(Cluster):
    def __init__(self, config):
        super(Ceph, self).__init__(config)
        self.ceph_osd_cmd = config.get('ceph-osd_cmd', '/usr/bin/ceph-osd')
        self.ceph_mon_cmd = config.get('ceph-mon_cmd', '/usr/bin/ceph-mon')
        self.log_dir = config.get('log_dir', "%s/log" % self.tmp_dir)
        self.pid_dir = config.get('pid_dir', "%s/pid" % self.tmp_dir)
        self.core_dir = config.get('core_dir', "%s/core" % self.tmp_dir)
        self.monitoring_dir = "%s/monitoring" % self.tmp_dir
        self.keyring_fn = "%s/keyring" % self.tmp_dir
        self.osdmap_fn = "%s/osdmap" % self.tmp_dir
        self.monmap_fn = "%s/monmap" % self.tmp_dir
        self.tmp_conf = '%s/ceph.conf' % self.tmp_dir
        self.osd_valgrind = config.get('osd_valgrind', None)
        self.mon_valgrind = config.get('mon_valgrind', None)
        self.tiering = config.get('tiering', False)
        self.ruleset_map = {}
        self.cur_ruleset = 1;
        self.idle_duration = config.get('idle_duration', 0)

    def initialize(self): 
        super(Ceph, self).initialize()

        # unmount any kernel rbd volumes
        self.rbd_unmount()

        # shutdown any old processes
        self.shutdown()

        # Cleanup old junk and create new junk
        self.cleanup()
        common.mkdir_p(self.tmp_dir)
        common.pdsh(settings.getnodes('head', 'clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.tmp_dir)
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.pid_dir)
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.log_dir)
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.monitoring_dir)
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.core_dir)
        self.distribute_conf()

        # Set the core directory
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'echo "%s/core.%%e.%%p.%%h.%%t" | sudo tee /proc/sys/kernel/core_pattern' % self.tmp_dir)

        # Create the filesystems
        self.setup_fs()

        # Build the cluster
        monitoring.start('%s/creation' % self.monitoring_dir)
        self.make_mons()
        self.make_osds()
        monitoring.stop()

        # Check Health
        monitoring.start('%s/initial_health_check' % self.monitoring_dir)
        self.check_health()
        monitoring.stop()

        # Wait for initial scrubbing to complete (This should only matter on pre-dumpling clusters)
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

        common.pdsh(nodes, 'sudo killall -9 massif-amd64-li', True)
        common.pdsh(nodes, 'sudo killall -9 memcheck-amd64-', True)
        common.pdsh(nodes, 'sudo killall -9 ceph-osd', True)
        common.pdsh(nodes, 'sudo killall -9 ceph-mon', True)
        common.pdsh(nodes, 'sudo killall -9 ceph-mds', True)
        common.pdsh(nodes, 'sudo killall -9 rados', True)
        common.pdsh(nodes, 'sudo killall -9 rest-bench', True)
        common.pdsh(nodes, 'sudo killall -9 radosgw', True)
        common.pdsh(nodes, 'sudo killall -9 radosgw-admin', True)
        common.pdsh(nodes, 'sudo /etc/init.d/apache2 stop', True)
        common.pdsh(nodes, 'sudo killall -9 pdsh', True)
        monitoring.stop()

    def cleanup(self):
        nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
        print 'Deleting %s' % self.tmp_dir
        common.pdsh(nodes, 'sudo rm -rf %s' % self.tmp_dir)

    def setup_fs(self):
        sc = settings.cluster
        fs = sc.get('fs')
        mkfs_opts = sc.get('mkfs_opts', '')
        mount_opts = sc.get('mount_opts', '')

        if fs == '':
             shutdown("No OSD filesystem specified.  Exiting.")

        for device in xrange (0,sc.get('osds_per_node')):
            osds = settings.getnodes('osds')
            common.pdsh(osds, 'sudo umount /dev/disk/by-partlabel/osd-device-%s-data' % device)
            common.pdsh(osds, 'sudo rm -rf %s/osd-device-%s-data' % (self.mnt_dir, device))
            common.pdsh(osds, 'sudo mkdir -p -m0755 -- %s/osd-device-%s-data' % (self.mnt_dir, device))

            if fs == 'tmpfs':
                print 'using tmpfs osds, not creating a file system.'
            elif fs == 'zfs':
                print 'ruhoh, zfs detected.  No mkfs for you!'
                common.pdsh(osds, 'sudo zpool destroy osd-device-%s-data' % device)
                common.pdsh(osds, 'sudo zpool create -f -O xattr=sa -m legacy osd-device-%s-data /dev/disk/by-partlabel/osd-device-%s-data' % (device, device))
                common.pdsh(osds, 'sudo zpool add osd-device-%s-data log /dev/disk/by-partlabel/osd-device-%s-zil' % (device, device))
                common.pdsh(osds, 'sudo mount %s -t zfs osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, device, self.mnt_dir, device))
            else: 
                common.pdsh(osds, 'sudo mkfs.%s %s /dev/disk/by-partlabel/osd-device-%s-data' % (fs, mkfs_opts, device))
                common.pdsh(osds, 'sudo mount %s -t %s /dev/disk/by-partlabel/osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, fs, device, self.mnt_dir, device))


    def distribute_conf(self):
        nodes = settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws')
        conf_file = self.config.get("conf_file")
        print "Distributing %s." % conf_file
        common.pdcp(nodes, '', conf_file, self.tmp_conf)
        common.pdsh(nodes, 'sudo mv /etc/ceph/ceph.conf /etc/ceph/ceph.conf.cbt.bak', True)
        common.pdsh(nodes, 'sudo ln -s %s /etc/ceph/ceph.conf' % self.tmp_conf)

    def make_mons(self):
        # Build and distribute the keyring
        common.pdsh(settings.getnodes('head'), 'ceph-authtool --create-keyring --gen-key --name=mon. %s --cap mon \'allow *\'' % self.keyring_fn)
        common.pdsh(settings.getnodes('head'), 'ceph-authtool --gen-key --name=client.admin --set-uid=0 --cap mon \'allow *\' --cap osd \'allow *\' --cap mds allow %s' % self.keyring_fn)
        common.rscp(settings.getnodes('head'), self.keyring_fn, '%s.tmp' % self.keyring_fn)
        common.pdcp(settings.getnodes('mons', 'osds', 'rgws', 'mds'), '', '%s.tmp' % self.keyring_fn, self.keyring_fn)

        # Build the monmap, retrieve it, and distribute it
        mons = settings.getnodes('mons').split(',')
        cmd = 'monmaptool --create --clobber'
        monhosts = settings.cluster.get('mons')
        print monhosts
        for monhost, mons in monhosts.iteritems():
           for mon, addr in mons.iteritems():
                cmd = cmd + ' --add %s %s' % (mon, addr)
        cmd = cmd + ' --print %s' % self.monmap_fn
        common.pdsh(settings.getnodes('head'), cmd)
        common.rscp(settings.getnodes('head'), self.monmap_fn, '%s.tmp' % self.monmap_fn)
        common.pdcp(settings.getnodes('mons'), '', '%s.tmp' % self.monmap_fn, self.monmap_fn)

        # Build the ceph-mons
        user = settings.cluster.get('user')
        for monhost, mons in monhosts.iteritems():
            if user:
                monhost = '%s@%s' % (user, monhost)
            for mon, addr in mons.iteritems():
                common.pdsh(monhost, 'sudo rm -rf %s/mon.%s' % (self.tmp_dir, mon))
                common.pdsh(monhost, 'mkdir -p %s/mon.%s' % (self.tmp_dir, mon))
                common.pdsh(monhost, 'sudo sh -c "ulimit -c unlimited && exec %s --mkfs -c %s -i %s --monmap=%s --keyring=%s"' % (self.ceph_mon_cmd, self.tmp_conf, mon, self.monmap_fn, self.keyring_fn))
                common.pdsh(monhost, 'cp %s %s/mon.%s/keyring' % (self.keyring_fn, self.tmp_dir, mon))
            
        # Start the mons
        for monhost, mons in monhosts.iteritems():
            if user:
                monhost = '%s@%s' % (user, monhost)
            for mon, addr in mons.iteritems():
                pidfile="%s/%s.pid" % (self.pid_dir, monhost)
                cmd = 'sudo sh -c "ulimit -c unlimited && exec %s -c %s -i %s --keyring=%s --pid-file=%s"' % (self.ceph_mon_cmd, self.tmp_conf, mon, self.keyring_fn, pidfile)
                if self.mon_valgrind:
                    cmd = "%s %s" % (common.setup_valgrind(self.mon_valgrind, 'mon.%s' % monhost, self.tmp_dir), cmd)
                else:
                    cmd = 'ceph-run %s' % cmd
                common.pdsh(monhost, 'sudo %s' % cmd)

    def make_osds(self):
        osdnum = 0
        osdhosts = settings.cluster.get('osds')

        for host in osdhosts:
            user = settings.cluster.get('user')
            if user:
                pdshhost = '%s@%s' % (user, host)

            for i in xrange(0, settings.cluster.get('osds_per_node')):            
                # Build the OSD
                osduuid = str(uuid.uuid4())
                key_fn = '%s/osd-device-%s-data/keyring' % (self.mnt_dir, i)
                common.pdsh(pdshhost, 'sudo ceph -c %s osd create %s' % (self.tmp_conf, osduuid))
                common.pdsh(pdshhost, 'sudo ceph -c %s osd crush add osd.%d 1.0 host=%s rack=localrack root=default' % (self.tmp_conf, osdnum, host))
                common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s -c %s -i %d --mkfs --mkkey --osd-uuid %s"' % (self.ceph_osd_cmd, self.tmp_conf, osdnum, osduuid))
                common.pdsh(pdshhost, 'sudo ceph -c %s -i %s auth add osd.%d osd "allow *" mon "allow profile osd"' % (self.tmp_conf, key_fn, osdnum))

                # Start the OSD
                pidfile="%s/ceph-osd.%d.pid" % (self.pid_dir, osdnum)
                cmd = '%s -c %s -i %d --pid-file=%s' % (self.ceph_osd_cmd, self.tmp_conf, osdnum, pidfile)
                if self.osd_valgrind:
                    cmd = "%s %s" % (common.setup_valgrind(self.osd_valgrind, 'osd.%d' % osdnum, self.tmp_dir), cmd)
                else:
                    cmd = 'ceph-run %s' % cmd

                common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && ulimit -c unlimited && exec %s"' % cmd)
                osdnum = osdnum+1


    def check_health(self, logfile=None):
        logline = ""
        if logfile:
            logline = "| tee -a %s" % logfile
        ret = 0

        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), 'ceph -c %s health %s' % (self.tmp_conf, logline))
            if "HEALTH_OK" in stdout:
                break
            else:
                ret = ret + 1
            print stdout
            time.sleep(1)
        return ret

    def check_scrub(self):
        print 'Waiting until Scrubbing completes...'
        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), 'ceph -c %s pg dump | cut -f 16 | grep "0.000000" | wc -l' % self.tmp_conf)
            if " 0\n" in stdout:
                break
            else:
                print stdout
            time.sleep(1)

    def dump_config(self, run_dir):
        common.pdsh(settings.getnodes('osds'), 'sudo ceph -c %s --admin-daemon /var/run/ceph/ceph-osd.0.asok config show > %s/ceph_settings.out' % (self.tmp_conf, run_dir))

    def dump_historic_ops(self, run_dir):
        common.pdsh(settings.getnodes('osds'), 'find /var/run/ceph/*.asok -maxdepth 1 -exec sudo ceph --admin-daemon {} dump_historic_ops \; > %s/historic_ops.out' % run_dir)

    def set_osd_param(self, param, value):
        common.pdsh(settings.getnodes('osds'), 'find /dev/disk/by-partlabel/osd-device-*data -exec readlink {} \; | cut -d"/" -f 3 | sed "s/[0-9]$//" | xargs -I{} sudo sh -c "echo %s > /sys/block/\'{}\'/queue/%s"' % (value, param))


    def __str__(self):
        return "foo"

    def create_recovery_test(self, run_dir, callback):
        rt_config = self.config.get("recovery_test", {})
        rt_config['run_dir'] = run_dir
        self.rt = RecoveryTestThread(rt_config, self, callback)
        self.rt.start()

    def wait_recovery_done(self):
        self.rt.join()

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
        print self.ruleset_map
        return self.ruleset_map[name]

    def make_profiles(self):
        crush_profiles = self.config.get('crush_profiles', {})
        for name,profile in crush_profiles.items():
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd crush add-bucket %s-root root' % (self.tmp_conf, name))
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd crush add-bucket %s-rack rack' % (self.tmp_conf, name))
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd crush move %s-rack root=%s-root' % (self.tmp_conf, name, name))
            # FIXME: We need to build a dict mapping OSDs to hosts and create a proper hierarchy!
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd crush add-bucket %s-host host' % (self.tmp_conf, name))
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd crush move %s-host rack=%s-rack' % (self.tmp_conf, name, name))
            
            osds = profile.get('osds', None)
            if not osds:
                raise Exception("No OSDs defined for crush profile, bailing!")
            for i in osds:
                common.pdsh(settings.getnodes('head'), 'ceph -c %s osd crush set %s 1.0 host=%s-host' % (self.tmp_conf, i, name))
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd crush rule create-simple %s %s-root osd' % (self.tmp_conf, name, name))
            self.set_ruleset(name)

        erasure_profiles = self.config.get('erasure_profiles', {})
        for name,profile in erasure_profiles.items():
            k = profile.get('erasure_k', 6)
            m = profile.get('erasure_m', 2)
	    common.pdsh(settings.getnodes('head'), 'ceph -c %s osd erasure-code-profile set %s ruleset-failure-domain=osd k=%s m=%s' % (self.tmp_conf, name, k, m))
            self.set_ruleset(name)

    def mkpool(self, name, profile_name):
        pool_profiles = self.config.get('pool_profiles', {'default': {}})
        profile = pool_profiles.get(profile_name, {})

        pg_size = profile.get('pg_size', 1024)
        pgp_size = profile.get('pgp_size', 1024)
        erasure_profile = profile.get('erasure_profile', '')
        replication = str(profile.get('replication', None))
        cache = profile.get('cache', None)
        crush_profile = profile.get('crush_profile', None)
        hit_set_type = profile.get('hit_set_type', None)
        hit_set_count = profile.get('hit_set_count', None)
        hit_set_period = profile.get('hit_set_period', None)
        target_max_objects = profile.get('target_max_objects', None)
        target_max_bytes = profile.get('target_max_bytes', None)

#        common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool delete %s %s --yes-i-really-really-mean-it' % (self.tmp_conf, name, name))
        common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool create %s %d %d %s' % (self.tmp_conf, name, pg_size, pgp_size, erasure_profile))

        if replication and replication == 'erasure':
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool create %s %d %d erasure %s' % (self.tmp_conf, name, pg_size, pgp_size, erasure_profile))
        else:
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool create %s %d %d' % (self.tmp_conf, name, pg_size, pgp_size))

        if replication and replication.isdigit():
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool set %s size %s' % (self.tmp_conf, name, replication))

        if crush_profile:
            ruleset = self.get_ruleset(crush_profile)
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool set %s crush_ruleset %s' % (self.tmp_conf, name, ruleset))
        if hit_set_type:
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool set %s hit_set_type %s' % (self.tmp_conf, name, hit_set_type))
        if hit_set_count:
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool set %s hit_set_count %s' % (self.tmp_conf, name, hit_set_count))
        if hit_set_period:
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool set %s hit_set_period %s' % (self.tmp_conf, name, hit_set_period))
        if target_max_objects:
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool set %s target_max_objects %s' % (self.tmp_conf, name, target_max_objects))
        if target_max_bytes:
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool set %s target_max_bytes %s' % (self.tmp_conf, name, target_max_bytes))
        print 'Checking Healh after pool creation.'
        self.check_health()

        # If there is a cache profile assigned, make a cache pool
        if cache:
            cache_profile = cache.get('pool_profile', 'default')
            cache_mode = cache.get('mode', 'writeback')
            cache_name = '%s-cache' % name
            self.mkpool(cache_name, cache_profile)
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd tier add %s %s' % (self.tmp_conf, name, cache_name))
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd tier cache-mode %s %s' % (self.tmp_conf, cache_name, cache_mode))
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd tier set-overlay %s %s' % (self.tmp_conf, name, cache_name))

    def rmpool(self, name, profile_name):
        pool_profiles = self.config.get('pool_profiles', {'default': {}})
        profile = pool_profiles.get(profile_name, {})
        cache = profile.get('cache', None)
        if cache:
            cache_profile = cache.get('pool_profile', 'default')
            cache_name = '%s-cache' % name

            # flush and remove the overlay and such
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd tier cache-mode %s forward' % (self.tmp_conf, cache_name))
            common.pdsh(settings.getnodes('head'), 'sudo rados -c %s -p %s cache-flush-evict-all' % (self.tmp_conf, cache_name))
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd tier remove-overlay %s' % (self.tmp_conf, name))
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd tier remove %s %s' % (self.tmp_conf, name, cache_name))

            # delete the cache pool
            self.rmpool(cache_name, cache_profile)
        common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool delete %s %s --yes-i-really-really-mean-it' % (self.tmp_conf, name, name))

    def rbd_unmount(self):
        common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec umount \'{}\' \;',True)
#        common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec rbd -c %s unmap \'{}\' \;' % self.tmp_conf)
        common.pdsh(settings.getnodes('clients'), 'sudo service rbdmap stop')
class RecoveryTestThread(threading.Thread):
    def __init__(self, config, cluster, callback):
        threading.Thread.__init__(self)
        self.config = config
        self.cluster = cluster
        self.callback = callback
        self.state = 'pre'
        self.states = {'pre': self.pre, 'osdout': self.osdout, 'osdin':self.osdin, 'done':self.done}
        self.stoprequest = threading.Event()
        self.outhealthtries = 0
        self.inhealthtries = 0
        self.maxhealthtries = 60

    def logcmd(self, message):
        return 'echo "[`date`] %s" >> %s/recovery.log' % (message, self.config.get('run_dir'))

    def pre(self):
        pre_time = self.config.get("pre_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Starting Recovery Test Thread, waiting %s seconds.' % pre_time))
        time.sleep(pre_time)
        lcmd = self.logcmd("Setting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), 'ceph -c %s ceph osd set noup;%s' % (self.cluster.tmp_conf, lcmd))
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s down." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd down %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd))
            lcmd = self.logcmd("Marking OSD %s out." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd out %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd))
        common.pdsh(settings.getnodes('head'), self.logcmd('Waiting for the cluster to break and heal'))

        self.state = 'osdout'

    def osdout(self):
        ret = self.cluster.check_health("%s/recovery.log" % self.config.get('run_dir'))
        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret))

        if self.outhealthtries < self.maxhealthtries and ret == 0:
            self.outhealthtries = self.outhealthtries + 1
            return # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.'))
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.'))

        lcmd = self.logcmd("Unsetting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), 'ceph -c %s ceph osd unset noup;%s' % (self.cluster.tmp_conf, lcmd))
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s up." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd up %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd))
            lcmd = self.logcmd("Marking OSD %s in." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd in %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd))

        self.state = "osdin"

    def osdin(self):
        # Wait until the cluster is healthy.
        ret = self.cluster.check_health("%s/recovery.log" % self.config.get('run_dir'))
        if self.inhealthtries < self.maxhealthtries and ret == 0:
            self.inhealthtries = self.inhealthtries + 1
            return # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.'))
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.'))

        post_time = self.config.get("post_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, completion in %s seconds.' % post_time))
        time.sleep(post_time)
        self.state = "done"

    def done(self):
        common.pdsh(settings.getnodes('head'), self.logcmd("Done.  Calling parent callback function."))
        self.callback()
        self.stoprequest.set()

    def join(self, timeout=None):
        common.pdsh(settings.getnodes('head'), self.logcmd('Received notification that parent is finished and waiting.'))
        super(RecoveryTestThread, self).join(timeout)

    def run(self):
        self.stoprequest.clear()
        while not self.stoprequest.isSet():
          self.states[self.state]()
        common.pdsh(settings.getnodes('head'), self.logcmd('Exiting recovery test thread.  Last state was: %s' % self.state))

