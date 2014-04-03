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
        self.log_dir = config.get('log_dir', "%s/log" % self.tmp_dir)
        self.pid_dir = config.get('pid_dir', "%s/pid" % self.tmp_dir)
        self.monitoring_dir = "%s/monitoring" % self.tmp_dir
        self.keyring_fn = "%s/keyring" % self.tmp_dir
        self.osdmap_fn = "%s/osdmap" % self.tmp_dir
        self.monmap_fn = "%s/monmap" % self.tmp_dir
        self.tmp_conf = '%s/ceph.conf' % self.tmp_dir
        self.osd_valgrind = config.get('osd_valgrind', False)
        self.mon_valgrind = config.get('mon_valgrind', False)

    def initialize(self): 
        super(Ceph, self).initialize()

        # First, shutdown any old processes
        self.shutdown()

        # Cleanup old junk and create new junk
        self.cleanup()
        common.mkdir_p(self.tmp_dir)
        common.pdsh(settings.getnodes('head', 'clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.tmp_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.pid_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.log_dir).communicate()
        common.pdsh(settings.getnodes('clients', 'mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s' % self.monitoring_dir).communicate()
        self.distribute_conf()

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

        # Peform Idle Monitoring
        monitoring.start("%s/idle_monitoring" % self.monitoring_dir)
        time.sleep(60)
        monitoring.stop()

        return True

    def shutdown(self):
        nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')

        common.pdsh(nodes, 'sudo killall -9 massif-amd64-li').communicate()
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
        print 'Deleting %s' % self.tmp_dir
        common.pdsh(nodes, 'sudo rm -rf %s' % self.tmp_dir).communicate()

    def setup_fs(self):
        sc = settings.cluster
        fs = sc.get('fs')
        mkfs_opts = sc.get('mkfs_opts', '')
        mount_opts = sc.get('mount_opts', '')

        if fs == '':
             shutdown("No OSD filesystem specified.  Exiting.")

        for device in xrange (0,sc.get('osds_per_node')):
            osds = settings.getnodes('osds')
            common.pdsh(osds, 'sudo umount /dev/disk/by-partlabel/osd-device-%s-data' % device).communicate()
            common.pdsh(osds, 'sudo rm -rf %s/osd-device-%s-data' % (self.mnt_dir, device)).communicate()
            common.pdsh(osds, 'sudo mkdir -p -m0755 -- %s/osd-device-%s-data' % (self.mnt_dir, device)).communicate()

            if fs == 'tmpfs':
                print 'using tmpfs osds, not creating a file system.'
            elif fs == 'zfs':
                print 'ruhoh, zfs detected.  No mkfs for you!'
                common.pdsh(osds, 'sudo zpool destroy osd-device-%s-data' % device).communicate()
                common.pdsh(osds, 'sudo zpool create -f -O xattr=sa -m legacy osd-device-%s-data /dev/disk/by-partlabel/osd-device-%s-data' % (device, device)).communicate()
                common.pdsh(osds, 'sudo zpool add osd-device-%s-data log /dev/disk/by-partlabel/osd-device-%s-zil' % (device, device)).communicate()
                common.pdsh(osds, 'sudo mount %s -t zfs osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, device, self.mnt_dir, device)).communicate()
            else: 
                common.pdsh(osds, 'sudo mkfs.%s %s /dev/disk/by-partlabel/osd-device-%s-data' % (fs, mkfs_opts, device)).communicate()
                common.pdsh(osds, 'sudo mount %s -t %s /dev/disk/by-partlabel/osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, fs, device, self.mnt_dir, device)).communicate()


    def distribute_conf(self):
        nodes = settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws')
        conf_file = self.config.get("conf_file")
        print "Distributing %s." % conf_file
        common.pdcp(nodes, '', conf_file, self.tmp_conf).communicate()

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
        print monhosts
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
                common.pdsh(monhost, 'sudo ceph-mon --mkfs -c %s -i %s --monmap=%s --keyring=%s' % (self.tmp_conf, mon, self.monmap_fn, self.keyring_fn)).communicate()
                common.pdsh(monhost, 'cp %s %s/mon.%s/keyring' % (self.keyring_fn, self.tmp_dir, mon)).communicate()
            
        # Start the mons
        for monhost, mons in monhosts.iteritems():
            if user:
                monhost = '%s@%s' % (user, monhost)
            for mon, addr in mons.iteritems():
                pidfile="%s/%s.pid" % (self.pid_dir, monhost)
                cmd = 'ceph-mon -c %s -i %s --keyring=%s --pid-file=%s' % (self.tmp_conf, mon, self.keyring_fn, pidfile)
                if self.mon_valgrind:
                    valdir = '%s/valgrind' % self.tmp_dir
                    common.pdsh(monhost, 'sudo mkdir -p -m0755 -- %s' % valdir).communicate()
                    logfile = '%s/ceph-mon.%s.log' % (valdir, mon)
                    outfile = '%s/ceph-mon.%s.out' % (valdir, mon)
                    cmd = 'valgrind --tool=massif --soname-synonyms=somalloc=*tcmalloc* --massif-out-file=%s --log-file=%s %s' % (outfile, logfile, cmd)
                else:
                    cmd = 'ceph-run %s' % cmd
                common.pdsh(monhost, 'sudo %s' % cmd).communicate()

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
                common.pdsh(pdshhost, 'sudo ceph -c %s osd create %s' % (self.tmp_conf, osduuid)).communicate()
                common.pdsh(pdshhost, 'sudo ceph -c %s osd crush add osd.%d 1.0 host=%s rack=localrack root=default' % (self.tmp_conf, osdnum, host)).communicate()
                common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && exec ceph-osd -c %s -i %d --mkfs --mkkey --osd-uuid %s"' % (self.tmp_conf, osdnum, osduuid)).communicate()
                common.pdsh(pdshhost, 'sudo ceph -c %s -i %s auth add osd.%d osd "allow *" mon "allow profile osd"' % (self.tmp_conf, key_fn, osdnum)).communicate()

                # Start the OSD
                pidfile="%s/ceph-osd.%d.pid" % (self.pid_dir, osdnum)
                cmd = 'ceph-osd -c %s -i %d --pid-file=%s' % (self.tmp_conf, osdnum, pidfile)
                if self.osd_valgrind:
                    valdir = '%s/valgrind' % self.tmp_dir
                    common.pdsh(pdshhost, 'sudo mkdir -p -m0755 -- %s' % valdir).communicate()
                    logfile = '%s/ceph-osd.%d.log' % (valdir, osdnum)
                    outfile = '%s/ceph-osd.%d.out' % (valdir, osdnum)
                    cmd = 'valgrind --tool=massif --soname-synonyms=somalloc=*tcmalloc* --massif-out-file=%s --log-file=%s %s' % (outfile, logfile, cmd)
                else:
                    cmd = 'ceph-run %s' % cmd
                common.pdsh(pdshhost, 'sudo sh -c "ulimit -n 16384 && exec %s"' % cmd).communicate()
                osdnum = osdnum+1


    def check_health(self, logfile=None):
        logline = ""
        if logfile:
            logline = "| tee -a %s" % logfile
        ret = 0

        while True:
            stdout, stderr = common.pdsh(settings.getnodes('head'), 'ceph -c %s health %s' % (self.tmp_conf, logline)).communicate()
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
            stdout, stderr = common.pdsh(settings.getnodes('head'), 'ceph -c %s pg dump | cut -f 16 | grep "0.000000" | wc -l' % self.tmp_conf).communicate()
            if " 0\n" in stdout:
                break
            else:
                print stdout
            time.sleep(1)

    def dump_config(self, run_dir):
        common.pdsh(settings.getnodes('osds'), 'sudo ceph -c %s --admin-daemon /var/run/ceph/ceph-osd.0.asok config show > %s/ceph_settings.out' % (self.tmp_conf, run_dir)).communicate()

    def dump_historic_ops(self, run_dir):
        common.pdsh(settings.getnodes('osds'), 'find /var/run/ceph/*.asok -maxdepth 1 -exec sudo ceph --admin-daemon {} dump_historic_ops \; > %s/historic_ops.out' % run_dir).communicate()

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
        common.pdsh(settings.getnodes('head'), self.logcmd('Starting Recovery Test Thread, waiting %s seconds.' % pre_time)).communicate()
        time.sleep(pre_time)
        lcmd = self.logcmd("Setting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), 'ceph -c %s ceph osd set noup;%s' % (self.cluster.tmp_conf, lcmd)).communicate()
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s down." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd down %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s out." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd out %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd)).communicate()
        common.pdsh(settings.getnodes('head'), self.logcmd('Waiting for the cluster to break and heal')).communicate()

        self.state = 'osdout'

    def osdout(self):
        ret = self.cluster.check_health("%s/recovery.log" % self.config.get('run_dir'))
        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()

        if self.outhealthtries < self.maxhealthtries and ret == 0:
            self.outhealthtries = self.outhealthtries + 1
            return # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()

        lcmd = self.logcmd("Unsetting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), 'ceph -c %s ceph osd unset noup;%s' % (self.cluster.tmp_conf, lcmd)).communicate()
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s up." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd up %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s in." % osdnum)
            common.pdsh(settings.getnodes('head'), 'ceph -c %s osd in %s;%s' % (self.cluster.tmp_conf, osdnum, lcmd)).communicate()

        self.state = "osdin"

    def osdin(self):
        # Wait until the cluster is healthy.
        ret = self.cluster.check_health("%s/recovery.log" % self.config.get('run_dir'))
        if self.inhealthtries < self.maxhealthtries and ret == 0:
            self.inhealthtries = self.inhealthtries + 1
            return # Cluster hasn't become unhealthy yet.

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()

        post_time = self.config.get("post_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, completion in %s seconds.' % post_time)).communicate()
        time.sleep(post_time)
        self.state = "done"

    def done(self):
        common.pdsh(settings.getnodes('head'), self.logcmd("Done.  Calling parent callback function.")).communicate()
        self.callback()
        self.stoprequest.set()

    def join(self, timeout=None):
        common.pdsh(settings.getnodes('head'), self.logcmd('Received notification that parent is finished and waiting.')).communicate()
        super(RecoveryTestThread, self).join(timeout)

    def run(self):
        self.stoprequest.clear()
        while not self.stoprequest.isSet():
          self.states[self.state]()
        common.pdsh(settings.getnodes('head'), self.logcmd('Exiting recovery test thread.  Last state was: %s' % self.state)).communicate()

