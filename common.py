import settings
import subprocess
import time
import os
import uuid

tmp_dir = settings.cluster.get("tmp_dir")
tmp_conf = "%s/ceph.conf" % tmp_dir
keyring_fn = "%s/keyring" % tmp_dir
osdmap_fn = "%s/ceph_osdmap.%s" % (tmp_dir,os.getpid())
monmap_fn = "%s/ceph_monmap.%s" % (tmp_dir,os.getpid())

def pdsh(nodes, command):
    args = ['pdsh', '-R', 'ssh', '-w', nodes, command]
    print('pdsh: %s' % args)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def pdcp(nodes, flags, localfile, remotefile):
    args = ['pdcp', '-R', 'ssh', '-w', nodes, localfile, remotefile]
    if flags:
        args = ['pdcp', '-R', 'ssh', '-w', nodes, flags, localfile, remotefile]
    print('pdcp: %s' % args)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def rpdcp(nodes, flags, remotefile, localfile):
    args = ['rpdcp', '-R', 'ssh', '-w', nodes, remotefile, localfile]
    if flags:
        args = ['rpdcp', '-R', 'ssh', '-w', nodes, flags, remotefile, localfile]
    print('rpdcp: %s'  % args)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def scp(node, localfile, remotefile):
    args = ['scp', localfile, '%s:%s' % (node, remotefile)]
    print('scp: %s' % args)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def rscp(node, remotefile, localfile):
    args = ['scp', '%s:%s' % (node, remotefile), localfile]
    print('rscp: %s' % args)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def check_health():
    print 'Waiting until Ceph is healthy...'

    tmp_dir = settings.cluster.get('tmp_dir')
    tmp_conf = '%s/ceph.conf' % tmp_dir

#    i = 0
#    j = 30
#    while True:
#        if i > j:
#            break
#        i += 1
#        print "Waiting %d/%d" % (i, j)
#        time.sleep(1)

    while True:
        stdout, stderr = pdsh(settings.getnodes('head'), 'ceph -c %s health' % tmp_conf).communicate()
        if "HEALTH_OK" in stdout:
            break
        else:
            print stdout
        time.sleep(1)

def check_scrub():
    tmp_dir = settings.cluster.get('tmp_dir')
    tmp_conf = '%s/ceph.conf' % tmp_dir

    print 'Waiting until Scrubbing completes...'
    while True:
        stdout, stderr = pdsh(settings.getnodes('head'), 'ceph -c %s pg dump | cut -f 16 | grep "0.000000" | wc -l' % tmp_conf).communicate()
        if " 0\n" in stdout:
            print stdout
            break
        else:
            print stdout
        time.sleep(1)

def make_remote_dir(remote_dir):
    print 'Making remote directory: %s' % remote_dir
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')
    pdsh(nodes, 'mkdir -p -m0755 -- %s' % remote_dir).communicate()

def sync_files(remote_dir, local_dir):
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws', 'mds')

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    rpdcp(nodes, '-r', remote_dir, local_dir).communicate()

def setup_ceph():

    print "Stopping ceph."
    stop_ceph()
    print "Deleting old ceph logs."
    purge_logs()
    print "Deleting old mon data."
    pdsh(settings.getnodes('mons'), 'sudo rm -rf /var/lib/ceph/mon/*').communicate()

    print 'Creating tmp directories'
    make_remote_dir(settings.cluster.get('tmp_dir'))
    print "Building the underlying OSD filesystem"
    setup_fs()
    print 'Distributing the ceph.conf file'
    setup_ceph_conf()
    print 'Running mkcephfs.'
    mkcephfs()
    print 'Starting Ceph.'
    start_ceph()
#    print 'Setting up pools'
#    setup_pools()
    print 'Checking Health.'
    check_health()

def start_ceph():
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')

    pdsh(nodes, 'sudo /etc/init.d/ceph start').communicate()
#    if rgws:
#        pdsh(rgws, 'sudo /etc/init.d/radosgw start;sudo /etc/init.d/apache2 start').communicate()

def stop_ceph():
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')

    pdsh(nodes, 'sudo /etc/init.d/ceph stop').communicate()
    pdsh(nodes, 'sudo killall -9 ceph-osd').communicate()
    pdsh(nodes, 'sudo killall -9 ceph-mon').communicate()

#    if rgws:
#        pdsh(rgws, 'sudo /etc/init.d/radosgw stop;sudo /etc/init.d/apache2 stop').communicate()

def setup_ceph_conf():
    tmp_dir = settings.cluster.get("tmp_dir")
    tmp_conf = "%s/ceph.conf" % tmp_dir
    nodes = settings.getnodes('head', 'clients', 'osds', 'mons', 'rgws')

    conf_file = settings.cluster.get("conf_file")
    print "Distributing %s." % conf_file
    pdcp(nodes, '', conf_file, tmp_conf).communicate()
#    pdsh(nodes, 'sudo cp /tmp/ceph.conf /etc/ceph/ceph.conf').communicate()

def setup_pools():
    head = settings.getnodes('head')

    # set the replication on the default pools to 1
    pdsh(head, 'sudo ceph osd pool set data size 1').communicate()
    pdsh(head, 'sudo ceph osd pool set metadata size 1').communicate()
    pdsh(head, 'sudo ceph osd pool set rbd size 1').communicate()

def purge_logs():
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')
    pdsh(nodes, 'sudo rm -rf /var/log/ceph/*').communicate()

def cleanup_tests():
    clients = settings.getnodes('clients')
    rgws = settings.getnodes('rgws')
    nodes = settings.getnodes('clients', 'osds', 'mons', 'rgws')

    pdsh(clients, 'sudo killall -9 rados;sudo killall -9 rest-bench').communicate()
    if rgws:
        pdsh(rgws, 'sudo killall -9 radosgw-admin').communicate()
    pdsh(nodes, 'sudo killall -9 pdcp').communicate()

    # cleanup the tmp_dir
    tmp_dir = settings.cluster.get("tmp_dir")
    print 'Deleting %s' % tmp_dir
    pdsh(nodes, 'sudo rm -rf %s' % tmp_dir).communicate()


def mkcephfs():
#    pdsh(settings.getnodes('head'), 'sudo mkcephfs -a -c /etc/ceph/ceph.conf').communicate()

    tmp_dir = settings.cluster.get('tmp_dir')
    osd_dir = settings.cluster.get('osd_dir', '%s/osds' % tmp_dir)
    tmp_conf = "%s/ceph.conf" % tmp_dir
    keyring_fn = "%s/keyring" % tmp_dir
    osdmap_fn = "%s/ceph_osdmap.%s" % (tmp_dir,os.getpid())
    monmap_fn = "%s/ceph_monmap.%s" % (tmp_dir,os.getpid())

    # Make the log dir
    pdsh(settings.getnodes('mons', 'osds', 'rgws', 'mds'), 'mkdir -p -m0755 -- %s/log' % tmp_dir).communicate()

    # Make the keyring, retrieve it, and distribute it
    pdsh(settings.getnodes('head'), 'ceph-authtool --create-keyring --gen-key --name=mon. %s --cap mon \'allow *\'' % keyring_fn).communicate()
    pdsh(settings.getnodes('head'), 'ceph-authtool --gen-key --name=client.admin --set-uid=0 --cap mon \'allow *\' --cap osd \'allow *\' --cap mds allow %s' % keyring_fn).communicate()
    rscp(settings.getnodes('head'), keyring_fn, '%s.tmp' % keyring_fn).communicate()
    pdcp(settings.getnodes('mons', 'osds', 'rgws', 'mds'), '', '%s.tmp' % keyring_fn, keyring_fn).communicate()

    # Build the monmap, retrieve it, and distribute it
    mons = settings.getnodes('mons').split(',')
    cmd = 'monmaptool --create --clobber'
    monhosts = settings.cluster.get('mons')
    print monhosts
    for monhost, mons in monhosts.iteritems():
       for mon, addr in mons.iteritems():
            cmd = cmd + ' --add %s %s' % (mon, addr)
    cmd = cmd + ' --print %s' % monmap_fn
    pdsh(settings.getnodes('head'), cmd).communicate()
    rscp(settings.getnodes('head'), monmap_fn, '%s.tmp' % monmap_fn).communicate()
    pdcp(settings.getnodes('mons'), '', '%s.tmp' % monmap_fn, monmap_fn).communicate()

    # Build the ceph-mons
    for monhost, mons in monhosts.iteritems():
        for mon, addr in mons.iteritems():
            pdsh(monhost, 'sudo rm -rf %s/mon.%s' % (tmp_dir, mon)).communicate()
            pdsh(monhost, 'mkdir -p %s/mon.%s' % (tmp_dir, mon)).communicate()
            pdsh(monhost, 'sudo ceph-mon --mkfs -c %s -i %s --monmap=%s --keyring=%s' % (tmp_conf, mon, monmap_fn, keyring_fn)).communicate()
            pdsh(monhost, 'cp %s %s/mon.%s/keyring' % (keyring_fn, tmp_dir, mon)).communicate()
            
    # Start the mons
    for monhost, mons in monhosts.iteritems():
        for mon, addr in mons.iteritems():
            pdsh(settings.getnodes('mons'), 'sudo ceph-run ceph-mon -c %s -i %s --keyring=%s' % (tmp_conf, mon, keyring_fn)).communicate()

#    CEPH_ADM="$CEPH_BIN/ceph -c $conf"
    
    # Build the OSDs
    osdnum = 0
    osdhosts = settings.getnodes('osds').split(',')

    for host in osdhosts:
        for i in xrange(0, settings.cluster.get('osds_per_node')):            
            osduuid = str(uuid.uuid4())
            pdsh(host, 'sudo ceph -c %s osd create %s' % (tmp_conf, osduuid)).communicate()
            pdsh(host, 'sudo ceph -c %s osd crush add osd.%s 1.0 host=%s rack=localrack root=default' % (tmp_conf, osdnum, host)).communicate()
            pdsh(host, 'sudo sh -c "ulimit -n 16384 && exec ceph-osd -c %s -i %s --mkfs --mkkey --osd-uuid %s"' % (tmp_conf, osdnum, osduuid)).communicate()

            key_fn = '%s/osd-device-%s-data/keyring' % (osd_dir, i)
            pdsh(host, 'sudo ceph -c %s -i %s auth add osd.%s osd "allow *" mon "allow profile osd"' % (tmp_conf, key_fn, osdnum)).communicate()
            pdsh(host, 'sudo sh -c "ulimit -n 16384 && exec ceph-run ceph-osd -c %s -i %s"' % (tmp_conf, osdnum)).communicate()
            osdnum = osdnum+1


#    pdsh(settings.getnodes(head), monstr)
#    pdcp(
#    pdcp(settings.getnodes(mons), monmap_fn, monmap_fn)
#    for i,mons in enumerate(mons):
#      monmap_fn = tmp_dir/ceph_osdmap
#      pdsh(mon, 'sudo rm -rf $s/mon.%s' (tmp_dir, i))
#      pdsh(mon, 'sudo mkdir -p -m0755 -- %s/mon.%s' % (tmp_dir, i))
#      pdsh(mon, 'sudo ceph-mon --mkfs -c %s -i %s --monmap=%s' % (tmp_conf, i, 
    
def setup_fs():
    sc = settings.cluster
    fs = sc.get('fs')
    mkfs_opts = sc.get('mkfs_opts', '')
    mount_opts = sc.get('mount_opts', '')
    tmp_dir = sc.get('tmp_dir')
    osd_dir = sc.get('osd_dir', '%s/osds' % tmp_dir)

    if fs == '':
        shutdown("No OSD filesystem specified.  Exiting.")

    for device in xrange (0,sc.get('osds_per_node')):
        osds = settings.getnodes('osds')
        pdsh(osds, 'sudo umount /dev/disk/by-partlabel/osd-device-%s-data' % device).communicate()
        pdsh(osds, 'sudo rm -rf %s/osd-device-%s-data' % (osd_dir, device)).communicate()
        pdsh(osds, 'sudo mkdir -p -m0755 -- %s/osd-device-%s-data' % (osd_dir, device)).communicate()

        if fs == 'zfs':
            print 'ruhoh, zfs detected.  No mkfs for you!'
            pdsh(osds, 'sudo zpool destroy osd-device-%s-data' % device).communicate()
            pdsh(osds, 'sudo zpool create -f -O xattr=sa -m legacy osd-device-%s-data /dev/disk/by-partlabel/osd-device-%s-data' % (device, device)).communicate()
            pdsh(osds, 'sudo zpool add osd-device-%s-data log /dev/disk/by-partlabel/osd-device-%s-zil' % (device, device)).communicate()
            pdsh(osds, 'sudo mount %s -t zfs osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, device, osd_dir, device)).communicate()
        else: 
            pdsh(osds, 'sudo mkfs.%s %s /dev/disk/by-partlabel/osd-device-%s-data' % (fs, mkfs_opts, device)).communicate()
            pdsh(osds, 'sudo mount %s -t %s /dev/disk/by-partlabel/osd-device-%s-data %s/osd-device-%s-data' % (mount_opts, fs, device, osd_dir, device)).communicate()

def dump_config(run_dir):
    tmp_dir = settings.cluster.get('tmp_dir')
    tmp_conf = '%s/ceph.conf' % tmp_dir

    pdsh(settings.getnodes('osds'), 'sudo ceph -c %s --admin-daemon /var/run/ceph/ceph-osd.0.asok config show > %s/ceph_settings.out' % (tmp_conf, run_dir)).communicate()

def dump_historic_ops(run_dir):
    pdsh(settings.getnodes('osds'), 'find /var/run/ceph/*.asok -maxdepth 1 -exec sudo ceph --admin-daemon {} dump_historic_ops \; > %s/historic_ops.out' % run_dir).communicate()

def set_osd_param(param, value):
    pdsh(settings.getnodes('osds'), 'find /dev/disk/by-partlabel/osd-device-*data -exec readlink {} \; | cut -d"/" -f 3 | sed "s/[0-9]$//" | xargs -I{} sudo sh -c "echo %s > /sys/block/\'{}\'/queue/%s"' % (value, param))
