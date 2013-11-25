import settings
import subprocess
import time
import os

#def get_nodes(nodes):
#    seen = {}
#    ret = ''
#    for node in nodes:
#        if node and not node in seen:
#            if ret:
#                ret += ','
#            ret += '%s' % node
#            seen[node] = True
#    print ret
#    return ret

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

def check_health():
    print 'Waiting until Ceph is healthy...'
    i = 0
    j = 30
#    while True:
#        if i > j:
#            break
#        i += 1
#        print "Waiting %d/%d" % (i, j)
#        time.sleep(1)

    while True:
        stdout, stderr = pdsh(settings.getnodes('head'), 'ceph health').communicate()
        if "HEALTH_OK" in stdout:
            break
        else:
            print stdout
        time.sleep(1)

def check_scrub():
    print 'Waiting until Scrubbing completes...'

    while True:
        stdout, stderr = pdsh(settings.getnodes('head'), 'ceph pg dump | cut -f 16 | grep "0.000000" | wc -l').communicate()
        if " 0\n" in stdout:
            print stdout
            break
        else:
            print stdout
        time.sleep(1)

def make_remote_dir(remote_dir):
    print 'Making remote directory: %s' % remote_dir
    nodes = settings.getnodes('clients', 'servers', 'mons', 'rgws')
    pdsh(nodes, 'mkdir -p -m0755 -- %s' % remote_dir).communicate()

def sync_files(remote_dir, local_dir):
    nodes = settings.getnodes('clients', 'servers', 'mons', 'rgws')

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    rpdcp(nodes, '-r', remote_dir, local_dir).communicate()

def setup_cluster():
#    print "Stopping ceph."
#    stop_ceph()
    setup_ceph_conf()

def setup_ceph():

    print "Stopping ceph."
    stop_ceph()
    print "Deleting old ceph logs."
    purge_logs()
    print "Deleting old mon data."
    pdsh(settings.getnodes('mons'), 'sudo rm -rf /var/lib/ceph/mon/*').communicate()
    print "Building the underlying OSD filesystem"
    setup_fs()
    print 'Running mkcephfs.'
    mkcephfs()
    print 'Starting Ceph.'
    start_ceph()
    print 'Setting up pools'
    setup_pools()
    print 'Checking Health.'
    check_health()

def start_ceph():
    nodes = settings.getnodes('clients', 'servers', 'mons', 'rgws')

    pdsh(nodes, 'sudo /etc/init.d/ceph start').communicate()
#    if rgws:
#        pdsh(rgws, 'sudo /etc/init.d/radosgw start;sudo /etc/init.d/apache2 start').communicate()

def stop_ceph():
    nodes = settings.getnodes('clients', 'servers', 'mons', 'rgws')

    pdsh(nodes, 'sudo /etc/init.d/ceph stop').communicate()
    pdsh(nodes, 'sudo killall -9 ceph-osd').communicate()
    pdsh(nodes, 'sudo killall -9 ceph-mon').communicate()

#    if rgws:
#        pdsh(rgws, 'sudo /etc/init.d/radosgw stop;sudo /etc/init.d/apache2 stop').communicate()

def setup_ceph_conf():
    nodes = settings.getnodes('head', 'clients', 'servers', 'mons', 'rgws')

    conf_file = settings.cluster.get("ceph.conf")
    print "Distributing %s." % conf_file
    pdcp(nodes, '', conf_file, '/tmp/ceph.conf').communicate()
    pdsh(nodes, 'sudo cp /tmp/ceph.conf /etc/ceph/ceph.conf').communicate()

def setup_pools():
    head = settings.getnodes('head')

    # set the replication on the default pools to 1
    pdsh(head, 'sudo ceph osd pool set data size 1').communicate()
    pdsh(head, 'sudo ceph osd pool set metadata size 1').communicate()
    pdsh(head, 'sudo ceph osd pool set rbd size 1').communicate()

def purge_logs():
    nodes = settings.getnodes('clients', 'servers', 'mons', 'rgws')
    pdsh(nodes, 'sudo rm -rf /var/log/ceph/*').communicate()

def cleanup_tests():
    clients = settings.getnodes('clients')
    rgws = settings.getnodes('rgws')
    nodes = settings.getnodes('clients', 'servers', 'mons', 'rgws')

    pdsh(clients, 'sudo killall -9 rados;sudo killall -9 rest-bench').communicate()
    if rgws:
        pdsh(rgws, 'sudo killall -9 radosgw-admin').communicate()
    pdsh(nodes, 'sudo killall -9 pdcp').communicate()

    # cleanup the tmp_dir
    tmp_dir = settings.cluster.get("tmp_dir")
    print 'Deleting %s' % tmp_dir
    pdsh(nodes, 'rm -rf %s' % tmp_dir).communicate()


def mkcephfs():
    pdsh(settings.getnodes('head'), 'sudo mkcephfs -a -c /etc/ceph/ceph.conf').communicate()

def setup_fs():
    sc = settings.cluster
    fs = sc.get('fs')
    mkfs_opts = sc.get('mkfs_opts', '')
    mount_opts = sc.get('mount_opts', '')

    if fs == '':
        shutdown("No OSD filesystem specified.  Exiting.")

    for device in xrange (0,sc.get('osds_per_node')):
        servers = settings.getnodes('servers')
        pdsh(servers, 'sudo umount /srv/osd-device-%s-data;sudo rm -rf /srv/osd-device-%s' % (device, device)).communicate()
        pdsh(servers, 'sudo mkdir /srv/osd-device-%s-data' % device).communicate()

        if fs == 'zfs':
            print 'ruhoh, zfs detected.  No mkfs for you!'
            pdsh(servers, 'sudo zpool destroy osd-device-%s-data' % device).communicate()
#            pdsh(servers, 'sudo mkfs.ext4 /dev/disk/by-partlabel/osd-device-%s-data' % device).communicate()
            pdsh(servers, 'sudo zpool create -f -O xattr=sa -m legacy osd-device-%s-data /dev/disk/by-partlabel/osd-device-%s-data' % (device, device)).communicate()
            pdsh(servers, 'sudo zpool add osd-device-%s-data log /dev/disk/by-partlabel/osd-device-%s-zil' % (device, device)).communicate()
            pdsh(servers, 'sudo mount %s -t zfs osd-device-%s-data /srv/osd-device-%s-data' % (mount_opts, device, device)).communicate()
        else: 
            pdsh(servers, 'sudo mkfs.%s %s /dev/disk/by-partlabel/osd-device-%s-data' % (fs, mkfs_opts, device)).communicate()
            pdsh(servers, 'sudo mount %s -t %s /dev/disk/by-partlabel/osd-device-%s-data /srv/osd-device-%s-data' % (mount_opts, fs, device, device)).communicate()

def dump_config(run_dir):
    pdsh(settings.getnodes('servers'), 'sudo ceph --admin-daemon /var/run/ceph/ceph-osd.0.asok config show > %s/ceph_settings.out' % run_dir).communicate()

def dump_historic_ops(run_dir):
    pdsh(settings.cluster.get('servers'), 'find /var/run/ceph/*.asok -maxdepth 1 -exec sudo ceph --admin-daemon {} dump_historic_ops \; > %s/historic_ops.out' % run_dir).communicate()

def set_osd_param(param, value):
    pdsh(settings.cluster.get('servers'), 'find /dev/disk/by-partlabel/osd-device-*data -exec readlink {} \; | cut -d"/" -f 3 | sed "s/[0-9]$//" | xargs -I{} sudo sh -c "echo %s > /sys/block/\'{}\'/queue/%s"' % (value, param))
