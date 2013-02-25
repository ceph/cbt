import settings
import subprocess
import time
import os

def get_nodes(nodes):
    seen = {}
    ret = ''
    for node in nodes:
        if node and not node in seen:
            if ret:
                ret += ','
            ret += '%s' % node
            seen[node] = True
    print ret
    return ret

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
        stdout, stderr = pdsh(settings.cluster.get('head'), 'ceph health').communicate()
        if "HEALTH_OK" in stdout:
            break
        else:
            print stdout
        time.sleep(1)

def make_remote_dir(remote_dir):
    print 'Making remote directory: %s' % remote_dir
    sc = settings.cluster
    nodes = get_nodes([sc.get('clients'), sc.get('servers'), sc.get('mons'), sc.get('rgws')])
    pdsh(nodes, 'mkdir -p -m0755 -- %s' % remote_dir).communicate()

def sync_files(remote_dir, local_dir):
    sc = settings.cluster
    nodes = get_nodes([sc.get('clients'), sc.get('servers'), sc.get('mons'), sc.get('rgws')])
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    rpdcp(nodes, '-r', remote_dir, local_dir).communicate()

def setup_cluster():
    print "Stopping ceph."
    stop_ceph()

    sc = settings.cluster
    nodes = get_nodes([sc.get('clients'), sc.get('servers'), sc.get('mons'), sc.get('rgws')])
    tmp_dir = sc.get("tmp_dir")
    print 'Deleting %s' % tmp_dir
    pdsh(nodes, 'rm -rf %s' % tmp_dir).communicate()
    setup_ceph_conf()

def setup_ceph():

    print "Stopping ceph."
    stop_ceph()
    print "Deleting old ceph logs."
    purge_logs()
    print "Deleting old mon data."
    pdsh(settings.cluster.get('mons'), 'sudo rm -rf /var/lib/ceph/mon/*').communicate()
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
    sc = settings.cluster
    nodes = get_nodes([sc.get('clients'), sc.get('servers'), sc.get('mons'), sc.get('rgws')])
    pdsh(nodes, 'sudo /etc/init.d/ceph start').communicate()
#    if rgws:
#        pdsh(rgws, 'sudo /etc/init.d/radosgw start;sudo /etc/init.d/apache2 start').communicate()

def stop_ceph():
    sc = settings.cluster
    nodes = get_nodes([sc.get('clients'), sc.get('servers'), sc.get('mons'), sc.get('rgws')])
    pdsh(nodes, 'sudo /etc/init.d/ceph stop').communicate()
#    if rgws:
#        pdsh(rgws, 'sudo /etc/init.d/radosgw stop;sudo /etc/init.d/apache2 stop').communicate()

def setup_ceph_conf():
    sc = settings.cluster
    nodes = get_nodes([sc.get('head'), sc.get('clients'), sc.get('servers'), sc.get('mons'), sc.get('rgws')])
    conf_file = sc.get("ceph.conf")
    print "Distributing %s." % conf_file
    pdcp(nodes, '', conf_file, '/tmp/ceph.conf').communicate()
    pdsh(nodes, 'sudo cp /tmp/ceph.conf /etc/ceph/ceph.conf').communicate()

def setup_pools():
    head = settings.cluster.get('head')

    # set the replication on the default pools to 1
    pdsh(head, 'sudo ceph osd pool set data size 1').communicate()
    pdsh(head, 'sudo ceph osd pool set metadata size 1').communicate()
    pdsh(head, 'sudo ceph osd pool set rbd size 1').communicate()

def purge_logs():
    sc = settings.cluster
    nodes = get_nodes([sc.get('clients'), sc.get('servers'), sc.get('mons'), sc.get('rgws')])
    pdsh(nodes, 'sudo rm -rf /var/log/ceph/*').communicate()

def mkcephfs():
    pdsh(settings.cluster.get('head'), 'sudo mkcephfs -a -c /etc/ceph/ceph.conf').communicate()

def setup_fs():
    sc = settings.cluster
    fs = sc.get('fs')
    mkfs_opts = sc.get('mkfs_opts', '')
    mount_opts = sc.get('mount_opts', '')

    if fs == '':
        shutdown("No OSD filesystem specified.  Exiting.")

    for device in xrange (0,sc.get('osds_per_node')):
        servers = sc.get('servers')
        pdsh(servers, 'sudo umount /srv/osd-device-%s-data;sudo rm -rf /srv/osd-device-%s' % (device, device)).communicate()
        pdsh(servers, 'sudo mkdir /srv/osd-device-%s-data' % device).communicate()
        pdsh(servers, 'sudo mkfs.%s %s /dev/disk/by-partlabel/osd-device-%s-data' % (fs, mkfs_opts, device)).communicate()
        pdsh(servers, 'sudo mount %s -t %s /dev/disk/by-partlabel/osd-device-%s-data /srv/osd-device-%s-data' % (mount_opts, fs, device, device)).communicate()

