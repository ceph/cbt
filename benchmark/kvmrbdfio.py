import subprocess
import common
import settings
import monitoring
import os
import time
import string

from benchmark import Benchmark

class KvmRbdFio(Benchmark):

    def __init__(self, config):
        super(KvmRbdFio, self).__init__(config)
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.total_procs = self.concurrent_procs * len(settings.getnodes('clients'))

        self.time =  str(config.get('time', '300'))
        self.iodepth = config.get('iodepth', 16)
        self.mode = config.get('mode', 'write')
        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.pgs = config.get('pgs', 2048)
        self.vol_size = config.get('vol_size', 65536)
        self.rep_size = config.get('rep_size', 1)
        self.rbdadd_mons = config.get('rbdadd_mons')
        self.rbdadd_options = config.get('rbdadd_options')
        # FIXME there are too many permutations, need to put results in SQLITE3 

        self.run_dir = '%s/kvmrbdfio/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.tmp_dir, int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)
        self.out_dir = '%s/kvmrbdfio/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.archive_dir, int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)

    def exists(self):
        if os.path.exists(self.out_dir):
            print 'Skipping existing test in %s.' % self.out_dir
            return True
        return False

    def initialize(self): 
        pass
#        self.cleanup()
#        super(KvmRbdFio, self).initialize()
        common.setup_cluster()
#        common.setup_ceph()

        # Setup the pools
#        common.pdsh(settings.cluster.get('head'), 'sudo ceph osd pool create rbdfio %d %d' % (self.pgs, self.pgs)).communicate()
#        common.pdsh(settings.cluster.get('head'), 'sudo ceph osd pool set rbdfio size 1').communicate()
#        print 'Checking Healh after pool creation.'
#        common.check_health()

#        common.pdsh(settings.cluster.get('clients'), 'sudo modprobe rbd').communicate()
#        for i in xrange(self.concurrent_procs):
        names = ""
        for i in xrange(self.concurrent_procs):
            letter = string.ascii_lowercase[i+1]
#            common.pdsh(settings.cluster.get('clients'), 'sudo rbd create rbdfio/rbdfio-`hostname -s`-%d --size %d' % (i, self.vol_size)).communicate()
#            common.pdsh(settings.cluster.get('clients'), 'sudo rbd map rbdfio-`hostname -s`-%d  --pool rbdfio --id admin' % i).communicate()
#            common.pdsh(settings.cluster.get('clients'), 'sudo echo "%s %s rbdfio rbdfio-`hostname -s`-%d" | sudo tee /sys/bus/rbd/add && sudo /sbin/udevadm settle' % (self.rbdadd_mons, self.rbdadd_options, i)).communicate()
            common.pdsh(settings.getnodes('clients'), 'sudo mkfs.xfs /dev/vd%s' % letter).communicate()
            common.pdsh(settings.getnodes('clients'), 'sudo mkdir /srv/rbdfio-`hostname -s`-%d' % i).communicate()
            common.pdsh(settings.getnodes('clients'), 'sudo mount -t xfs -o noatime,inode64 /dev/vd%s /srv/rbdfio-`hostname -s`-%d' %(letter, i)).communicate()

        # Create the run directory
        common.make_remote_dir(self.run_dir)

    def run(self):
        super(KvmRbdFio, self).run()
        # We'll always drop caches for rados bench
        self.dropcaches()

        monitoring.start(self.run_dir)

        time.sleep(5)
        names = ""
        for i in xrange(self.concurrent_procs):
            names += "--name=/srv/rbdfio-`hostname -s`-%d/cbt-kvmrbdfio " % i
        out_file = '%s/output' % self.run_dir
        pre_cmd = 'sudo fio --rw=read -ioengine=sync --numjobs=1 --bs=4M --runtime=1 --size %dM %s > /dev/null' % (self.vol_size * 9/10, names)
        fio_cmd = 'sudo fio --rw=%s -ioengine=%s --runtime=%s --numjobs=1 --direct=1 --bs=%dB --iodepth=%d --size %dM %s > %s' %  (self.mode, self.ioengine, self.time, self.op_size, self.iodepth, self.vol_size * 9/10, names, out_file)
        print 'Attempting to populating fio files...'
        common.pdsh(settings.getnodes('clients'), pre_cmd).communicate()
        print 'Running rbd fio %s test.' % self.mode
        common.pdsh(settings.getnodes('clients'), fio_cmd).communicate()
#        ps = []
#        for i in xrange(self.concurrent_procs):
#            out_file = '%s/output.%s' % (self.run_dir, i)
#            p = common.pdsh(settings.cluster.get('clients'), 'sudo fio --rw=%s -ioengine=%s --runtime=%s --name=/srv/rbdfio-`hostname -s`-%d/cbt-rbdfio --numjobs=1 --direct=1 --bs=%dB --iodepth=%d --size %dM > %s' % (self.mode, self.ioengine, self.time, i, self.op_size, self.iodepth, self.vol_size * 9/10, out_file))
#            ps.append(p)
#        for p in ps:
#            p.wait()
        monitoring.stop(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)


    def cleanup(self):
         common.pdsh(settings.getnodes('clients'), 'sudo umount /srv/*').communicate()
#         common.pdsh(settings.cluster.get('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec umount \'{}\' \;').communicate()
#         common.pdsh(settings.cluster.get('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec rbd unmap \'{}\' \;').communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(KvmRbdFio, self).__str__())
