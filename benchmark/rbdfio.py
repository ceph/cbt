import subprocess
import common
import settings
import monitoring
import os

from benchmark import Benchmark

class RbdFio(Benchmark):

    def __init__(self, config):
        super(RbdFio, self).__init__(config)
        self.tmp_dir = self.cluster.tmp_dir
        self.tmp_conf = self.cluster.tmp_conf
        self.time =  str(config.get('time', '300'))
        self.concurrent_procs = config.get('concurrent_procs', 1)
        self.iodepth = config.get('iodepth', 16)
        self.mode = config.get('mode', 'write')
        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.pgs = config.get('pgs', 2048)
        self.vol_size = config.get('vol_size', 65536)
        self.rep_size = config.get('rep_size', 1)
        self.rbdadd_mons = config.get('rbdadd_mons')
        self.rbdadd_options = config.get('rbdadd_options', 'share')
        self.client_ra = config.get('client_ra', 128)
        # FIXME there are too many permutations, need to put results in SQLITE3
        self.run_dir = '%s/rbdfio/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.tmp_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.concurrent_procs), int(self.iodepth), self.mode)
        self.out_dir = '%s/rbdfio/osd_ra-%08d/client_ra-%08d/op_size-%08d/concurrent_procs-%03d/iodepth-%03d/%s' % (self.archive_dir, int(self.osd_ra), int(self.client_ra), int(self.op_size), int(self.concurrent_procs), int(self.iodepth), self.mode)
        self.use_existing = config.get('use_existing', True)

    def exists(self):
        if os.path.exists(self.out_dir):
            print 'Skipping existing test in %s.' % self.out_dir
            return True
        return False

    def initialize(self): 
        super(RbdFio, self).initialize()
        self.cleanup()

        if not self.use_existing:
            self.cluster.initialize()
            self.cluster.dump_config(self.run_dir)

            # Setup the pools
            monitoring.start("%s/pool_monitoring" % self.run_dir)
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool create rbdfio %d %d' % (self.tmp_conf, self.pgs, self.pgs)).communicate()
            common.pdsh(settings.getnodes('head'), 'sudo ceph -c %s osd pool set rbdfio size 1' % self.tmp_conf).communicate()
            print 'Checking Healh after pool creation.'
            self.cluster.check_health()
            monitoring.stop()

            # Mount the filesystem
            common.pdsh(settings.getnodes('clients'), 'sudo modprobe rbd').communicate()
            for i in xrange(self.concurrent_procs):
                common.pdsh(settings.getnodes('clients'), 'sudo rbd -c %s create rbdfio/rbdfio-`hostname -s`-%d --size %d' % (self.tmp_conf, i, self.vol_size)).communicate()
                common.pdsh(settings.getnodes('clients'), 'sudo echo "%s %s rbdfio rbdfio-`hostname -s`-%d" | sudo tee /sys/bus/rbd/add && sudo /sbin/udevadm settle' % (self.rbdadd_mons, self.rbdadd_options, i)).communicate()
                common.pdsh(settings.getnodes('clients'), 'sudo mkfs.xfs /dev/rbd/rbdfio/rbdfio-`hostname -s`-%d' % i).communicate()
                common.pdsh(settings.getnodes('clients'), 'sudo mkdir -p -m0755 -- %s/mnt/rbdfio-`hostname -s`-%d' % (self.tmp_dir, i)).communicate()
                common.pdsh(settings.getnodes('clients'), 'sudo mount -t xfs -o noatime,inode64 /dev/rbd/rbdfio/rbdfio-`hostname -s`-%d %s/mnt/rbdfio-`hostname -s`-%d' % (i, self.tmp_dir, i)).communicate()

        print 'Running scrub monitoring'
        monitoring.start("%s/scrub_monitoring" % self.run_dir)
        self.cluster.check_scrub()
        monitoring.stop()

        # Create the run directory
        common.make_remote_dir(self.run_dir)

    def run(self):
        super(RbdFio, self).run()
        # Set client readahead
        self.set_client_param('read_ahead_kb', self.client_ra)

        # We'll always drop caches for rados bench
        self.dropcaches()

        common.make_remote_dir(self.run_dir)
        monitoring.start(self.run_dir)
        # Run rados bench
        print 'Running rbd fio %s test.' % self.mode
        names = ""
        for i in xrange(self.concurrent_procs):
            names += "--name=%s/mnt/rbdfio-`hostname -s`-%d/cbt-rbdfio " % (self.tmp_dir, i)
        out_file = '%s/output' % self.run_dir
        fio_cmd = 'sudo fio --rw=%s -ioengine=%s --runtime=%s --numjobs=1 --direct=1 --bs=%dB --iodepth=%d --size %dM %s > %s' %  (self.mode, self.ioengine, self.time, self.op_size, self.iodepth, self.vol_size * 9/10, names, out_file)
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
         common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec umount \'{}\' \;').communicate()
         common.pdsh(settings.getnodes('clients'), 'sudo find /dev/rbd* -maxdepth 0 -type b -exec rbd -c %s unmap \'{}\' \;' % self.tmp_conf).communicate()

    def set_client_param(self, param, value):
         common.pdsh(settings.getnodes('clients'), 'find /sys/block/rbd* -exec sudo sh -c "echo %s > {}/queue/%s" \;' % (value, param)).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(RbdFio, self).__str__())
