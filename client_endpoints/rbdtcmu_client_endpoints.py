import common

from .ceph_client_endpoints import CephClientEndpoints

class RbdTcmuClientEndpoints(CephClientEndpoints):
    def create(self):
        self.create_rbd()

    def mount(self):
        self.mount_rbd()

    def map_rbd(self, node, rbd_name):
        common.pdsh(node, 'sudo targetcli /backstores/user:rbd create cfgstring=%s/%s name=%s size=%sM' % (self.pool, rbd_name, rbd_name, self.endpoint_size), continue_if_error=False).communicate()
        stdout, stderr = common.pdsh(node, 'sudo targetcli /loopback create', continue_if_error=False).communicate()
        wwn = stdout.rstrip().rpartition(": ")[2].rpartition(" ")[2][:-1]
        common.pdsh(node, 'sudo targetcli /loopback/%s/luns create /backstores/user:rbd/%s' % (wwn, rbd_name), continue_if_error=False).communicate()
        stdout, stderr = common.pdsh(node, 'cat /sys/kernel/config/target/loopback/%s/tpgt_1/address' % wwn, continue_if_error=False).communicate()
        address = stdout.rstrip().rpartition(": ")[2]
        stdout, stderr = common.pdsh(node, 'ls /sys/class/scsi_disk/%s:0/device/block' % address, continue_if_error=False).communicate()
        return '/dev/%s' % stdout.rstrip().rpartition(": ")[2]

