import common

from .ceph_client_endpoints import CephClientEndpoints

class RbdTcmuClientEndpoints(CephClientEndpoints):
    def create(self):
        self.create_rbd()

    def mount(self):
        self.mount_rbd()

    def map_rbd(self, node, rbd_name):
        common.pdsh(node, f'sudo targetcli /backstores/user:rbd create cfgstring={self.pool}/{rbd_name} name={rbd_name} size={self.endpoint_size}M',
                    continue_if_error=False).communicate()
        stdout, stderr = common.pdsh(node, f'sudo targetcli /loopback create', continue_if_error=False).communicate()
        wwn = stdout.rstrip().rpartition(": ")[2].rpartition(" ")[2][:-1]
        common.pdsh(node, f'sudo targetcli /loopback/{wwn}/luns create /backstores/user:rbd/{rbd_name}', continue_if_error=False).communicate()
        stdout, stderr = common.pdsh(node, f'cat /sys/kernel/config/target/loopback/{wwn}/tpgt_1/address', continue_if_error=False).communicate()
        address = stdout.rstrip().rpartition(": ")[2]
        stdout, stderr = common.pdsh(node, f'ls /sys/class/scsi_disk/{address}:0/device/block', continue_if_error=False).communicate()
        return '/dev/%s' % stdout.rstrip().rpartition(": ")[2]

