import common

from .ceph_client_endpoints import CephClientEndpoints

class RbdNbdClientEndpoints(CephClientEndpoints):
    def create(self):
        self.create_rbd()

    def mount(self):
        self.mount_rbd()

    def map_rbd(self, node, rbd_name):
        cmd = 'sudo %s map %s/%s' % (self.rbd_nbd_cmd, self.pool, rbd_name) 
        stdout, stderr = common.pdsh(node, cmd, continue_if_error=False).communicate()
        return stdout.rstrip().rpartition(": ")[2]
