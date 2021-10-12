import common

from .ceph_client_endpoints import CephClientEndpoints

class RbdKernelClientEndpoints(CephClientEndpoints):
    def __init__(self, cluster,config):
        super(RbdKernelClientEndpoints, self).__init__(cluster, config)

        # Kernel RBD breaks if certain features are disabled
        self.disabled_features = config.get('disabled_features', 'deep-flatten,fast-diff,object-map')

    def create(self):
        self.create_rbd()

    def mount(self):
        self.mount_rbd()

    def map_rbd(self, node, rbd_name):
        cmd = 'sudo %s map %s/%s --id admin --options noshare' % (self.rbd_cmd, self.pool, rbd_name)
        stdout, stderr = common.pdsh(node, cmd, continue_if_error=False).communicate()
        return stdout.rstrip().rpartition(": ")[2]

    def create_recovery_image(self):
        self.create_rbd_recovery()

    def create_scrubbing_image(self):
        self.create_rbd_scrubbing()
