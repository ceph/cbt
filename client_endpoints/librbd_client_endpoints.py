import common

from .ceph_client_endpoints import CephClientEndpoints

class LibrbdClientEndpoints(CephClientEndpoints):
    def __init__(self, cluster, config):
        super(LibrbdClientEndpoints, self).__init__(cluster, config)

    def create(self):
        self.create_rbd()

    def mount(self):
        # Don't mount anything, just set the endpoints to the pool/rbd names
        for ep_num in range(0, self.endpoints_per_client):
            rbd_name = self.get_local_rbd_name(ep_num)
            self.endpoints.append("%s/%s" % (self.pool, rbd_name))
        self.endpoint_type = "rbd"
        return self.get_endpoints()

    def create_recovery_image(self):
        self.create_rbd_recovery()
