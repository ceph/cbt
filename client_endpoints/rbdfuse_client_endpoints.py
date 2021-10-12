import common
import logging

from .ceph_client_endpoints import CephClientEndpoints

logger = logging.getLogger("cbt")


class RbdFuseClientEndpoints(CephClientEndpoints):
    def _init__(self, config, cluster):
        super().__init__(cluster, config)

    def create(self):
        self.create_rbd()

    def mount(self):
        self.mount_rbd()

    def map_rbd(self, node, rbd_name):
        fuse_dir = '%s/%s-fuse' % (self.mnt_dir, self.name)

        # Check to make sure that fuse is not already mapped.
        stdout, stderr = common.pdsh(node, 'sudo ps aux | grep %s' % self.rbd_fuse_cmd, continue_if_error=False).communicate()
        if fuse_dir in stdout:
            raise ValueError('RBD-Fuse was already mapped at %s!' % fuse_dir)
        common.pdsh(node, 'sudo mkdir -p -m0755 -- %s' % fuse_dir, continue_if_error=False).communicate()
        common.pdsh(node, 'sudo %s %s -p %s' % (self.rbd_fuse_cmd, fuse_dir, self.pool), continue_if_error=False).communicate()
        logger.info('Mapped RBD-Fuse pool %s to %s' % (self.pool, fuse_dir))

        return '%s/%s' % (fuse_dir, rbd_name)

    def create_recovery_image(self):
        self.create_rbd_recovery()

    def create_scrubbing_image(self):
        self.create_rbd_scrubbing()
