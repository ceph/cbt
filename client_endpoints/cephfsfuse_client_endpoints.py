import common

from ceph_client_endpoints import CephClientEndpoints

class CephfsFuseClientEndpoints(CephClientEndpoints):
    def create(self):
        self.create_fs()

    def mount(self):
        self.mount_fs()

    def mount_fs_helper(self, node, dir_name):
        cmd = 'sudo %s --client_mds_namespace=%s -m %s %s' % (self.ceph_fuse_cmd, self.name, ','.join(self.mon_addrs), dir_name)
        common.pdsh(node, cmd, continue_if_error=False).communicate()
