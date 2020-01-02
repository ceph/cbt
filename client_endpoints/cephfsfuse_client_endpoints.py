import common

from .ceph_client_endpoints import CephClientEndpoints

class CephfsFuseClientEndpoints(CephClientEndpoints):
    def create(self):
        self.create_fs()

    def mount(self):
        self.mount_fs()

    def mount_fs_helper(self, node, dir_name):
        cmd = 'sudo %s -c %s --client_mds_namespace=%s %s' % (self.ceph_fuse_cmd, self.tmp_conf, self.name, dir_name)
        common.pdsh(node, cmd, continue_if_error=False).communicate()
