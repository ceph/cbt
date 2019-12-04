import common

from .ceph_client_endpoints import CephClientEndpoints

class CephfsKernelClientEndpoints(CephClientEndpoints):
    def create(self):
        self.create_fs()

    def mount(self):
        self.mount_fs()

    def mount_fs_helper(self, node, dir_name):
        cmd = 'sudo %s %s:/ %s -o name=admin,secretfile=%s,mds_namespace=%s' % (self.mount_cmd, ','.join(self.mon_addrs), dir_name, self.client_secret, self.name)
        common.pdsh(node, cmd, continue_if_error=False).communicate()
