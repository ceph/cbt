import common

from ceph_client_endpoints import CephClientEndpoints

class CephfsKernelClientEndpoints(CephClientEndpoints):
    def create(self):
        self.create_fs()

    def mount(self):
        self.mount_fs()

    def mount_fs_helper(self, node, dir_name):
        kmon_addrs = [addr + ':/' for addr in self.mon_addrs]
        cmd = 'sudo mount -t ceph -o mds_namespace=%s %s %s' % (self.name, ','.join(kmon_addrs), dir_name)
        common.pdsh(node, cmd, continue_if_error=False).communicate()
