import common
import settings
import logging
import time

from client_endpoints import ClientEndpoints

logger = logging.getLogger("cbt")

class CephClientEndpoints(ClientEndpoints):
    def __init__(self, cluster, config):
        super(CephClientEndpoints, self).__init__(cluster, config)
        self.ceph_cmd = cluster.ceph_cmd
        self.ceph_fuse_cmd = cluster.ceph_fuse_cmd
        self.rbd_cmd = cluster.rbd_cmd
        self.rbd_nbd_cmd = cluster.rbd_nbd_cmd
        self.rbd_fuse_cmd = cluster.rbd_fuse_cmd
        self.tmp_conf = cluster.tmp_conf
        self.pool = None
        self.pool_profile = config.get('pool_profile', 'default')
        self.data_pool = None
        self.data_pool_profile = config.get('data_pool_profile', None)
        self.order = config.get('order', 22)
        self.disabled_features = config.get('disabled_features', None)

        # get the list of mons
        self.mon_addrs  = []
        mon_hosts = self.cluster.get_mon_hosts()
        for mon_host, mons in mon_hosts.iteritems():
            for mon, addr in mons.iteritems():
                 self.mon_addrs.append(addr)

    def get_rbd_name(self, node, ep_num):
        node_part = node.rpartition("@")[2]
        return '%s-%d' % (node_part, ep_num)

    def get_local_rbd_name(self, ep_num):
        return '`%s`-%d' % (common.get_fqdn_cmd(), ep_num)

    def get_dir_name(self, ep_num):
        return '%s/%s/%s' % (self.mnt_dir, self.name, ep_num)

    def create_fs(self):
        self.pool = self.name
        self.data_pool = self.name
        self.cluster.rmpool(self.pool, self.pool_profile)
        self.cluster.mkpool(self.pool, self.pool_profile, 'cephfs')
        if self.data_pool_profile:
            self.data_pool = '%s-data' % self.name
            self.cluster.rmpool(self.data_pool, self.data_pool_profile)
            self.cluster.mkpool(self.data_pool, self.data_pool_profile, 'cephfs')
        else:
            self.data_pool = self.pool
        fs_new_cmd = 'sudo %s -c %s fs new %s %s %s' % (self.ceph_cmd,
                                                        self.tmp_conf,
                                                        self.name,
                                                        self.pool,
                                                        self.data_pool)
        common.pdsh(settings.getnodes('head'), fs_new_cmd, continue_if_error=False).communicate()

    def mount_fs(self):
        for ep_num in xrange(0, self.endpoints_per_client):
            dir_name = self.get_dir_name(ep_num) 
            for node in common.get_fqdn_list('clients'):
                common.pdsh(node, 'sudo mkdir -p -m0755 -- %s' % dir_name, continue_if_error=False).communicate()
                # FIXME: Apparently something is racey because we can get:
                # "mount error 2 = No such file or directory" without the pause.
                time.sleep(1)
                self.mount_fs_helper(node, dir_name)
            self.endpoints.append(dir_name)
        self.endpoints_type = "directory"
        return self.get_endpoints()

    def mount_fs_helper(self, node, dir_name):
        pass

    def create_rbd(self):
        self.pool = self.name
        dp_option = ''

        self.cluster.rmpool(self.pool, self.pool_profile)
        self.cluster.mkpool(self.pool, self.pool_profile, 'rbd')
        if self.data_pool_profile:
            self.data_pool = '%s-data' % self.name
            dp_option = '--data-pool %s' % self.data_pool
            self.cluster.rmpool(self.data_pool, self.data_pool_profile)
            self.cluster.mkpool(self.data_pool, self.data_pool_profile, 'rbd')

        for node in common.get_fqdn_list('clients'):
            for ep_num in xrange(0, self.endpoints_per_client):
                rbd_name = self.get_rbd_name(node, ep_num)

                # Make the RBD Image
                cmd = '%s -c %s create %s --pool %s --size %s %s --order %s' % (self.rbd_cmd, self.tmp_conf, rbd_name, self.pool, self.endpoint_size, dp_option, self.order)
                common.pdsh(settings.getnodes('head'), cmd, continue_if_error=False).communicate()

                # Disable Features
                if self.disabled_features:
                    cmd = 'sudo %s feature disable %s/%s %s' % (self.rbd_cmd, self.pool, rbd_name, self.disabled_features)
                    common.pdsh(settings.getnodes('head'), cmd, continue_if_error=False).communicate()

    def mount_rbd(self):
        for ep_num in xrange(0, self.endpoints_per_client):
            dir_name = self.get_dir_name(ep_num) 
            for node in common.get_fqdn_list('clients'):
                rbd_name = self.get_rbd_name(node, ep_num)
                rbd_device = self.map_rbd(node, rbd_name)

                logger.info(rbd_device)

                # mkfs
                common.pdsh(node, 'sudo mkfs.xfs %s' % rbd_device, continue_if_error=False).communicate()

                # mkdir
                common.pdsh(node, 'sudo mkdir -p -m0755 -- %s' % dir_name, continue_if_error=False).communicate()

                # mount
                common.pdsh(node, 'sudo mount -t xfs %s %s' % (rbd_device, dir_name),
                            continue_if_error=False).communicate()
            self.endpoints.append(dir_name)
        self.endpoints_type = "directory"
        return self.get_endpoints()

    def map_rbd(self, node, rbd_name):
        pass
