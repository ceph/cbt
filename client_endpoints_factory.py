import settings

from cluster.ceph import Ceph

from client_endpoints.librbd_client_endpoints import LibrbdClientEndpoints
from client_endpoints.rbdkernel_client_endpoints import RbdKernelClientEndpoints
from client_endpoints.rbdnbd_client_endpoints import RbdNbdClientEndpoints
from client_endpoints.rbdfuse_client_endpoints import RbdFuseClientEndpoints
from client_endpoints.rbdtcmu_client_endpoints import RbdTcmuClientEndpoints
from client_endpoints.cephfskernel_client_endpoints import CephfsKernelClientEndpoints
from client_endpoints.cephfsfuse_client_endpoints import CephfsFuseClientEndpoints

ce_objects = {}

def get(cluster, name):
    if isinstance(cluster, Ceph):
        return get_ceph(cluster, name)

def get_ceph(cluster, name):
    ce_config = settings.client_endpoints.get(name, None)

    if ce_config == None:
        raise ValueError('No client_endpoints with name "%s" found.' % name)

    cclass = cluster.getclass()
    key = "%s-%s" % (cclass, name)

    if key in ce_objects:
        return ce_objects[key]

    driver = ce_config.get('driver', None)
    if driver is None:
        raise ValueError('No driver defined in the "%s" client_endpoints.' % name)
    elif driver == "librbd":
        ce_objects[key] = LibrbdClientEndpoints(cluster, ce_config)
    elif driver == "rbd-kernel":
        ce_objects[key] = RbdKernelClientEndpoints(cluster, ce_config)
    elif driver == "rbd-nbd":
        ce_objects[key] = RbdNbdClientEndpoints(cluster, ce_config)
    elif driver == "rbd-fuse":
        ce_objects[key] = RbdFuseClientEndpoints(cluster, ce_config)
    elif driver == "rbd-tcmu":
        ce_objects[key] = RbdTcmuClientEndpoints(cluster, ce_config)
    elif driver == "cephfs-kernel":
        ce_objects[key] = CephfsKernelClientEndpoints(cluster, ce_config)
    elif driver == "cephfs-fuse":
        ce_objects[key] = CephfsFuseClientEndpoints(cluster, ce_config)
    else:
        raise ValueError('%s clusters do not support "%s" client_endpoints.' % (cclass, driver))
    return ce_objects[key]
