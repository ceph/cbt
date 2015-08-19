import os
import unittest

from oktest import ok

from cluster import openstack


var_name = "OS_USERNAME"
message = "No openstack credentials provided!. Run 'source openrc' with you openrc first"

os_params = {
    'security_group': 'cbt_ssh_for_free',
    'aa_group_name': 'cbt_aa_{0}',
    'keypair_name': 'cbt',
    'keypair_file_public': '/tmp/cbt.pub',
    'keypair_file_private': '/tmp/cbt.priv',
    'flavor': {
        'name': 'cbt_1024',
        'ram_size': 1024,
        'hdd_size': 10,
        'cpu_count': 1
    },
    'image': {
        'name': "cbt_cirros",
        'url': 'http://download.cirros-cloud.net/0.3.4/cirros-0.3.4-x86_64-disk.img'
    }
}


vm_params = dict(
    count=2,
    image={'name': os_params['image']['name']},
    flavor={'name': os_params['flavor']['name']},
    group_name='cbt_ut',
    keypair_name=os_params['keypair_name'],
    vol_sz=100,
    network_zone_name='net04',
    flt_ip_pool='net04_ext',
    name_templ='cbt-{group}-{id}',
    aa_group_name=os_params['aa_group_name'],
    security_group=os_params['security_group'],
    keypair_file_private=os_params['keypair_file_private'],
    user='cirros'
)


max_vm_per_compute = 8


class TestOSIntegration(unittest.TestCase):
    @unittest.skipIf(var_name not in os.environ, message)
    def test_00_get_credentials(self):
        creds = openstack.get_OS_credentials({'creds': 'ENV'})
        ok(creds.username) == os.environ['OS_USERNAME']
        ok(creds.tenant_name) == os.environ['OS_TENANT_NAME']
        ok(creds.password) == os.environ['OS_PASSWORD']
        ok(creds.auth_url) == os.environ['OS_AUTH_URL']
        ok(creds.insecure) == (os.environ.get('OS_INSECURE') in ('1', 'true', 'True'))
        ok(creds.region) == os.environ.get('OS_REGION')

    @unittest.skipIf(var_name not in os.environ, message)
    def test_01_connect(self):
        creds = openstack.get_OS_credentials({'creds': 'ENV'})
        conn = openstack.nova_connect(creds)
        ok(conn.servers.list()).is_a(list)

        # check, that connection cached
        ok(openstack.nova_connect(creds)).is_(conn)

    @unittest.skipIf(var_name not in os.environ, message)
    def test_02_prepare(self):
        creds = openstack.get_OS_credentials({'creds': 'ENV'})
        conn = openstack.nova_connect(creds)
        ok(conn.servers.list()).is_a(list)

        # check, that connection cached
        openstack.prepare_OS(creds, conn, os_params, max_vm_per_compute=max_vm_per_compute)

        ok(os_params['image']['name']).in_([i.name for i in conn.images.list()])
        ok(os_params['keypair_file_public']).is_file()
        ok(os_params['keypair_file_private']).is_file()
        ok(os_params['flavor']['name']).in_([i.name for i in conn.flavors.list()])
        ok(os_params['keypair_name']).in_([i.name for i in conn.keypairs.list()])

        names = [i.name for i in conn.server_groups.list()]
        for i in range(max_vm_per_compute):
            ok(os_params['aa_group_name'].format(i)).in_(names)

        ok(os_params['security_group']).in_([i.name for i in conn.security_groups.list()])

    @unittest.skipIf(var_name not in os.environ, message)
    def test_03_start_vm(self):
        creds = openstack.get_OS_credentials({'creds': 'ENV'})
        conn = openstack.nova_connect(creds)
        openstack.prepare_OS(creds, conn, os_params, max_vm_per_compute=max_vm_per_compute)
        list(openstack.launch_vms(conn, vm_params))
