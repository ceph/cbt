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
    group_name='UT',
    keypair_name=os_params['keypair_name'],
    vol_sz=5,
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

    def setUp(self):
        if var_name in os.environ and not hasattr(self, 'nova'):
            self.creds = openstack.get_OS_credentials({'creds': 'ENV'})
            self.nova = openstack.nova_connect(self.creds)
            self.cinder = openstack.cinder_connect(self.creds)
        self.vm_ids = []

    def tearDown(self):
        if len(self.vm_ids) != 0:
            openstack.clear_all(self.nova, vm_ids=self.vm_ids)

    @unittest.skipIf(var_name not in os.environ, message)
    def test_00_get_credentials(self):
        ok(self.creds.username) == os.environ['OS_USERNAME']
        ok(self.creds.tenant_name) == os.environ['OS_TENANT_NAME']
        ok(self.creds.password) == os.environ['OS_PASSWORD']
        ok(self.creds.auth_url) == os.environ['OS_AUTH_URL']
        ok(self.creds.insecure) == (os.environ.get('OS_INSECURE') in ('1', 'true', 'True'))
        ok(self.creds.region) == os.environ.get('OS_REGION')

    @unittest.skipIf(var_name not in os.environ, message)
    def test_01_connect(self):
        ok(self.nova.servers.list()).is_a(list)

        # check, that connection cached
        ok(openstack.nova_connect(self.creds)).is_(self.nova)
        ok(openstack.cinder_connect(self.creds)).is_(self.nova)

    @unittest.skipIf(var_name not in os.environ, message)
    def test_02_prepare(self):
        ok(self.nova.servers.list()).is_a(list)

        # check, that connection cached
        openstack.prepare_OS(self.creds, self.nova, os_params, max_vm_per_compute=max_vm_per_compute)

        ok(os_params['image']['name']).in_([i.name for i in self.nova.images.list()])
        ok(os_params['keypair_file_public']).is_file()
        ok(os_params['keypair_file_private']).is_file()
        ok(os_params['flavor']['name']).in_([i.name for i in self.nova.flavors.list()])
        ok(os_params['keypair_name']).in_([i.name for i in self.nova.keypairs.list()])

        names = [i.name for i in self.nova.server_groups.list()]
        for i in range(max_vm_per_compute):
            ok(os_params['aa_group_name'].format(i)).in_(names)

        ok(os_params['security_group']).in_([i.name for i in self.nova.security_groups.list()])

    @unittest.skipIf(var_name not in os.environ, message)
    def test_03_start_vm(self):
        self.vm_ids = None

        openstack.prepare_OS(self.creds, self.nova,
                             os_params, max_vm_per_compute=max_vm_per_compute)
        conn_strs, self.vm_ids = zip(*openstack.launch_vms(self.nova, self.cinder, vm_params))
        cbt_srv = [vm for vm in self.nova.servers.list() if vm.id in self.vm_ids]
        ok(len(cbt_srv)) == vm_params['count']
        # check can connect
