import re
import os
import stat
import time
import urllib
import os.path
import logging
import warnings
import threading
import subprocess
import collections

from concurrent.futures import ThreadPoolExecutor
from novaclient.exceptions import NotFound
from novaclient.client import Client as n_client
from cinderclient.v1.client import Client as c_client
try:
    import psutil
except ImportError:
    psutil = None

__doc__ = """
Module used to reliably spawn set of VM's, evenly distributed across
openstack cluster. Main functions:

    get_OS_credentials - extract openstack credentials from different sources
    nova_connect - connect to nova api
    cinder_connect - connect to cinder api
    find - find VM with given prefix in name
    prepare_OS - prepare tenant for usage
    launch_vms - reliably start set of VM in parallel with volumes and floating IP
    clear_all - clear VM and volumes
"""

logger = logging.getLogger("cbt.OS")


NOVA_CONNECTION = None
CINDER_CONNECTION = None


OSCreds_fields = ["username", "password",
                  "tenant_name", "auth_url",
                  "insecure", "region"]
OSCreds = collections.namedtuple("OSCreds", OSCreds_fields)


def run_locally(cmd, input_data="", timeout=20):
    """Run external process with timeout

    params:
        cmd: str - command line
        input_data: str="" - process input
        timeout: int=20 - execution timeout in seconds

    returns:
        process output:str
    """
    shell = isinstance(cmd, basestring)
    proc = subprocess.Popen(cmd,
                            shell=shell,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    res = []

    def thread_func():
        rr = proc.communicate(input_data)
        res.extend(rr)

    thread = threading.Thread(target=thread_func,
                              name="Local cmd execution")
    thread.daemon = True
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        if psutil is not None:
            parent = psutil.Process(proc.pid)
            for child in parent.children(recursive=True):
                child.kill()
            parent.kill()
        else:
            proc.kill()

        thread.join()
        raise RuntimeError("Local process timeout: " + str(cmd))

    out, err = res
    if 0 != proc.returncode:
        raise subprocess.CalledProcessError(proc.returncode,
                                            cmd, out + err)

    return out


def get_OS_credentials_from_openrc(path):
    """return openstack credentials from openrc file
    params:
        path: str, openrc file path

    returns: OSCreds
    """

    fc = open(path).read()

    for sep in (":::", "@@@", "###"):
        echo = 'echo "{0}"'.format(
            sep.join("$OS_" + field.upper() for field in OSCreds_fields)
        )

        try:
            data = run_locally(['/bin/bash'], input_data=fc + "\n" + echo)
            data = data.strip()
            if data.count(sep) > len(OSCreds_fields) - 1:
                continue

            vals = dict(zip(OSCreds_fields, data.split(sep)))
            vals['insecure'] = (vals['insecure'] in ('1', 'True', 'true'))
            assert (vals['auth_url'].startswith("https://") or
                    vals['auth_url'].startswith("http://"))
            return OSCreds(**vals)
        except:
            logger.exception("Failed to get creads from openrc file: " + data)
            raise

    raise RuntimeError("Can't extract password from creds file!")


def get_OS_credentials(params):
    """
    get openstack credentials from config

    params: {'creds': str or dict}

    creds: 'ENV'
        take credentials from environ. Standard environment varialbes is used,
        as after
        # source openrc

    creds: str
        openrc file path

    creds:
        OS_USERNAME: str
        OS_PASSWORD: str
        OS_TENANT_NAME: str
        OS_AUTH_URL: str
        [OS_INSECURE: bool]
        [OS_REGION: str]

    returns:
        OSCreds
    """
    if 'creds' not in params:
        raise ValueError("No openstack credentials provided")

    creds = params['creds']
    if 'ENV' == creds:
        logger.debug("Using OS credentials from shell environment")
        os_creds = OSCreds(os.environ['OS_USERNAME'],
                           os.environ['OS_PASSWORD'],
                           os.environ['OS_TENANT_NAME'],
                           os.environ['OS_AUTH_URL'],
                           os.environ.get('OS_INSECURE', False),
                           os.environ.get('OS_REGION'))
    elif isinstance(creds, basestring):
        logger.debug("Using OS credentials from %s rc file", creds)
        if not os.path.isfile(creds):
            raise ValueError("Can't find file %s with openstack credentials", creds)
        os_creds = get_OS_credentials_from_openrc(creds)
    elif isinstance(creds, dict):
        logger.debug("Using OS credentials, provided expicitly in config")
        os_creds = OSCreds(creds['OS_USERNAME'],
                           creds['OS_PASSWORD'],
                           creds['OS_TENANT_NAME'],
                           creds['OS_AUTH_URL'],
                           creds.get('OS_INSECURE', False),
                           os.environ.get('OS_REGION'))
    else:
        raise ValueError("Can't found any OS credentials in config")

    logger.info(("OS_CREDS: " + " ".join(
        "{0}={1!r}".format(attr, getattr(os_creds, attr))
        for attr in OSCreds_fields))
    )

    return os_creds


def nova_connect(os_creds=None):
    """
    connect to nova api, return novaclient.Client instance
    returns caches connections, if possible
    cache connection in module variable

    params:
        os_creds: OSCreds

    returns:
        nova client object
    """
    global NOVA_CONNECTION

    if NOVA_CONNECTION is None:
        NOVA_CONNECTION = n_client('1.1',
                                   os_creds.username,
                                   os_creds.password,
                                   os_creds.tenant_name,
                                   os_creds.auth_url,
                                   insecure=os_creds.insecure,
                                   region_name=os_creds.region)
    return NOVA_CONNECTION


def cinder_connect(os_creds=None):
    """
    connect to cinder api, return novaclient.Client instance
    returns caches connections, if possible
    cache connection in module variable

    params:
        os_creds: OSCreds

    returns:
        conder client object
    """
    global CINDER_CONNECTION

    if CINDER_CONNECTION is None:
        CINDER_CONNECTION = c_client(os_creds.username,
                                     os_creds.password,
                                     os_creds.tenant_name,
                                     os_creds.auth_url,
                                     insecure=os_creds.insecure,
                                     region_name=os_creds.region)
    return CINDER_CONNECTION


def find(nova, name_prefix):
    """find VM with given prefix in name

    params:
        nova: novaclient connection
        name_prefix: str - vm name predix

    returns:
        iterator (server_ip:str, server_id:int)
    """

    for srv in nova.servers.list():
        if srv.name.startswith(name_prefix):
            for ips in srv.addresses.values():
                for ip in ips:
                    if ip.get("OS-EXT-IPS:type", None) == 'floating':
                        yield ip['addr'], srv.id
                        break


def prepare_OS(os_creds, nova, params, max_vm_per_compute=8):
    """prepare openstack for futher usage

    Creates server groups, security rules, keypair, flavor
    and upload VM image from web. In case if object with
    given name already exists, skip preparation part.
    Don't check, that existing object has required attributes

    params:
        os_creds: OSCreds
        nova: novaclient connection
        params: dict {
            security_group:str - security group name with allowed ssh and ping
            aa_group_name:str - template for anti-affinity group names. Should
                                receive one integer parameter, like "cbt_aa_{0}"
            keypair_name: str - OS keypair name
            keypair_file_public: str - path to public key file
            keypair_file_private: str - path to private key file

            flavor:dict - flavor params
                name, ram_size, hdd_size, cpu_count
                    as for novaclient.Client.flavor.create call

            image:dict - image params
                'name': image name
                'url': image url
        }
        max_vm_per_compute: int=8 maximum expected amount of VM, per
                            compute host. Used to create appropriate
                            count of server groups for even placement

    returns: None
    """
    allow_ssh(nova, params['security_group'])

    MAX_VM_PER_NODE = 8
    serv_groups = map(params['aa_group_name'].format,
                      range(MAX_VM_PER_NODE))

    for serv_groups in serv_groups:
        get_or_create_aa_group(nova, serv_groups)

    create_keypair(nova,
                   params['keypair_name'],
                   params['keypair_file_public'],
                   params['keypair_file_private'])

    create_image(os_creds, nova, params['image']['name'],
                 params['image']['url'])

    create_flavor(nova, **params['flavor'])


def create_keypair(nova, name, pub_key_path, priv_key_path):
    """create and upload keypair into nova, if doesn't exists yet

    Create and upload keypair into nova, if keypair with given bane
    doesn't exists yet. Uses key from files, if file doesn't exists -
    create new keys, and store'em into files.

    parameters:
        nova: nova connection
        name: str - ketpair name
        pub_key_path: str - path for public key
        priv_key_path: str - path for private key

    returns: None
    """

    pub_key_exists = os.path.exists(pub_key_path)
    priv_key_exists = os.path.exists(priv_key_path)

    try:
        kpair = nova.keypairs.find(name=name)
        # if file not found- delete and recreate
    except NotFound:
        kpair = None

    if pub_key_exists and not priv_key_exists:
        raise EnvironmentError("Private key file doesn't exists")

    if not pub_key_exists and priv_key_exists:
        raise EnvironmentError("Public key file doesn't exists")

    if kpair is None:
        if pub_key_exists:
            with open(pub_key_path) as pub_key_fd:
                return nova.keypairs.create(name, pub_key_fd.read())
        else:
            key = nova.keypairs.create(name)

            with open(priv_key_path, "w") as priv_key_fd:
                priv_key_fd.write(key.private_key)
            os.chmod(priv_key_path, stat.S_IREAD | stat.S_IWRITE)

            with open(pub_key_path, "w") as pub_key_fd:
                pub_key_fd.write(key.public_key)
    elif not priv_key_exists:
        raise EnvironmentError("Private key file doesn't exists," +
                               " but key uploaded openstack." +
                               " Either set correct path to private key" +
                               " or remove key from openstack")


def get_or_create_aa_group(nova, name):
    """create anti-affinity server group, if doesn't exists yet

    parameters:
        nova: nova connection
        name: str - group name

    returns: str - group id
    """
    try:
        group = nova.server_groups.find(name=name)
    except NotFound:
        group = nova.server_groups.create(name=name,
                                          policies=['anti-affinity'])

    return group.id


def allow_ssh(nova, group_name):
    """create sequrity group for ping and ssh

    parameters:
        nova: nova connection
        group_name: str - group name

    returns: str - group id
    """
    try:
        secgroup = nova.security_groups.find(name=group_name)
    except NotFound:
        secgroup = nova.security_groups.create(group_name,
                                               "allow ssh/ping to node")
        nova.security_group_rules.create(secgroup.id,
                                         ip_protocol="tcp",
                                         from_port="22",
                                         to_port="22",
                                         cidr="0.0.0.0/0")

        nova.security_group_rules.create(secgroup.id,
                                         ip_protocol="icmp",
                                         from_port=-1,
                                         cidr="0.0.0.0/0",
                                         to_port=-1)
    return secgroup.id


def create_image(os_creds, nova, name, url):
    """upload image into glance from given URL, if given image doesn't exisis yet

    parameters:
        os_creds: OSCreds object - openstack credentials, should be same,
                                   as used when connectiong given novaclient
        nova: nova connection
        name: str - image name
        url: str - image download url

    returns: None
    """
    try:
        nova.images.find(name=name)
        return
    except NotFound:
        pass

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        tempnam = os.tempnam()

    try:
        urllib.urlretrieve(url, tempnam)

        cmd = "OS_USERNAME={0.username}"
        cmd += " OS_PASSWORD={0.password}"
        cmd += " OS_TENANT_NAME={0.tenant_name}"
        cmd += " OS_AUTH_URL={0.auth_url}"
        if os_creds.region:
            cmd += " OS_REGION={0.region}"
        cmd += " glance {1} image-create --name {2} $opts --file {3}"
        cmd += " --disk-format qcow2 --container-format bare --is-public true"

        cmd = cmd.format(os_creds,
                         '--insecure' if os_creds.insecure else "",
                         name,
                         tempnam)

        timeout = os.stat(tempnam).st_size
        run_locally(cmd, timeout=timeout)
    finally:
        if os.path.exists(tempnam):
            os.unlink(tempnam)


def create_flavor(nova, name, ram_size, hdd_size, cpu_count):
    """create flavor, if doesn't exisis yet

    parameters:
        nova: nova connection
        name: str - flavor name
        ram_size: int - ram size (UNIT?)
        hdd_size: int - root hdd size (UNIT?)
        cpu_count: int - cpu cores

    returns: None
    """
    try:
        nova.flavors.find(name=name)
        return
    except NotFound:
        pass

    nova.flavors.create(name, cpu_count, ram_size, hdd_size)


def create_volume(cinder, size, name):
    """create volume

    parameters:
        cinder: cinder connection
        size: int - size (UNIT?)
        name: str - volume name

    returns: Volume object
    """

    vol = cinder.volumes.create(size=size, display_name=name)
    err_count = 0

    while vol.status != 'available':
        if vol.status == 'error':
            if err_count == 3:
                logger.critical("Fail to create volume")
                raise RuntimeError("Fail to create volume")
            else:
                err_count += 1
                cinder.volumes.delete(vol)
                time.sleep(1)
                vol = cinder.volumes.create(size=size, display_name=name)
                continue
        time.sleep(1)
        vol = cinder.volumes.get(vol.id)
    return vol


def wait_for_server_active(nova, server, timeout=300):
    """waiting till server became active

    parameters:
        nova: nova connection
        server: server object
        timeout: int - seconds to wait till raise an exception

    returns: None
    """

    t = time.time()
    while True:
        time.sleep(1)
        sstate = getattr(server, 'OS-EXT-STS:vm_state').lower()

        if sstate == 'active':
            return True

        if sstate == 'error':
            return True

        if time.time() - t > timeout:
            return False

        server = nova.servers.get(server)


def get_floating_ips(nova, pool, amount):
    """allocate flationg ips

    parameters:
        nova: nova connection
        pool:str floating ip pool name
        amount:int - ip count

    returns: [ip object]
    """

    ip_list = nova.floating_ips.list()

    if pool is not None:
        ip_list = [ip for ip in ip_list if ip.pool == pool]

    return [ip for ip in ip_list if ip.instance_id is None][:amount]


def launch_vms(nova, params, already_has_count=0):
    """launch virtual servers

    Parameters:
        nova: nova client
        params: dict {
            count: str or int - server count. If count is string it should be in
                                one of bext forms: "=INT" or "xINT". First mean
                                to spawn (INT - already_has_count) servers, and
                                all should be evenly distributed across all compute
                                nodes. xINT mean spawn COMPUTE_COUNT * INT servers.
            image: dict {'name': str - image name}
            flavor: dict {'name': str - flavor name}
            group_name: str - group name, used to create uniq server name
            keypair_name: str - ssh keypais name
            keypair_file_private: str - path to private key
            user: str - vm user name
            vol_sz: int or None - volume size, or None, if no volume
            network_zone_name: str - network zone name
            flt_ip_pool: str - floating ip pool
            name_templ: str - server name template, should receive two parameters
                              'group and id, like 'cbt-{group}-{id}'
            aa_group_name: str scheduler group name
            security_group: str - security group name
        }
        already_has_count: int=0 - how many servers already exists. Used to distribute
                                   new servers evenly across all compute nodes, taking
                                   old server in accout
    returns: generator of str - server credentials, in format USER@IP:KEY_PATH

    """
    logger.debug("Calculating new vm count")
    count = params['count']
    nova = nova_connect()
    lst = nova.services.list(binary='nova-compute')
    srv_count = len([srv for srv in lst if srv.status == 'enabled'])

    if isinstance(count, basestring):
        if count.startswith("x"):
            count = srv_count * int(count[1:])
        else:
            assert count.startswith('=')
            count = int(count[1:]) - already_has_count

    if count <= 0:
        logger.debug("Not need new vms")
        return

    logger.debug("Starting new nodes on openstack")

    assert isinstance(count, (int, long))

    srv_params = "img: {image[name]}, flavor: {flavor[name]}".format(**params)
    msg_templ = "Will start {0} servers with next params: {1}"
    logger.info(msg_templ.format(count, srv_params))

    vm_params = dict(
        img_name=params['image']['name'],
        flavor_name=params['flavor']['name'],
        group_name=params['group_name'],
        keypair_name=params['keypair_name'],
        vol_sz=params.get('vol_sz'),
        network_zone_name=params.get("network_zone_name"),
        flt_ip_pool=params.get('flt_ip_pool'),
        name_templ=params.get('name_templ'),
        scheduler_hints={"group": params['aa_group_name']},
        security_group=params['security_group'],
        sec_group_size=srv_count
    )

    # precache all errors before start creating vms
    private_key_path = params['keypair_file_private']
    user = params['user']

    for ip, os_node in create_vms_mt(nova, count, **vm_params):
        yield "{0}@{1}:{2}".format(user, ip, private_key_path), os_node.id


def get_free_server_grpoups(nova, template):
    """get fre server groups, that match given name template

    parameters:
        nova: nova connection
        template:str - name template
        amount:int - ip count

    returns: generator or str - server group names
    """
    for g in nova.server_groups.list():
        if g.members == []:
            if re.match(template, g.name):
                yield str(g.name)


class Allocate(object):
    "placeholder"
    pass


def create_vms_mt(nova, amount, group_name, keypair_name, img_name,
                  flavor_name, vol_sz=None, network_zone_name=None,
                  flt_ip_pool=None, name_templ='cbt-{group}-{id}',
                  scheduler_hints=None, security_group=None,
                  sec_group_size=None):
    """create vm's in parralel

    parameters: see in launch_vms description
    returns: list of VM objects
    """
    with ThreadPoolExecutor(max_workers=16) as executor:
        if network_zone_name is not None:
            network_future = executor.submit(nova.networks.find,
                                             label=network_zone_name)
        else:
            network_future = None

        fl_future = executor.submit(nova.flavors.find, name=flavor_name)
        img_future = executor.submit(nova.images.find, name=img_name)

        if flt_ip_pool is not None:
            ips_future = executor.submit(get_floating_ips,
                                         nova, flt_ip_pool, amount)
            logger.debug("Wait for floating ip")
            ips = ips_future.result()
            ips += [Allocate] * (amount - len(ips))
        else:
            ips = [None] * amount

        logger.debug("Getting flavor object")
        fl = fl_future.result()
        logger.debug("Getting image object")
        img = img_future.result()

        if network_future is not None:
            logger.debug("Waiting for network results")
            nics = [{'net-id': network_future.result().id}]
        else:
            nics = None

        names = []
        for i in range(amount):
            names.append(name_templ.format(group=group_name, id=i))

        futures = []
        logger.debug("Requesting new vm's")

        orig_scheduler_hints = scheduler_hints.copy()

        MAX_SHED_GROUPS = 32
        for start_idx in range(MAX_SHED_GROUPS):
            pass

        group_name_template = scheduler_hints['group'].format("\\d+")
        groups = list(get_free_server_grpoups(nova, group_name_template + "$"))
        groups.sort()

        for idx, (name, flt_ip) in enumerate(zip(names, ips), 2):

            scheduler_hints = None
            if orig_scheduler_hints is not None and sec_group_size is not None:
                if "group" in orig_scheduler_hints:
                    scheduler_hints = orig_scheduler_hints.copy()
                    scheduler_hints['group'] = groups[idx // sec_group_size]

            if scheduler_hints is None:
                scheduler_hints = orig_scheduler_hints.copy()

            params = (nova, name, keypair_name, img, fl,
                      nics, vol_sz, flt_ip, scheduler_hints,
                      flt_ip_pool, [security_group])

            futures.append(executor.submit(create_vm, *params))
        res = [future.result() for future in futures]
        logger.debug("Done spawning virtual servers")
        return res


def create_vm(nova, name, keypair_name, img,
              fl, nics, vol_sz=None,
              flt_ip=False,
              scheduler_hints=None,
              pool=None,
              security_groups=None,
              max_retry=3,
              delete_tout=120):
    """create vm

    parameters: see in launch_vms description
    returns: VM objects
    """
    for i in range(max_retry):
        srv = nova.servers.create(name,
                                  flavor=fl,
                                  image=img,
                                  nics=nics,
                                  key_name=keypair_name,
                                  scheduler_hints=scheduler_hints,
                                  security_groups=security_groups)

        if not wait_for_server_active(nova, srv):
            logger.debug("Server %s fails to start. Kill it and try again", srv)
            nova.servers.delete(srv)

            try:
                for j in range(delete_tout):
                    srv = nova.servers.get(srv.id)
                    time.sleep(1)
                else:
                    raise RuntimeError("Server %s delete timeout", srv.id)
            except NotFound:
                pass
        else:
            break
    else:
        raise RuntimeError("Failed to start server")

    if vol_sz is not None:
        vol = create_volume(vol_sz, name)
        nova.volumes.create_server_volume(srv.id, vol.id, None)

    if flt_ip is Allocate:
        flt_ip = nova.floating_ips.create(pool)

    if flt_ip is not None:
        srv.add_floating_ip(flt_ip)

    return flt_ip.ip, nova.servers.get(srv.id)


def clear_all(nova, ids=None, name_templ=None):
    """delete given vm's with volumes.

    Delete vm and attached volumes either by name template
    or by id's

    parameters:
        nova: nova connection
        ids - list of vm id's
        name_templ:str - regular expression for VM name

    returns: None
    """

    def need_delete(srv):
        if name_templ is not None:
            return re.match(name_templ.format("\\d+"), srv.name) is not None
        else:
            return srv.id in ids

    volumes_to_delete = []
    cinder = cinder_connect()
    for vol in cinder.volumes.list():
        for attachment in vol.attachments:
            if attachment['server_id'] in ids:
                volumes_to_delete.append(vol)
                break

    deleted_srvs = set()
    for srv in nova.servers.list():
        if need_delete(srv):
            logger.debug("Deleting server %s", srv.name)
            nova.servers.delete(srv)
            deleted_srvs.add(srv.id)

    count = 0
    while True:
        if count % 60 == 0:
            logger.debug("Waiting till all servers are actually deleted")
        all_id = set(srv.id for srv in nova.servers.list())
        if len(all_id.intersection(deleted_srvs)) == 0:
            break
        count += 1
        time.sleep(1)
    logger.debug("Done, deleting volumes")

    # wait till vm actually deleted

    # logger.warning("Volume deletion commented out")
    for vol in volumes_to_delete:
        logger.debug("Deleting volume %s", vol.display_name)
        cinder.volumes.delete(vol)

    logger.debug("Clearing done (yet some volumes may still deleting)")
