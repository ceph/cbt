class ClientEndpoints(object):
    def __init__(self, cluster, config):
        self.config = config
        self.cluster = cluster
        self.driver = self.config.get('driver', None)
        self.name = 'cbt-%s' % self.driver
        self.mnt_dir = cluster.mnt_dir
        self.endpoint_size = self.config.get('endpoint_size', '4096')
        self.endpoint_type = None
        self.endpoints_per_client = self.config.get('endpoints_per_client', 1)
        self.endpoints = []
        self.initialized = False

    def initialize(self):
        self.create()
        self.mount()
        self.initialized = True

    def get_initialized(self):
        return self.initialized

    def get_endpoints(self):
        return self.endpoints

    def get_endpoint_type(self):
        return self.endpoint_type

    def get_endpoints_per_client(self):
        return self.endpoints_per_client

    def get_endpoint_size(self):
        return self.endpoint_size

    def create(self):
        pass

    def mount(self):
        pass

    def umount(self):
        pass

    def remove(self):
        pass
