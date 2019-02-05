class ClientEndpoints(object):
    def __init__(self, cluster, config):
        self.config = config
        self.cluster = cluster
        self.driver = self.config.get('driver', None)
        self.name = 'cbt-%s' % self.driver
        self.mnt_dir = cluster.mnt_dir
        self.endpoint_size = self.config.get('endpoint_size', '1073741824')
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

    def get_endpoints_type(self):
        return self.endpoints_type

    def get_endpoints_per_client(self):
        return self.endpoints_per_client

    def create(self):
        pass

    def mount(self):
        pass

    def umount(self):
        pass

    def remove(self):
        pass
