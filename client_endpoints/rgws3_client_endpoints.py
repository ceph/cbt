import common

from .ceph_client_endpoints import CephClientEndpoints

class RgwS3ClientEndpoints(CephClientEndpoints):
    def __init__(self, cluster, config):
        super(RgwS3ClientEndpoints, self).__init__(cluster, config)

    def create(self):
        self.access_key = self.config.get('access_key', '03VIHOWDVK3Z0VSCXBNH')
        self.secret_key = self.config.get('secret_key', 'KTTxQIIJV3uNox21vcqxWIpHMUOApWVWsJKdHwgG')
        self.user = self.config.get('user', 'cbt')
        self.cluster.add_s3_user(self.user, self.access_key, self.secret_key)

    def mount(self):
        # Don't actually mount anything, just set the endpoints
        urls = self.config.get('urls', self.cluster.get_urls())
        for ep_num in range(0, self.endpoints_per_client):
           url = urls[ep_num % len(urls)]
           self.endpoints.append({"url": url, "access_key": self.access_key, "secret_key": self.secret_key})
        self.endpoint_type = "s3"
        return self.get_endpoints()
