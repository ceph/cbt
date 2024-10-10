class Cluster:
    def __init__(self, config):
        self.config = config
        base_tmp = config.get("tmp_dir", "/tmp/cbt")
        self.mnt_dir = config.get("mnt_dir", "%s/%s" % (base_tmp, "mnt"))
        self.tmp_dir = "%s/%s" % (base_tmp, config.get("clusterid"))
        self.archive_dir = "%s/%s" % (config.get("archive_dir"), config.get("clusterid"))
        self.tmp_conf = config.get("tmp_conf", "/tmp/cbt")

    def get_mnt_dir(self):
        return self.mnt_dir

    def getclass(self):
        return self.__class__.__name__

    def initialize(self):
        pass

    def cleanup(self):
        pass

    # Adding these 4 in here for fio refactor. Ideally the cluster class will
    # eventually be an abstract base class (ABC), but that is work for the
    # future
    def dump_config(self, run_dir):
        pass

    def create_recovery_test(self, run_dir, callback, test_type="blocking"):
        pass

    def wait_start_io(self):
        pass

    def wait_recovery_done(self):
        pass

    def dump_historic_ops(self, run_dir):
        pass

    def __str__(self):
        return str(self.config)
