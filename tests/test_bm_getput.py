""" Unit tests for the Benchmarkgetput class """

import unittest
import hashlib
import json
import benchmarkfactory
import settings
from cluster.ceph import Ceph


class TestBenchmarkgetput(unittest.TestCase):
    """ Sanity tests for Benchmarkgetput """
    archive_dir = "/tmp"
    iteration = {'acceptable': [1,2,3], 'iteration': 0}
    cluster = {}
    cl_name = "tools/invariant.yaml"
    bl_name = "tools/baseline.json"
    bl_json = {}
    bl_md5 = 'aa42ab3c2da0e01ecec18add853f7d79'
    md5_returned = None

    @classmethod
    def setUpClass(cls):
        with open(cls.bl_name, 'rb') as f:
            data = f.read()
            f.close()
        cls.md5_returned = hashlib.md5(data).hexdigest()
        settings.mock_initialize(config_file=cls.cl_name)
        cls.cluster = Ceph.mockinit(settings.cluster)
        with open(cls.bl_name, 'r') as f:
            cls.bl_json = json.load(f)
            f.close()

    @classmethod
    def tearDownClass(cls):
        cls.cluster = None
        cls.bl_json = None

    def test_valid_baseline(self):
        """ Verify the baseline has not been compromised """
        self.assertEqual( self.bl_md5, str(self.md5_returned) )

    def test_valid_archive_dir(self):
        """ Basic sanity attribute identity archive_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['archive_dir'], b.__dict__['archive_dir'])

    def test_valid_auth_urls(self):
        """ Basic sanity attribute identity auth_urls check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['auth_urls'], b.__dict__['auth_urls'])

    def test_valid_cmd_path(self):
        """ Basic sanity attribute identity cmd_path check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['cmd_path'], b.__dict__['cmd_path'])

    def test_valid_cmd_path_full(self):
        """ Basic sanity attribute identity cmd_path_full check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['cmd_path_full'], b.__dict__['cmd_path_full'])

    def test_valid_config(self):
        """ Basic sanity attribute identity config check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['config'], b.__dict__['config'])

    def test_valid_container_prefix(self):
        """ Basic sanity attribute identity container_prefix check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['container_prefix'], b.__dict__['container_prefix'])

    def test_valid_ctype(self):
        """ Basic sanity attribute identity ctype check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['ctype'], b.__dict__['ctype'])

    def test_valid_debug(self):
        """ Basic sanity attribute identity debug check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['debug'], b.__dict__['debug'])

    def test_valid_grace(self):
        """ Basic sanity attribute identity grace check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['grace'], b.__dict__['grace'])

    def test_valid_key(self):
        """ Basic sanity attribute identity key check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['key'], b.__dict__['key'])

    def test_valid_log_bw(self):
        """ Basic sanity attribute identity log_bw check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['log_bw'], b.__dict__['log_bw'])

    def test_valid_log_iops(self):
        """ Basic sanity attribute identity log_iops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['log_iops'], b.__dict__['log_iops'])

    def test_valid_log_lat(self):
        """ Basic sanity attribute identity log_lat check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['log_lat'], b.__dict__['log_lat'])

    def test_valid_logops(self):
        """ Basic sanity attribute identity logops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['logops'], b.__dict__['logops'])

    def test_valid_object_prefix(self):
        """ Basic sanity attribute identity object_prefix check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['object_prefix'], b.__dict__['object_prefix'])

    def test_valid_op_size(self):
        """ Basic sanity attribute identity op_size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['op_size'], b.__dict__['op_size'])

    def test_valid_ops_per_proc(self):
        """ Basic sanity attribute identity ops_per_proc check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['ops_per_proc'], b.__dict__['ops_per_proc'])

    def test_valid_osd_ra(self):
        """ Basic sanity attribute identity osd_ra check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['osd_ra'], b.__dict__['osd_ra'])

    def test_valid_osd_ra_changed(self):
        """ Basic sanity attribute identity osd_ra_changed check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['osd_ra_changed'], b.__dict__['osd_ra_changed'])

    def test_valid_out_dir(self):
        """ Basic sanity attribute identity out_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['out_dir'], b.__dict__['out_dir'])

    def test_valid_pool_profile(self):
        """ Basic sanity attribute identity pool_profile check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['pool_profile'], b.__dict__['pool_profile'])

    def test_valid_procs(self):
        """ Basic sanity attribute identity procs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['procs'], b.__dict__['procs'])

    def test_valid_run_dir(self):
        """ Basic sanity attribute identity run_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['run_dir'], b.__dict__['run_dir'])

    def test_valid_runtime(self):
        """ Basic sanity attribute identity runtime check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['runtime'], b.__dict__['runtime'])

    def test_valid_subuser(self):
        """ Basic sanity attribute identity subuser check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['subuser'], b.__dict__['subuser'])

    def test_valid_test(self):
        """ Basic sanity attribute identity test check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['test'], b.__dict__['test'])

    def test_valid_tmp_conf(self):
        """ Basic sanity attribute identity tmp_conf check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['tmp_conf'], b.__dict__['tmp_conf'])

    def test_valid_user(self):
        """ Basic sanity attribute identity user check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['user'], b.__dict__['user'])

    def test_valid_valgrind(self):
        """ Basic sanity attribute identity valgrind check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'getput', self.iteration)
        self.assertEqual(self.bl_json['getput']['valgrind'], b.__dict__['valgrind'])

if __name__ == '__main__':
    unittest.main()
