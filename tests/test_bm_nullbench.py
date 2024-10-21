""" Unit tests for the Benchmarknullbench class """

import unittest
import hashlib
import json
import benchmarkfactory
import settings
from cluster.ceph import Ceph


class TestBenchmarknullbench(unittest.TestCase):
    """ Sanity tests for Benchmarknullbench """
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
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['archive_dir'], b.__dict__['archive_dir'])

    def test_valid_cmd_path(self):
        """ Basic sanity attribute identity cmd_path check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['cmd_path'], b.__dict__['cmd_path'])

    def test_valid_cmd_path_full(self):
        """ Basic sanity attribute identity cmd_path_full check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['cmd_path_full'], b.__dict__['cmd_path_full'])

    def test_valid_config(self):
        """ Basic sanity attribute identity config check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['config'], b.__dict__['config'])

    def test_valid_log_bw(self):
        """ Basic sanity attribute identity log_bw check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['log_bw'], b.__dict__['log_bw'])

    def test_valid_log_iops(self):
        """ Basic sanity attribute identity log_iops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['log_iops'], b.__dict__['log_iops'])

    def test_valid_log_lat(self):
        """ Basic sanity attribute identity log_lat check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['log_lat'], b.__dict__['log_lat'])

    def test_valid_osd_ra(self):
        """ Basic sanity attribute identity osd_ra check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['osd_ra'], b.__dict__['osd_ra'])

    def test_valid_osd_ra_changed(self):
        """ Basic sanity attribute identity osd_ra_changed check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['osd_ra_changed'], b.__dict__['osd_ra_changed'])

    def test_valid_run_dir(self):
        """ Basic sanity attribute identity run_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['run_dir'], b.__dict__['run_dir'])

    def test_valid_valgrind(self):
        """ Basic sanity attribute identity valgrind check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'nullbench', self.iteration)
        self.assertEqual(self.bl_json['nullbench']['valgrind'], b.__dict__['valgrind'])

if __name__ == '__main__':
    unittest.main()
