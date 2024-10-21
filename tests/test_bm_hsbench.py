""" Unit tests for the Benchmarkhsbench class """

import unittest
import hashlib
import json
import benchmarkfactory
import settings
from cluster.ceph import Ceph


class TestBenchmarkhsbench(unittest.TestCase):
    """ Sanity tests for Benchmarkhsbench """
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
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['archive_dir'], b.__dict__['archive_dir'])

    def test_valid_bucket_prefix(self):
        """ Basic sanity attribute identity bucket_prefix check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['bucket_prefix'], b.__dict__['bucket_prefix'])

    def test_valid_buckets(self):
        """ Basic sanity attribute identity buckets check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['buckets'], b.__dict__['buckets'])

    def test_valid_client_endpoints(self):
        """ Basic sanity attribute identity client_endpoints check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['client_endpoints'], b.__dict__['client_endpoints'])

    def test_valid_cmd_path(self):
        """ Basic sanity attribute identity cmd_path check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['cmd_path'], b.__dict__['cmd_path'])

    def test_valid_cmd_path_full(self):
        """ Basic sanity attribute identity cmd_path_full check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['cmd_path_full'], b.__dict__['cmd_path_full'])

    def test_valid_config(self):
        """ Basic sanity attribute identity config check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['config'], b.__dict__['config'])

    def test_valid_duration(self):
        """ Basic sanity attribute identity duration check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['duration'], b.__dict__['duration'])

    def test_valid_log_bw(self):
        """ Basic sanity attribute identity log_bw check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['log_bw'], b.__dict__['log_bw'])

    def test_valid_log_iops(self):
        """ Basic sanity attribute identity log_iops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['log_iops'], b.__dict__['log_iops'])

    def test_valid_log_lat(self):
        """ Basic sanity attribute identity log_lat check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['log_lat'], b.__dict__['log_lat'])

    def test_valid_loop(self):
        """ Basic sanity attribute identity loop check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['loop'], b.__dict__['loop'])

    def test_valid_max_keys(self):
        """ Basic sanity attribute identity max_keys check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['max_keys'], b.__dict__['max_keys'])

    def test_valid_modes(self):
        """ Basic sanity attribute identity modes check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['modes'], b.__dict__['modes'])

    def test_valid_object_prefix(self):
        """ Basic sanity attribute identity object_prefix check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['object_prefix'], b.__dict__['object_prefix'])

    def test_valid_objects(self):
        """ Basic sanity attribute identity objects check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['objects'], b.__dict__['objects'])

    def test_valid_osd_ra(self):
        """ Basic sanity attribute identity osd_ra check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['osd_ra'], b.__dict__['osd_ra'])

    def test_valid_osd_ra_changed(self):
        """ Basic sanity attribute identity osd_ra_changed check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['osd_ra_changed'], b.__dict__['osd_ra_changed'])

    def test_valid_out_dir(self):
        """ Basic sanity attribute identity out_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['out_dir'], b.__dict__['out_dir'])

    def test_valid_per_client_object_prefix(self):
        """ Basic sanity attribute identity per_client_object_prefix check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['per_client_object_prefix'], b.__dict__['per_client_object_prefix'])

    def test_valid_prefill_flag(self):
        """ Basic sanity attribute identity prefill_flag check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['prefill_flag'], b.__dict__['prefill_flag'])

    def test_valid_prefill_modes(self):
        """ Basic sanity attribute identity prefill_modes check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['prefill_modes'], b.__dict__['prefill_modes'])

    def test_valid_region(self):
        """ Basic sanity attribute identity region check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['region'], b.__dict__['region'])

    def test_valid_report_intervals(self):
        """ Basic sanity attribute identity report_intervals check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['report_intervals'], b.__dict__['report_intervals'])

    def test_valid_run_dir(self):
        """ Basic sanity attribute identity run_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['run_dir'], b.__dict__['run_dir'])

    def test_valid_size(self):
        """ Basic sanity attribute identity size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['size'], b.__dict__['size'])

    def test_valid_threads(self):
        """ Basic sanity attribute identity threads check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['threads'], b.__dict__['threads'])

    def test_valid_tmp_conf(self):
        """ Basic sanity attribute identity tmp_conf check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['tmp_conf'], b.__dict__['tmp_conf'])

    def test_valid_valgrind(self):
        """ Basic sanity attribute identity valgrind check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'hsbench', self.iteration)
        self.assertEqual(self.bl_json['hsbench']['valgrind'], b.__dict__['valgrind'])

if __name__ == '__main__':
    unittest.main()
