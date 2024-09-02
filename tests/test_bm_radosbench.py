""" Unit tests for the Benchmarkradosbench class """

import unittest
import hashlib
import json
import benchmarkfactory
import settings
from cluster.ceph import Ceph


class TestBenchmarkradosbench(unittest.TestCase):
    """ Sanity tests for Benchmarkradosbench """
    archive_dir = "/tmp"
    iteration = {'acceptable': [1,2,3], 'iteration': 0}
    cluster = {}
    cl_name = "tools/invariant.yaml"
    bl_name = "tools/baseline.json"
    bl_json = {}
    bl_md5 = 'e6b6fcd2be74bd08939c64a249ab2125'
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
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['archive_dir'], b.__dict__['archive_dir'])

    def test_valid_cmd_path(self):
        """ Basic sanity attribute identity cmd_path check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['cmd_path'], b.__dict__['cmd_path'])

    def test_valid_cmd_path_full(self):
        """ Basic sanity attribute identity cmd_path_full check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['cmd_path_full'], b.__dict__['cmd_path_full'])

    def test_valid_concurrent_ops(self):
        """ Basic sanity attribute identity concurrent_ops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['concurrent_ops'], b.__dict__['concurrent_ops'])

    def test_valid_concurrent_procs(self):
        """ Basic sanity attribute identity concurrent_procs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['concurrent_procs'], b.__dict__['concurrent_procs'])

    def test_valid_config(self):
        """ Basic sanity attribute identity config check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['config'], b.__dict__['config'])

    def test_valid_log_bw(self):
        """ Basic sanity attribute identity log_bw check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['log_bw'], b.__dict__['log_bw'])

    def test_valid_log_iops(self):
        """ Basic sanity attribute identity log_iops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['log_iops'], b.__dict__['log_iops'])

    def test_valid_log_lat(self):
        """ Basic sanity attribute identity log_lat check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['log_lat'], b.__dict__['log_lat'])

    def test_valid_max_objects(self):
        """ Basic sanity attribute identity max_objects check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['max_objects'], b.__dict__['max_objects'])

    def test_valid_object_set_id(self):
        """ Basic sanity attribute identity object_set_id check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['object_set_id'], b.__dict__['object_set_id'])

    def test_valid_op_size(self):
        """ Basic sanity attribute identity op_size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['op_size'], b.__dict__['op_size'])

    def test_valid_osd_ra(self):
        """ Basic sanity attribute identity osd_ra check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['osd_ra'], b.__dict__['osd_ra'])

    def test_valid_osd_ra_changed(self):
        """ Basic sanity attribute identity osd_ra_changed check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['osd_ra_changed'], b.__dict__['osd_ra_changed'])

    def test_valid_out_dir(self):
        """ Basic sanity attribute identity out_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['out_dir'], b.__dict__['out_dir'])

    def test_valid_pool(self):
        """ Basic sanity attribute identity pool check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['pool'], b.__dict__['pool'])

    def test_valid_pool_per_proc(self):
        """ Basic sanity attribute identity pool_per_proc check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['pool_per_proc'], b.__dict__['pool_per_proc'])

    def test_valid_pool_profile(self):
        """ Basic sanity attribute identity pool_profile check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['pool_profile'], b.__dict__['pool_profile'])

    def test_valid_prefill_objects(self):
        """ Basic sanity attribute identity prefill_objects check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['prefill_objects'], b.__dict__['prefill_objects'])

    def test_valid_prefill_time(self):
        """ Basic sanity attribute identity prefill_time check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['prefill_time'], b.__dict__['prefill_time'])

    def test_valid_read_only(self):
        """ Basic sanity attribute identity read_only check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['read_only'], b.__dict__['read_only'])

    def test_valid_read_time(self):
        """ Basic sanity attribute identity read_time check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['read_time'], b.__dict__['read_time'])

    def test_valid_readmode(self):
        """ Basic sanity attribute identity readmode check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['readmode'], b.__dict__['readmode'])

    def test_valid_run_dir(self):
        """ Basic sanity attribute identity run_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['run_dir'], b.__dict__['run_dir'])

    def test_valid_time(self):
        """ Basic sanity attribute identity time check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['time'], b.__dict__['time'])

    def test_valid_tmp_conf(self):
        """ Basic sanity attribute identity tmp_conf check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['tmp_conf'], b.__dict__['tmp_conf'])

    def test_valid_valgrind(self):
        """ Basic sanity attribute identity valgrind check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['valgrind'], b.__dict__['valgrind'])

    def test_valid_write_omap(self):
        """ Basic sanity attribute identity write_omap check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['write_omap'], b.__dict__['write_omap'])

    def test_valid_write_only(self):
        """ Basic sanity attribute identity write_only check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['write_only'], b.__dict__['write_only'])

    def test_valid_write_time(self):
        """ Basic sanity attribute identity write_time check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'radosbench', self.iteration)
        self.assertEqual(self.bl_json['radosbench']['write_time'], b.__dict__['write_time'])

if __name__ == '__main__':
    unittest.main()
