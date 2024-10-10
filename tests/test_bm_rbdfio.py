""" Unit tests for the Benchmarkrbdfio class """

import unittest
import hashlib
import json
import benchmarkfactory
import settings
from cluster.ceph import Ceph


class TestBenchmarkrbdfio(unittest.TestCase):
    """ Sanity tests for Benchmarkrbdfio """
    archive_dir = "/tmp"
    iteration = {'acceptable': [1,2,3], 'iteration': 0}
    cluster = {}
    cl_name = "tools/invariant.yaml"
    bl_name = "tools/baseline.json"
    bl_json = {}
    bl_md5 = '30f2e8cc8a8aca6538d818919834ef27'
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
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['archive_dir'], b.__dict__['archive_dir'])

    def test_valid_client_ra(self):
        """ Basic sanity attribute identity client_ra check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['client_ra'], b.__dict__['client_ra'])

    def test_valid_cmd_path(self):
        """ Basic sanity attribute identity cmd_path check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['cmd_path'], b.__dict__['cmd_path'])

    def test_valid_cmd_path_full(self):
        """ Basic sanity attribute identity cmd_path_full check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['cmd_path_full'], b.__dict__['cmd_path_full'])

    def test_valid_concurrent_procs(self):
        """ Basic sanity attribute identity concurrent_procs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['concurrent_procs'], b.__dict__['concurrent_procs'])

    def test_valid_config(self):
        """ Basic sanity attribute identity config check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['config'], b.__dict__['config'])

    def test_valid_direct(self):
        """ Basic sanity attribute identity direct check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['direct'], b.__dict__['direct'])

    def test_valid_end_fsync(self):
        """ Basic sanity attribute identity end_fsync check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['end_fsync'], b.__dict__['end_fsync'])

    def test_valid_iodepth(self):
        """ Basic sanity attribute identity iodepth check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['iodepth'], b.__dict__['iodepth'])

    def test_valid_ioengine(self):
        """ Basic sanity attribute identity ioengine check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['ioengine'], b.__dict__['ioengine'])

    def test_valid_log_avg_msec(self):
        """ Basic sanity attribute identity log_avg_msec check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['log_avg_msec'], b.__dict__['log_avg_msec'])

    def test_valid_log_bw(self):
        """ Basic sanity attribute identity log_bw check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['log_bw'], b.__dict__['log_bw'])

    def test_valid_log_iops(self):
        """ Basic sanity attribute identity log_iops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['log_iops'], b.__dict__['log_iops'])

    def test_valid_log_lat(self):
        """ Basic sanity attribute identity log_lat check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['log_lat'], b.__dict__['log_lat'])

    def test_valid_mode(self):
        """ Basic sanity attribute identity mode check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['mode'], b.__dict__['mode'])

    def test_valid_names(self):
        """ Basic sanity attribute identity names check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['names'], b.__dict__['names'])

    def test_valid_numjobs(self):
        """ Basic sanity attribute identity numjobs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['numjobs'], b.__dict__['numjobs'])

    def test_valid_op_size(self):
        """ Basic sanity attribute identity op_size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['op_size'], b.__dict__['op_size'])

    def test_valid_osd_ra(self):
        """ Basic sanity attribute identity osd_ra check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['osd_ra'], b.__dict__['osd_ra'])

    def test_valid_osd_ra_changed(self):
        """ Basic sanity attribute identity osd_ra_changed check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['osd_ra_changed'], b.__dict__['osd_ra_changed'])

    def test_valid_out_dir(self):
        """ Basic sanity attribute identity out_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['out_dir'], b.__dict__['out_dir'])

    def test_valid_pool_profile(self):
        """ Basic sanity attribute identity pool_profile check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['pool_profile'], b.__dict__['pool_profile'])

    def test_valid_poolname(self):
        """ Basic sanity attribute identity poolname check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['poolname'], b.__dict__['poolname'])

    def test_valid_ramp(self):
        """ Basic sanity attribute identity ramp check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['ramp'], b.__dict__['ramp'])

    def test_valid_random_distribution(self):
        """ Basic sanity attribute identity random_distribution check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['random_distribution'], b.__dict__['random_distribution'])

    def test_valid_rbdadd_mons(self):
        """ Basic sanity attribute identity rbdadd_mons check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['rbdadd_mons'], b.__dict__['rbdadd_mons'])

    def test_valid_rbdadd_options(self):
        """ Basic sanity attribute identity rbdadd_options check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['rbdadd_options'], b.__dict__['rbdadd_options'])

    def test_valid_run_dir(self):
        """ Basic sanity attribute identity run_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['run_dir'], b.__dict__['run_dir'])

    def test_valid_rwmixread(self):
        """ Basic sanity attribute identity rwmixread check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['rwmixread'], b.__dict__['rwmixread'])

    def test_valid_rwmixwrite(self):
        """ Basic sanity attribute identity rwmixwrite check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['rwmixwrite'], b.__dict__['rwmixwrite'])

    def test_valid_time(self):
        """ Basic sanity attribute identity time check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['time'], b.__dict__['time'])

    def test_valid_total_procs(self):
        """ Basic sanity attribute identity total_procs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['total_procs'], b.__dict__['total_procs'])

    def test_valid_valgrind(self):
        """ Basic sanity attribute identity valgrind check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['valgrind'], b.__dict__['valgrind'])

    def test_valid_vol_object_size(self):
        """ Basic sanity attribute identity vol_object_size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['vol_object_size'], b.__dict__['vol_object_size'])

    def test_valid_vol_size(self):
        """ Basic sanity attribute identity vol_size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'rbdfio', self.iteration)
        self.assertEqual(self.bl_json['rbdfio']['vol_size'], b.__dict__['vol_size'])

if __name__ == '__main__':
    unittest.main()
