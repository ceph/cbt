""" Unit tests for the Benchmarkfio class """

import unittest
import hashlib
import json
import benchmarkfactory
import settings
from cluster.ceph import Ceph


class TestBenchmarkfio(unittest.TestCase):
    """ Sanity tests for Benchmarkfio """
    archive_dir = "/tmp"
    iteration = {'acceptable': [1,2,3], 'iteration': 0}
    cluster = {}
    cl_name = "tools/invariant.yaml"
    bl_name = "tools/baseline.json"
    bl_json = {}
    bl_md5 = 'b62e2394b5bac4dea01cceace04d0359'
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
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['archive_dir'], b.__dict__['archive_dir'])

    def test_valid_bs(self):
        """ Basic sanity attribute identity bs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['bs'], b.__dict__['bs'])

    def test_valid_bsrange(self):
        """ Basic sanity attribute identity bsrange check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['bsrange'], b.__dict__['bsrange'])

    def test_valid_bssplit(self):
        """ Basic sanity attribute identity bssplit check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['bssplit'], b.__dict__['bssplit'])

    def test_valid_client_endpoints(self):
        """ Basic sanity attribute identity client_endpoints check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['client_endpoints'], b.__dict__['client_endpoints'])

    def test_valid_cmd_path(self):
        """ Basic sanity attribute identity cmd_path check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['cmd_path'], b.__dict__['cmd_path'])

    def test_valid_cmd_path_full(self):
        """ Basic sanity attribute identity cmd_path_full check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['cmd_path_full'], b.__dict__['cmd_path_full'])

    def test_valid_config(self):
        """ Basic sanity attribute identity config check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['config'], b.__dict__['config'])

    def test_valid_direct(self):
        """ Basic sanity attribute identity direct check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['direct'], b.__dict__['direct'])

    def test_valid_end_fsync(self):
        """ Basic sanity attribute identity end_fsync check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['end_fsync'], b.__dict__['end_fsync'])

    def test_valid_fio_out_format(self):
        """ Basic sanity attribute identity fio_out_format check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['fio_out_format'], b.__dict__['fio_out_format'])

    def test_valid_iodepth(self):
        """ Basic sanity attribute identity iodepth check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['iodepth'], b.__dict__['iodepth'])

    def test_valid_ioengine(self):
        """ Basic sanity attribute identity ioengine check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['ioengine'], b.__dict__['ioengine'])

    def test_valid_log_avg_msec(self):
        """ Basic sanity attribute identity log_avg_msec check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['log_avg_msec'], b.__dict__['log_avg_msec'])

    def test_valid_log_bw(self):
        """ Basic sanity attribute identity log_bw check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['log_bw'], b.__dict__['log_bw'])

    def test_valid_log_iops(self):
        """ Basic sanity attribute identity log_iops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['log_iops'], b.__dict__['log_iops'])

    def test_valid_log_lat(self):
        """ Basic sanity attribute identity log_lat check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['log_lat'], b.__dict__['log_lat'])

    def test_valid_logging(self):
        """ Basic sanity attribute identity logging check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['logging'], b.__dict__['logging'])

    def test_valid_mode(self):
        """ Basic sanity attribute identity mode check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['mode'], b.__dict__['mode'])

    def test_valid_norandommap(self):
        """ Basic sanity attribute identity norandommap check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['norandommap'], b.__dict__['norandommap'])

    def test_valid_numjobs(self):
        """ Basic sanity attribute identity numjobs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['numjobs'], b.__dict__['numjobs'])

    def test_valid_op_size(self):
        """ Basic sanity attribute identity op_size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['op_size'], b.__dict__['op_size'])

    def test_valid_osd_ra(self):
        """ Basic sanity attribute identity osd_ra check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['osd_ra'], b.__dict__['osd_ra'])

    def test_valid_osd_ra_changed(self):
        """ Basic sanity attribute identity osd_ra_changed check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['osd_ra_changed'], b.__dict__['osd_ra_changed'])

    def test_valid_out_dir(self):
        """ Basic sanity attribute identity out_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['out_dir'], b.__dict__['out_dir'])

    def test_valid_prefill_flag(self):
        """ Basic sanity attribute identity prefill_flag check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['prefill_flag'], b.__dict__['prefill_flag'])

    def test_valid_prefill_iodepth(self):
        """ Basic sanity attribute identity prefill_iodepth check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['prefill_iodepth'], b.__dict__['prefill_iodepth'])

    def test_valid_procs_per_endpoint(self):
        """ Basic sanity attribute identity procs_per_endpoint check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['procs_per_endpoint'], b.__dict__['procs_per_endpoint'])

    def test_valid_ramp(self):
        """ Basic sanity attribute identity ramp check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['ramp'], b.__dict__['ramp'])

    def test_valid_random_distribution(self):
        """ Basic sanity attribute identity random_distribution check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['random_distribution'], b.__dict__['random_distribution'])

    def test_valid_rate_iops(self):
        """ Basic sanity attribute identity rate_iops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['rate_iops'], b.__dict__['rate_iops'])

    def test_valid_recov_test_type(self):
        """ Basic sanity attribute identity recov_test_type check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['recov_test_type'], b.__dict__['recov_test_type'])

    def test_valid_run_dir(self):
        """ Basic sanity attribute identity run_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['run_dir'], b.__dict__['run_dir'])

    def test_valid_rwmixread(self):
        """ Basic sanity attribute identity rwmixread check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['rwmixread'], b.__dict__['rwmixread'])

    def test_valid_rwmixwrite(self):
        """ Basic sanity attribute identity rwmixwrite check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['rwmixwrite'], b.__dict__['rwmixwrite'])

    def test_valid_size(self):
        """ Basic sanity attribute identity size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['size'], b.__dict__['size'])

    def test_valid_sync(self):
        """ Basic sanity attribute identity sync check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['sync'], b.__dict__['sync'])

    def test_valid_time(self):
        """ Basic sanity attribute identity time check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['time'], b.__dict__['time'])

    def test_valid_time_based(self):
        """ Basic sanity attribute identity time_based check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['time_based'], b.__dict__['time_based'])

    def test_valid_valgrind(self):
        """ Basic sanity attribute identity valgrind check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'fio', self.iteration)
        self.assertEqual(self.bl_json['fio']['valgrind'], b.__dict__['valgrind'])

if __name__ == '__main__':
    unittest.main()
