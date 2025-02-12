""" Unit tests for the Benchmarklibrbdfio class """

import unittest
import hashlib
import json
import benchmarkfactory
import settings
from cluster.ceph import Ceph


class TestBenchmarklibrbdfio(unittest.TestCase):
    """ Sanity tests for Benchmarklibrbdfio """
    archive_dir = "/tmp"
    iteration = {'acceptable': [1,2,3], 'iteration': 0}
    cluster = {}
    cl_name = "tools/invariant.yaml"
    bl_name = "tools/baseline.json"
    bl_json = {}
    bl_md5 = '84dd2f3a66eab442cc3825e0d57a9e3f'
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

    def test_valid__ioddepth_per_volume(self):
        """ Basic sanity attribute identity _ioddepth_per_volume check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['_ioddepth_per_volume'], b.__dict__['_ioddepth_per_volume'])

    def test_valid_archive_dir(self):
        """ Basic sanity attribute identity archive_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['archive_dir'], b.__dict__['archive_dir'])

    def test_valid_base_run_dir(self):
        """ Basic sanity attribute identity base_run_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['base_run_dir'], b.__dict__['base_run_dir'])

    def test_valid_cmd_path(self):
        """ Basic sanity attribute identity cmd_path check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['cmd_path'], b.__dict__['cmd_path'])

    def test_valid_cmd_path_full(self):
        """ Basic sanity attribute identity cmd_path_full check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['cmd_path_full'], b.__dict__['cmd_path_full'])

    def test_valid_config(self):
        """ Basic sanity attribute identity config check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['config'], b.__dict__['config'])

    def test_valid_data_pool(self):
        """ Basic sanity attribute identity data_pool check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['data_pool'], b.__dict__['data_pool'])

    def test_valid_data_pool_profile(self):
        """ Basic sanity attribute identity data_pool_profile check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['data_pool_profile'], b.__dict__['data_pool_profile'])

    def test_valid_end_fsync(self):
        """ Basic sanity attribute identity end_fsync check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['end_fsync'], b.__dict__['end_fsync'])

    def test_valid_fio_out_format(self):
        """ Basic sanity attribute identity fio_out_format check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['fio_out_format'], b.__dict__['fio_out_format'])

    def test_valid_global_fio_options(self):
        """ Basic sanity attribute identity global_fio_options check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['global_fio_options'], b.__dict__['global_fio_options'])

    def test_valid_idle_monitor_sleep(self):
        """ Basic sanity attribute identity idle_monitor_sleep check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['idle_monitor_sleep'], b.__dict__['idle_monitor_sleep'])

    def test_valid_iodepth(self):
        """ Basic sanity attribute identity iodepth check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['iodepth'], b.__dict__['iodepth'])

    def test_valid_log_avg_msec(self):
        """ Basic sanity attribute identity log_avg_msec check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['log_avg_msec'], b.__dict__['log_avg_msec'])

    def test_valid_log_bw(self):
        """ Basic sanity attribute identity log_bw check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['log_bw'], b.__dict__['log_bw'])

    def test_valid_log_iops(self):
        """ Basic sanity attribute identity log_iops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['log_iops'], b.__dict__['log_iops'])

    def test_valid_log_lat(self):
        """ Basic sanity attribute identity log_lat check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['log_lat'], b.__dict__['log_lat'])

    def test_valid_mode(self):
        """ Basic sanity attribute identity mode check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['mode'], b.__dict__['mode'])

    def test_valid_names(self):
        """ Basic sanity attribute identity names check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['names'], b.__dict__['names'])

    def test_valid_no_sudo(self):
        """ Basic sanity attribute identity no_sudo check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['no_sudo'], b.__dict__['no_sudo'])

    def test_valid_norandommap(self):
        """ Basic sanity attribute identity norandommap check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['norandommap'], b.__dict__['norandommap'])

    def test_valid_numjobs(self):
        """ Basic sanity attribute identity numjobs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['numjobs'], b.__dict__['numjobs'])

    def test_valid_op_size(self):
        """ Basic sanity attribute identity op_size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['op_size'], b.__dict__['op_size'])

    def test_valid_osd_ra(self):
        """ Basic sanity attribute identity osd_ra check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['osd_ra'], b.__dict__['osd_ra'])

    def test_valid_osd_ra_changed(self):
        """ Basic sanity attribute identity osd_ra_changed check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['osd_ra_changed'], b.__dict__['osd_ra_changed'])

    def test_valid_out_dir(self):
        """ Basic sanity attribute identity out_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['out_dir'], b.__dict__['out_dir'])

    def test_valid_pgs(self):
        """ Basic sanity attribute identity pgs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['pgs'], b.__dict__['pgs'])

    def test_valid_pool_name(self):
        """ Basic sanity attribute identity pool_name check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['pool_name'], b.__dict__['pool_name'])

    def test_valid_pool_profile(self):
        """ Basic sanity attribute identity pool_profile check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['pool_profile'], b.__dict__['pool_profile'])

    def test_valid_precond_time(self):
        """ Basic sanity attribute identity precond_time check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['precond_time'], b.__dict__['precond_time'])

    def test_valid_prefill_vols(self):
        """ Basic sanity attribute identity prefill_vols check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['prefill_vols'], b.__dict__['prefill_vols'])

    def test_valid_procs_per_volume(self):
        """ Basic sanity attribute identity procs_per_volume check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['procs_per_volume'], b.__dict__['procs_per_volume'])

    def test_valid_ramp(self):
        """ Basic sanity attribute identity ramp check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['ramp'], b.__dict__['ramp'])

    def test_valid_random_distribution(self):
        """ Basic sanity attribute identity random_distribution check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['random_distribution'], b.__dict__['random_distribution'])

    def test_valid_rate_iops(self):
        """ Basic sanity attribute identity rate_iops check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['rate_iops'], b.__dict__['rate_iops'])

    def test_valid_rbdname(self):
        """ Basic sanity attribute identity rbdname check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['rbdname'], b.__dict__['rbdname'])

    def test_valid_recov_pool_name(self):
        """ Basic sanity attribute identity recov_pool_name check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['recov_pool_name'], b.__dict__['recov_pool_name'])

    def test_valid_recov_pool_profile(self):
        """ Basic sanity attribute identity recov_pool_profile check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['recov_pool_profile'], b.__dict__['recov_pool_profile'])

    def test_valid_recov_test_type(self):
        """ Basic sanity attribute identity recov_test_type check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['recov_test_type'], b.__dict__['recov_test_type'])

    def test_valid_run_dir(self):
        """ Basic sanity attribute identity run_dir check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['run_dir'], b.__dict__['run_dir'])

    def test_valid_rwmixread(self):
        """ Basic sanity attribute identity rwmixread check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['rwmixread'], b.__dict__['rwmixread'])

    def test_valid_rwmixwrite(self):
        """ Basic sanity attribute identity rwmixwrite check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['rwmixwrite'], b.__dict__['rwmixwrite'])

    def test_valid_time(self):
        """ Basic sanity attribute identity time check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['time'], b.__dict__['time'])

    def test_valid_time_based(self):
        """ Basic sanity attribute identity time_based check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['time_based'], b.__dict__['time_based'])

    def test_valid_total_procs(self):
        """ Basic sanity attribute identity total_procs check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['total_procs'], b.__dict__['total_procs'])

    def test_valid_use_existing_volumes(self):
        """ Basic sanity attribute identity use_existing_volumes check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['use_existing_volumes'], b.__dict__['use_existing_volumes'])

    def test_valid_valgrind(self):
        """ Basic sanity attribute identity valgrind check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['valgrind'], b.__dict__['valgrind'])

    def test_valid_vol_object_size(self):
        """ Basic sanity attribute identity vol_object_size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['vol_object_size'], b.__dict__['vol_object_size'])

    def test_valid_vol_size(self):
        """ Basic sanity attribute identity vol_size check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['vol_size'], b.__dict__['vol_size'])

    def test_valid_volumes_per_client(self):
        """ Basic sanity attribute identity volumes_per_client check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['volumes_per_client'], b.__dict__['volumes_per_client'])

    def test_valid_wait_pgautoscaler_timeout(self):
        """ Basic sanity attribute identity wait_pgautoscaler_timeout check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['wait_pgautoscaler_timeout'], b.__dict__['wait_pgautoscaler_timeout'])

    def test_valid_workloads(self):
        """ Basic sanity attribute identity workloads check"""
        b = benchmarkfactory.get_object(self.archive_dir,
                                            self.cluster, 'librbdfio', self.iteration)
        self.assertEqual(self.bl_json['librbdfio']['workloads'], b.__dict__['workloads'])

if __name__ == '__main__':
    unittest.main()
