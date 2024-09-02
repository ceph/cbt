""" Unit tests for the Benchmarkfactory class """

import unittest
import benchmarkfactory
from log_support import setup_loggers


class TestBenchmarkFactory(unittest.TestCase):
    """ Sanity tests for Benchmarkfactory """
    def test_permutations_1(self):
        """ Basic sanity permutations """
        config = {"x": 12, "y": True, "z": {1: 2}, "t": [1, 2, 4]}
        cfgs = list(benchmarkfactory.all_configs(config))
        self.assertEqual(len(cfgs), 3)
        self.assertEqual([dict] * 3, list(map(type, cfgs)))
        tvals = []

        for cfg in cfgs:
            for field in 'xyz':
                self.assertEqual(cfg[field], config[field])
            tvals.append(cfg['t'])

        self.assertEqual(sorted(tvals), sorted(config['t']))

    def test_permutations_2(self):
        """ Basic sanity permutations """
        config = {"x": 12, "y": True, "z": {1: 2}, "t": [1, 2, 4], "j": [7, True, 'gg']}
        cfgs = list(benchmarkfactory.all_configs(config))
        self.assertEqual(len(cfgs), 9)
        self.assertEqual([dict] * 9, list(map(type, cfgs)))

        tjvals = []

        for cfg in cfgs:
            for field in 'xyz':
                self.assertEqual(cfg[field], config[field])
            tjvals.append((cfg['t'], cfg['j']))

        for tval in config['t']:
            for jval in config['j']:
                self.assertEqual(1, tjvals.count((tval, jval)))

    def test_permutations_0(self):
        """ Basic sanity permutations """
        config = {"x": 12, "y": True, "z": {1: 2}}
        cfgs = list(benchmarkfactory.all_configs(config))
        self.assertEqual(len(cfgs), 1)
        self.assertEqual(cfgs[0], config)

if __name__ == '__main__':
    setup_loggers(log_fname='/tmp/cbt-utest.log')
    unittest.main()
