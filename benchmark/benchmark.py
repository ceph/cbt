import logging
from abc import ABC, abstractmethod

import hashlib
import os
import json
import yaml
import settings
import common

logger = logging.getLogger('cbt')


class Benchmark(object):
    def __init__(self, archive_dir, cluster, config):
        self.acceptable = config.pop('acceptable', {})
        self.config = config
        self.cluster = cluster
        hashable = json.dumps(sorted(self.config.items())).encode()
        digest = hashlib.sha1(hashable).hexdigest()[:8]
        self.archive_dir = os.path.join(archive_dir,
                                        'results',
                                        '{:0>8}'.format(config.get('iteration')),
                                        'id-{}'.format(digest))
        # This would show several dirs if run continuously
        logger.info("Results dir: %s", self.archive_dir )
        self.run_dir = os.path.join(settings.cluster.get('tmp_dir'),
                                    '{:0>8}'.format(config.get('iteration')),
                                    self.getclass())
        self.osd_ra = config.get('osd_ra', '0')
        self.cmd_path = ''
        self.valgrind = config.get('valgrind', None)
        self.cmd_path_full = ''
        self.log_iops = config.get('log_iops', True)
        self.log_bw = config.get('log_bw', True)
        self.log_lat = config.get('log_lat', True)
        if self.valgrind is not None:
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)

        self.osd_ra_changed = False
        if self.osd_ra:
            self.osd_ra_changed = True
        else:
            self.osd_ra = common.get_osd_ra()

    def create_data_analyzer(self, run, host, proc):
        pass

    def _compare_client_results(self, client_run, self_analyzer, baseline_analyzer):
        from .lis import Lispy, Env
        # normalize the names
        aliases = {'bandwidth': 'Bandwidth (MB/sec)',
                   'iops_avg': 'Average IOPS',
                   'iops_stddev': 'Stddev IOPS',
                   'latency_avg': 'Average Latency(s)',
                   'cpu_cycles_per_op': 'Cycles per operation'}
        res_outputs = []  # list of dictionaries containing the self and baseline benchmark results
        compare_results = []
        self_analyzer_res = {}
        baseline_analyzer_res = {}
        for alias in self.acceptable:
            name = aliases[alias]
            self_getter = getattr(self_analyzer, 'get_' + alias)
            if self_getter == None:
                logger.info('CPU Cycles Per Operation metric is not configured for this benchmark')
                continue
            self_analyzer_res[name] = self_getter()
            if self_analyzer_res[name] is None:
                paranoid_path = "/proc/sys/kernel/perf_event_paranoid"
                with open(paranoid_path) as f:
                    paranoid_level = int(f.read())
                    if paranoid_level >= 1:
                        msg = ('''Perf must be run by user with CAP_SYS_ADMIN to extract'''
                        '''CPU related metrics. Or you could set %s to 0,'''
                        '''which is %d now''')
                        logger.warning('%s. %s is %d', msg, paranoid_path, paranoid_level)
                continue
            baseline_getter = getattr(baseline_analyzer, 'get_' + alias)
            baseline_analyzer_res[name] = baseline_getter()
        res_outputs.append(self_analyzer_res)
        res_outputs.append(baseline_analyzer_res)
        for alias, stmt in list(self.acceptable.items()):
            name = aliases[alias]
            result, baseline = [float(j[name]) for j in res_outputs]
            # safer than eval()
            env = Env(None, result=result, baseline=baseline)
            lispy = Lispy()
            accepted = lispy.eval(lispy.parse(stmt), env)
            result = Result(client_run, alias, result, baseline, stmt, accepted)
            compare_results.append(result)
        return compare_results

    def evaluate(self, baseline):
        runs = []
        if self.prefill_time or self.prefill_objects:
            runs.append('prefill')
        if not self.read_only:
            runs.append('write')
        if not self.write_only:
            runs.append(self.readmode)
        results = []
        for run in runs:
            for client in settings.getnodes('clients').split(','):
                host = settings.host_info(client)["host"]
                for proc in range(self.concurrent_procs):
                    self_analyzer = self.create_data_analyzer(run, host, proc)
                    baseline_analyzer = baseline.create_data_analyzer(run, host, proc)
                    client_run = '{run}/{client}/{proc}'.format(run=run, client=client, proc=proc)
                    compare_results = self._compare_client_results(client_run, self_analyzer, baseline_analyzer)
                    results.extend(compare_results)
            # TODO: check results from monitors
        return results

    def cleandir(self):
        # Wipe and create the run directory
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

    def getclass(self):
        return self.__class__.__name__

    def initialize(self):
        pass

    def initialize_endpoints(self):
        pass

    def prefill(self):
        pass

    def run(self):
        if self.osd_ra and self.osd_ra_changed:
            logger.info('Setting OSD Read Ahead to: %s', self.osd_ra)
            self.cluster.set_osd_param('read_ahead_kb', self.osd_ra)

        logger.debug('Cleaning existing temporary run directory: %s', self.run_dir)
        common.pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws'), 'sudo rm -rf %s' % self.run_dir).communicate()
        if self.valgrind is not None:
            logger.debug('Adding valgrind to the command path.')
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)
        # Set the full command path
        self.cmd_path_full += self.cmd_path

        # Store the parameters of the test run
        config_file = os.path.join(self.archive_dir, 'benchmark_config.yaml')
        if not os.path.exists(self.archive_dir):
            os.makedirs(self.archive_dir)
        if not os.path.exists(config_file):
            config_dict = dict(cluster=self.config)
            with open(config_file, 'w') as fd:
                yaml.dump(config_dict, fd, default_flow_style=False)

    def exists(self):
        return False

    def compare(self, baseline):
        logger.warn('%s does not support "compare" yet', self.getclass())

    def cleanup(self):
        pass

    def dropcaches(self):
        nodes = settings.getnodes('clients', 'osds')

        common.pdsh(nodes, 'sync').communicate()
        common.pdsh(nodes, 'echo 3 | sudo tee /proc/sys/vm/drop_caches').communicate()

    def __str__(self):
        return str(self.config)


class DataAnalyzer(ABC):
    def __init__(self, archive_dir, run, host, proc):
        super().__init__()
        self.archive_dir = archive_dir

    @abstractmethod
    def get_cpu_cycles_per_op(self):
        pass

    @abstractmethod
    def get_latency_avg(self):
        pass

    @abstractmethod
    def get_bandwidth(self):
        pass

    @abstractmethod
    def get_iops_avg(self):
        pass

    @abstractmethod
    def get_iops_stddev(self):
        pass


class Result:
    def __init__(self, run, alias, result, baseline, stmt, accepted):
        self.run = run
        self.alias = alias
        self.result = result
        self.baseline = baseline
        self.stmt = stmt
        self.accepted = accepted

    def __str__(self):
        fmt = '{run}: {alias}: {stmt}:: {result}/{baseline}  => {status}'
        return fmt.format(run=self.run, alias=self.alias, stmt=self.stmt,
                          result=self.result, baseline=self.baseline,
                          status="accepted" if self.accepted else "rejected")
