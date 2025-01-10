"""
The intention of this class is to uniquely specify a Test run, this is normally via a name (str)
that succintly describes the following sections of (preferential) information:

* the cluster configuration: this encompasses
- the OSD type being exercised, eg. Crimson (default Bluestore), Classic, Cyan, Seastore
- the number of OSDs
- the number of Seastar Reactors (only for Crimson)
- the number of CPUs used for FIO clients (or other benchmark)

* the workload: this is normally worked out by the client (eg. FIO)
- whether it is a random (r/w, propotion), or sequential I/O, the blocksize
- the iodepth
- the number of jobs

* intended type of postprocessing: Response Curves, Latency target, core CPU balance strategy, etc.

Typically, other than timestamps, a partial order over a list of TestRunSpecs can be established
by the scalars:
* iodepth, and number of jobs.
so that a table of TestRunSpecs can be sorted in increasing order of these values.
Such a TestRunTable is normally used for Response Curves.

A comparison among sets of Response Curves (TestRunTables) can take place over the same workload,
only ranging a single parameter over the cluster configurations, or the sandbox used to compare
performance impact of code changes.

Constructor: expect a (string) seed, normally the "prefix" argument when executing a test plan in aprg, from
this, work out a nested dictionary: the keys specify the different TestRuns (eg, for each typical workload)
names, and values are dictionaries
specifying a regex to use to encode/decode the TestRunSpec, as well some accessors and comparison.
"""

import logging
import re

__author__ = "Jose J Palacios-Perez"
logger = logging.getLogger(__name__)


class TestRunSpec(object):
    # This is used to specify a dictionary with keys the names and values those
    # cyan_8osd_5reactor_8fio_bal_osd_lt_1procs_randwrite.zip
    # prefixed
    RE_RUN_SPEC = re.compile(r"""^(?P<config>[^_]+)_
            (?P<osd>\d+)osd_         # OSD num
            (?P<reactor>\d+)reactor_ # Reactor num
            (?P<FIO>\d+)fio_         # FIO CPU num
            (bal_osd_|bal_socket_|default_)?
            (lt_|mj_)?
            (?P<proc>\d+)procs_
            (randread|randwrite|seqread|seqwrite)
    """)

    _workloads = ["randread", "randwrite", "seqread", "seqwrite"]

    # Split by '_' and parse each token individually
    RE_OSD_TYPE = re.compile(r"^(crimson|cyan|classic)")
    RE_OSD_NUM = re.compile(r"(\d+)osd")
    RE_REACTOR_NUM = re.compile(r"(\d+)reactor")
    RE_FIO_CPU_NUM = re.compile(r"(\d+)fio")
    RE_FIO_PROC_NUM = re.compile(r"(\d+)procs")
    RE_JOB_SPEC = re.compile(r"(bal_osd_|bal_socket_|default_)?(lt_|mj_)?")
    RE_WORKLOAD = re.compile(r"(randread|randwrite|seqread|seqwrite)")

    # We use these keys as attributes when populating the _dictionary
    tr_spec_re = {
        "osd_type": RE_OSD_TYPE,
        "osd_num": RE_OSD_NUM,
        "reactor_num": RE_REACTOR_NUM,
        "fio_cpu_num": RE_FIO_CPU_NUM,
        "fio_proc_num": RE_FIO_PROC_NUM,
        "job_spec": RE_JOB_SPEC,
        "workload": RE_WORKLOAD,
    }

    def __init__(self, prefix: str, dir: str, _d: dict = {}):
        self.prefix = prefix
        self.prefix = prefix
        self.fio_proc_num = 1  # default
        self.suite = {}
        if _d:
            for key, value in _d.items():
                setattr(self, key, value)

    def _parse_by_split(self, seed):
        """
        Parses a seed by splitting the seed on '_'
        """
        splitted = re.split("_", self.seed)
        for x in splitted:
            for k, v in self.tr_spec_re.items():
                match = v.match(x)
                if match:
                    groups = match.groups()
                    setattr(self, k, groups[0])
                continue

    def parse_test_run(self, string: str):
        """
        Applies the global regexes above to split the string into its known components
        """
        match = self.RE_RUN_SPEC.match(string)
        if match:
            groups = match.groups()
            for i, key in enumerate(self.tr_spec_re.keys()):
                setattr(self, key, groups[i])

    def get_test_result(self, workload):
        """
        Generates a string according to the run_fio.sh convention
        TEST_RESULT=${TEST_PREFIX}_${NUM_PROCS}procs_${map[${WORKLOAD}]}
        """
        return f"{self.prefix}_{self.fio_proc_num}procs_{workload}"

    def get_test_run_filenames(self, test_result):
        """
        Produce the list of corresponding filenames produced by the fio-parse-jsons.py script
        during postprocessing
        The keys are used to indicate which data to compare

        Note that the actual output from FIO has the following convention:
        fio_cyan_8osd_5reactor_8fio_default_lt_16job_16io_4k_randread_p0.json
        fio_${TEST_NAME}.json
        where
         TEST_NAME=${TEST_PREFIX}_${job}job_${io}io_${BLOCK_SIZE_KB}_${map[${WORKLOAD}]}_p${i}
        """
        return {
            # params to jc to produce top_pid_json
            "top_out": f"{test_result}_top.out",
            "top_json": f"{test_result}_top.json",
            # params to parse-top.py:
            "top_pid_json": f"{test_result}_pid.json",
            "cpu_avg_json": f"{test_result}_cpu_avg.json",  # aka OSD_CPU_AVG
            # params to fio-parse-jsons:
            "osd_test_list": f"{test_result}_list",
            "tr_table_json": f"{test_result}.json",  # source for comparison
            "disk_stat_json": f"{test_result}_diskstat.json",
            "_dat": f"{test_result}.dat",
        }

    def gen_typical_suite(self):
        """
        Generates a dictionary with keys the test run specs for the four typical workloads
        Can be used to traverse a given dir and examine the corresponding response Latency
        data to produce a comparison chart
        """
        for wk in self._workloads:
            tr = self.get_test_result(wk)
            trd = self.get_test_run_filenames(tr)
            self.suite[tr] = trd
            # self.suite.update({tr: trd})

            # For comparison: this is a TestRunTable:
            # cyan_3osd_3react_unbal_1procs_randread_d/cyan_3osd_3react_unbal_1procs_randread.json
            # This is the corresponding _dat:
            # cyan_3osd_3react_unbal_1procs_randread.dat
            # and these the indiv CPU/MEM (continuous in time for the response latency)
            # FIO_cyan_3osd_3react_unbal_1procs_randread_top_cpu.dat
            # OSD_cyan_3osd_3react_unbal_1procs_randread_top_cpu.dat
