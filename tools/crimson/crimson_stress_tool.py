#!/usr/bin/env python3
import argparse
import math
import os
import shutil
import threading
import time
import re


# Divid all test threads into two categories,Test Case Based Thread and
# Time Point Based Thread. The test threads that only care of what is the
# system going on at a time point, such as reactor utilization are
# classified as Time Point Based Thread and the basic test case threads
# (such as rados bench) that are only the single small test case are
# classified as Test Case Based Thread.
# For developer, you can write the test class you want by implementing
# the Task interfaces and adding it to to testclient_threadclass_list
# or timepoint_threadclass_list in the class Environmen to extend this tool.
# set the start_time to decide when will the test start after thread starts.
class Task(threading.Thread):
    def __init__(self, env, id):
        super().__init__()
        self.thread_num = env.thread_num
        self.start_time = 0
        self.result = None
        self.log = env.args.log
        self.id = id #(tester_id, thread_id)

    # rewrite method create_command() to define the command
    # this class will execute
    def create_command(self):
        raise NotImplementedError

    # don't need to rewite this method
    def run(self):
        time.sleep(self.start_time)
        command = self.create_command()
        print(command)
        self.result = os.popen(command)

        if self.log:
            task_log_path = self.log + "/" + str(self.id[0])+"/" \
                + str(self.id[1]) + "." + type(self).__name__
            with open(task_log_path, "w") as f:
                f.write(self.result.read())
            f.close()
            self.result = open(task_log_path, "r")

    # rewrite method analyse() to analyse the output from executing the
    # command and return a result dict as format {param : result}
    # and the value type should be float
    def analyse(self) -> dict:
        raise NotImplementedError

    # optional.rewrite the method to set the task before this thread
    @staticmethod
    def pre_process(env):
        pass

    # optional.rewrite the method to set the task after this thread
    # you can use env and the result of a case generate by
    # TesterExecutor, which include case test results
    @staticmethod
    def post_process(env, test_case_result):
        pass


class RadosRandWriteThread(Task):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.start_time = 0.01
        self.task_set = env.args.bench_taskset
        self.block_size = env.args.block_size
        self.time = env.args.time
        self.pool = env.pool
        self.iops_key = "rw_IOPS"
        self.latency_key = "rw_Latency"
        self.bandwidth_key = "rw_Bandwidth"

    def create_command(self):
        rados_bench_write = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " write -t " \
            + str(self.thread_num) + " -b " + self.block_size + " " \
            + "--no-cleanup"
        return rados_bench_write

    def analyse(self):
        result_dic = {}  # IOPS, Lantency, Bandwidth
        line = self.result.readline()
        while line:
            if line[0] == 'A':
                element = line.split()
                if element[1] == "IOPS:":
                    result_dic[self.iops_key] = float(element[2])
                if element[1] == "Latency(s):":
                    result_dic[self.latency_key] = round(float(element[2])*1000, 3) #ms
            if line[0] == 'B':
                element = line.split()
                result_dic[self.bandwidth_key] = round(float(element[2]), 4)
            line = self.result.readline()
        self.result.close()
        return result_dic

    @staticmethod
    def post_process(env, test_case_result):
        ratio = env.testclient_threadclass_ratio_map[RadosRandWriteThread]
        test_case_result["rw_IOPS"] *= \
            int(test_case_result['Client_num'] * ratio)
        test_case_result["rw_Bandwidth"] *= \
            int(test_case_result['Client_num'] * ratio)


class RadosSeqWriteThread(RadosRandWriteThread):
    def __init__(self, env, id):
        super().__init__(env,id)
        self.iops_key = "sw_IOPS"
        self.latency_key = "sw_Latency"
        self.bandwidth_key = "sw_Bandwidth"
        self.block_size = env.args.block_size

    def create_command(self):
        rados_bench_write = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " write -t " \
            + str(self.thread_num) \
            + " -b " + self.block_size \
            + " --no-cleanup"
        return rados_bench_write

    @staticmethod
    def pre_process(env):
        env.rados_pre_write(env.args.warmup_block_size, \
                env.args.warmup_thread_num, env.args.warmup_time)

    @staticmethod
    def post_process(env, test_case_result):
        ratio = env.testclient_threadclass_ratio_map[RadosSeqWriteThread]
        test_case_result["sw_IOPS"] *= \
            int(test_case_result['Client_num'] * ratio)
        test_case_result["sw_Bandwidth"] *= \
            int(test_case_result['Client_num'] * ratio)


class RadosRandReadThread(RadosRandWriteThread):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.iops_key = "rr_IOPS"
        self.latency_key = "rr_Latency"
        self.bandwidth_key = "rr_Bandwidth"

    def create_command(self):
        rados_bench_rand_read = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " rand -t " \
            + str(self.thread_num) \
            + " --no-cleanup"
        return rados_bench_rand_read

    @staticmethod
    def pre_process(env):
        env.rados_pre_write(env.args.warmup_block_size, \
                env.args.warmup_thread_num, env.args.warmup_time)

    @staticmethod
    def post_process(env, test_case_result):
        ratio = env.testclient_threadclass_ratio_map[RadosRandReadThread]
        test_case_result["rr_IOPS"] *= \
            int(test_case_result['Client_num'] * ratio)
        test_case_result["rr_Bandwidth"] *= \
            int(test_case_result['Client_num'] * ratio)


class RadosSeqReadThread(RadosRandWriteThread):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.iops_key = "sr_IOPS"
        self.latency_key = "sr_Latency"
        self.bandwidth_key = "sr_Bandwidth"

    def create_command(self):
        rados_bench_seq_read = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " seq -t " \
            + str(self.thread_num) \
            + " --no-cleanup"
        return rados_bench_seq_read

    @staticmethod
    def pre_process(env):
        env.rados_pre_write(env.args.warmup_block_size, \
                env.args.warmup_thread_num, env.args.warmup_time)

    @staticmethod
    def post_process(env, test_case_result):
        ratio = env.testclient_threadclass_ratio_map[RadosSeqReadThread]
        test_case_result["sr_IOPS"] *= \
            int(test_case_result['Client_num'] * ratio)
        test_case_result["sr_Bandwidth"] *= \
            int(test_case_result['Client_num'] * ratio)


class FioRBDRandWriteThread(Task):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.task_set = env.args.bench_taskset
        self.rw = "randwrite"
        self.io_depth = env.thread_num
        self.io_engine = "rbd"
        self.num_job = 1
        self.pool = env.pool
        self.run_time = env.args.time
        self.bs = env.args.block_size
        self.images = env.images
        self.lat = 'rw_Latency'
        self.bw = 'rw_Bandwidth'
        self.iops = 'rw_IOPS'

    def get_a_image(self):
        return self.images.pop(0)  # atomic

    def create_command(self):
        return "sudo taskset -c " + self.task_set \
            + " fio" \
            + " -ioengine=" + self.io_engine \
            + " -pool=" + str(self.pool) \
            + " -rbdname=" + self.get_a_image() \
            + " -direct=1" \
            + " -iodepth=" + str(self.io_depth) \
            + " -rw=" + self.rw \
            + " -bs=" + self.bs \
            + " -numjobs=" + str(self.num_job) \
            + " -runtime=" + self.run_time \
            + " -group_reporting" \
            + " -name=fio"

    def analyse(self):
        result_dic = {}
        line = self.result.readline()
        while line:
            temp_lis = line.split()
            if temp_lis:
                if temp_lis[0] == "lat":
                    match = re.search(r'avg=.*?,', line)
                    if match:
                        match_res = match.group()
                        result_dic[self.lat] = float(match_res[4:-1])/1000  # ms
                if temp_lis[0] == "bw":
                    match_res = re.search(r'avg=.*?,', line).group()
                    result_dic[self.bw] = float(match_res[4:-1])*1024/1000000  # MB/s
                if temp_lis[0] == "iops":
                    match_res = re.search(r'avg=.*?,', line).group()
                    result_dic[self.iops] = float(match_res[4:-1])
            line = self.result.readline()
        self.result.close()
        return result_dic

    @staticmethod
    def pre_process(env):
        env.create_images()

    @staticmethod
    def post_process(env, test_case_result):
        # clear the images record in class env
        env.remove_images()
        # merge all clients bw and iops results
        ratio = env.testclient_threadclass_ratio_map[FioRBDRandWriteThread]
        test_case_result["rw_Bandwidth"] *= \
            int(test_case_result['Client_num'] * ratio)
        test_case_result["rw_IOPS"] *= \
            int(test_case_result['Client_num'] * ratio)


class FioRBDRandReadThread(FioRBDRandWriteThread):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.rw = "randread"
        self.lat = 'rr_Latency'
        self.bw = 'rr_Bandwidth'
        self.iops = 'rr_IOPS'

    @staticmethod
    def pre_process(env):
        env.create_images()
        env.fio_pre_write('randwrite', env.args.warmup_block_size, env.args.warmup_time)

    @staticmethod
    def post_process(env, test_case_result):
        env.remove_images()
        ratio = env.testclient_threadclass_ratio_map[FioRBDRandReadThread]
        test_case_result["rr_Bandwidth"] *= \
            int(test_case_result['Client_num'] * ratio)
        test_case_result["rr_IOPS"] *= \
            int(test_case_result['Client_num'] * ratio)


class FioRBDSeqReadThread(FioRBDRandWriteThread):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.rw = "read"
        self.lat = 'sr_Latency'
        self.bw = 'sr_Bandwidth'
        self.iops = 'sr_IOPS'

    @staticmethod
    def pre_process(env):
        env.create_images()
        env.fio_pre_write('write', env.args.warmup_block_size, env.args.warmup_time)

    @staticmethod
    def post_process(env, test_case_result):
        env.remove_images()
        ratio = env.testclient_threadclass_ratio_map[FioRBDSeqReadThread]
        test_case_result["sr_Bandwidth"] *= \
            int(test_case_result['Client_num'] * ratio)
        test_case_result["sr_IOPS"] *= \
            int(test_case_result['Client_num'] * ratio)


class FioRBDSeqWriteThread(FioRBDRandWriteThread):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.rw = "write"
        self.lat = 'sw_Latency'
        self.bw = 'sw_Bandwidth'
        self.iops = 'sw_IOPS'
        self.bs = env.args.block_size

    @staticmethod
    def pre_process(env):
        env.create_images()
        env.rados_pre_write(env.args.warmup_block_size, \
                env.args.warmup_thread_num, env.args.warmup_time)

    @staticmethod
    def post_process(env, test_case_result):
        env.remove_images()
        ratio = env.testclient_threadclass_ratio_map[FioRBDSeqWriteThread]
        test_case_result["sw_Bandwidth"] *= \
            int(test_case_result['Client_num'] * ratio)
        test_case_result["sw_IOPS"] *= \
            int(test_case_result['Client_num'] * ratio)


class ReactorUtilizationCollectorThread(Task):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.start_time = int(env.args.time)/2
        self.osd = "osd.0"
        self.task_set = env.args.bench_taskset

    def create_command(self):
        command = "sudo taskset -c " + self.task_set \
            + " bin/ceph tell " \
            + self.osd + " dump_metrics reactor_utilization"
        return command

    def analyse(self):
        result_dic = {}  # reactor_utilization
        line = self.result.readline()
        while line:
            temp_lis = line.split()
            if temp_lis[0] == "\"value\":":
                result_dic['Reactor_Utilization'] = round(float(temp_lis[1]), 2)
                break
            line = self.result.readline()
        self.result.close()
        return result_dic


class PerfThread(Task):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.start_time = int(env.args.time)/2
        self.last_time = 5000  # 5s
        self.pid_list = env.pid
        self.task_set = env.args.bench_taskset

    def create_command(self):
        command = "sudo taskset -c " + self.task_set \
            + " perf stat --timeout " + str(self.last_time)
        command += " -e cpu-clock,context-switches,cpu-migrations," \
            + "cpu-migrations,cycles,instructions" \
            + ",branches,branch-misses,cache-misses,cache-references"
        if self.pid_list:
            command += " -p "
            command += str(self.pid_list[0])
            for pid_index in range(1, len(self.pid_list)):
                command += ","
                command += str(self.pid_list[pid_index])
        command += " 2>&1"
        return command

    def analyse(self):
        result_dic = {}
        line = self.result.readline()
        cpu_time = 1
        while line:
            temp_lis = line.split()
            if len(temp_lis) > 0:
                if temp_lis[1] == "msec":
                    cpu_time = float(temp_lis[0].replace(",", ""))
                    result_dic['CPU-Utilization'] = round(float(temp_lis[4])*100, 2)
                if temp_lis[1] == "context-switches":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['Context-Switches(K/s)'] \
                            = round(float(float(value)/cpu_time), 3)
                if temp_lis[1] == "cpu-migrations":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['CPU-Migrations(/s)'] \
                            = round(float(float(value)*1000/cpu_time), 3)
                if temp_lis[1] == "page-faults":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['Page-Faults(K/s)'] \
                            = round(float(float(value)/cpu_time), 3)
                if temp_lis[1] == "cycles":
                    result_dic['CPU_Cycle(GHz)'] = temp_lis[3]
                if temp_lis[1] == "instructions":
                    value = temp_lis[3].replace(",", "")
                    result_dic['Instruction_per_Cycle'] = value
                if temp_lis[1] == "branches":
                    index_name = "Branches(" + temp_lis[4] + ")"
                    value = temp_lis[3].replace(",", "")
                    result_dic[index_name] = value
                if temp_lis[1] == "branch-misses":
                    result_dic['Branch-Misses'] = temp_lis[3]
                if temp_lis[1] == "cache-misses":
                    result_dic['Cache-Misses(%)'] = temp_lis[3]
            line = self.result.readline()
        self.result.close()
        return result_dic

class PerfRecordThread(Task):
    def __init__(self, env, id):
        super().__init__(env, id)
        # perf record from 1/4 time to 1/2 time
        self.start_time = round(int(env.args.time) * 0.25)
        self.last_time = round(int(env.args.time) * 0.5)
        self.pid_list = env.pid
        self.task_set = env.args.bench_taskset

    def create_command(self):
        command = "sudo taskset -c " + self.task_set \
            + " perf record -a -g"
        if self.pid_list:
            command += " -p "
            command += str(self.pid_list[0])
            for pid_index in range(1, len(self.pid_list)):
                command += ","
                command += str(self.pid_list[pid_index])
        command += " -o perf.data"
        command += " -- sleep "
        command += str(self.last_time)
        command += " 2>&1"
        return command

    def analyse(self):
        result_dic = {}
        return result_dic

    @staticmethod
    def post_process(self, test_case_result):
        print("perf.data generated at current directory.")
        # generate fire flame if there are stackcollapse-perf.pl
        # and flamegraph.pl in the current directory
        # these tools are in https://github.com/brendangregg/FlameGraph
        stackcollapse_perf = False
        flamegraph = False
        file_list = os.listdir('.')
        for file in file_list:
            if file ==  "stackcollapse-perf.pl":
                stackcollapse_perf = True
            if file == "flamegraph.pl":
                flamegraph = True
        if stackcollapse_perf and flamegraph:
            time.sleep(5)
            os.system("sudo perf script -i perf.data | ./stackcollapse-perf.pl \
                --all | ./flamegraph.pl > flamegraph.svg")
            print("flamegraph generated at current directory.")
        else:
            print("cannot find flamegraph scripts, will not generate flamegraph.")
        return

class IOStatThread(Task):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.start_time = 0
        self.dev = "sda"  # default if no args.dev
        if env.args.dev:
            self.dev = env.get_disk_name()
        self.task_set = env.args.bench_taskset

    def create_command(self):
        command = "sudo taskset -c " + self.task_set \
            + " iostat -x -k -d -y " + env.args.time + " 1"
        return command

    def analyse(self):
        result_dic = {}
        line = self.result.readline()
        while line:
            temp_lis = line.split()
            if temp_lis and temp_lis[0] == self.dev:
                result_dic['Device_IPS'] = float(temp_lis[1])
                result_dic['Device_OPS'] = float(temp_lis[7]) 
                result_dic['Device_Read(MB/s)'] \
                        = round(float(temp_lis[2])/1000, 3)  # MB per second
                result_dic['Device_Write(MB/s)'] \
                        = round(float(temp_lis[8])/1000, 3)  # MB per second
                result_dic['Device_aqu-sz'] = float(temp_lis[19]) 
                # The average queue length of the requests
                result_dic['Device_Rawait(ms)'] = float(temp_lis[5]) # ms
                result_dic['Device_Wawait(ms)'] = float(temp_lis[11]) # ms
                break
            line = self.result.readline()
        self.result.close()
        return result_dic


class CPUFreqThread(Task):
    def __init__(self, env, id):
        super().__init__(env, id)
        self.start_time = int(env.args.time)/2 + 1

    def create_command(self):
        command = "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq"
        return command

    def analyse(self):
        result_dic = {}
        line = self.result.readline()
        result_dic['CPU_Freq(Ghz)'] = round(float(int(line)/1000000), 3)
        self.result.close()
        return result_dic


class Tester():
    def __init__(self, env, tester_id):
        self.env = env
        self.client_num = env.client_num
        self.thread_num = env.thread_num
        self.trmap = env.testclient_threadclass_ratio_map
        self.tplist = env.timepoint_threadclass_list
        self.base_result = env.base_result
        self.ratio_client_num = 0
        self.test_case_threads = list()
        self.tester_id = tester_id
        self.init()

    def init(self):
        thread_id = 0
        for thread in self.trmap:
            sub_ratio_client_num = int(self.trmap[thread] * self.client_num)
            self.ratio_client_num += sub_ratio_client_num
            for n in range(sub_ratio_client_num):
                task_id = (self.tester_id, thread_id)
                self.test_case_threads.append(thread(self.env, task_id))
                thread_id += 1

        for thread in self.tplist:
            task_id = (self.tester_id, thread_id)
            self.test_case_threads.append(thread(self.env, task_id))
            thread_id += 1

    def run(self):
        print("client num:%d, thread num:%d testing"
              % (self.client_num, self.thread_num))
        for thread in self.test_case_threads:
            thread.start()
        for thread in self.test_case_threads:
            thread.join()
        test_case_result = dict()
        key_count = dict()
        for thread_index in range(self.ratio_client_num):
            base_test_case_result = self.test_case_threads[thread_index].analyse()
            for key in base_test_case_result:
                if key not in test_case_result:
                    test_case_result[key] = base_test_case_result[key]
                    key_count[key] = 1
                else:
                    test_case_result[key] += base_test_case_result[key]
                    key_count[key] += 1
        for key in test_case_result:
            test_case_result[key] /= key_count[key]
        for thread_index in \
                range(self.ratio_client_num, len(self.test_case_threads)):
            timepoint_thread_result = \
                self.test_case_threads[thread_index].analyse()
            for key in timepoint_thread_result:
                if key not in test_case_result:
                    test_case_result[key] = timepoint_thread_result[key]
        test_case_result['Thread_num'] = self.thread_num
        test_case_result['Client_num'] = self.client_num
        return test_case_result


class TesterExecutor():
    def __init__(self):
        self.result_list = list()  # [dict]

    def run(self, env):
        print('running...')
        tester_id = 0
        for client_num in env.args.client_list:
            for thread_num in env.args.thread_list:
                env.client_num = client_num
                env.thread_num = thread_num
                env.before_run_case(tester_id)
                tester = Tester(env, tester_id)
                temp_result = tester.run()
                test_case_result = env.base_result.copy()
                test_case_result.update(temp_result)
                env.after_run_case(test_case_result, tester_id)
                self.result_list.append(test_case_result)
                tester_id += 1

    def get_result_list(self):
        return self.result_list

    def output(self, output, horizontal, filters):
        output = output + ".csv"
        f_result = open(output,"w")
        if horizontal:
            for key in self.result_list[0]:
                if key not in filters:
                    print('%25s '%(key), end ='')
            print()
            for result in self.result_list:
                for key in result:
                    if key not in filters:
                        print('%25s '%(str(result[key])), end = '')
                print()
        else:
            for key in self.result_list[0]:
                if key not in filters:
                    print('%25s '%(key), end ='')
                    for result in self.result_list:
                        print('%14.13s'%(str(result[key])), end ='')
                    print()
                    
        keylist = list(self.result_list[0].keys())
        keylen = len(keylist)
        for i in range(keylen):
            if i > 0:
                f_result.write(",")
            f_result.write(keylist[i])
        for result in self.result_list:
            f_result.write('\n')
            for i in range(keylen):
                if keylist[i] not in filters:
                    if i > 0:
                        f_result.write(",")
                    f_result.write(str(result[keylist[i]])) 
        f_result.close()


class Environment():
    def __init__(self, args):
        self.args = args
        self.testclient_threadclass_ratio_map = {}
        self.timepoint_threadclass_list = []
        self.base_result = dict()
        self.pid = list()
        self.tid = list()
        self.pool = "_benchtest_"
        self.images = []
        self.thread_num = -1
        self.client_num = -1
        self.test_num = -1
        self.base_result['Block_size'] = args.block_size
        self.base_result['Time'] = args.time
        self.base_result['Tool'] = ""
        self.base_result['Version'] = None
        self.base_result['OPtype'] = "Mixed"
        self.backend_list = ['seastore', 'bluestore', 'memstore', 'cyanstore']
        self.store = ""

    def init_thread_list(self):
        # 1. add the test case based thread classes and the ratio to the dict.
        if self.args.rand_write:
            self.testclient_threadclass_ratio_map[RadosRandWriteThread] = \
                self.args.rand_write
        if self.args.rand_read:
            self.testclient_threadclass_ratio_map[RadosRandReadThread] = \
                self.args.rand_read
        if self.args.seq_write:
            self.testclient_threadclass_ratio_map[RadosSeqWriteThread] = \
                self.args.seq_write
        if self.args.seq_read:
            self.testclient_threadclass_ratio_map[RadosSeqReadThread] = \
                self.args.seq_read

        if self.testclient_threadclass_ratio_map:
            self.base_result['Tool'] = "Rados Bench"

        if self.args.fio_rbd_rand_write:
            self.testclient_threadclass_ratio_map[FioRBDRandWriteThread] = \
                self.args.fio_rbd_rand_write
        if self.args.fio_rbd_rand_read:
            self.testclient_threadclass_ratio_map[FioRBDRandReadThread] = \
                self.args.fio_rbd_rand_read
        if self.args.fio_rbd_seq_write:
            self.testclient_threadclass_ratio_map[FioRBDSeqWriteThread] = \
                self.args.fio_rbd_seq_write
        if self.args.fio_rbd_seq_read:
            self.testclient_threadclass_ratio_map[FioRBDSeqReadThread] = \
                self.args.fio_rbd_seq_read

        if not self.testclient_threadclass_ratio_map:
            raise Exception("Please set at least one base test.")
        elif not self.base_result['Tool']:
            self.base_result['Tool'] = "Fio RBD"
        
        if len(self.testclient_threadclass_ratio_map) == 1:
            self.test_num = 1
            Mkeys = list(self.testclient_threadclass_ratio_map.keys())
            test_name = str(Mkeys[0])
            if "RandWrite" in test_name:
                self.base_result['OPtype'] = "Rand Write"
            elif "RandRead" in test_name:
                self.base_result['OPtype'] = "Rand Read"
            elif "SeqWrite" in test_name:
                self.base_result['OPtype'] = "Seq Write"
            elif "SeqRead" in test_name:
                self.base_result['OPtype'] = "Seq Read"        

        # 2. add the time point based case thread classes to the list.
        if self.args.reactor_utilization:
            self.timepoint_threadclass_list.append(
                ReactorUtilizationCollectorThread)
        if self.args.perf:
            self.timepoint_threadclass_list.append(PerfThread)
        if self.args.perf_record:
            self.timepoint_threadclass_list.append(PerfRecordThread)
        if self.args.iostat:
            self.timepoint_threadclass_list.append(IOStatThread)
        if self.args.freq:
            self.timepoint_threadclass_list.append(CPUFreqThread)

    def general_pre_processing(self):
        os.system("sudo killall -9 -w ceph-mon ceph-mgr ceph-osd \
                crimson-osd rados node")
        os.system("sudo rm -rf ./dev/* ./out/*")

        # get ceph version
        version = self.get_version()
        if version:
            self.base_result['Version'] = version
        else:
            raise Exception("Can not read git log from ..")

        # vstart. change the command here if you want to set other start params
        command = "sudo OSD=" + str(self.args.osd)
        command += " MGR=1 MON=1 MDS=0 RGW=0 ../src/vstart.sh -n -x \
                --without-dashboard "
        if self.args.crimson:
            command += "--crimson "
            self.base_result['OSD'] = "Crimson"
        else:
            self.base_result['OSD'] = "Classic"

        backend = self.args.store
        if backend in self.backend_list:
            command += " --" + backend
        else:
            raise Exception("Please input the correct backend.")
        self.base_result['Store'] = backend.capitalize()

        if self.args.dev:
            if backend == "seastore":
                command += " --seastore-devs " + self.args.dev
            elif backend == "bluestore":
                command += " --bluestore-devs " + self.args.dev
            else:
                raise Exception("Store and dev don't match.")

        command += " --nodaemon --redirect-output --nolockdep"

        # config bluestore op num
        if self.args.smp:
            if self.args.crimson and backend == "bluestore":
                command += " -o 'crimson_alien_op_num_threads = " + \
                        str(self.args.smp) + "'"
            if not self.args.crimson:
                if self.args.smp <= 8:
                    command += " -o 'osd_op_num_shards = 8'"
                else:
                    command += " -o 'osd_op_num_shards = " + \
                        str(self.args.smp) + "'"

        # config multicore for crimson
        if self.args.smp != 0 and self.args.crimson:
            command += " --crimson-smp " + str(self.args.smp)

        # start ceph
        os.system(command)

        # find osd pids
        while not self.pid:
            time.sleep(1)
            p_pid = os.popen("pidof crimson-osd ceph-osd")
            line = p_pid.readline().split()
            for item in line:
                self.pid.append(int(item))
        # find all osd tids
        for p in self.pid:
            p_tid = os.popen("ls /proc/"+str(p)+"/task")
            line = p_tid.readline()
            while line:
                element = line.split()
                for t in element:
                    self.tid.append(int(t))
                line = p_tid.readline()

        # config multicore for classic
        # all classic osds will use cpu range 0-(smp*osd-1)
        if self.args.smp != 0 and not self.args.crimson:
            core = self.args.smp * self.args.osd
            for p in self.pid:
                os.system("sudo taskset -pc 0-" + str(core-1) + " " + str(p))
            for t in self.tid:
                os.system("sudo taskset -pc 0-" + str(core-1) + " " + str(t))

        # pool
        os.system("sudo bin/ceph osd pool create " + self.pool + " 64 64")

        # waiting for rados completely ready
        time.sleep(20)

    def general_post_processing(self):
        # killall
        os.system("sudo killall -9 -w ceph-mon ceph-mgr ceph-osd \
                crimson-osd rados node")
        # delete dev
        os.system("sudo rm -rf ./dev/* ./out/*")
        self.pid = list()
        self.tid = list()

    def pre_processing(self, tester_id):
        print('pre processing...')
        # prepare test group directory
        if self.args.log:
            os.makedirs(self.args.log+"/"+str(tester_id))

        for thread in self.testclient_threadclass_ratio_map:
            thread.pre_process(self)
        for thread in self.timepoint_threadclass_list:
            thread.pre_process(self)

    def post_processing(self, test_case_result, tester_id):
        print('post processing...')
        for thread in self.testclient_threadclass_ratio_map:
            thread.post_process(self, test_case_result)
        for thread in self.timepoint_threadclass_list:
            thread.post_process(self, test_case_result)

        if self.test_num == 1:   # Rename these columns if only one test type
            keys_list = list(test_case_result.keys())
            for key in keys_list:
                if "Bandwidth" in key:
                    test_case_result["Bandwidth(MB/s)"] = \
                        test_case_result.pop(key)
                elif "Latency" in key:
                    test_case_result["Latency(ms)"] = \
                        test_case_result.pop(key)
                elif "IOPS" in key:
                    test_case_result["IOPS"] = \
                        test_case_result.pop(key)

        # move osd log to log path before remove them
        if self.args.log:
            tester_log_path = self.args.log + "/" + str(tester_id)
            os.system("sudo mv out/osd.* " + tester_log_path + "/")

    def before_run_case(self, tester_id):
        self.general_pre_processing()
        self.pre_processing(tester_id)

    def after_run_case(self, test_case_result, tester_id):
        self.post_processing(test_case_result, tester_id)
        self.general_post_processing()

    def get_disk_name(self):
        par = self.args.dev.split('/')[-1]
        lsblk = os.popen("lsblk")
        last = None
        line = lsblk.readline()
        while line:
            ll = line.split()
            if ll[0][0:2] == '├─' or ll[0][0:2] == '└─':
                if ll[0][2:] == par:
                    return last
            else:
                last = ll[0]
            line = lsblk.readline()
        return par

    def get_version(self):
        month_dic={
            "Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04",
            "May":"05", "Jun":"06", "Jul":"07", "Aug":"08",
            "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12",
        }
        gitlog = os.popen("git log ..")
        line = gitlog.readline()
        version = None
        while line:
            ll = line.split()
            if ll[0] == "Date:":
                version = ll[5] + month_dic[ll[2]] + ll[3]
                break
            line = gitlog.readline()
        return version

    def create_images(self):
        image_name_prefix = "images_"
        # must be client_num here.
        for i in range(self.client_num):
            image_name = image_name_prefix + str(i)
            print(image_name)
            command = "sudo bin/rbd create " + image_name \
                + " --size 20G --image-format=2 \
                    --rbd_default_features=3 --pool " + self.pool
            command += " 2>/dev/null"
            os.system(command)
            self.images.append(image_name)
        print('images create OK.')

    def remove_images(self):
        self.images = []

    def fio_pre_write(self, rw, bs, time):
        pool = self.pool
        thread_num = self.thread_num
        class ImageWriteThread(threading.Thread):
            def __init__(self, image):
                super().__init__()
                self.command = "sudo fio" \
                    + " -ioengine=" + "rbd" \
                    + " -pool=" + pool \
                    + " -rbdname=" + image \
                    + " -direct=1" \
                    + " -iodepth=" + str(thread_num) \
                    + " -rw=" + rw \
                    + " -bs=" + str(bs) \
                    + " -numjobs=1" \
                    + " -runtime=" + str(time) \
                    + " -group_reporting -name=fio"
            def run(self):
                os.system(self.command + " >/dev/null")
        thread_list = []
        for image in self.images:
            thread_list.append(ImageWriteThread(image))
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()
        print('fio pre write OK.')

    def rados_pre_write(self, block_size, thread_num, time):
        env_write_command = "sudo bin/rados bench -p " + self.pool + " " \
            + str(time) + " write -t " \
            + str(thread_num) + " -b " + str(block_size) + " " \
            + "--no-cleanup"
        os.system(env_write_command + " >/dev/null")
        print('rados pre write OK.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--client-list',
                        nargs='+',
                        type=int,
                        required=True,
                        help='clients list')
    parser.add_argument('--thread-list',
                        nargs='+',
                        type=int,
                        required=True,
                        help='threads list')
    parser.add_argument('--bench-taskset',
                        type=str,
                        default="1-32",
                        help='which processors will bench thread execute on')
    parser.add_argument('--block-size',
                        type=str,
                        required=True,
                        help='data block size')
    parser.add_argument('--warmup-block-size',
                        type=str,
                        default="4K",
                        help='warmup data block size, default 4KB')
    parser.add_argument('--time',
                        type=str,
                        default="10",
                        help='test time for every test case')
    parser.add_argument('--warmup-time',
                        type=str,
                        default="10",
                        help='warmup time for every test case, default 10s')
    parser.add_argument('--warmup-thread-num',
                        type=str,
                        default="64",
                        help='warmup thread num for every test case, default 64')
    parser.add_argument('--dev',
                        type=str,
                        help='test device path, default is the vstart default \
                    settings, creating a virtual block device on current device')
    parser.add_argument('--output',
                        type=str,
                        default="result",
                        help='path of all output result after integrating')
    parser.add_argument('--output-horizontal',
                        action='store_true',
                        help='all results of one test case will be in one line')
    parser.add_argument('--crimson',
                        action='store_true',
                        help='use crimson-osd instead')
    parser.add_argument('--store',
                        type=str,
                        default='bluestore',
                        help='choose from seastore, cyanstore,\
                    memstore or bluestore')
    parser.add_argument('--osd',
                        type=int,
                        default = 1,
                        help='how many osds')
    parser.add_argument('--smp',
                        type=int,
                        default = 0,
                        help='core per osd')
    parser.add_argument('--log',
                        type=str,
                        default = None,
                        help='directory to store logs, no log by default. Will \
                    store all tasks results and osd log and osd stdout')

    # test case based thread param
    parser.add_argument('--rand-write',
                        type=float,
                        default=0,
                        help='ratio of rados bench rand write clients')
    parser.add_argument('--rand-read',
                        type=float,
                        default=0,
                        help='ratio of rados bench rand read clients')
    parser.add_argument('--seq-write',
                        type=float,
                        default=0,
                        help='ratio of rados bench seq write clients')
    parser.add_argument('--seq-read',
                        type=float,
                        default=0,
                        help='ratio of rados bench seq read clients')
    parser.add_argument('--fio-rbd-rand-write',
                        type=float,
                        default=0,
                        help='ratio of fio rand write clients')
    parser.add_argument('--fio-rbd-rand-read',
                        type=float,
                        default=0,
                        help='ratio of fio rand read clients')
    parser.add_argument('--fio-rbd-seq-write',
                        type=float,
                        default=0,
                        help='ratio of fio seq write clients')
    parser.add_argument('--fio-rbd-seq-read',
                        type=float,
                        default=0,
                        help='ratio of fio seq read clients')

    # time point based thread param
    parser.add_argument('--reactor-utilization',
                        action='store_true',
                        help='collect the reactor utilization')
    parser.add_argument('--perf',
                        action='store_true',
                        help='collect perf information')
    parser.add_argument('--perf-record',
                        action='store_true',
                        help='collect perf record information')
    parser.add_argument('--iostat',
                        action='store_true',
                        help='collect iostat information')
    parser.add_argument('--freq',
                        action='store_true',
                        help='collect cpu frequency information')
    args = parser.parse_args()

    # which item should not be showed in the output
    filters = []

    # prepare log directory
    if args.log:
        e = os.listdir(".")
        if args.log in e:
            shutil.rmtree(args.log)
        os.makedirs(args.log)

    env = Environment(args)

    # change this method to add new thread class
    env.init_thread_list()

    # execute the tester in the tester matrix
    tester_executor = TesterExecutor()
    tester_executor.run(env)
    tester_executor.output(args.output, args.output_horizontal, filters)
    print('done.')
