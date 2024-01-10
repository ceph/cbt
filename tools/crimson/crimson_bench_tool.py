#!/usr/bin/env python3
import argparse
import os
import threading
import time
import re
from subprocess import Popen, PIPE

# Divid all test threads into three categories, Test Case Based Thread, Time
# Point Based Thread and Time Continuous thread.
# The test threads that only care about what is the system going on at a time
# point, such as reactor utilization are classified as Time Point Based Thread
# and the basic test case threads (such as rados bench) that are only the
# single small test case are classified as Test Case Based Thread. Meanwhile,
# tasks such as perf which can last for a period are classified as Time
# Continuous thread.
# For developer, you can write the test class you want by implementing
# the Task interfaces and adding it to to testclient_threadclass_ratio_map
# ,timepoint_threadclass_num_map or timecontinuous_threadclass_list
# in the class Environmen to extend this tool.
# set the start_time to decide when will the test start after thread starts.
class Task(threading.Thread):
    def __init__(self, env, id, start_time=0):
        super().__init__()
        self.env = env
        self.thread_num = env.thread_num
        self.start_time = start_time
        self.result = None
        self.log = env.log
        self.id = id #(tester_id, thread_id)
        self.task_log_path = f"{env.tester_log_path}/" \
            f"{self.id[1]}.{type(self).__name__}.{self.start_time}"

    # rewrite method create_command() to define the command
    # this class will execute
    def create_command(self):
        raise NotImplementedError

    # don't need to rewite this method
    def run(self):
        time.sleep(self.start_time)
        command = self.create_command()
        print(command)

        proc = Popen("exec " + command, shell=True, \
                     stdout=PIPE, encoding="utf-8")
        done = proc.poll()
        fail = self.env.check_failure()
        while done is None and not fail:
            time.sleep(1)
            done = proc.poll()
            fail = self.env.check_failure()
        if done is not None:
            ret = proc.stdout.read()
            with open(self.task_log_path, "w") as f:
                f.write(ret)
            f.close()
            self.result = open(self.task_log_path, "r")
        if fail:
            proc.kill()

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
        super().__init__(env, id, start_time=0.01)
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
        env.rados_pre_write()


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
        env.rados_pre_write()


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
        env.rados_pre_write()


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
        env.fio_pre_write()

    @staticmethod
    def post_process(env, test_case_result):
        env.remove_images()


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
        env.fio_pre_write()

    @staticmethod
    def post_process(env, test_case_result):
        env.remove_images()


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
        env.rados_pre_write()

    @staticmethod
    def post_process(env, test_case_result):
        env.remove_images()


class ReactorUtilizationCollectorThread(Task):
    def __init__(self, env, id, start_time):
        super().__init__(env, id, start_time)
        self.osd = "osd.0"
        self.task_set = env.args.bench_taskset
        if env.args.osd != 1:
            raise Exception("ru only support single osd for now.")

    def create_command(self):
        command = "sudo taskset -c " + self.task_set \
            + " bin/ceph tell " \
            + self.osd + " dump_metrics reactor_utilization"
        return command

    def analyse(self):
        result_dic = {}  # reactor_utilization
        line = self.result.readline()
        shard = 0
        while line:
            temp_lis = line.split()
            if temp_lis[0] == "\"value\":":
                result_dic['Reactor_Utilization_' + str(shard)] = \
                    round(float(temp_lis[1]), 2)
                shard += 1
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
        command += " -o "+ self.task_log_path +".perf.data"
        command += " -- sleep "
        command += str(self.last_time)
        command += " 2>&1"

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
            command += " && sudo perf script -i " + self.task_log_path \
                + ".perf.data | ./stackcollapse-perf.pl --all | ./flamegraph.pl > " \
                + self.task_log_path + ".flamegraph.svg"
        else:
            print("cannot find flamegraph scripts, will not generate flamegraph.")
        return command

    def analyse(self):
        result_dic = {}
        return result_dic

class IOStatThread(Task):
    def __init__(self, env, id, start_time):
        super().__init__(env, id, start_time)
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
    def __init__(self, env, id, start_time):
        super().__init__(env, id, start_time)

    def create_command(self):
        command = "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq"
        return command

    def analyse(self):
        result_dic = {}
        line = self.result.readline()
        result_dic['CPU_Freq(Ghz)'] = round(float(int(line)/1000000), 3)
        self.result.close()
        return result_dic

class TestFailError(Exception):
    def __init__(self, message, env):
        env.set_failure_signal()
        self.message = message
        with os.popen("date +%H:%M:%S") as date:
            line = date.readline()
            time = line.split()[0]
        error_msg = f"time:{time} test:{env.test_num} client:{env.client_num} "\
            f"thread:{env.thread_num} smp:{env.smp_num} {message}"
        print(error_msg)
        os.system(f"echo \"{error_msg}\" >> {env.failure_log}")

    def __str__(self):
        return self.message

class FailureDetect(threading.Thread):
    def __init__(self, env):
        super().__init__()
        self.time = int(env.args.time)
        self.task_set = env.args.bench_taskset
        self.client_num = env.client_num
        self.env = env

        self.track_client = "bin/rados fio"
        self.track_osd = "crimson-osd ceph-osd"

        # if client still alive after waiting for time + tolerance_time,
        # this detector will consider this tester failed.
        self.tolerance_time = env.args.tolerance_time

    def run(self):
        time.sleep(1)
        wait_count = 1
        p_pids = os.popen(f"sudo taskset -c {self.task_set} "\
                          f"pidof {self.track_client}")
        res = p_pids.readline().split()
        while(len(res) != self.client_num):
            time.sleep(1)
            wait_count += 1
            if wait_count > self.time + self.tolerance_time:
                raise TestFailError("Tester failed: clients startup failed.", self.env)
            p_pids = os.popen(f"sudo taskset -c {self.task_set} "\
                              f"pidof {self.track_client}")
            res = p_pids.readline().split()

        wait_time = 0
        while(wait_time < self.time + self.tolerance_time):
            # check osd
            p_osd_pids_af = os.popen(f"sudo taskset -c {self.task_set} "\
                             f"pidof {self.track_osd}")
            res = p_osd_pids_af.readline().split()
            if (not len(res)):
                raise TestFailError("Tester failed: osd crashed unexpectedly.", self.env)

            wait_time += 1
            time.sleep(1)

        # check clients
        p_pids_af = os.popen(f"sudo taskset -c {self.task_set} "\
                             f"pidof {self.track_client}")
        res = p_pids_af.readline().split()
        if (len(res)):
            raise TestFailError("Tester failed: client didn't close.", self.env)


class Tester():
    def __init__(self, env, tester_id):
        self.env = env
        self.client_num = env.client_num
        self.thread_num = env.thread_num
        self.trmap = env.testclient_threadclass_ratio_map
        self.tpnmap = env.timepoint_threadclass_num_map
        self.tpclist = env.timecontinuous_threadclass_list
        self.test_case_tasks = list()
        self.timepoint_tasks = list()
        self.timecontinuous_tasks = list()
        self.tester_id = tester_id
        self.init()

    def init(self):
        thread_id = 0
        for thread in self.trmap:
            sub_ratio_client_num = int(self.trmap[thread] * self.client_num)
            for n in range(sub_ratio_client_num):
                task_id = (self.tester_id, thread_id)
                self.test_case_tasks.append(thread(self.env, task_id))
                thread_id += 1

        for thread in self.tpnmap:
            tp_num = self.tpnmap[thread]
            gap = int(self.env.args.time) / (tp_num + 1)
            start_time = gap
            for n in range(tp_num):
                task_id = (self.tester_id, thread_id)
                self.timepoint_tasks.append(thread(self.env, task_id, start_time))
                start_time += gap
                thread_id += 1

        for thread in self.tpclist:
            task_id = (self.tester_id, thread_id)
            self.timecontinuous_tasks.append(thread(self.env, task_id))
            thread_id += 1

        self.detector = FailureDetect(self.env)

    def run(self):
        print("client num:%d, thread num:%d testing"
              % (self.client_num, self.thread_num))
        for thread in self.test_case_tasks:
            thread.start()
        for thread in self.timepoint_tasks:
            thread.start()
        for thread in self.timecontinuous_tasks:
            thread.start()
        self.detector.start()

        for thread in self.test_case_tasks:
            thread.join()
        for thread in self.timepoint_tasks:
            thread.join()
        for thread in self.timecontinuous_tasks:
            thread.join()
        self.detector.join()

        if self.env.check_failure():
            raise TestFailError("Tester Failed", self.env)

        test_case_result = dict()

        # will add all test results(such as IOPS, BW) from every test clients
        for thread in self.test_case_tasks:
            res = thread.analyse()
            for key in res:
                # results such as Latency should be divided by client number
                if "Latency" in key:
                    sub_client_num = self.trmap[type(thread)] * self.client_num
                    if key not in test_case_result:
                        test_case_result[key] = (res[key] / sub_client_num)
                    else:
                        test_case_result[key] += (res[key] / sub_client_num)
                else:
                    if key not in test_case_result:
                        test_case_result[key] = res[key]
                    else:
                        test_case_result[key] += res[key]

        # will calculate the average of all time point tasks.
        key_count = dict()
        for thread in self.timepoint_tasks:
            res = thread.analyse()
            for key in res:
                if key not in test_case_result:
                    test_case_result[key] = res[key]
                    key_count[key] = 1
                else:
                    test_case_result[key] += res[key]
                    key_count[key] += 1
        for key in test_case_result:
            if key in key_count:
                test_case_result[key] /= key_count[key]

        for thread in self.timecontinuous_tasks:
            res = thread.analyse()
            for key in res:
                if key not in test_case_result:
                    test_case_result[key] = res[key]
                else:
                    raise Exception("duplicated key for different \
                        timecontinuous_tasks.")

        test_case_result['Thread_num'] = self.thread_num
        test_case_result['Client_num'] = self.client_num
        return test_case_result

    def rollback(self, retry_count):
        tester_failure_log_path = f"{self.env.failure_osd_log}/"\
            f"{self.tester_id}_{retry_count}/"
        os.makedirs(tester_failure_log_path)
        os.system(f"sudo mv out/osd.* {tester_failure_log_path}")
        os.system(f"sudo rm -rf {self.env.tester_log_path}")

        self.env.general_post_processing()
        self.env.reset_failure_signal()


class TesterExecutor():
    def __init__(self):
        self.result_list = list()  # [dict]

    def run(self, env):
        print('running...')
        tester_count = 0
        for client_index, client_num in enumerate(env.args.client):
            env.client_num = client_num
            for thread_num in env.args.thread:
                env.smp_num = env.smp_list[client_index]
                env.thread_num = thread_num
                tester_id = f"{tester_count}.client{{{env.client_num}}}_thread" \
                    f"{{{env.thread_num}}}_smp{{{env.smp_num}}}"

                retry_count = 0
                test_case_result = dict()
                succeed = False
                tester = None
                while not succeed:
                    if retry_count > env.args.retry_limit:
                        os.system(f"touch {env.log}/__failed__")
                        raise Exception(f"Test Failed: Maximum retry limit exceeded.")
                    try:
                        env.before_run_case(tester_id)
                        tester = Tester(env, tester_id)
                        temp_result = tester.run()
                        test_case_result = env.base_result.copy()
                        test_case_result.update(temp_result)
                    except TestFailError:
                        print("will retry...")
                        retry_count += 1
                        test_case_result = dict()
                        if tester:
                            tester.rollback(retry_count)
                            del tester
                    else:
                        succeed = True
                env.after_run_case(test_case_result)
                self.result_list.append(test_case_result)
                tester_count += 1

    def get_result_list(self):
        return self.result_list

    def output(self, output, horizontal, filters):
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
        self.timepoint_threadclass_num_map = {}
        self.timecontinuous_threadclass_list = []
        self.smp_list = []
        self.base_result = dict()
        self.pid = list()
        self.tid = list() # without alien threads
        self.tid_alien = list()
        self.pool = "_benchtest_"
        self.pool_size = None
        self.images = []
        self.thread_num = -1
        self.client_num = -1
        self.smp_num = -1
        self.test_num = -1
        self.base_result['Block_size'] = args.block_size
        self.base_result['Time'] = args.time
        self.base_result['Core'] = -1
        self.base_result['Tool'] = ""
        self.base_result['Version'] = None
        self.base_result['OPtype'] = "Mixed"
        self.backend_list = ['seastore', 'bluestore', 'memstore', 'cyanstore']
        self.store = ""
        self.log = ""
        self.test_case_id = 0
        self.tester_log_path = ""
        self.failure_log = ""
        self.failure_osd_log = ""
        self.FAILURE_SIGNAL = False

        if len(self.args.smp) == 1:
            for _ in self.args.client:
                self.smp_list.append(self.args.smp[0])
        else:
            self.smp_list = self.args.smp
            if len(self.args.smp) != len(self.args.client):
                raise Exception("smp list should match the client list")

        # decide pool size
        if self.args.pool_size:
            if self.args.osd < self.args.pool_size:
                raise Exception("pool size should <= osd number")
            self.pool_size = self.args.pool_size
        else:
            if self.args.osd < 3:
                self.pool_size = self.args.osd

        # prepare log directory
        if not self.args.log:
            self.log = "log"
            with os.popen("date +%Y%m%d.%H%M%S") as date:
                line = date.readline()
                res = line.split()[0]
                self.log = f"{self.log}.{res}"
        else:
            self.log = self.args.log
        self.add_log_suffix()

        os.makedirs(self.log)
        self.failure_log = f"{self.log}/failure_log.txt"
        os.system(f"touch {self.failure_log}")

        self.failure_osd_log = f"{self.log}/failure_osd_log"
        os.makedirs(self.failure_osd_log)

    def add_log_suffix(self):
        if self.args.crimson:
            self.log += "_crimson"
        else:
            self.log += "_classic"
        self.log += f"_{self.args.store}"
        self.log += f"_osd:{self.args.osd}_ps:{self.pool_size}"

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

        # 2. add the time point based case thread classes to the map.
        if self.args.ru:
            self.timepoint_threadclass_num_map[ReactorUtilizationCollectorThread] = \
                self.args.ru
        if self.args.iostat:
            self.timepoint_threadclass_num_map[IOStatThread] = \
                self.args.iostat
        if self.args.freq:
            self.timepoint_threadclass_num_map[CPUFreqThread] = \
                self.args.freq

        # 3. add the time continuous based case thread classes to the list.
        if self.args.perf:
            self.timecontinuous_threadclass_list.append(PerfThread)
        if self.args.perf_record:
            self.timecontinuous_threadclass_list.append(PerfRecordThread)

    def general_pre_processing(self):
        os.system("sudo killall -9 -w ceph-mon ceph-mgr ceph-osd \
                crimson-osd rados node")
        os.system("sudo rm -rf ./dev/* ./out/*")

        # get ceph version
        version, commitID = self.get_version_and_commitID()
        if version:
            self.base_result['Version'] = version
            self.base_result['CommitID'] = commitID
        else:
            raise Exception("Can not read git log from ..")

        # vstart. change the command here if you want to set other start params
        command = "sudo OSD=" + str(self.args.osd)
        command += " MGR=1 MON=1 MDS=0 RGW=0 ../src/vstart.sh -n -x \
                --without-dashboard --no-restart "
        if self.args.crimson:
            command += "--crimson "
            self.base_result['OSD'] = "Crimson"
            # config multicore for crimson
            if self.args.crimson:
                command += " --crimson-smp " + str(self.smp_num)
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

        # add additional crimson bluestore ceph config
        crimson_alien_thread_cpu_cores = ''
        if self.args.crimson and backend == "bluestore":
            op_num_threads = self.smp_num
            if self.args.crimson_alien_op_num_threads:
                op_num_threads = \
                    self.args.crimson_alien_op_num_threads[self.test_case_id]
            command += f" -o 'crimson_alien_op_num_threads = {op_num_threads}'"
            if self.args.crimson_alien_thread_cpu_cores:
                crimson_alien_thread_cpu_cores = \
                    self.args.crimson_alien_thread_cpu_cores[self.test_case_id]
                alien_core_range = crimson_alien_thread_cpu_cores.split('-')
                if 0 <= int(alien_core_range[0]) <= self.smp_num-1 or \
                    0 <= int(alien_core_range[1]) <= self.smp_num-1:
                    raise Exception("alien threads' cores should not collide "
                                     "with non-alien threads' cores")
                if int(alien_core_range[1]) < int(alien_core_range[0]):
                    raise Exception("wrong value for crimson_alien_thread_cpu_cores")

            else:
                crimson_alien_thread_cpu_cores = f"0-{self.smp_num - 1}"
            command += f" -o 'crimson_alien_thread_cpu_cores = {crimson_alien_thread_cpu_cores}'"
        if not self.args.crimson:
            if self.args.osd_op_num_shards:
                command += f" -o 'osd_op_num_shards = "\
                    f"{self.args.osd_op_num_shards[self.test_case_id]}'"
            else:
                if self.smp_num <= 8:
                    command += " -o 'osd_op_num_shards = 8'"
                else:
                    command += f" -o 'osd_op_num_shards = {self.smp_num}'"
            if self.args.osd_op_num_threads_per_shard:
                command += f" -o 'osd_op_num_threads_per_shard = "\
                    f"{self.args.osd_op_num_threads_per_shard[self.test_case_id]}'"
            if self.args.ms_async_op_threads:
                command += f" -o 'ms_async_op_threads = "\
                    f"{self.args.ms_async_op_threads[self.test_case_id]}'"
        if backend == "seastore":
            command += " -o 'seastore_cache_lru_size = 512M'"
            command += " -o 'seastore_max_concurrent_transactions = 128'"
        if backend == "memstore":
            command += " -o 'memstore_device_bytes = 8G'"

        # start ceph
        ceph_start_max_watting_time = 40
        start_proc = Popen("exec " + command, shell=True, \
                           stdout=PIPE, encoding="utf-8")
        wait_count = 0
        done = start_proc.poll()
        while done is None:
            time.sleep(1)
            done = start_proc.poll()
            wait_count += 1
            if wait_count > ceph_start_max_watting_time:
                start_proc.kill()
                raise TestFailError("Tester failed: osd startup failed.", self)

        # find osd pids
        while not self.pid:
            time.sleep(1)
            p_pid = os.popen("pidof crimson-osd ceph-osd")
            line = p_pid.readline().split()
            for item in line:
                self.pid.append(int(item))
        # find all osd tids and alienstore tids(if there are)
        for p in self.pid:
            _tid = os.listdir(f"/proc/{p}/task")
            while(len(_tid) <= 1):
                time.sleep(1)
                _tid = os.listdir(f"/proc/{p}/task")

            if self.args.crimson and backend == "bluestore":
                for t in _tid:
                    res = os.popen(f"cat /proc/{t}/comm")
                    line = res.readline().split()
                    if line:
                        t_name = line[0]
                        if t_name in ['alien-store-tp', 'bstore_aio'] or 'rocksdb' in t_name:
                            self.tid_alien.append(t)
                            print(f"found alien threads {t_name}, tid {t}")
                        else:
                            self.tid.append(t)
                            print(f"found threads {t_name}, tid {t}")
            else:
                self.tid.extend(_tid)
                print("found threads:(", end="")
                for item in _tid:
                    print(f"{item} ", end="")
                print(f") for process {p}")

        # config multicore for classic
        # all classic osds will use cpu range 0-(smp*osd-1)
        # crimson multicore settings has already been set in vstart smp param.
        if not self.args.crimson:
            core = self.smp_num * self.args.osd
            for p in self.pid:
                os.system("sudo taskset -pc 0-" + str(core-1) + " " + str(p))
            for t in self.tid:
                os.system("sudo taskset -pc 0-" + str(core-1) + " " + str(t))

        # bond all alienstore threads to crimson_alien_thread_cpu_cores limited cores
        if self.args.crimson and backend == "bluestore":
            for t in self.tid_alien:
                os.system(f"sudo taskset -pc {crimson_alien_thread_cpu_cores} {t}")

        self.base_result['Core'] = self.smp_num * self.args.osd

        # pool
        os.system(f"sudo bin/ceph osd pool create {self.pool} "
                    f"{self.args.pg} {self.args.pg}")
        if self.pool_size:
            os.system(f"sudo bin/ceph osd pool set {self.pool}" \
                    f" size {self.pool_size} --yes-i-really-mean-it")
            os.system(f"sudo bin/ceph osd pool set {self.pool}" \
                    f" min_size {self.pool_size} --yes-i-really-mean-it")
        else:
            # use ceph default setting when osd >= 3
            pass

        # waiting for rados completely ready
        time.sleep(20)

    def general_post_processing(self):
        # killall
        os.system("sudo killall -9 -w ceph-mon ceph-mgr ceph-osd \
                crimson-osd rados fio node")
        # delete dev
        os.system("sudo rm -rf ./dev/* ./out/*")
        self.pid = list()
        self.tid = list()

        # group gap
        time.sleep(self.args.gap)

    def pre_processing(self, tester_id):
        print('pre processing...')
        # prepare test group directory
        self.tester_log_path = self.log+"/"+str(tester_id)
        os.makedirs(self.tester_log_path)

        for thread in self.testclient_threadclass_ratio_map:
            thread.pre_process(self)
        for thread in self.timepoint_threadclass_num_map:
            thread.pre_process(self)
        for thread in self.timecontinuous_threadclass_list:
            thread.pre_process(self)

    def post_processing(self, test_case_result):
        print('post processing...')
        for thread in self.testclient_threadclass_ratio_map:
            thread.post_process(self, test_case_result)
        for thread in self.timepoint_threadclass_num_map:
            thread.post_process(self, test_case_result)
        for thread in self.timecontinuous_threadclass_list:
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
        os.system("sudo mv out/osd.* " + self.tester_log_path + "/")

    def before_run_case(self, tester_id):
        print(f"Running the {self.test_case_id} th test case")
        self.general_pre_processing()
        self.pre_processing(tester_id)

    def after_run_case(self, test_case_result):
        self.post_processing(test_case_result)
        self.general_post_processing()
        self.test_case_id += 1

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

    def get_version_and_commitID(self):
        month_dic={
            "Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04",
            "May":"05", "Jun":"06", "Jul":"07", "Aug":"08",
            "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12",
        }
        gitlog = os.popen("git log ..")
        line = gitlog.readline()
        commitID = None
        version = None
        while line:
            ll = line.split()
            if ll[0] == "commit" :
                commitID = ll[1][:8]
            if ll[0] == "Date:":
                version = ll[5] + month_dic[ll[2]] + ll[3]
                break
            line = gitlog.readline()
        return version, commitID

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

    # will fullly prewrite the image with the same block size as read by default.
    def fio_pre_write(self):
        print('fio pre write START.')
        pool = self.pool
        thread_num = self.thread_num
        bs = ""
        if self.args.warmup_block_size:
            bs = self.args.warmup_block_size
        else:
            bs = self.args.block_size
        class ImageWriteThread(threading.Thread):
            def __init__(self, image, args_time):
                super().__init__()
                self.command = "sudo fio" \
                    + " -ioengine=" + "rbd" \
                    + " -pool=" + pool \
                    + " -rbdname=" + image \
                    + " -direct=1" \
                    + " -iodepth=" + str(thread_num) \
                    + " -rw=write" \
                    + " -bs=" + str(bs) \
                    + " -numjobs=1" \
                    + " -group_reporting -name=fio"
                if args_time:
                    self.command += " -runtime=" + str(args_time)
                else:
                    self.command += " -size=100%"
            def run(self):
                print(self.command)
                os.system(self.command + " >/dev/null")
        thread_list = []
        for image in self.images:
            thread_list.append(ImageWriteThread(image, self.args.warmup_time))
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()
        print('fio pre write OK.')

    def rados_pre_write(self):
        print('rados pre write START.')
        block_size = ""
        time = ""
        if self.args.warmup_block_size:
            block_size = self.args.warmup_block_size
        else:
            block_size = self.args.block_size
        if self.args.warmup_time:
            time = self.args.warmup_time
        else:
            time = 5 * int(self.args.time)
        thread_num = self.thread_num
        env_write_command = "sudo bin/rados bench -p " + self.pool + " " \
            + str(time) + " write -t " \
            + str(thread_num) + " -b " + str(block_size) + " " \
            + "--no-cleanup"
        print(env_write_command)
        os.system(env_write_command + " >/dev/null")
        print('rados pre write OK.')

    def set_failure_signal(self):
        self.FAILURE_SIGNAL = True

    def reset_failure_signal(self):
        self.FAILURE_SIGNAL = False

    def check_failure(self):
        return self.FAILURE_SIGNAL


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--client',
                        nargs='+',
                        type=int,
                        required=True,
                        help='clients list')
    parser.add_argument('--thread',
                        nargs='+',
                        type=int,
                        default=[128],
                        help='threads list')
    parser.add_argument('--smp',
                        nargs='+',
                        type=int,
                        default=[1],
                        help='core per osd list, default to be 1, should be one-to-one \
                              correspondence with client list if not default. But if only input \
                              one value, this tool will automatically extend it to match the \
                              client list')
    parser.add_argument('--build',
                        type=str,
                        default='.',
                        help='build directory of ceph. Default to be .')
    parser.add_argument('--bench-taskset',
                        type=str,
                        default="1-32",
                        help='which processors will bench thread execute on')
    parser.add_argument('--block-size',
                        type=str,
                        required=True,
                        help='data block size')
    parser.add_argument('--pg',
                        type=int,
                        default=128,
                        help='pg number for pool, default to be 128')
    parser.add_argument('--pool-size',
                        type=int,
                        default=None,
                        help='pool size. By default, pool_size = osd if osd < 3 \
                        or use ceph default setting')
    parser.add_argument('--time',
                        type=str,
                        default="10",
                        help='test time for every test case')
    parser.add_argument('--warmup-block-size',
                        type=str,
                        default="",
                        help='warmup data block size, default equal to block size')
    parser.add_argument('--warmup-time',
                        type=str,
                        default="",
                        help='warmup time for every test case, default equal to \
                    5 * time in rados read case or or filling the entire rbd image \
                    in fio read case')
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
    parser.add_argument('--log',
                        type=str,
                        default = None,
                        help='directory prefix to store logs, ./log_date by default.\
                    This tool will add _crimson/classic_backend_osd_poolsize to be log \
                    dir name and store all tasks results and osd log and osd stdout.\
                    e.g. By default, log directory might be named log_20231222.165125\
                    _crimson_bluestore_osd:1_ps:1')
    parser.add_argument('--gap',
                        type=int,
                        default = 1,
                        help='time gap between different test cases')

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
    parser.add_argument('--ru',
                        type=int,
                        help='how many time point to collect the \
                            reactor utilization')
    parser.add_argument('--iostat',
                        type=int,
                        help='how many time point to collect iostat information')
    parser.add_argument('--freq',
                        type=int,
                        help='how many time point to collect cpu frequency information')

    # time continuous based thread param
    parser.add_argument('--perf',
                        action='store_true',
                        help='collect perf information')
    parser.add_argument('--perf-record',
                        action='store_true',
                        help='collect perf record information')

    # ceph config param
    parser.add_argument('--crimson-alien-op-num-threads',
                        nargs='+',
                        type=int,
                        default=None,
                        help='set crimson_alien_op_num_threads. \
                            Equal to smp number by default.')
    parser.add_argument('--crimson-alien-thread-cpu-cores',
                        nargs='+',
                        type=str,
                        default=None,
                        help='set crimson_alien_thread_cpu_cores. The input should be ranges. \
                            Such as 0-3 2-6. 0-smp number-1 by default. For Crimson with \
                            AlienStore, alien-store-tp, bstore_aio and rocksdb threads will \
                            also be binded to this CPU range.')
    parser.add_argument('--osd-op-num-shards',
                        nargs='+',
                        type=int,
                        default=None,
                        help='set osd_op_num_shards. \
                            Equal to smp number when > 8, else = 8 by default.')
    parser.add_argument('--osd-op-num-threads-per-shard',
                        nargs='+',
                        type=int,
                        default=None,
                        help='set osd_op_num_threads_per_shard.')
    parser.add_argument('--ms-async-op-threads',
                        nargs='+',
                        type=int,
                        default=None,
                        help='set ms_async_op_threads.')

    parser.add_argument('--retry-limit',
                        type=int,
                        default=5,
                        help='max retry limit for every test client')
    parser.add_argument('--tolerance-time',
                        type=int,
                        default=10,
                        help='tolerance time for every test client, if waiting for a task \
                            more than time+tolerance time, this task will be considered as failed and \
                            this task will automatically retry')

    args = parser.parse_args()

    os.chdir(args.build)
    print(f"target ceph build directory: {args.build}")

    # which item should not be showed in the output
    filters = []

    env = Environment(args)

    # change this method to add new thread class
    env.init_thread_list()

    # execute the tester in the tester matrix
    tester_executor = TesterExecutor()
    tester_executor.run(env)

    output_dir = f"{env.log}/{args.output}.csv"
    tester_executor.output(output_dir, args.output_horizontal, filters)
    print('done.')
