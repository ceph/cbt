#!/usr/bin/env python3 
import argparse
import math
import os
import threading
import time


# Divid all test threads into two categories, which should implement
# ITestCaseBasedThread or ITimePointBasedThread. The test threads that
# only care of what is the system going on at a time point, such as 
# reactor utilization are classified as ITimePointBasedThread and the 
# basic test case threads(such as rados bench) that are only the single 
# small test case are classified as ITestCaseBasedThread.
# For developer, you can write the test class you want by implementing
# the ITestCaseBasedThread or ITimePointBasedThread interfaces and adding 
# it to to testclient_threadclass_list or timepoint_threadclass_list in 
# the class Environmen to extend this test tools. 
# set the start_time to decide when will the test start after thread starts.
# the tast case based threads interface.
class ITestCaseBasedThread(threading.Thread):
    def __init__(self, thread_num, env):
        threading.Thread.__init__(self)
        self.thread_num = thread_num
        self.start_time = 0 
        self.result = None
    # rewrite method create_command() to define the command this class will 
    # execute
    def create_command(self):
        raise NotImplementedError
    # don't need to rewite this method
    def run(self):
        time.sleep(self.start_time)
        self.result =  os.popen(self.create_command())
    # rewrite method analyse() to analyse the output from executing the 
    # command and return a result dict as format {param : result}
    # and the value type should be float
    def analyse(self) ->dict:
        raise NotImplementedError
    # optional.
    # rewrite the method to set the task before this thread
    @staticmethod
    def pre_process(env):
        pass
    # optional.
    # rewrite the method to set the task after this thread
    # you can use env and the result of a case generate by 
    # TesterExecutor, which include case test results
    @staticmethod
    def post_process(env, test_case_result):
        pass


# the time point based threads interface. 
class ITimePointBasedThread(threading.Thread):
    def __init__(self, env):
        threading.Thread.__init__(self)
        self.start_time = 1 
        self.result = None
    def create_command(self):
        raise NotImplementedError
    def run(self):
        time.sleep(self.start_time)
        self.result = os.popen(self.create_command())
    def analyse(self) ->dict:
        raise NotImplementedError
    @staticmethod
    def pre_process(env):
        pass
    @staticmethod
    def post_process(env, test_case_result):
        pass


class ExecRadosBenchThread(ITestCaseBasedThread):
    def __init__(self, thread_num, env):
        ITestCaseBasedThread.__init__(self, thread_num, env)
        self.task_set = env.args.taskset
        self.block_size = env.args.block_size
        self.time = env.args.time
        self.pool = env.pool
        self.iops_key = "write_iops"
        self.latency_key = "write_latency"
        self.bandwidth_key = "write_bandwidth"
    def create_command(self):
        rados_bench_write = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " write -t " \
            + str(self.thread_num) + " -b " + self.block_size + " " \
            + "--no-cleanup"
        return rados_bench_write
    def analyse(self):
        result_dic = {} #iops, lantency, bandwidth
        line = self.result.readline()
        while line:
            if line[0] == 'A':
                element = line.split()
                if element[1]=="IOPS:":
                    result_dic[self.iops_key] = float(element[2])
                if element[1]=="Latency(s):":
                    result_dic[self.latency_key] = float(element[2])
            if line[0] == 'B':
                element = line.split()
                result_dic[self.bandwidth_key] = float(element[2])
            line = self.result.readline()
        self.result.close()
        return result_dic
    @staticmethod
    def post_process(env, test_case_result):
        ratio = env.testclient_threadclass_ratio_map[ExecRadosBenchThread]
        test_case_result["write_iops"] *= int(test_case_result['client_num'] * ratio)
        test_case_result["write_bandwidth"] *= int(test_case_result['client_num'] * ratio)


class ExecRadosBenchRandReadThread(ExecRadosBenchThread):
    def __init__(self, thread_num, env):
        ExecRadosBenchThread.__init__(self, thread_num, env)
        self.iops_key = "read_iops"
        self.latency_key = "read_latency"
        self.bandwidth_key = "read_bandwidth"
    def create_command(self):
        rados_bench_rand_read = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " rand -t " \
            + str(self.thread_num) \
            + " --no-cleanup"
        return rados_bench_rand_read
    @staticmethod
    def pre_process(env):
        env_write_command = "sudo bin/rados bench -p " + env.pool + " " \
            + env.args.time + " write -t " \
            + str(10) + " -b " + env.args.block_size + " " \
            + "--no-cleanup"
        os.system(env_write_command + " >/dev/null")
        print('rados rand read test environment OK')
    @staticmethod
    def post_process(env, test_case_result):
        ratio = env.testclient_threadclass_ratio_map[ExecRadosBenchRandReadThread]
        test_case_result["read_iops"] *= int(test_case_result['client_num'] * ratio)
        test_case_result["read_bandwidth"] *= int(test_case_result['client_num'] * ratio)
        

class ReactorUtilizationCollectorThread(ITimePointBasedThread):
    def __init__(self, env):
        ITimePointBasedThread.__init__(self, env)
        self.start_time = int(env.args.time)/2
        self.osd = "osd.0"
    def create_command(self):
        command = "sudo bin/ceph tell " \
            + self.osd + " dump_metrics reactor_utilization"
        return command
    def analyse(self):
        result_dic = {} #reactor_utilization
        line = self.result.readline()
        while line:
            temp_lis = line.split()
            if temp_lis[0] == "\"value\":":
                result_dic['reactor_utilization'] = float(temp_lis[1])
                break
            line = self.result.readline()
        self.result.close()
        return result_dic


class PerfThread(ITimePointBasedThread):
    def __init__(self, env):
        ITimePointBasedThread.__init__(self, env)
        self.start_time = int(env.args.time)/2
        self.last_time = 1000 #1s
        self.pid_list = env.pid
    def create_command(self):
        command = "sudo perf stat --timeout " + str(self.last_time)
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
        while line:  
            temp_lis = line.split()
            if len(temp_lis)>0:
                if temp_lis[1] == "msec":
                    result_dic['task-clock'] = float(temp_lis[0])   #msec
                if temp_lis[1] == "context-switches":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['context-switches'] = value
                if temp_lis[1] == "cpu-migrations":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['cpu-migrations'] = value
                if temp_lis[1] == "page-faults":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['page-faults'] = value
                if temp_lis[1] == "cycles":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['cpu_cycle'] = value
                if temp_lis[1] == "instructions":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['instructions'] = value
                if temp_lis[1] == "branches":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['branches'] = value
                if temp_lis[1] == "branch-misses":
                    value = int(temp_lis[0].replace(",", ""))
                    result_dic['branch-misses'] = value
            line = self.result.readline()
        self.result.close()
        return result_dic


class Tester():
    def __init__(self, env, client_num, thread_num):
        self.env = env
        self.client_num = client_num
        self.thread_num = thread_num
        self.trmap = env.testclient_threadclass_ratio_map
        self.tplist = env.timepoint_threadclass_list
        self.ratio_client_num = 0
        self.test_case_threads = list()
        self.init()
    def init(self):
        for thread in self.trmap:
            sub_ratio_client_num = int(self.trmap[thread] * self.client_num)
            self.ratio_client_num += sub_ratio_client_num
            for n in range(sub_ratio_client_num):
                self.test_case_threads.append(thread(self.thread_num, self.env))
        for thread in self.tplist:
            self.test_case_threads.append(thread(self.env))
    def run(self):
        print("client num:%d, thread num:%d testing"
                    %(self.client_num, self.thread_num))
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
        for thread_index in range(self.ratio_client_num, len(self.test_case_threads)):
            timepoint_thread_result = \
                    self.test_case_threads[thread_index].analyse()
            for key in timepoint_thread_result:
                if key not in test_case_result:
                    test_case_result[key] = timepoint_thread_result[key]
        test_case_result['thread_num'] = self.thread_num
        test_case_result['client_num'] = self.client_num
        return test_case_result


class TesterExecutor():
    def __init__(self):
        self.result_list = list() #[dict] 
    def run(self, env):
        print('running...')
        for client_num in env.args.client_list:
            for thread_num in env.args.thread_list:
                env.before_run_case()            
                tester = Tester(env, client_num, thread_num)
                test_case_result = tester.run()
                env.after_run_case(test_case_result)
                self.result_list.append(test_case_result)
    def get_result_list(self):
        return self.result_list
    def output(self, output, horizontal, filters):
        f_result = open(output,"w")
        if horizontal:
            for key in self.result_list[0]:
                if key not in filters:
                    print('%20s '%(key), end ='')
                    f_result.write('%20s '%(key))
            print()
            f_result.write('\n')
            for result in self.result_list:
                for key in result:
                    if key not in filters:
                        print('%20s '%(str(result[key])), end = '')
                        f_result.write('%20s '%(str(result[key])))
                print()
                f_result.write('\n')
        else:
            for key in self.result_list[0]:
                if key not in filters:
                    print('%20s '%(key), end ='')
                    f_result.write('%20s '%(key))
                    for result in self.result_list:
                        print('%14.13s'%(str(result[key])), end ='')
                        f_result.write('%14.13s'%(str(result[key])))
                    print()
                    f_result.write('\n')
        f_result.close()
                

class Environment():
    def __init__(self, args):
        self.args = args
        self.testclient_threadclass_ratio_map = {}
        self.timepoint_threadclass_list = []
        self.pid = list()
        self.tid = list()
        self.pool = "_benchtest_" 
    
    def init_thread_list(self):
        # 1. add the test case based thread classes and the ratio to the dict.
        if self.args.write:
            self.testclient_threadclass_ratio_map[ExecRadosBenchThread] = \
                    args.write
        if self.args.rand_read:
            self.testclient_threadclass_ratio_map[ExecRadosBenchRandReadThread] = \
                    args.rand_read
        
        if not self.testclient_threadclass_ratio_map:
            raise Exception("Please set at least one base test.")
        
        # 2. add the time point based case thread classes to the list.
        timepoint_threadclass_list = []
        if self.args.reactor_utilization:
            self.timepoint_threadclass_list.append(ReactorUtilizationCollectorThread)
        if self.args.perf:
            self.timepoint_threadclass_list.append(PerfThread)

    def general_pre_processing(self):
        # vstart. change the command here if you want to set other start params
        command = "sudo MGR=1 MON=1 OSD=1 MDS=0 RGW=0 ../src/vstart.sh -n -x \
                --without-dashboard "
        scenario = self.args.scenario
        if scenario == "crimson-seastore":
            command += "--crimson --seastore"
            if self.args.dev:
                command += " --seastore-devs " + self.args.dev
        elif scenario == "crimson-cyanstore":
            command += "--crimson --cyanstore"
        elif scenario == "classic-memstore":
            command += "--memstore"
        elif scenario == "classic-bluestore":
            if self.args.dev:
                command += " --bluestore-devs " + self.args.dev
        else: 
            raise Exception("Please input the correct scenario.")
        command += " --nodaemon --redirect-output"
        
        #config ceph
        if scenario == "classic-memstore" or scenario == "classic-bluestore":
            command += " -o 'ms_async_op_threads = 1 \
                    osd_op_num_threads_per_shard = 1 osd_op_num_shards = 1'"
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
        
        # find and pin osd tids 
        for t in self.tid:
            os.system("sudo taskset -pc 0 " + str(t))
        
        # pool
        os.system("sudo bin/ceph osd pool create " + self.pool + " 64 64")
    def general_post_processing(self):
        # killall
        os.system("sudo killall -9 -w ceph-mon ceph-mgr ceph-osd \
                crimson-osd rados")
        # delete dev
        os.system("sudo rm -rf ./dev/* ./out/*")
        self.pid = list() 
        self.tid = list()
    def pre_processing(self):
        print('pre processing...')
        for thread in self.testclient_threadclass_ratio_map:
            thread.pre_process(self)
        for thread in self.timepoint_threadclass_list:
            thread.pre_process(self)
    def post_processing(self, test_case_result):
        print('post processing...')
        for thread in self.testclient_threadclass_ratio_map:
            thread.post_process(self, test_case_result)
        for thread in self.timepoint_threadclass_list:
            thread.post_process(self, test_case_result)
    def before_run_case(self):
        self.general_pre_processing()
        self.pre_processing()
    def after_run_case(self, test_case_result):
        self.post_processing(test_case_result)
        self.general_post_processing()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--client-list',
            nargs = '+',
            type = int,
            required = True,
            help = 'clients list')
    parser.add_argument('--thread-list',
            nargs = '+',
            type = int,
            required = True,
            help = 'threads list')
    parser.add_argument('--taskset',
            type = str,
            default = "1-32",
            help = 'which processors will bench thread execute on')
    parser.add_argument('--block-size',
            type = str,
            default = "4096",
            help = 'data block size')
    parser.add_argument('--time',
            type = str,
            default = "10",
            help = 'test time for every test case')
    parser.add_argument('--dev',
            type = str,
            help = 'test device path, default is the current device')
    parser.add_argument('--output',
            type = str,
            default = "result.txt",
            help = 'path of all output result after integrating')
    parser.add_argument('--output-horizontal',
            type = bool,
            default = False,
            help = 'all results of one test case will be in one line')
    parser.add_argument('--scenario',
            type = str,
            default = 'crimson-seastore',
            help = 'choose from crimson-seastore, crimson-cyanstore,\
                    classic-memstore or classic-bluestore')
    
    # test case based thread param 
    parser.add_argument('--write',
            type = float,
            default = 0,
            help = 'ratio of rados bench write clients')
    parser.add_argument('--rand-read',
            type = float,
            default = 0,
            help = 'ratio of rados bench rand read clients')
    
    # time point based thread param
    parser.add_argument('--reactor-utilization',
            type = bool,
            default = False,
            help = 'set True to collect the reactor utilization')
    parser.add_argument('--perf',
            type = bool,
            default = False,
            help = 'set True to collect perf information')
    args = parser.parse_args()

    # which item should not be showed in the output
    filters = []

    env = Environment(args)
    
    # change this method to add new thread class
    env.init_thread_list()

    # execute the tester in the tester matrix
    tester_executor = TesterExecutor()
    tester_executor.run(env)
    tester_executor.output(args.output, args.output_horizontal, filters)
    print('done.')

