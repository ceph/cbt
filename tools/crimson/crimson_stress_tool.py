#!/usr/bin/env python3 
import argparse
import math
import os
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
    def __init__(self, env):
        super().__init__()
        self.thread_num = env.thread_num
        self.start_time = 0 
        self.result = None
    
    # rewrite method create_command() to define the command 
    # this class will execute
    def create_command(self):
        raise NotImplementedError
    
    # don't need to rewite this method
    def run(self):
        time.sleep(self.start_time)
        self.result =  os.popen(self.create_command())
    
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
    def __init__(self, env):
        super().__init__(env)
        self.task_set = env.args.taskset
        self.block_size = env.args.block_size
        self.time = env.args.time
        self.pool = env.pool
        self.iops_key = "rw_iops"
        self.latency_key = "rw_latency"
        self.bandwidth_key = "rw_bandwidth"
    
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
        ratio = env.testclient_threadclass_ratio_map[RadosRandWriteThread]
        test_case_result["rw_iops"] *= \
                int(test_case_result['client_num'] * ratio)
        test_case_result["rw_bandwidth"] *= \
                int(test_case_result['client_num'] * ratio)


class RadosSeqWriteThread(RadosRandWriteThread):
    def __init__(self, env):
        super().__init__(env)
        self.iops_key = "sw_iops"
        self.latency_key = "sw_latency"
        self.bandwidth_key = "sw_bandwidth"
    def create_command(self):
        rados_bench_write = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " write -t " \
            + str(self.thread_num) +  " " \
            + "--no-cleanup"
        return rados_bench_write
    @staticmethod
    def post_process(env, test_case_result):
        ratio = env.testclient_threadclass_ratio_map[RadosSeqWriteThread]
        test_case_result["sw_iops"] *= \
                int(test_case_result['client_num'] * ratio)
        test_case_result["sw_bandwidth"] *= \
                int(test_case_result['client_num'] * ratio)


class RadosRandReadThread(RadosRandWriteThread):
    def __init__(self, env):
        super().__init__(env)
        self.iops_key = "rr_iops"
        self.latency_key = "rr_latency"
        self.bandwidth_key = "rr_bandwidth"
    
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
        ratio = env.testclient_threadclass_ratio_map[RadosRandReadThread]
        test_case_result["rr_iops"] *= \
                int(test_case_result['client_num'] * ratio)
        test_case_result["rr_bandwidth"] *= \
                int(test_case_result['client_num'] * ratio)


class RadosSeqReadThread(RadosRandWriteThread):
    def __init__(self, env):
        super().__init__(env)
        self.iops_key = "sr_iops"
        self.latency_key = "sr_latency"
        self.bandwidth_key = "sr_bandwidth"
    
    def create_command(self):
        rados_bench_seq_read = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " seq -t " \
            + str(self.thread_num) \
            + " --no-cleanup"
        return rados_bench_seq_read
    
    @staticmethod
    def pre_process(env):
        env_write_command = "sudo bin/rados bench -p " + env.pool + " " \
            + " 60 write -t " \
            + str(300) + " " \
            + "--no-cleanup"
        os.system(env_write_command + " >/dev/null")
        print('rados seq read test environment OK')
    
    @staticmethod
    def post_process(env, test_case_result):
        ratio = env.testclient_threadclass_ratio_map[RadosSeqReadThread]
        test_case_result["sr_iops"] *= \
                int(test_case_result['client_num'] * ratio)
        test_case_result["sr_bandwidth"] *= \
                int(test_case_result['client_num'] * ratio)

class FioRBDRandWriteThread(Task):
    def __init__(self, env):
        super().__init__(env)
        self.task_set = env.args.taskset
        self.rw = "randwrite"
        self.io_depth = env.thread_num  
        self.io_engine = "rbd"
        self.num_job = 1 
        self.pool = env.pool
        self.run_time = env.args.time
        self.bs = env.args.block_size
        self.images = env.images
        self.lat = 'fio_rw_lat'
        self.bw = 'fio_rw_bw'
        self.iops = 'fio_rw_iops'
    
    def get_a_image(self):
        return self.images.pop(0)  # atomic      
    
    def create_command(self):
        return "sudo taskset -c " + self.task_set \
                + " fio" \
                + " -ioengine="+ self.io_engine \
                + " -pool=" + str(self.pool) \
                + " -rbdname=" + self.get_a_image() \
                + " -direct=1" \
                + " -iodepth=" + str(self.io_depth) \
                + " -rw=" + self.rw \
                + " -bs=" + self.bs \
                + " -numjobs=" + str(self.num_job) \
                + " -runtime=" +self.run_time \
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
                        result_dic[self.lat] = float(match_res[4:-1])/1000000 #s
                if temp_lis[0] == "bw":
                    match_res = re.search(r'avg=.*?,', line).group()
                    result_dic[self.bw] = float(match_res[4:-1])/1000 #MB/s
                if temp_lis[0] == "iops":
                    match_res = re.search(r'avg=.*?,', line).group()
                    result_dic[self.iops] = float(match_res[4:-1]) 
            line = self.result.readline()
        self.result.close()
        return result_dic
    
    @staticmethod
    def pre_process(env):  
        image_name_prefix = "fio_test_rbd_"
        # must be client_num here.
        for i in range(env.client_num):
            image_name = image_name_prefix + str(i)
            print(image_name)
            command = "sudo bin/rbd create " + image_name \
                    + " --size 20G --image-format=2 \
                    --rbd_default_features=3 --pool " + env.pool
            command += " 2>/dev/null"
            os.system(command)
            env.images.append(image_name)
    
    @staticmethod
    def post_process(env, test_case_result):
        # clear the images record in class env
        env.images = []
        # merge all clients bw and iops results
        ratio = env.testclient_threadclass_ratio_map[FioRBDRandWriteThread]
        test_case_result["fio_rw_bw"] *= \
                int(test_case_result['client_num'] * ratio)
        test_case_result["fio_rw_iops"] *= \
                int(test_case_result['client_num'] * ratio)


class FioRBDRandReadThread(FioRBDRandWriteThread):
    def __init__(self, env):
        super().__init__(env)
        self.rw = "randread"
        self.lat = 'fio_rr_lat'
        self.bw = 'fio_rr_bw'
        self.iops = 'fio_rr_iops'
  
    @staticmethod
    def post_process(env, test_case_result):
        env.images = []
        ratio = env.testclient_threadclass_ratio_map[FioRBDRandReadThread]
        test_case_result["fio_rr_bw"] *= \
                int(test_case_result['client_num'] * ratio)
        test_case_result["fio_rr_iops"] *= \
                int(test_case_result['client_num'] * ratio)
     


class ReactorUtilizationCollectorThread(Task):
    def __init__(self, env):
        super().__init__(env)
        self.start_time = int(env.args.time)/2
        self.osd = "osd.0"
        self.task_set = env.args.taskset
    
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


class PerfThread(Task):
    def __init__(self, env):
        super().__init__(env)
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
                    result_dic['cpus-utilized'] = float(temp_lis[4])  
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


class IOStatThread(Task):
    def __init__(self, env):
        super().__init__(env)
        self.start_time = int(env.args.time)/2
        self.dev = "sda"  #default if no args.dev
        if env.args.dev:
            self.dev = env.get_disk_name()
    
    def create_command(self):
        command = "iostat -x -k -d -y 1 1" 
        return command
    
    def analyse(self):
        result_dic = {} 
        line = self.result.readline()
        while line:
            temp_lis = line.split()
            if temp_lis and temp_lis[0] == self.dev:
                result_dic['iostat_ips'] = float(temp_lis[1])
                result_dic['iostat_ops'] = float(temp_lis[7]) 
                result_dic['iostat_read'] = float(temp_lis[2]) # kB/s
                result_dic['iostat_write'] = float(temp_lis[8]) # kB/s
                result_dic['iostat_avgrq-sz'] = float(temp_lis[21]) # sector/io
                result_dic['iostat_rawait'] = float(temp_lis[5]) # ms
                result_dic['iostat_wawait'] = float(temp_lis[11]) # ms
                break
            line = self.result.readline()
        self.result.close()
        return result_dic


class CPUFreqThread(Task):
    def __init(self, env):
         super().__init__(env)
         self.start_time = int(env.args.time)/2 + 1
    
    def create_command(self):
        command = "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq"
        return command
    
    def analyse(self):
        result_dic = {}
        line = self.result.readline()
        result_dic['cpu_freq'] = float(int(line)/1000000)
        self.result.close()
        return result_dic


class Tester():
    def __init__(self, env):
        self.env = env
        self.client_num = env.client_num
        self.thread_num = env.thread_num
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
                self.test_case_threads.append(thread(self.env))
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
        for thread_index in \
                range(self.ratio_client_num, len(self.test_case_threads)):
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
                env.client_num = client_num
                env.thread_num = thread_num
                env.before_run_case()            
                tester = Tester(env)
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
        self.images = []
        self.thread_num = -1
        self.client_num = -1

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
        if self.args.fio_rbd_rand_write:
            self.testclient_threadclass_ratio_map[FioRBDRandWriteThread] = \
                    self.args.fio_rbd_rand_write
        if self.args.fio_rbd_rand_read:
            self.testclient_threadclass_ratio_map[FioRBDRandReadThread] = \
                    self.args.fio_rbd_rand_read

        if not self.testclient_threadclass_ratio_map:
            raise Exception("Please set at least one base test.")
        
        # 2. add the time point based case thread classes to the list.
        timepoint_threadclass_list = []
        if self.args.reactor_utilization:
            self.timepoint_threadclass_list.append(ReactorUtilizationCollectorThread)
        if self.args.perf:
            self.timepoint_threadclass_list.append(PerfThread)
        if self.args.iostat:
            self.timepoint_threadclass_list.append(IOStatThread)
        if self.args.freq:
            self.timepoint_threadclass_list.append(CPUFreqThread)

    def general_pre_processing(self):
        os.system("sudo killall -9 -w ceph-mon ceph-mgr ceph-osd \
                crimson-osd rados")
        os.system("sudo rm -rf ./dev/* ./out/*")
        
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
        if self.args.single_core:
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
        if self.args.single_core:
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
    
    def get_disk_name(self):
        par = self.args.dev.split('/')[-1] 
        lsblk = os.popen("lsblk")
        last = None
        line = lsblk.readline()
        while line:
            ll = line.split()
            if ll[0][0:2] == '├─' or ll[0][0:2] =='└─':
                if ll[0][2:] == par:
                    return last
            else:
                last = ll[0]
            line = lsblk.readline()
        return par 


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
            help = 'test device path, default is the vstart default \
                    settings, creating a virtual block device on current device')
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
    parser.add_argument('--single-core',
            type = bool,
            default = True,
            help = 'run osds in single core')

    # test case based thread param 
    parser.add_argument('--rand-write',
            type = float,
            default = 0,
            help = 'ratio of rados bench rand write clients')
    parser.add_argument('--rand-read',
            type = float,
            default = 0,
            help = 'ratio of rados bench rand read clients')
    parser.add_argument('--seq-write',
            type = float,
            default = 0,
            help = 'ratio of rados bench seq write clients')
    parser.add_argument('--seq-read',
            type = float,
            default = 0,
            help = 'ratio of rados bench seq read clients')
    parser.add_argument('--fio-rbd-rand-write',
            type = float,
            default = 0,
            help = 'ratio of fio rand write clients')
    parser.add_argument('--fio-rbd-rand-read',
            type = float,
            default = 0,
            help = 'ratio of fio rand read clients')

    # time point based thread param
    parser.add_argument('--reactor-utilization',
            type = bool,
            default = False,
            help = 'set True to collect the reactor utilization')
    parser.add_argument('--perf',
            type = bool,
            default = False,
            help = 'set True to collect perf information')
    parser.add_argument('--iostat',
            type = bool,
            default = False,
            help = 'set True to collect iostat information')
    parser.add_argument('--freq',
            type = bool,
            default = False,
            help = 'set True to collect cpu frequency information')
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

