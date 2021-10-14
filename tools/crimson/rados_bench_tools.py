#!/usr/bin/env python3

import argparse
import math
import os
import threading
import time

class ExecRadosBenchThread(threading.Thread):
    def __init__(self, thread_nums, client_nums, path):
        threading.Thread.__init__(self)
        self.thread_nums = thread_nums
        self.client_nums = client_nums
        self.path = path
        self.task_set = args.taskset
        self.block_size = args.block_size
        self.time = args.time
        self.pool = args.pool
    def creat_rados_bench_write_command(self):
        rados_bench_write = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " write -t " \
            + str(self.thread_nums) + " -b " + self.block_size + " "
        output = ">> " + self.path + "/" + "client_" + str(self.client_nums) \
            + "_rados_bench_thread_" + str(self.thread_nums) + ".txt"
        return rados_bench_write + output
    def run(self):
        print("client nums:%d, thread nums:%d testing"
                %(self.client_nums, self.thread_nums))
        os.system(self.creat_rados_bench_write_command())

class ReactorUtilizationCollectorThread(threading.Thread):
    def __init__(self, thread_nums, client_nums, path, start_time):
        threading.Thread.__init__(self)
        self.start_time = start_time
        self.thread_nums = thread_nums
        self.client_nums = client_nums
        self.path = path
        self.osd = "osd.0"
    def create_ceph_metrix_command(self):
        command = "sudo bin/ceph tell " \
            + self.osd + " dump_metrics reactor_utilization"
        output = ">> " + self.path + "/" + "client_" + str(self.client_nums) \
            + "_seastar_bench_thread_" + str(self.thread_nums) + ".txt"
        return command + output
    def run(self):
        time.sleep(self.start_time)
        os.system(self.create_ceph_metrix_command()) 

def single_client_bench(lis, rados_bench_path, seastar_bench_path):
    for thread_nums in lis:
        client_thread = ExecRadosBenchThread(thread_nums, 1, rados_bench_path)
        ceph_metrix_thread = ReactorUtilizationCollectorThread(
            thread_nums, 1, seastar_bench_path, int(args.time)/2)
        client_thread.start()
        ceph_metrix_thread.start()
        client_thread.join()
        ceph_metrix_thread.join()
    print("Done.")

def multi_client_bench(n, thread_nums, client_bench_path, client_seastar_path):
    client_list = list()
    seastar_list = list()
    for client_nums in range(n):
        client_thread = ExecRadosBenchThread( 
            thread_nums,client_nums, client_bench_path)
        ceph_metrix_thread = ReactorUtilizationCollectorThread(
            thread_nums, client_nums, client_seastar_path, int(args.time)/2)
        client_list.append(client_thread)
        seastar_list.append(ceph_metrix_thread)
    for index in range(n):
        client_list[index].start()
        seastar_list[index].start()
    for index in range(n):
        client_list[index].join()
        seastar_list[index].join()
    print("Done.")


def multi_group_multi_client_bench( 
    lis, thread_nums, rados_bench_path, seastar_bench_path): 
    for n in lis:
        client_bench_path = rados_bench_path + "/" + str(n)
        client_seastar_path = seastar_bench_path + "/" + str(n)
        os.makedirs(client_bench_path)
        os.makedirs(client_seastar_path)
        multi_client_bench(n, thread_nums, client_bench_path, client_seastar_path)

def result_sort_rule_thread(tupl):
    return int(tupl[1])

def result_sort_rule_client(tupl):
    return int(tupl[0])
    
def append_result_to_file(f, result):
    f.write("CLIENTS  THREADS  IOPS  BANDWIDTCH  LATENCY REACTOR_UTILIZATION\n")
    for tupl in result:
        line = str(tupl[0]) + "         " + str(tupl[1]) + "        " \
            + str(tupl[2]) + "    " + str(tupl[3]) + "    " + str(tupl[4]) \
            + "    " + str(tupl[5]) + "\n"
        f.write(line)

def transform_ceph_seastar_bench_filename(rados_bench_filename):
    return  rados_bench_filename.replace("rados","seastar")
    

def get_reactor_utilization_value_by_ceph_seastar_bench_filename(
    seastar_bench_path, seastar_bench_filename):
    file_path = seastar_bench_path + "/" + seastar_bench_filename
    f = open(file_path)
    line = f.readline()
    while line:
        temp_lis = line.split()
        if temp_lis[0] == "\"value\":":
            f.close()
            return temp_lis[1]
        line = f.readline()

def get_rados_bench_result(rados_bench_path, seastar_bench_path):
    result = list()
    file_names = os.listdir(rados_bench_path)
    for result_file in file_names:
        client_nums = result_file.split('_')[1]
        thread_nums = result_file.split('_')[5].split('.')[0]
        result_file_path = rados_bench_path + "/" + result_file
        iops = latency = bandwidth = None
        f = open(result_file_path)
        line = f.readline()
        while line:
            if line[0] == 'A':
                element = line.split()
                if element[1]=="IOPS:":
                    iops = element[2]
                if element[1]=="Latency(s):":
                    latency = element[2]
            if line[0] == 'B':
                element = line.split()
                bandwidth = element[2] 
            line = f.readline()
        f.close()

        #reactor_utilization
        reactor_utilization = \
            get_reactor_utilization_value_by_ceph_seastar_bench_filename(
                seastar_bench_path, 
                transform_ceph_seastar_bench_filename(result_file))
        result.append((client_nums, thread_nums, iops, 
                bandwidth, latency, reactor_utilization))
    
    result.sort(key=result_sort_rule_thread)
    result.sort(key=result_sort_rule_client)
    append_result_to_file(f_result,result)
    print(result)
    return result

def analyse_rados_bench_result(result):
    sum_iops = sum_bandwith = sum_latency = sum_reactor_utilization = 0
    length = len(result)
    for data in result:
        sum_iops += int(data[2])
        sum_bandwith += float(data[3])
        sum_latency += float(data[4])
        sum_reactor_utilization += float(data[5])
    return (result[-1][0], 
            result[-1][1], 
            sum_iops/length,
            sum_bandwith/length, sum_latency/length, 
            sum_reactor_utilization/length)

def analyse_multi_rados_bench_result(rados_bench_path, seastar_bench_path):
    result = list()
    client_dir = os.listdir(rados_bench_path)
    for client in client_dir:
        client_result = get_rados_bench_result(
            rados_bench_path + "/" + client, seastar_bench_path + "/" + client) 
        client_result_avg = analyse_rados_bench_result(client_result) 
        result.append(client_result_avg)
    result.sort(key = result_sort_rule_client)
    f_result.write("=Every client average results:\n")
    append_result_to_file(f_result,result)
    print('every client avg results:')
    print(result)
    return result        

def clean_up(path_list):
    for path in path_list:
        os.system("sudo rm -rf " + path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=
            'For single client stress case, please set --client=1 and --thread-list. \
            In this case, parameter --client-list and --thread will be invalid. \
            In multiple client stress case, please set --thread and --client-list. \
            In this case, parameter --client and --thread-list will be invalid.')
    parser.add_argument('--clean-up',
            type = bool,
            default = False,
            help = 'clean up all intermediate results')
    parser.add_argument('--client',
            type = int,
            default = 0,
            help = 'the number of clients in all test case')
    parser.add_argument('--thread',
            type = int,
            default = 16,
            help = 'the number of threads in all test case')
    parser.add_argument('--thread-list',
            type = str,
            default = "[10]",
            help = 'the number of threads in each test case')
    parser.add_argument('--client-list',
            type = str,
            default = "[1]",
            help = 'the number of clients in each test case')
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
            help = 'test time')
    parser.add_argument('--pool',
            type = str,
            default = "benchtest",
            help = 'pool')
    parser.add_argument('--path-radosbench',
            type = str,
            default = "rados_bench_result",
            help = 'path of every rados bench test results')
    parser.add_argument('--path-seastar',
            type = str,
            default = "rados_seastar_result",
            help = 'path of every seastar performance data results')
    parser.add_argument('--output',
            type = str,
            default = "result.txt",
            help = 'path of all output result after integrating')
    args = parser.parse_args()

    f_result = open(args.output,"w")

    rados_bench_path = args.path_radosbench
    seastar_bench_path = args.path_seastar
    if not os.path.exists(rados_bench_path):
        os.makedirs(rados_bench_path)
    else:
        if len(os.listdir(rados_bench_path)) != 0:
            clean_up([rados_bench_path])
            os.makedirs(rados_bench_path)
    if not os.path.exists(seastar_bench_path):
        os.makedirs(seastar_bench_path)
    else:
        if len(os.listdir(seastar_bench_path)) != 0:
            clean_up([seastar_bench_path])
            os.makedirs(seastar_bench_path)

    if args.client == 1:
        single_client_bench(eval(args.thread_list), 
                rados_bench_path, seastar_bench_path)
        get_rados_bench_result(rados_bench_path, seastar_bench_path)
    else:
        multi_group_multi_client_bench(eval(args.client_list),
                args.thread, rados_bench_path, seastar_bench_path)
        analyse_multi_rados_bench_result(rados_bench_path, seastar_bench_path)

    if args.clean_up is True:
        clean_up([rados_bench_path, seastar_bench_path])

    f_result.close()
