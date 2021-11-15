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
# it to to testclient_threadclass_list or timepoint_threadclass_list to 
# extend this test tools. 
# set the start_time to decide when will the test start after thread starts.
# the tast case based threads interface.
class ITestCaseBasedThread(threading.Thread):
    def __init__(self, thread_num, args):
        threading.Thread.__init__(self)
        self.thread_num = thread_num
        self.args = args
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


# the time point based threads interface. 
class ITimePointBasedThread(threading.Thread):
    def __init__(self, args):
        threading.Thread.__init__(self)
        self.args = args
        self.start_time = 1 
        self.result = None
    def create_command(self):
        raise NotImplementedError
    def run(self):
        time.sleep(self.start_time)
        self.result = os.popen(self.create_command())
    def analyse(self) ->dict:
        raise NotImplementedError


class ExecRadosBenchThread(ITestCaseBasedThread):
    def __init__(self, thread_num, args):
        ITestCaseBasedThread.__init__(self, thread_num, args)
        self.task_set = self.args.taskset
        self.block_size = self.args.block_size
        self.time = self.args.time
        self.pool = self.args.pool
    def create_command(self):
        rados_bench_write = "sudo taskset -c " + self.task_set \
            + " bin/rados bench -p " + self.pool + " " \
            + self.time + " write -t " \
            + str(self.thread_num) + " -b " + self.block_size + " "
        return rados_bench_write
    def analyse(self):
        result_dic = {} #iops, lantency, bandwidth
        line = self.result.readline()
        while line:
            if line[0] == 'A':
                element = line.split()
                if element[1]=="IOPS:":
                    result_dic['iops'] = float(element[2])
                if element[1]=="Latency(s):":
                    result_dic['latency'] = float(element[2])
            if line[0] == 'B':
                element = line.split()
                result_dic['bandwidth'] = float(element[2])
            line = self.result.readline()
        self.result.close()
        return result_dic


class ReactorUtilizationCollectorThread(ITimePointBasedThread):
    def __init__(self, args):
        ITimePointBasedThread.__init__(self, args)
        self.start_time = int(self.args.time)/2
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


class TesterMetrix():
    def __init__(self, args \
            , testclient_threadclass_list, timepoint_threadclass_list):
        self.thread_list = args.thread_list
        self.client_list = args.client_list
        self.testclient_threadclass_list = testclient_threadclass_list
        self.timepoint_threadclass_list = timepoint_threadclass_list
        self.metrix = list() #[[ITestCaseBasedThread or ITimePointBasedThread]]
        self.desc_metrix = list() #[[(thread_num, client_num)]]
        self.init_metrix()
    def init_metrix(self):
        for thread_num in self.thread_list:
            row = list()
            desc_row = list()
            for client_num in self.client_list:
                test_case_threads = list()
                for thread in self.testclient_threadclass_list:
                    for n in range(client_num):
                        test_case_threads.append(thread(thread_num, args))
                for thread in self.timepoint_threadclass_list:
                    test_case_threads.append(thread(args)) 
                row.append(test_case_threads)
                desc_row.append((thread_num, client_num))
            self.metrix.append(row)
            self.desc_metrix.append(desc_row)
    def get_tester_metrix(self):
        return self.metrix
    def get_desc_metrix(self):
        return self.desc_metrix


class TesterExecutor():
    def __init__(self, tester_metrix, desc_metrix):
        self.result_list = list() #[dict] 
        for row_index in range(len(tester_metrix)):
            for test_case_threads_index in range(len(tester_metrix[row_index])):
                test_case_threads = tester_metrix[row_index][test_case_threads_index]
                
                desc = desc_metrix[row_index][test_case_threads_index]
                thread_num = desc[0]
                client_num = desc[1]
                print("client num:%d, thread num:%d testing"
                            %(client_num, thread_num))
                for thread in test_case_threads:
                    thread.start()
                for thread in test_case_threads:
                    thread.join()
                test_case_result = dict()
                for thread_index in range(client_num):
                    base_test_case_result = test_case_threads[thread_index].analyse()
                    for key in base_test_case_result:
                        if key not in test_case_result:
                            test_case_result[key] = base_test_case_result[key]
                        else:
                            test_case_result[key] += base_test_case_result[key]
                for key in test_case_result:
                    test_case_result[key] /= client_num
                for thread_index in range(client_num, len(test_case_threads)):
                    timepoint_thread_result = test_case_threads[thread_index].analyse()
                    for key in timepoint_thread_result:
                        if key not in test_case_result:
                            test_case_result[key] =  timepoint_thread_result[key]

                test_case_result['thread_num'] = thread_num
                test_case_result['client_num'] = client_num
                self.result_list.append(test_case_result)
    def output(self, output):
        f_result = open(output,"w")
        for key in self.result_list[0]:
            print('%20s'%(key), end ='')
            f_result.write('%20s'%(key))
        print('\n')
        f_result.write('\n')
        for result in self.result_list:
            for key in result:
                print('%20s'%(str(result[key])), end = '')
                f_result.write('%20s'%(str(result[key])))
            print('\n')
            f_result.write('\n')
           

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--thread-list',
            nargs = '+',
            type = int,
            required = True,
            help = 'threads list')
    parser.add_argument('--client-list',
            nargs = '+',
            type = int,
            required = True,
            help = 'clients list')
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
    parser.add_argument('--pool',
            type = str,
            default = "benchtest",
            help = 'pool')
    parser.add_argument('--output',
            type = str,
            default = "result.txt",
            help = 'path of all output result after integrating')
    parser.add_argument('--reactor-utilization',
        type = bool,
        default = False,
        help = 'set True to collect the reactor utilization')
    args = parser.parse_args()

    # add the test thread class to the lists below.
    testclient_threadclass_list = [ExecRadosBenchThread]
    timepoint_threadclass_list = []
    if args.reactor_utilization:
        timepoint_threadclass_list.append(ReactorUtilizationCollectorThread)
    # init the tester metrix
    tester_metrix = TesterMetrix(args \
            , testclient_threadclass_list, timepoint_threadclass_list)
    # execute the tester in the tester metrix
    tester_executor = TesterExecutor(tester_metrix.get_tester_metrix() \
            , tester_metrix.get_desc_metrix())
    tester_executor.output(args.output)

