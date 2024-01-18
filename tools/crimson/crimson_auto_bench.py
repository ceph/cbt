#!/usr/bin/env python3
import yaml
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt

''' directory structure:
    autobench.{date}/rep:{repeat_id}/test:{test_id}
    test:{test_id} is the output directory from crimson_bench_tool.

    graphic directory:
    graphic.autobench.{date}/test:{test_id}
'''

no_value_attributes= ['crimson', 'output_horizontal', 'perf', \
                      'perf_record', 'iostat']

# transfer to crimson_bench_tool param
def trans(param):
    res = f"--{param}"
    res = res.replace('_', '-')
    return res

def prefix_match(prefix, target):
    prefix = prefix.lower()
    target = target.lower()
    if target[:len(prefix)] == prefix:
        return True
    else:
        return False

def get_version_and_commitID():
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

def record_system_info(root, configs):
    path = f"{root}/sys_info.txt"
    os.system(f"touch {path}")

    os.system(f"echo \"0.Ceph Source Code Info\">> {path}")
    version, commitID = get_version_and_commitID()
    os.system(f"echo \"version: {version}\" >> {path}")
    os.system(f"echo \"commit ID: {commitID}\" >> {path}")
    os.system(f"echo >> {path}")

    os.system(f"echo \"1.Linux Release\">> {path}")
    os.system(f"lsb_release -a >> {path}")
    os.system(f"echo >> {path}")

    os.system(f"echo \"2.Linux Kernel\">> {path}")
    os.system(f"uname -r  >> {path}")
    os.system(f"echo >> {path}")

    os.system(f"echo \"3.CPU\" >> {path}")
    os.system(f"lscpu >> {path}")
    os.system(f"echo >> {path}")

    os.system(f"echo \"4.Disk\" >> {path}")
    os.system(f"lsblk -O --raw -a >> {path}")
    os.system(f"echo >> {path}")
    os.system(f"sudo fdisk -l >> {path}")
    os.system(f"echo >> {path}")
    # will only show device detail configed in config yaml
    devs = set()
    for config in configs:
        if 'dev' in config:
            devs.add(config['dev'])
    for dev_id, dev in enumerate(devs):
        os.system(f"echo \"4.{dev_id+1} Disk: {dev}\" >> {path}")
        if dev[5:8] == 'nvm':
            os.system(f"sudo nvme id-ctrl {dev} >> {path}")
        else:
            os.system(f"sudo hdparm -I {dev} >> {path}")
        os.system(f"echo >> {path}")

    os.system(f"echo \"5.Memory\" >> {path}")
    os.system(f"lsmem >> {path}")
    os.system(f"echo >> {path}")
    os.system(f"sudo dmidecode -t memory >> {path}")
    os.system(f"echo >> {path}")

    os.system(f"echo \"6.Board\" >> {path}")
    os.system(f"sudo dmidecode|grep -A16 \"System Information$\" >> {path}")
    os.system(f"echo >> {path}")

    os.system(f"echo \"7.PCIe\" >> {path}")
    os.system(f"lspci -vv >> {path}")
    os.system(f"echo >> {path}")

def read_config(config_file, x = None, comp = None):
    config_file_pwd = f"{real_path}/{config_file}"
    f = open(config_file_pwd, 'r')
    _config = yaml.safe_load_all(f.read())
    f.close()

    configs = list()
    for config in _config:
        configs.append(config)

        for key in config:
            if key in no_value_attributes and config[key] != True:
                raise Exception("Error: no value attributes should only be True")

    if x:
        for config in configs:
            if args.x not in config:
                raise Exception("Error: x param should exist in config yaml file")
            if type(config[args.x]) != str or len(config[args.x].split()) <= 1:
                raise Exception("Error: cannot use single param as x axis")
    if comp:
        if not x:
            raise Exception("Error: must input --x when using --comp")
        same_x = configs[0][x]
        for config in configs:
            if config[x] != same_x:
                raise Exception("Error: x must be the same "\
                                "between different test in --comp mode")
        for index in comp:
            if index > len(configs) or index <= 0:
                raise Exception(f"Error: comp index error. Shoud between" \
                                f" 1-{len(configs)}")
    return configs

def do_bench(config_file, configs, repeat):
    # all files' root for this auto bench test
    root = f"{real_path}"
    with os.popen("date +%Y%m%d.%H%M%S") as date:
        line = date.readline()
        res = line.split()[0]
        root = f"{root}/autobench.{res}"
    os.makedirs(root)
    record_system_info(root, configs)
    os.system(f"cp {config_file} {root}/config.yaml")

    # do bench
    for repeat_id in range(repeat):
        repeat_path = f"{root}/rep:{repeat_id}"
        os.makedirs(repeat_path)
        for test_id, test_config in enumerate(configs):
            command = f"{real_path}/crimson_bench_tool.py"
            for key in test_config:
                if key in no_value_attributes:
                    command += f" {trans(key)}"
                else:
                    command += f" {trans(key)} {test_config[key]}"
            test_path_prefix = f"{repeat_path}/test:{test_id}"
            command += f" --log {test_path_prefix}"
            print(command)
            os.system(command)
    return root

def read_results(root):
    root_files = os.listdir(root)
    failure_log = 'failure_log.txt'
    failure_path = f"{root}/{failure_log}"
    first = False
    if failure_log not in root_files:
        first = True
        os.system(f"touch {failure_path}")

    repeat = len(os.listdir(root)) - 3 # skip config.yaml, failure_log.txt, sys_info.txt
    # read root directory to get results
    results = dict()
    # results - repeat_results - test_results - (tester_id, all results)
    for repeat_id in range(repeat):
        repeat_path = f"{root}/rep:{repeat_id}"
        repeat_results = dict()
        tests = os.listdir(repeat_path)
        tests.sort()
        for test_id, test_name in enumerate(tests):
            test_path = f"{repeat_path}/{test_name}"
            test_results = dict()
            files = os.listdir(test_path)
            if '__failed__' not in files:
                result_path = f"{test_path}/result.csv"
                res = pd.read_csv(result_path)
                for case in range(len(res)):
                    test_results[case] = res.iloc[case]
                repeat_results[test_id] = test_results
            else:
                repeat_results[test_id] = False
                if first:
                    os.system(f"echo \"test failed in rep:{repeat_id},"\
                              f" test:{test_id}\" >> {failure_path}")

        results[repeat_id] = repeat_results
    return results

def print_results(results):
    for repeat_id in results:
        for test_id in results[repeat_id]:
            print(f"repeat_id:{repeat_id}, test_id:{test_id}")
            print(results[repeat_id][test_id])

def delete_and_create_at_local(path):
    files = os.listdir('.')
    if path not in files:
        os.makedirs(path)
    else:
        os.system(f"sudo rm -rf {path}")
        os.makedirs(path)

def adjust_results(results, y):
    '''
        transfer to [[[r1t1c1y, r2t1c1y, r3t1c1y], [r1t1c2y, r2t1c2y, r3t1c3y], ...], => pics1
                     [[r1t2c1y, r2t2c1y, r3t2c1y], [r1t2c2y, r2t2c2y, r3t2c3y], ...],  => pics2
                     ...]
        r1t1c1y means repeat 1, test 1, case 1, y
        t - c - r
        one pics for one test case, correspoding to one config block in yaml file
        the most inner list represent multiple repeat target y for one tester(case) in one test
    '''
    test_num = len(results[0])
    all_test_res = list()
    for test_id in range(test_num):
        case_num = 0
        for repeat_id in results:
            if results[repeat_id][test_id]:
                _case_num = len(results[repeat_id][test_id])
                if case_num != 0 and _case_num != case_num:
                    raise Exception("Error: cases num changed\
                                    between different repeat for one same test")
                case_num = _case_num
        if case_num == 0:
            # all repeat of this test failed
            all_test_res.append([])
            continue

        test_res = list()
        for case_id in range(case_num):
            case_res = list()
            for repeat_id in results:
                if results[repeat_id][test_id] != False:
                    match_count = 0
                    case = results[repeat_id][test_id][case_id]
                    for y_f in case.keys().to_list():
                        if prefix_match(y, y_f):
                            y_res = results[repeat_id][test_id][case_id][y_f]
                            match_count += 1
                    if match_count > 1:
                        raise Exception(f"Error: {y} matches multiple y indexes"\
                                        f", please input full y index name")
                    if match_count == 0:
                        raise Exception(f"Error: {y} didn't match any y index")
                    case_res.append(y_res)
                else:
                    pass
            test_res.append(case_res)
        all_test_res.append(test_res)
    return all_test_res

def draw(analysed_results, configs, x, y, res_path, comp):
    for test_id, test in enumerate(analysed_results):
        if test == []:
            print('failed happen')
            continue
        if comp and test_id+1 not in comp:
            continue
        x_data = configs[test_id][x].split()
        y_data = test
        y_data_mean = list()
        for items in y_data:
            y_data_mean.append(sum(items)/len(items))

        df = pd.DataFrame({f'{x}':[], f'{y}':[]})
        for x_id, x_content in enumerate(x_data):
            for y_content in y_data[x_id]:
                df.loc[len(df.index)] = {f'{x}': x_content, f'{y}' : y_content}

        plt.title(f"{x}-{y}".lower())
        plt.xlabel(f"{x}")
        plt.ylabel(f"{y}")
        plt.plot(f'{x}', f'{y}', data=df, linestyle='none',\
                 marker='o', label=f'test_{test_id}')
        plt.plot(x_data, y_data_mean, linestyle='-', label=f'test_{test_id} mean')
        plt.legend()
        # TODO: additional information to graphics
        if not comp:
            plt.savefig(f'{res_path}/test:{test_id}_x:{x}_y:{y}.png'.lower())
            plt.close()

        # raw data to csv
        df.to_csv(f'{res_path}/test:{test_id}_x:{x}_y:{y}.csv'.lower())
        # average to csv
        df_avg = pd.DataFrame({f'{x}':[], f'{y}_avg':[]})
        for x_id, x_content in enumerate(x_data):
            df_avg.loc[len(df_avg.index)] = \
                {f'{x}': x_content, f'{y}_avg' : y_data_mean[x_id]}
        df_avg.to_csv(f'{res_path}/test:{test_id}_x:{x}_y:{y}_avg.csv'.lower())
    if comp:
        plt.savefig(f'{res_path}/test_x:{x}_y:{y}.png'.lower())
        plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--run',
                        action='store_true',
                        help= "do bench, analyse results and draw pictures,\
                            require --x, --y")
    parser.add_argument('--bench',
                        action='store_true',
                        help= "only do bench")
    parser.add_argument('--ana',
                        type=str,
                        default=None,
                        help= "the root directory storing the raw bench data, to"\
                            " adjust results and draw pictures. require --x, --y")
    parser.add_argument('--comp',
                        nargs='+',
                        type=int,
                        default=None,
                        help= "will merge target index tests into one graphics. The "\
                            "index corresponds to the order in which the configuration "\
                            "appears in the config file.")
    parser.add_argument('--clean',
                        action='store_true',
                        help= "remove all history graphic and bench results")

    parser.add_argument('--config',
                        type=str,
                        default="bench_config.yaml",
                        help="bench config yaml file path, bench_config.yaml by default")

    parser.add_argument('--repeat',
                        type=int,
                        default=1,
                        help="repeat time for every tests, default to be 1")
    parser.add_argument('--x',
                        type=str,
                        help="x axis of result graphics, the main variable in the target"\
                            " graphics to draw x-y graphics. required when --ana or --run."\
                            " x can be smp, client, thread, osd_op_num_shards, etc. all the"\
                            " parameters that can be multiple in the crimson bench tool can be x.")
    parser.add_argument('--y',
                        nargs='+',
                        type=str,
                        default=["IOPS"],
                        help="the label name of y asix of the result graphics, IOPS by default")

    args = parser.parse_args()
    res_path_prefix = 'graphic'
    files = os.listdir('.')
    real_path = (os.path.dirname(os.path.realpath(__file__)))

    _ana = 0
    if args.ana:
        _ana = 1
    if args.run + args.bench + _ana +args.clean != 1:
        raise Exception("Error: should run in one of run/bench/ana/clean")
    if args.run:
        if not args.x:
            raise Exception("Error should input --x to run")
        configs = read_config(args.config, x=args.x, comp=args.comp)
        root = do_bench(args.config, configs, args.repeat)
        results = read_results(root)
        print(root)
        res_path = f"{root}.{res_path_prefix}"
        delete_and_create_at_local(res_path)
        for y in args.y:
            analysed_results = adjust_results(results, y)
            draw(analysed_results, configs, args.x, y, res_path, args.comp)

    if args.bench:
        configs = read_config(args.config)
        root = do_bench(args.config, configs, args.repeat)
        print(root)

    if args.ana:
        root = ''
        if args.ana[-1] == '/':
            root = args.ana[:-1]
        else:
            root = args.ana
        if not args.x:
            raise Exception("Error: should input --x to analyse")
        configs = read_config(f"{root}/config.yaml", x=args.x, comp=args.comp)
        results = read_results(root)
        res_path = f"{res_path_prefix}.{root}"
        delete_and_create_at_local(res_path)
        for y in args.y:
            analysed_results = adjust_results(results, y)
            draw(analysed_results, configs, args.x, y, res_path, args.comp)

    if args.clean:
        os.system("sudo rm -rf autobench.*")
        os.system("sudo rm -rf graphic.autobench.*")
