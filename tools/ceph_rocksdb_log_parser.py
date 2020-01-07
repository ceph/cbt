#!/usr/bin/python3
import argparse
import json
import datetime

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time(dt):
    return (dt - epoch).total_seconds() 

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--summary', required=False, type=bool, default=True, help='Include summary information for this log.')
    parser.add_argument('-l', '--level', required=False, type=int, default=-1, help='Level of compaction events to print. Defaults to all.')

    parser.add_argument("FILE", help="collectl log output files to parse", nargs="+")
    args = parser.parse_args()
    return args

def print_summary(logs):
    data = [
               ["Compaction Statistics"],
               ["Total OSD Log Duration (seconds)"],
               ["Number of Compaction Events"],
               ["Avg Compaction Time (seconds)"], 
               ["Total Compaction Time (seconds)"],
               ["Avg Output Size: (MB)"],
               ["Total Output Size: (MB)"],
               ["Total Input Records"],
               ["Total Output Records"],
               ["Avg Output Throughput (MB/s)"],
               ["Avg Input Records/second"],
               ["Avg Output Records/second"],
               ["Avg Output/Input Ratio"]
           ]
    for log in logs:
        for ele_a, ele_b in zip(data, log.get_summary_data()):
            ele_a.append(ele_b)
    for row in data:
        print("\t".join(map(str, row)))

class LogData():
    def __init__(self, ctx, fn):
        self.ctx = ctx
        self.fn = fn
        self.last = None
        self.events = []
        self.start_dt = None
        self.end_dt = None
        self.read_data(fn)

    def read_data(self, fn):
        f = open(fn, 'r')
        for line in f:

            # We need to get the smallest timestamp in the log and set it 
#            ts = ' '.join(line.split(' ', 2)[0:1])
            ts = line[0:23]
            if not ts:
                continue;

            try: 
                # Octopus and newer format
                dt = datetime.datetime.strptime(ts[0:23], "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError:
                try:
                    # Nautilus and older format
                    dt = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
                except: 
                    pass

            if self.start_dt is None or self.start_dt > dt:
                self.start_dt = dt
            if self.end_dt is None or self.end_dt < dt:
                self.end_dt = dt
            # For now we are only interested in "event: compaction finished"
            if '"event": "compaction_finished"' not in line:
                continue
#            print line
            json_str = '{' + line.split('{', 1)[-1]
#            print json_str

            # Add the Event if it matches the level
            event = CompactionEvent(dt, self, json_str)
            if self.ctx.level == -1 or self.ctx.level == event.get_output_level():
               self.events.append(event) 

    def print_events(self):
       for event in self.events:
           event.print_data()

    def get_summary_data(self):
        return [
                   self.fn,
                   (self.end_dt - self.start_dt).total_seconds(),
                   len(self.events),
                   self.get_avg_compaction_time_seconds(),
                   self.get_total_compaction_time_seconds(),
                   self.get_avg_output_size_mb(),
                   self.get_total_output_size_mb(),
                   self.get_total_input_records(),
                   self.get_total_output_records(),
                   self.get_avg_output_throughput(),
                   self.get_avg_input_rs(),
                   self.get_avg_output_rs(),
                   self.get_avg_oi_ratio()
               ]

    def get_avg_compaction_time_seconds(self):
        sl = [event for event in self.events if event.get_compaction_time_seconds() != -1]
        if len(sl) == 0:
            return -1
        return sum(e.get_compaction_time_seconds() for e in sl) / len(sl)

    def get_total_compaction_time_seconds(self):
        sl = [event for event in self.events if event.get_compaction_time_seconds != -1]
        if len(sl) == 0:
            return -1

        return sum(e.get_compaction_time_seconds() for e in sl)
            
    def get_avg_output_size_mb(self):
        sl = [event for event in self.events if event.get_total_output_size() != -1]
        if len(sl) == 0:
            return -1
        return sum(e.get_total_output_size() for e in sl) / (len(sl) * 1.0*1024*1024)

    def get_total_output_size_mb(self):
        sl = [event for event in self.events if event.get_total_output_size() != -1]
        if len(sl) == 0:
            return -1
        return sum(e.get_total_output_size() for e in sl) / (1.0*1024*1024)

    def get_total_input_records(self):
        sl = [event for event in self.events if event.get_num_input_records() != -1]
        if len(sl) == 0:
            return -1
        return sum(e.get_num_input_records() for e in sl) 

    def get_total_output_records(self):
        sl = [event for event in self.events if event.get_num_output_records() != -1]
        if len(sl) == 0:
            return -1
        return sum(e.get_num_output_records() for e in sl)

    def get_avg_output_throughput(self):
        sl = [event for event in self.events if event.get_output_throughput() != -1]
        if len(sl) == 0:
            return -1
        return sum(e.get_output_throughput() for e in sl) / len(sl)

    def get_avg_input_rs(self):
        sl = [event for event in self.events if event.get_input_rs() != -1]
        if len(sl) == 0:
            return -1
        return sum(e.get_input_rs() for e in sl) / len(sl)

    def get_avg_output_rs(self):
        sl = [event for event in self.events if event.get_output_rs() != -1]
        if len(sl) == 0:
            return -1
        return sum(e.get_output_rs() for e in sl) / len(sl)

    def get_avg_oi_ratio(self):
        sl = [event for event in self.events if event.get_oi_ratio() != -1]
        if len(sl) == 0:
            return -1
        return sum(e.get_oi_ratio() for e in sl) / len(sl)


    def print_rows(self):
        ph = ['start_offset', 'compaction_time_seconds', 'output_level', 'num_output_files', 'total_output_size', 'num_input_records', 'num_output_records', 'output (MB/s)', 'input (r/s)', 'output (r/s)', 'output/input ratio']
        print('\t'.join(ph))
        for event in self.events:
            event.print_row()

class CompactionEvent():
    def __init__(self, dt, parent, json_str):
        self.dt = dt
        self.parent = parent
        self.json_str = json_str
        self.data = json.loads(json_str)

    def print_data(self):
        print(json.dumps(self.data, sort_keys=True, indent=4, separators=(',', ': ')))

    def get_compaction_time_micros(self):
        return self.data.get('compaction_time_micros', -1)

    def get_compaction_time_seconds(self):
        return self.get_compaction_time_micros() / 1000000.0

    def unix_time(self):
        return unix_time(self.dt)

    def get_rel_start_seconds(self):
        return (self.dt - self.parent.start_dt).total_seconds()

    def get_output_level(self):
        return self.data.get('output_level', -1)

    def get_num_output_files(self):
        return self.data.get('num_output_files', -1)

    def get_total_output_size(self):
        return self.data.get('total_output_size', -1)

    def get_num_input_records(self):
        return self.data.get('num_input_records', -1)

    def get_num_output_records(self):
        return self.data.get('num_output_records', -1)

    def get_output_throughput(self):
        return self.get_total_output_size() / (1024*1024*self.get_compaction_time_seconds())

    def get_input_rs(self):
        return self.get_num_input_records() / self.get_compaction_time_seconds()

    def get_output_rs(self):
        return self.get_num_output_records() / self.get_compaction_time_seconds()

    def get_oi_ratio(self):
        return 1.0 * self.get_num_output_records() / self.get_num_input_records()

    def print_row(self):
        pl = []
        pl.append(self.get_rel_start_seconds())
        pl.append(self.get_compaction_time_seconds())
        pl.append(self.get_output_level())
        pl.append(self.get_num_output_files())
        pl.append(self.get_total_output_size())
        pl.append(self.get_num_input_records())
        pl.append(self.get_num_output_records())
        pl.append(self.get_output_throughput())
        pl.append(self.get_input_rs())
        pl.append(self.get_output_rs())
        pl.append(self.get_oi_ratio())
        print('\t'.join(map(str, pl)))

if __name__ == '__main__':
    ctx = parse_args()
    logs = []
    for fn in ctx.FILE:
        logs.append(LogData(ctx, fn))

    if ctx.summary:
        print_summary(logs)
    for log in logs:
        print('')
        print(log.fn)
        print('')
        log.print_rows() 
