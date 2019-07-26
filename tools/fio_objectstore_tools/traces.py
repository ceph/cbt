#!env python3

import babeltrace
import sys
import json
import os
import subprocess
import re
import datetime
import itertools


def open_trace(rdir):
    tdir_prefix = os.path.join(rdir, 'trace/ust/uid')
    uid = os.listdir(tdir_prefix)[0]
    ret = babeltrace.TraceCollection()
    ret.add_trace(os.path.join(tdir_prefix, uid, '64-bit'), 'ctf')
    return ret.events

class Event(object):
    @staticmethod
    def map_name_to_subtype(name):
        if 'bluestore:transaction_' in name:
            return TEvent.map_name_to_subtype(name)
        else:
            return None

    def __init__(self, name, timestamp, properties):
        self.name = name
        self.timestamp = timestamp
        self.properties = self.filter_properties(properties)

    def __getitem__(self, key):
        return self.properties.get(key)

    def filter_properties(self, properties):
        pass

    def __str__(self):
        return "Event(name: {name}, timestamp: {timestamp}, {properties})".format(
            name=self.name,
            timestamp=self.timestamp,
            properties=self.properties)


class TEvent(Event):
    @staticmethod
    def get_subtypes():
        return \
            { 'bluestore:transaction_state_duration': TStateDuration
            , 'bluestore:transaction_total_duration': TTotalDuration
            , 'bluestore:transaction_commit_latency': TCommitLatency
            , 'bluestore:transaction_kv_submit_latency': TKVSubmitLatency
            , 'bluestore:transaction_kv_sync_latency': TKVSyncLatency
            , 'bluestore:transaction_initial_state': TInitial
            , 'bluestore:transaction_initial_state_rocksdb': TRocksInitial
            }

    def get_param_map(self):
        pass

    def filter_properties(self, properties):
        ret = dict(((v[0], v[1](properties[k])) for k, v
                    in self.get_param_map().items()))
        ret.update({
            'sequencer_id': int(properties['sequencer_id']),
            'tid': int(properties['tid'])
        })
        return ret

    @staticmethod
    def get_param_types():
        param_list = itertools.chain(
            *((t for _, t in v.get_param_map().items())
              for _, v in TEvent.get_subtypes().items()))
        return dict(((v[0], tuple(v[1:])) for v in param_list))

    @staticmethod
    def map_name_to_subtype(name):
        return TEvent.get_subtypes()[name]

    def get_event_id(self):
        return (self['sequencer_id'], self['tid'])

    def get_params(self):
        ret = self.properties.copy()
        del ret['sequencer_id']
        del ret['tid']
        return ret

    def is_final_event(self):
        return False


class TStateDuration(TEvent):
    STATE_MAP = \
        { 19: "prepare"
        , 20: "aio_wait"
        , 21: "io_done"
        , 22: "kv_queued"
        , 23: "kv_submitted"
        , 24: "kv_done"
        , 25: "deferred_queued"
        , 26: "deferred_cleanup"
        , 27: "deferred_done"
        , 28: "finishing"
        , 29: "done"
        }

    @staticmethod
    def get_param_map():
        return dict((v, ('state_' + v + '_duration', float, 's')) for _, v
                    in TStateDuration.STATE_MAP.items())

    def filter_properties(self, properties):
        state_name = TStateDuration.STATE_MAP[int(properties['state'])]
        return {
            'state_' + state_name + '_duration': float(properties['elapsed']),
            'sequencer_id': int(properties['sequencer_id']),
            'tid': int(properties['tid'])
            }

class TTotalDuration(TEvent):
    @staticmethod
    def get_param_map():
        return { 'elapsed': ('total_duration', float, 's') }

    def is_final_event(self):
        return True


class TCommitLatency(TEvent):
    @staticmethod
    def get_param_map():
        return { 'elapsed': ('commit_latency', float, 's') }


class TKVSubmitLatency(TEvent):
    @staticmethod
    def get_param_map():
        return { 'elapsed': ('kv_submit_latency', float, 's') }


class TKVSyncLatency(TEvent):
    @staticmethod
    def get_param_map():
        return \
            { 'elapsed': ('kv_sync_latency', float, 's')
            , 'kv_batch_size': ('kv_batch_size', int, 'n')
            , 'deferred_done_batch_size': ('deferred_done_batch_size', int, 'n')
            , 'deferred_stable_batch_size': ('deferred_stable_batch_size', int, 'n')
            }


class TInitial(TEvent):
    @staticmethod
    def get_param_map():
        return dict(((k, (k, t, u)) for k, t, u in
            [ ('current_kv_throttle_cost', int, 'bytes')
            , ('current_deferred_throttle_cost', int, 'bytes')
            , ('pending_kv_ios', int, 'ios')
            , ('pending_deferred_ios', int, 'ios')
            , ('ios_started_since_last_traced_io', int, 'iops')
            , ('ios_completed_since_last_traced_io', int, 'iops')
            , ('throttle_time', float, 's')
            ]))


class TRocksInitial(TEvent):
    @staticmethod
    def get_param_map():
        return dict(((k, (k, int, 'n')) for k in [
	    'rocksdb_base_level',
	    'rocksdb_estimate_pending_compaction_bytes',
	    'rocksdb_cur_size_all_mem_tables',
            'rocksdb_compaction_pending',
            'rocksdb_mem_table_flush_pending',
            'rocksdb_num_running_compactions',
            'rocksdb_num_running_flushes',
            'rocksdb_actual_delayed_write_rate'
        ]))


DATE = '(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d\d\d\d\d\d\d\d\d)'
OFFSET = '\(\+(\?\.\?+|\d\.\d\d\d\d\d\d\d\d\d)\)'
NAME = 'bluestore:[a-z_]*'
PAIRS = '{((?: [a-z_]+ = [0-9.e+-]+[ ,])+)}'
RE = re.compile(
    '\[' + DATE + '\] [a-z0-9]* (?P<name>' + NAME + '): { \d* }, ' + PAIRS
    )
def parse(line):
    res = RE.match(line)
    if not res:
        print(line)
    assert res
    groups = res.groups()
    start = datetime.datetime.strptime(groups[0][:-3], "%Y-%m-%d %H:%M:%S.%f")
    name = groups[1]
    props = {}
    for pair in (x.strip() for x in groups[2].split(',')):
        k, v = pair.split('=')
        k = k.strip()
        props[k] = v.strip()
    return Event.map_name_to_subtype(name)(name, start.timestamp(), props)


def test():
    test = '[20:01:49.773714486] (+?.?????????) incerta05 bluestore:transaction_initial_state: { 22 }, { sequencer_id = 1, tid = 1, transaction_bytes = 399, transaction_ios = 1, total_pending_bytes = 399, total_pending_ios = 1, total_pending_kv = 1 }'
    test2 = '[20:01:49.774633030] (+0.000918544) incerta05 bluestore:transaction_state_duration: { 22 }, { sequencer_id = 1, tid = 1, state = 19, elapsed = 4859 }'
    parse(test)
    parse(test2)


def open_trace(rdir):
    tdir_prefix = os.path.join(rdir, 'trace/')
    CMD = ['babeltrace', '--no-delta', '--clock-date', '-n', 'payload',
           tdir_prefix]
    proc = subprocess.Popen(
        CMD,
        bufsize=524288,
        stdout=subprocess.PIPE,
        stderr=sys.stderr)
    for line in proc.stdout.readlines():
        yield parse(line.decode("utf-8"))


class Write(object):
    @staticmethod
    def get_features():
        ret = {
            'time': (lambda e: e.get_start(), float, 's')
        }
        ret.update(dict(
            ((k, ((lambda k1: lambda e: e.get_param(k1))(k), v[0], v[1]))
             for k, v in TEvent.get_param_types().items())
        ))
        return ret

    def __init__(self, event_id):
        self.__id = event_id
        self.__state_durations = {}
        self.__start = None
        self.__duration = None
        self.__commit_latency = None
        self.__params = {}

    def consume_event(self, event, start):
        #assert event_id(event) == self.__id
        if isinstance(event, TInitial):
            assert self.__start is None
            self.__start = event.timestamp - start

        if isinstance(event, TEvent):
            self.__params.update(event.get_params())
            return event.is_final_event()
        else:
            assert False, "{} not a valid event".format(event)
            return True

    def to_primitive(self):
        return {
            'type': 'write',
            'id': {
                'sequencer_id': self.__id[0],
                'tid': self.__id[1],
                },
            'state_durations': self.__state_durations,
            'start': self.__start,
            'duration': self.__duration,
            'params': self.__params
        }

    def get_start(self):
        assert self.__start is not None
        return self.__start

    def get_param(self, param):
        if param not in self.__params:
            print("{} not in {}".format(param, self.__params))
        assert param in self.__params
        return self.__params[param]


class Aggregator(object):
    def check(self, event):
        return False

    def consume(self, event, start):
        return None


class WriteAggregator(Aggregator):
    def __init__(self):
        self.__live = {}
        self.__count = 0

    def check(self, event):
        return issubclass(type(event), TEvent)

    def consume(self, event, start):
        eid = event.get_event_id()
        if eid not in self.__live:
            self.__live[eid] = Write(eid)

        if self.__live[eid].consume_event(event, start):
            self.__count += 1
            y = self.__live[eid]
            del self.__live[eid]
            return y
        else:
            return None


def iterate_structured_trace(trace):
    count = 0
    last = 0.0
    start = None
    aggregators = [
        WriteAggregator()
        ]
    for event in trace:
        if start is None:
            start = event.timestamp
        if event.timestamp > last + 10:
            last = event.timestamp
            print("Trace processed up to {}s".format(
                int(event.timestamp - start)))
        for agg in aggregators:
            if agg.check(event):
                ret = agg.consume(event, start)
                if ret is not None:
                    yield ret


def dump_structured_trace(tdir, fd):
    trace = open_trace(tdir)
    for p in iterate_structured_trace(trace):
        json.dump(p.to_primitive(), fd, sort_keys=True, indent=2)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump-structured-trace', type=str,
                        help='generate json dump of writes')
    args = parser.parse_args()

    dump_structured_trace(args.dump_structured_trace, sys.stdout)
