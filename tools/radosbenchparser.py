#!/usr/bin/python3

import argparse
import math

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--interval', required=False, type=int, default=1000, help='interval of time in seconds.')
    parser.add_argument('-d', '--divisor', required=False, type=int, default=1, help='divide the results by this value.')
    parser.add_argument('-f', '--full', dest='full', action='store_true', default=False, help='print full output.')
    parser.add_argument('-A', '--all', dest='allstats', action='store_true', default=False,
                        help='print all stats for each interval.')
    parser.add_argument('-a', '--average', dest='average', action='store_true', default=False, help='print the average for each interval.')
    parser.add_argument('-s', '--sum', dest='sum', action='store_true', default=False, help='print the sum for each interval.')
    parser.add_argument('-t', '--type', dest='type', default='curmb', 
                        choices=['curops', 'started', 'finished', 'avgmb', 'curmb', 'avglat'],
                        help='select the rados bench output data to work with (default: %(default)s)')
    parser.add_argument("FILE", help="collectl log output files to parse", nargs="+")
    args = parser.parse_args()
    return args

class Interval():
    def __init__(self, ctx, start, end, series):
        self.ctx = ctx
        self.start = start
        self.end = end
        self.series = series

    def get_samples(self):
        return [item for sublist in [ts.get_samples(self.start, self.end) for ts in self.series] for item in sublist]

    def get_min(self):
        return min([sample.value for sample in self.get_samples()])

    def get_max(self):
        return max([sample.value for sample in self.get_samples()])

    def get_wa(self, samples, weight):
        total = 0
        for sample in samples:
            total += sample.value * sample.get_weight(self.start, self.end)
        return total / weight

    def get_wa_list(self):
        samples_list = [ts.get_samples(self.start, self.end) for ts in self.series]
        return [self.get_wa(samples, 1) for samples in samples_list]

    def get_wa_sum(self):
        return sum(self.get_wa_list())

    def get_wa_avg(self):
        return self.get_wa_sum() / len(self.series)

    def get_wp(self, p):
        samples = self.get_samples()
        samples.sort(key=lambda x: x.value)

        weight = 0
        last = None
        cur = None

        # first find the two samples that straddle the percentile based on weight
        for sample in samples:
            if weight >= len(self.series) * p:
                break
            weight += sample.get_weight(self.start, self.end)
            last = cur
            cur = sample

        # next find weights based inversely on the distance to the percentile boundary
        ld = len(self.series) - weight + cur.get_weight(self.start, self.end)
        cd = weight - len(self.series) * p
        lw =  1 - (ld / (ld + cd))
        cw =  1 - (cd / (ld + cd))

        return last.value * lw + cur.value * cw

    @staticmethod
    def get_ftime(series):
        ftime = 0
        for ts in series:
            if ftime == 0 or ts.last.end < ftime:
                ftime = ts.last.end
        return ftime

    @staticmethod
    def get_intervals(series, itime):
        intervals = []
        ftime = Interval.get_ftime(series)
        start = 0
        end = itime
        while (start < ftime):
            end = ftime if ftime < end else end
            intervals.append(Interval(ctx, start, end, series))
            start += itime
            end += itime
        return intervals

class TimeSeries():
    def __init__(self, ctx, fn):
        self.ctx = ctx
        self.last = None
        self.samples = []
        self.read_data(fn)

    def read_data(self, fn):
        f = open(fn, 'r')
        p_time = 0
        for line in f:

            # First cleanup whitespace
            line = ' '.join(line.split()).rstrip()

            # Throw away lines that don't start with a digit or a space
            if not line[0].isdigit():
                continue
            # Throw away the periodic summary lines
            if 'min lat' in line:
                continue
            # Throw away the first second with no data
            if line[0] == '0':
                continue

            names = ('time', 'curops', 'started', 'finished', 'avgmb', 'curmb', 'lastlat', 'avglat')
            valuesdict = dict(list(zip(names, line.split())))

            value = valuesdict[ctx.type] 
            time = valuesdict['time']
            self.add_sample(p_time, int(time), float(value))
            p_time = int(time)

    def add_sample(self, start, end, value):
        sample = Sample(ctx, start, end, value)
        if not self.last or self.last.end < end:
            self.last = sample
        self.samples.append(sample)

    def get_samples(self, start, end):
        sample_list = []
        for s in self.samples:
            if s.get_weight(start, end) > 0:
                sample_list.append(s)
        return sample_list

class Sample():
    def __init__(self, ctx, start, end, value):
        self.ctx = ctx
        self.start = start
        self.end = end
        self.value = value

    def get_weight(self, start, end):
        # short circuit if not within the bound
        if (end < self.start or start > self.end):
            return 0
        sbound = self.start if start < self.start else start
        ebound = self.end if end > self.end else end
        return float(ebound-sbound) / (end-start)

class Printer():
    def __init__(self, ctx, series):
        self.ctx = ctx
        self.series = series
        self.ffmt = "%0.3f"

    def format(self, data):
        if isinstance(data, float) or isinstance(data, int):
            data = data / self.ctx.divisor
            return self.ffmt % data
        return data

    def print_full(self):
        for i in Interval.get_intervals(self.series, ctx.interval):
            value = ', '.join(self.format(j) for j in i.get_wa_list())
            print("%s, %s" % (self.ffmt % i.end, value))

    def print_sums(self):
        for i in Interval.get_intervals(self.series, ctx.interval):
            print("%s, %s" % (self.ffmt % i.end, self.format(i.get_wa_sum())))


    def print_averages(self):
        for i in Interval.get_intervals(self.series, ctx.interval):
            print("%s, %s" % (self.ffmt % i.end, self.format(i.get_wa_avg())))

    def print_all_stats(self):
        print('end-time, samples, min, avg, median, 90%, 95%, 99%, max')
        for i in Interval.get_intervals(self.series, ctx.interval):
            print((', '.join([
                self.ffmt % i.end,
                "%d" % len(i.get_samples()),
                self.format(i.get_min()),
                self.format(i.get_wa_avg()),
                self.format(i.get_wp(0.5)),
                self.format(i.get_wp(0.9)),
                self.format(i.get_wp(0.95)),
                self.format(i.get_wp(0.99)),
                self.format(i.get_max())
            ])))

    def print_default(self):
        interval = Interval.get_intervals(self.series, Interval.get_ftime(series))[0]
        print(self.format(interval.get_wa_sum()))

if __name__ == '__main__':
    ctx = parse_args()
    series = []
    for fn in ctx.FILE:
        series.append(TimeSeries(ctx, fn))

    p = Printer(ctx, series)

    if ctx.sum:
        p.print_sums()
    elif ctx.average:
        p.print_averages()
    elif ctx.full:
        p.print_full()
    elif ctx.allstats:
        p.print_all_stats()
    else:
        p.print_default()


