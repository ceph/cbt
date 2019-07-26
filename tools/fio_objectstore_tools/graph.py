#!env python3

import numpy as np
import matplotlib
import matplotlib.figure
from traces import Write
import matplotlib.backends
import matplotlib.backends.backend_pdf as backend
from scipy import interpolate

FEATURES = Write.get_features()

def weighted_quantile(values, quantiles, sample_weight=None,
                      values_sorted=False):
    """
    Adapted from: https://stackoverflow.com/questions/21844024/weighted-percentile-using-numpy
    Very close to numpy.percentile, but supports weights.
    NOTE: quantiles should be in [0, 1]!
    :param values: numpy.array with data
    :param quantiles: array-like with many quantiles needed
    :param sample_weight: array-like of the same length as `array`
    :param values_sorted: bool, if True, then will avoid sorting of
        initial array
    :return: numpy.array with computed quantiles.
    """
    values = np.array(values)
    quantiles = np.array(quantiles)
    if sample_weight is None:
        sample_weight = np.ones(len(values))
    sample_weight = np.array(sample_weight)
    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), \
        'quantiles should be in [0, 1]'

    if not values_sorted:
        sorter = np.argsort(values)
        values = values[sorter]
        sample_weight = sample_weight[sorter]

    weighted_quantiles = np.cumsum(sample_weight) - 0.5 * sample_weight
    weighted_quantiles -= weighted_quantiles[0]
    weighted_quantiles /= weighted_quantiles[-1]
    return np.interp(quantiles, weighted_quantiles, values)

def generate_throughput(start, ios):
    P = 1.0
    tp = np.empty_like(start)
    for i in range(len(start)):
        limit = i
        count = 0
        while limit < len(start) and start[limit] - start[i] < P:
            count += ios[limit]
            limit += 1

        if start[limit - 1] - start[i] < (P/4):
            tp[i] = 0
        else:
            tp[i] = count / (start[limit - 1] - start[i])
    return tp

SECONDARY_FEATURES = {
    'prepare_kv_queued_and_submitted': (
        ('state_prepare_duration', 'state_kv_submitted_duration', 'state_kv_queued_duration'),
        's',
        float,
        lambda x, y, z: x + y + z),
    'kv_sync_size': (
        ('kv_batch_size', 'deferred_done_batch_size', 'deferred_stable_batch_size'),
        'n',
        int,
        lambda x, y, z: x + y + z),
    'deferred_batch_size': (
        ('deferred_done_batch_size', 'deferred_stable_batch_size'),
        'n',
        int,
        lambda x, y: x + y),
    'incomplete_ios': (
        ('total_pending_ios', 'total_pending_deferred_ios'),
        'n',
        int,
        lambda x, y: x + y),
    'committing_state': (
        ('state_prepare_duration', 'state_kv_queued_duration', 'state_kv_submitted_duration'),
        's',
        int,
        lambda x, y, z: x + y + z),
    'commit_latency_no_throttle': (
        ('commit_latency', 'throttle_time'),
        's',
        int,
        lambda x, y: x - y),
    'throughput': (
        ('time', 'ios_completed_since_last_traced_io'),
        'iops',
        float,
        lambda t, ios: generate_throughput(t, ios)),
    'weight': (
        ('ios_started_since_last_traced_io',),
        'ratio',
        float,
        lambda x: x.astype(float)),
    'total_throttle': (
        ('current_kv_throttle_cost', 'current_deferred_throttle_cost'),
        'bytes',
        float,
        lambda x, y: x + y),
}

def get_unit(feat):
    if feat in FEATURES:
        return FEATURES[feat][2]
    elif feat in SECONDARY_FEATURES:
        return SECONDARY_FEATURES[feat][1]
    else:
        assert False, "{} isn't a valid feature".format(feat)

def get_dtype(feat):
    if feat in FEATURES:
        return FEATURES[feat][1]
    elif feat in SECONDARY_FEATURES:
        return SECONDARY_FEATURES[feat][2]
    else:
        assert False, "{} isn't a valid feature".format(feat)

def get_features(features):
    s = set()
    gmap = {}
    for ax in features:
        if ax in FEATURES:
            s.add(ax)
            gmap[ax] = (lambda name: (lambda x: x[name]))(ax)
        elif ax in SECONDARY_FEATURES:
            pfeat = list(SECONDARY_FEATURES[ax][0])
            for f in pfeat:
                s.add(f)
            gmap[ax] = (lambda name, pf: lambda x: SECONDARY_FEATURES[name][3](
                *[x[feat] for feat in pf]))(ax, pfeat)
        else:
            assert False, "Invalid feature {}".format(ax)
    return s, gmap

def to_arrays(pfeats, events):
    arrays = [(pfeat, FEATURES[pfeat][0], FEATURES[pfeat][1], [])
              for pfeat in pfeats]

    count = 0
    SIZE = 4096

    for event in events:
        if (count % SIZE == 0):
            for pfeat, _, dtype, l in arrays:
                l.append(np.zeros(SIZE, dtype=dtype))

        offset = count % SIZE
        for name, f, _, l in arrays:
            l[-1][offset] = f(event)

        count += 1

    last_size = count % SIZE
    for name, _, _, l in arrays:
        l[-1] = l[-1][:last_size]

    return dict(((feat, np.concatenate(l).ravel()) for feat, _, _, l in arrays))

class Graph(object):
    def sources(self):
        pass

    def graph(self, ax, *sources):
        pass

    def name(self):
        pass

class Scatter(Graph):
    def __init__(self, x, y, ymax=0):
        self.__sources = [x, y]
        self.__ymax = ymax
        self.__xname = x
        self.__yname = y
        self.__xunit = get_unit(x)
        self.__yunit = get_unit(y)

    def sources(self):
        return ['weight', self.__xname, self.__yname]

    def graph(self, ax, w, x, y):
        bins, x_e, y_e = np.histogram2d(x, y, bins=1000, weights=w)
        z = interpolate.interpn(
            (0.5*(x_e[1:] + x_e[:-1]) , 0.5*(y_e[1:]+y_e[:-1])),
            bins,
            np.vstack([x,y]).T,
            method = "nearest",
            fill_value = None,
            bounds_error = False)

        idx = z.argsort()

        ax.set_xlabel(
            "{name} ({unit})".format(name=self.__xname, unit=self.__xunit),
            fontsize=FONTSIZE
        )
        ax.set_ylabel(
            "{name} ({unit})".format(name=self.__yname, unit=self.__yunit),
            fontsize=FONTSIZE)
        ax.scatter(
            x[idx], y[idx],
            c=z[idx],
            s=1,
            rasterized=True)

        xsortidx = x.argsort()
        xs = x[xsortidx]
        ys = y[xsortidx]
        ws = w[xsortidx]
        per_point = max(150, (len(xs)//50))
        lines = [(t, c, []) for t, c in [(.5, 'green'), (.95, 'red')]]
        limits = []
        idx = 0
        min_per_tick = (xs[-1] - xs[0])/100.0

        if self.__ymax != 0:
            top = weighted_quantile(ys, [self.__ymax], ws)[0]
            bottom = weighted_quantile(ys, [1 - self.__ymax], ws)[0]
            ax.set_ylim(top=top, bottom=bottom - (0.05 * (top - bottom)))

        while idx < len(xs):
            limit = min(
                len(xs),
                max(idx + per_point,
                    np.searchsorted(xs, xs[idx] + min_per_tick)))
            limits.append((xs[idx] + xs[limit - 1]) / 2.0)
            quantiles = weighted_quantile(
                ys[idx:limit],
                [t for t, _, _ in lines],
                ws[idx:limit])
            for p, d in zip(quantiles, [d for _, _, d in lines]):
                d.append(p)
            idx = limit
        for t, c, d in lines:
            ax.plot(limits, d, 'go--', linewidth=1, markersize=2, color=c)

    def name(self):
        return "Scatter({}, {})".format(self.__xname, self.__yname)


class Histogram(Graph):
    def __init__(self, p):
        self.__param = p
        self.__unit = get_unit(p)

    def sources(self):
        return ['weight', self.__param]

    def graph(self, ax, w, p):
        ax.set_xlabel(
            "{name} ({unit})".format(name=self.__param, unit=self.__unit),
            fontsize=FONTSIZE
        )
        ax.set_ylabel(
            "N",
            fontsize=FONTSIZE)
        ax.hist(p, weights=w, bins=50)

    def name(self):
        return "Histogram({})".format(self.__param)

FONTSIZE=4
matplotlib.rcParams.update({'font.size': FONTSIZE})

def graph(events, name, path, graph_format, mask_params=None, masker=None):
    if mask_params is None:
        mask_params = []
    features = set([ax for row in graph_format for g in row for ax in g.sources()]
                   + mask_params)
    pfeat, feat_to_array = get_features(features)

    cols = to_arrays(pfeat, events)

    print("Generated arrays")

    arrays = dict(((feat, t(cols)) for feat, t in feat_to_array.items()))

    if masker is not None:
        mask = masker(*[arrays[x] for x in mask_params])
        arrays = dict(((feat, ar[mask]) for feat, ar in arrays.items()))

    fig = matplotlib.figure.Figure()
    fig.suptitle(name)

    rows = len(graph_format)
    cols = len(graph_format[0])

    fig.set_figwidth(8)
    fig.set_figheight(4 * cols)

    for nrow in range(rows):
        for ncol in range(cols):
            index = (nrow * cols) + ncol + 1
            ax = fig.add_subplot(rows, cols, index)
            grapher = graph_format[nrow][ncol]
            grapher.graph(
                ax,
                *[arrays.get(n) for n in grapher.sources()])

            print("Generated subplot {}".format(grapher.name()))

    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.1, top=0.95)

    if path is not None:
        fig.set_canvas(backend.FigureCanvas(fig))
        print("Generating image")
        fig.savefig(
            path,
            dpi=200,
            format='png')
    else:
        import matplotlib.pyplot as plt
        plt.show()
