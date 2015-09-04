import math

try:
    from scipy import stats
    from numpy import array, linalg
    from scipy.optimize import leastsq
    from numpy.polynomial.chebyshev import chebfit, chebval
    no_numpy = False
except ImportError:
    no_numpy = True

__doc__ = """
This module contains functions for processing test results.
Main function is data_property.
"""


def average(data):
    return sum(data) / len(data)


def mediana(vals):
    return sum(vals) / len(vals)


def deviation(vals):
    med = mediana(vals)
    squares_sum = sum(abs(med - i) ** 2.0 for i in vals)
    return ((squares_sum / len(vals)) ** 0.5)


def round_3_digit(val):
    return round_val_and_deviation((val, val / 10.0))[0]


def round_val_and_deviation(val, dev):
    if dev < 1E-7:
        return val, dev

    dev_div = 10.0 ** (math.floor(math.log10(dev)) - 1)

    dev1 = int(dev / dev_div) * dev_div
    val1 = int(val / dev_div) * dev_div

    return [type(val)(val1), type(dev)(dev1)]


def approximate_curve(x, y, xnew, curved_coef):
    """returns ynew - y values of some curve approximation"""
    if no_numpy:
        raise ValueError("No numpy found")

    return chebval(xnew, chebfit(x, y, curved_coef))


def approximate_line(x, y, xnew, relative_dist=False):
    """ x, y - test data, xnew - dots, where we want find approximation
        if not relative_dist distance = y - newy
        returns ynew - y values of linear approximation"""

    if no_numpy:
        raise ValueError("No numpy found")

    # convert to numpy.array (don't work without it)
    ox = array(x)
    oy = array(y)

    # set approximation function
    def func_line(tpl, x):
        return tpl[0] * x + tpl[1]

    def error_func_rel(tpl, x, y):
        return 1.0 - y / func_line(tpl, x)

    def error_func_abs(tpl, x, y):
        return y - func_line(tpl, x)

    # choose distance mode
    error_func = error_func_rel if relative_dist else error_func_abs

    tpl_initial = tuple(linalg.solve([[ox[0], 1.0], [ox[1], 1.0]],
                                     oy[:2]))

    # find line
    tpl_final, success = leastsq(error_func,
                                 tpl_initial[:],
                                 args=(ox, oy))

    # if error
    if success not in range(1, 5):
        raise ValueError("No line for this dots")

    # return new dots
    return func_line(tpl_final, array(xnew))


def difference(y, ynew):
    """returns average and maximum relative and
       absolute differences between y and ynew
       result may contain None values for y = 0
       return value - tuple:
       [(abs dif, rel dif) * len(y)],
       (abs average, abs max),
       (rel average, rel max)"""

    abs_dlist = []
    rel_dlist = []

    for y1, y2 in zip(y, ynew):
        # absolute
        abs_dlist.append(y1 - y2)

        if y1 > 1E-6:
            rel_dlist.append(abs(abs_dlist[-1] / y1))
        else:
            raise ZeroDivisionError("{0!r} is too small".format(y1))

    da_avg = sum(abs_dlist) / len(abs_dlist)
    dr_avg = sum(rel_dlist) / len(rel_dlist)

    return (zip(abs_dlist, rel_dlist),
            (da_avg, max(abs_dlist)), (dr_avg, max(rel_dlist))
            )


class StatProperties(object):
    """
    Statustical properties of array of data
    average
    mediana
    perc_95 - 95 percentile
    perc_05 - 5 percentile
    deviation
    confidence - 95% confidence interval for average
    min
    max
    raw - original data
    """
    def __init__(self):
        # average value
        self.average = None

        # mediana value
        self.mediana = None

        # 95 percentile
        self.perc_95 = None

        # 5 percentile
        self.perc_05 = None

        # deviation
        self.deviation = None

        # 95% confidence interval for average
        self.confidence = None

        # minimal and maximum value
        self.min = None
        self.max = None

        # array of raw values
        self.raw = None

    def rounded_average_conf(self):
        return round_val_and_deviation(self.average, self.confidence)

    def rounded_average_dev(self):
        return round_val_and_deviation(self.average, self.deviation)

    def __str__(self):
        return "{0}({1} ~ {2})".format(self.__class__.__name__,
                                       round_3_digit(self.average),
                                       round_3_digit(self.deviation))

    def __repr__(self):
        return str(self)


def data_property(data, confidence=0.95):
    """
    calculate StatProperties for data
    """
    res = StatProperties()
    if len(data) == 0:
        return res

    data = sorted(data)
    res.average, res.deviation = round_val_and_deviation(data)
    res.max = data[-1]
    res.min = data[0]

    ln = len(data)
    if ln % 2 == 0:
        res.mediana = (data[ln / 2] + data[ln / 2 - 1]) / 2
    else:
        res.mediana = data[ln / 2]

    res.perc_95 = data[int((ln - 1) * 0.95)]
    res.perc_05 = data[int((ln - 1) * 0.05)]

    if not no_numpy and ln >= 3:
        res.confidence = stats.sem(data) * \
                         stats.t.ppf((1 + confidence) / 2, ln - 1)
    else:
        res.confidence = res.deviation

    res.raw = data[:]
    return res
