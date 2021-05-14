#!/usr/bin/python3

import json
import time
import sys
import numpy as np
from matplotlib import pyplot as plt

class Fioplotter():
  def __init__(self, ctx):
    # Initialize basic parameters
    self.destination = ctx.destdir
    self.basefile = '/client_stats_plot_'
    self.baseplotfilename = self.destination + self.basefile

  def get_plot_filename(self):
    date = time.strftime("%m-%d-%Y", time.localtime())
    ltime = time.strftime("%I:%M-%S%p", time.localtime())
    timestamp = date + "_" + ltime
    plotfilename = self.baseplotfilename + self.metric + "_" + timestamp
    # Add the specified extension
    plotfilename = plotfilename + '.' + self.imgformat
    return plotfilename

  def generate_plot():
    pass

class Barplot(Fioplotter):
  def __init__(self, ctx, pj):
    super().__init__(ctx)
    self.ctx = ctx
    self.pj = pj
    self.metric = ctx.metric
    self.imgformat = ctx.imgformat
    self.plotdata = pj.get_fiometric_data()

    # Initialize generic plot information
    self.ylabel = ''
    self.xlabel = ''
    self.title = ''
    self.xticklabels = pj.get_fiometric_keys()
    self.index = np.arange(len(self.xticklabels))
    self.yticklabels = []
    self.legend = []
    self.bar_width = 0.1
    # Generate plot metadata based on metric
    if self.metric == 'bw':
      self.bwdata = []
      self.set_bw_metadata()
    if self.metric == 'lat':
      self.set_lat_metadata()
    if self.metric == 'pct':
      self.set_pct_metadata()
    self.generate_plot()

  def set_bw_metadata(self):
    self.ylabel = 'Throughput (MiB/s)'
    self.xlabel = 'Test Runs'
    self.title = self.ctx.optype + ' avg throughput'
    for f in self.plotdata.keys():
      self.bwdata.append(round(list(self.plotdata[f].values())[0], 3))

  def set_lat_metadata(self):
    self.ylabel = 'latency (msec)'
    self.xlabel = 'latency type'
    self.title = self.ctx.optype + ' avg latencies'

  def set_pct_metadata(self):
    self.ylabel = 'latency (msec)'
    self.xlabel = 'clat percentiles'
    self.title = self.ctx.optype + ' clat percentiles'

  def generate_plot(self):
    # Create subplot
    fig, ax = plt.subplots()

    if self.metric == 'bw':
      ax.bar(self.xticklabels, self.bwdata, self.bar_width)
      ax.set_xticklabels(self.xticklabels, rotation=45, ha='right')
      self.legend.append(self.metric)
      ax.legend(self.legend, loc='upper right', framealpha=0.5)
    else:
      width = 0
      for f in self.plotdata.keys():
        y = list(self.plotdata[f].values())
        ax.bar(self.index + width, y, self.bar_width, label=f)
        width += self.bar_width
      if len(self.xticklabels):
        ax.set_xticklabels(self.xticklabels)
      ax.legend()

    ax.set_ylabel(self.ylabel)
    ax.set_xlabel(self.xlabel)
    ax.set_title(self.title)
    ax.set_xticks(self.index)
    if len(self.yticklabels):
      ax.set_yticklabels(self.yticklabels)
    fig.tight_layout()

    plt.grid(b=True, which='major', color='#666666', linestyle='-', alpha=0.5)
    plt.minorticks_on()
    plt.grid(b=True, which='minor', color='#999999', linestyle='-', alpha=0.2)

    plotfilename = self.get_plot_filename()
    plt.savefig(plotfilename, format=self.imgformat, dpi=300)
    print("Created plot: ", plotfilename)

class Lineplot(Fioplotter):
  def __init__(self, ctx, pcsv):
    super().__init__(ctx)
    self.ctx = ctx
    self.pcsv = pcsv
    self.metric = ctx.metric
    self.imgformat = ctx.imgformat
    self.createsubplot = ctx.subplot
    self.timerange = ctx.timerange

    # Initialize generic plot information
    self.ylabel = ''
    self.xlabel = ''
    self.title = ''
    self.xticklabels = []
    self.yticklabels = []
    self.legend = []

    # Create the plot(s)
    self.generate_plot()

  def set_plot_metadata(self, mean=None, stdev=None, settitle=True, setxlabel=True):
    if len(self.legend): self.legend.clear()
    self.title = ""
    if setxlabel:
      self.xlabel = 'time (secs)'

    # Treat percentile a little different
    if self.metric == 'pct':
      self.ylabel = 'latency (msec)'
      self.title = " Avg, 95th, 99th and 99.5th Percentiles"
      for col in self.pcsv.fiopctdataframe.columns:
        if col == 'time' or col == 'samples':
          continue
        self.legend.append(col)
      return

    if mean is not None:
      mean = round(float(mean), 2)
    if stdev is not None:
      stdev = round(float(stdev), 2)
    self.legend.append(self.metric)
    legend = ""
    if self.metric == 'lat':
      self.ylabel = 'latency (msec)'
      legend = " mean: %s msec stdev: %s msec" % (str(mean), str(stdev))
    if self.metric == 'bw':
      self.ylabel = 'bandwidth (MiB/s)'
      legend = " mean: %s MiB/s stdev: %s MiB/s" % (str(mean), str(stdev))
    if settitle:
      self.title = self.ctx.optype + ' ' + self.metric
    self.legend.append(legend)

  def generate_plot(self):
    if self.metric == 'pct':
      getrange = (self.timerange is not None and len(self.timerange))
      fig, ax = plt.subplots()
      self.set_plot_metadata()
      self.plot_pct_chart(ax)
    elif self.createsubplot:
      subplotrows = 2 # Two plots in one chart for now
      subplotcols = 1
      settitle = True
      setxlabel = False
      fig, ax = plt.subplots(subplotrows, subplotcols)
      for i in range(subplotrows):
        if i == (subplotrows - 1):
          mean, stdev, startrow, endrow = self.pcsv.get_df_stats(getrange=True)
          settitle = False
          setxlabel = True
        else:
          mean, stdev, startrow, endrow = self.pcsv.get_df_stats()
        self.set_plot_metadata(mean[0], stdev, settitle, setxlabel)
        self.plot_chart(ax[i], startrow, endrow, mean)
    else:
      getrange = (self.timerange is not None and len(self.timerange))
      mean, stdev, startrow, endrow = self.pcsv.get_df_stats(getrange)
      fig, ax = plt.subplots()
      self.set_plot_metadata(mean[0], stdev)
      self.plot_chart(ax, startrow, endrow, mean)

    plotfilename = self.get_plot_filename()
    plt.savefig(plotfilename, format=self.imgformat, dpi=300)
    print("Created plot: ", plotfilename)

  def plot_chart(self, ax=None, startrow=0, endrow=0, mean=None):
    if not ax or not endrow:
      print("Error: Nothing to plot!")
      sys.exit(1)

    # Plot the metric chart
    ax.plot(self.pcsv.fiodataframe.loc[startrow:endrow, 'time'],\
            self.pcsv.fiodataframe.loc[startrow:endrow, self.metric],\
            linewidth=1, alpha=0.8)
    # Plot the average/mean line over the metric chart if requested
    if mean is not None and (len(mean) == (endrow - startrow) + 1):
      ax.plot(self.pcsv.fiodataframe.loc[startrow:endrow, 'time'], mean,\
              color='r', linestyle='--', linewidth=0.8)
    self.apply_subplot_labels(ax)

  def plot_pct_chart(self, ax=None):
    if not ax:
      print("Error: Nothing to plot!")
      sys.exit(1)

    # Get columns from dataframe
    for col in self.pcsv.fiopctdataframe.columns:
      if col == 'time' or col == 'samples':
        continue
      # Plot the metric chart
      ax.plot(self.pcsv.fiopctdataframe['time'],\
              self.pcsv.fiopctdataframe[col],\
              linewidth=1, alpha=0.8)
    self.apply_subplot_labels(ax)

  def apply_subplot_labels(self, ax=None):
    if ax is None: return
    ax.set_ylabel(self.ylabel)
    ax.set_xlabel(self.xlabel)
    ax.set_title(self.title)
    if len(self.xticklabels):
      ax.set_xticklabels(self.xticklabels)
    if len(self.yticklabels):
      ax.set_yticklabels(self.yticklabels)
      ax.set_yticks(np.arange(len(self.yticklabels)))
    ax.legend(self.legend, loc='upper right')
    ax.grid(b=True, which='major', color='#666666', linestyle='-', alpha=0.5)
    ax.minorticks_on()
    ax.grid(b=True, which='minor', color='#999999', linestyle='-', alpha=0.2)

