#!/usr/bin/python3

import json
import time
import numpy as np
from matplotlib import pyplot as plt

class Fioplotter():
  def __init__(self, ctx):
    # Initialize basic parameters
    self.destination = ctx.destdir
    self.basefile = '/client_stats_plot_'
    self.baseplotfilename = self.destination + self.basefile

  def get_plot_filename(self):
    date = time.strftime("%m_%d_%Y", time.localtime())
    ltime = time.strftime("%I_%M_%S%p", time.localtime())
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
    ax.set_xticks(self.index)

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
    if len(self.yticklabels):
      ax.set_yticklabels(self.yticklabels)
    fig.tight_layout()

    plt.grid(which='major', color='#666666', linestyle='-', alpha=0.5)
    plt.minorticks_on()
    plt.grid(which='minor', color='#999999', linestyle='-', alpha=0.2)

    plotfilename = self.get_plot_filename()
    plt.savefig(plotfilename, format=self.imgformat, dpi=300)
    print("Created plot: ", plotfilename)

    print("Created plot: ", plotfilename, "\n")

