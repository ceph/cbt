#!/usr/bin/python3

import json
import time
import numpy as np
import fioplotcommon as common
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
    plotfilename = self.baseplotfilename + self.stattype + "_" + timestamp + ".pdf"
    return plotfilename

class Barplot(Fioplotter):
  def __init__(self, ctx, data, stat):
    super().__init__(ctx)
    self.ctx = ctx
    self.plotdata = common.sort_map_data_by_key(data)
    self.stattype = stat

    # Initialize generic plot information
    self.ylabel = ''
    self.xlabel = ''
    self.title = ''
    self.xticklabels = []
    self.yticklabels = []
    self.bar_width = 0.2
    self.index = 0
    if stat == 'lat':
      self.set_avg_clt_lat_metadata()
    elif stat == 'pct':
      self.set_pct_clt_lat_metadata()
    self.generate_bar_plot()

  def set_avg_clt_lat_metadata(self):
    self.ylabel = 'latency (msec)'
    self.xlabel = 'latency type'
    self.title = self.ctx.optype + '-avg client latencies'
    # TODO: Extract from plot data
    self.xticklabels = ['slat', 'clat', 'lat']
    self.index = np.arange(len(self.xticklabels))

  def set_pct_clt_lat_metadata(self):
    self.ylabel = 'latency (msec)'
    self.xlabel = 'clat percentiles'
    self.title = self.ctx.optype + '-clat client percentiles'
    # TODO: Extract from plot data
    self.xticklabels = ['95th', '99th', '99.50th', '99.90th', '99.95th', '99.99th']
    self.index = np.arange(len(self.xticklabels))

  def generate_bar_plot(self):
    width = 0

    # Create subplot
    fig, ax = plt.subplots()

    for f in self.plotdata.keys():
      y = list(self.plotdata[f].values())
      ax.bar(self.index + width, y, self.bar_width, label=f)
      width += self.bar_width

    ax.set_ylabel(self.ylabel)
    ax.set_xlabel(self.xlabel)
    ax.set_title(self.title)
    ax.set_xticks(self.index)
    if len(self.xticklabels):
      ax.set_xticklabels(self.xticklabels)
    if len(self.yticklabels):
      ax.set_yticklabels(self.yticklabels)
    ax.legend()
    fig.tight_layout()

    plt.grid(b=True, which='major', color='#666666', linestyle='-', alpha=0.5)
    plt.minorticks_on()
    plt.grid(b=True, which='minor', color='#999999', linestyle='-', alpha=0.2)

    plotfilename = self.get_plot_filename()
    plt.savefig(plotfilename)
    print("Created plot: ", plotfilename, "\n")

