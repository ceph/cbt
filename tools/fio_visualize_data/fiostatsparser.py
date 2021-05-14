#!/usr/bin/python3

import os
import sys
import json
import time
import fioplotcommon as common
import pandas as pd
import numpy as np

class Fiostatsparser():
  def __init__(self, ctx):
    self.fiojsonfiles = []
    self.fiocsvfiles = []
    self.getfiofiles(ctx.srcdir, ctx.ftype)
    # Output CSV filea
    self.basecsvfile = '/client_csv_data_'
    self.baseopcsvfn = ctx.destdir + self.basecsvfile
    self.csvsep = ","
    self.csvfilename = None

  def getfiofiles(self, srcdir, ftype):
    files = [os.path.join(srcdir, fname) for fname in next(os.walk(srcdir))[2]]

    # Filter files based on passed filetype
    for f in files:
      if ftype in os.path.split(f)[1]:
        self.fiojsonfiles.append(f)
      else:
        self.fiocsvfiles.append(f)

  def get_output_csv_filename(self):
    if not self.csvfilename:
      date = time.strftime("%m-%d-%Y", time.localtime())
      ltime = time.strftime("%I:%M-%S%p", time.localtime())
      timestamp = date + "_" + ltime
      self.csvfilename = self.baseopcsvfn + timestamp + ".txt"
    return self.csvfilename

class Parsejson(Fiostatsparser):
  def __init__(self, ctx):
    super().__init__(ctx)
    self.JOBS = 'jobs'
    self.BWBYTES = 'bw_bytes'
    self.IOPS = 'iops'
    self.SLAT = 'slat_ns'
    self.CLAT = 'clat_ns'
    self.LAT = 'lat_ns'
    self.MEAN = 'mean'
    self.PERCT = 'percentile'
    self._95TH = '95.000000'
    self._99TH = '99.000000'
    self._995TH = '99.500000'
    self._999TH = '99.900000'
    self._9995TH = '99.950000'
    self._9999TH = '99.990000'
    self.MILLION = 1000000
    self.MBYTE = 1024 * 1024
    self.OPTYPE = ctx.optype
    self.metric = ctx.metric
    self.fioglobalkeys = []
    self.fiometrickeys = {}
    self.fiometricdata = {}
    self.fiosorteddata = {}
    # Parse data and convert to csv format
    self.parse_json_data()
    self.dump_stats_in_csv()

  def parse_json_data(self):
    self.fiometricdata[self.metric] = {}
    self.fiometrickeys[self.metric] = []
    for f in self.fiojsonfiles:
      with open(f, 'r') as json_file:
        f_info = os.fstat(json_file.fileno())
        if f_info.st_size == 0:
          print('JSON input file %s is empty'%f)
          sys.exit(1)

        # load json data
        json_data = json.load(json_file)
        # Extract latency specific data
        for data in json_data:
          if data == self.JOBS:
            op_data = json_data[self.JOBS][0][self.OPTYPE]
            fn = os.path.split(f)[1]
            self.fioglobalkeys.append(fn)
            self.fiometricdata[self.metric][fn] = {}

            # Get bandwidth & iops data
            if self.metric == 'bw':
              bwdata = self.parse_bw_stats(op_data)
              self.fiometricdata[self.metric][fn].update(bwdata)
            # Get latency data
            if self.metric == 'lat':
              latdata = self.parse_latency_stats(op_data)
              self.fiometricdata[self.metric][fn].update(latdata)
            # Get percentile dataa
            if self.metric == 'pct':
              pctdata = self.parse_percentile_stats(op_data)
              self.fiometricdata[self.metric][fn].update(pctdata)

  def parse_bw_stats(self, data):
    bwstats = {}
    bwstats[self.BWBYTES] = float(data[self.BWBYTES])/self.MBYTE
    #bwstats[self.IOPS] = float(data[self.IOPS])
    return bwstats

  def parse_latency_stats(self, data):
    stats = {}
    keys = [self.SLAT, self.CLAT, self.LAT]
    for key in keys:
      stats[key] =  float(data[key][self.MEAN])/self.MILLION
      # Set the latency related keys.
      if not (self.fiometrickeys[self.metric]) or\
        (len(self.fiometrickeys[self.metric]) < len(keys)):
        self.fiometrickeys[self.metric].append(key.split('_')[0])
    return stats

  def parse_percentile_stats(self, data):
    clatpctstats = data[self.CLAT][self.PERCT]
    pstats = {}
    keys = [self._95TH, self._99TH, self._995TH, self._999TH, self._9995TH, self._9999TH]
    for key in keys:
      pstats[key] = float(clatpctstats[key])/self.MILLION
      # Set the percentile related keys.
      if not (self.fiometrickeys[self.metric]) or\
        (len(self.fiometrickeys[self.metric]) < len(keys)):
        self.fiometrickeys[self.metric].append("%.2f" % float(key))
    return pstats

  def get_fiometric_data(self):
    return common.sort_map_data_by_key(self.fiometricdata[self.metric])

  def get_fioglobal_keys(self):
    return self.fioglobalkeys

  def get_fiometric_keys(self):
    if self.metric == 'bw':
      self.fiometrickeys[self.metric] = self.fiosorteddata.keys()
    return self.fiometrickeys[self.metric]

  def dump_stats_in_csv(self):
    fiodata = {}
    # Build stats to write
    for fn in self.fioglobalkeys:
      statsdata = []
      statsdata.append(fn)
      for value in [*self.fiometricdata[self.metric][fn].values()]:
        statsdata.append(value)
      fiodata[fn] = statsdata[1:]

    # Sort the entire data according to filename
    self.fiosorteddata = common.sort_map_data_by_key(fiodata)

    # Open csv file
    with open(self.get_output_csv_filename(), "w+") as f:
      # Convert to csv
      for fn in self.fiosorteddata.keys():
        csvstr = [str(data) for data in self.fiosorteddata[fn]]
        csvstr.insert(0, fn)
        csventry = self.csvsep.join(csvstr) + "\n"
        # Write to csv file
        f.write(csventry)
    print("Created CSV result file: %s" % f.name)

class Parsecsv(Fiostatsparser):
  def __init__(self, ctx):
    super().__init__(ctx)
    self.KILO = 1000
    self.MILLION = 1000000
    self.KBYTE = 1024
    self._4KBYTE = 4 * 1024
    self.MBYTE = 1024 * 1024
    self.OPTYPE = ctx.optype
    self.metric = ctx.metric
    self.timerange = ctx.timerange
    self.subplot = ctx.subplot
    self.fiocsvfile = ctx.filename
    self.fiodataframe = []
    self.fiopctdataframe = []
    self.parse_csv_data()

  def set_df_column_headers(self):
    if not len(self.fiodataframe):
      print("Error: Empty dataframe!")
      sys.exit(1)

    # Number of columns depends on fio version
    if (len(self.fiodataframe.columns) == 4):
      self.fiodataframe.columns = ['time', self.metric, 'rw', 'offset']
    elif (len(self.fiodataframe.columns) == 5):
      self.fiodataframe.columns = ['time', self.metric, 'rw', 'offset', 'cmdPrio']
    else:
      print("fiostatsparser error: Unable to determine CSV format...aborting.")
      sys.exit(1)

  def parse_csv_data(self):
    with open(self.fiocsvfile, 'r') as csv_file:
      f_info = os.fstat(csv_file.fileno())
      if f_info.st_size == 0:
        print('CSV input file %s is empty'%f)
        sys.exit(1)

      # Read the csv file
      try:
        self.fiodataframe = pd.read_csv(csv_file, sep=',', header=None)
      except ValueError as e:
        print("Error in reading %s. Please check the file format." % self.fiocsvfile)
        print("Exception Details: ", str(e))
        sys.exit(1)

      # Format the data
      self.set_df_column_headers()
      self.fiodataframe['time'] = self.fiodataframe['time'] / self.KILO
      if self.metric == 'lat':
        self.fiodataframe[self.metric] = self.fiodataframe[self.metric] / self.MILLION
      if self.metric == 'bw':
        self.fiodataframe[self.metric] = (self.fiodataframe[self.metric] * self._4KBYTE) / (self.MBYTE)
      if self.metric == 'pct':
        self.fiodataframe['lat'] = self.fiodataframe[self.metric] / self.MILLION
        self.generate_percentile_stats()

  def get_df_stats(self, getrange=False):
    if self.metric is None:
      print("Error: Unknown metric!")
      sys.exit(1)

    start = 0
    end = 0
    if getrange and len(self.timerange):
      start = self.timerange[0]
      end = self.timerange[1]
      print("Getting data with range: %s end: %s" % (start, end))

    # Find row that matches the start time
    startrow = self.fiodataframe.loc[self.fiodataframe['time'] >= float(start)].index[0]
    if not end:
      endrow = len(self.fiodataframe) - 1
    else:
      endrow = self.fiodataframe.loc[self.fiodataframe['time'] >= float(end)].index[0]

    #print("startrow: %d, endrow: %d" % (startrow, endrow))

    # Calculate mean and stdev of the dataframe metric
    mean = self.fiodataframe.loc[startrow:endrow, self.metric].mean()
    stdev = self.fiodataframe.loc[startrow:endrow, self.metric].std()
    #print("MEAN: %f" % mean)
    #print("STDEV: %f" % stdev)

    # Create pandas series of the mean value.
    mean = pd.Series([mean] * ((endrow - startrow) + 1))

    return mean, stdev, startrow, endrow

  def generate_percentile_stats(self):
    start_t = 0
    end_t = 0
    if self.timerange is not None and len(self.timerange):
      start_t = float(self.timerange[0])
      end_t = float(self.timerange[1])

    # Find start row
    if not end_t:
      end_t = self.fiodataframe['time'][len(self.fiodataframe) - 1]

    print("Calculating percentile[95, 99, 99.5] from start: %s to end: %s\n" % (start_t, end_t))
    self.fiopctdataframe = pd.DataFrame({}, columns = ['time', 'samples', 'avg', '95th', '99th', '99.5th'])
    delimiter = ', '
    colheaders = delimiter.join(self.fiopctdataframe.columns.values.tolist())
    print(colheaders)
    # Start a loop that calculates percentile stats for
    # each second between the desired time range.
    for t in np.arange(start_t, (end_t - 1), 1.0):
      start_r = self.fiodataframe.loc[self.fiodataframe['time'] >= t].index[0]
      end_r = self.fiodataframe.loc[self.fiodataframe['time'] >= (t+1.0)].index[0]
      tmp_df = self.fiodataframe.loc[start_r:end_r, 'lat'].sort_values()
      # Find the percentile in the sorted range
      pct_list = tmp_df.quantile([.95,.99,.995]).to_list()
      mean = self.fiodataframe.loc[start_r:end_r, 'lat'].mean()
      samples = "%d" % ((end_r - start_r) + 1)
      pctdata = { 'time': t + 1, 'samples': samples, 'avg': mean,\
                  '95th': pct_list[0], '99th': pct_list[1], '99.5th': pct_list[2] }
      self.fiopctdataframe = self.fiopctdataframe.append(pctdata, ignore_index=True)
      entry = self.fiopctdataframe.values[len(self.fiopctdataframe) - 1].tolist()
      print(delimiter.join(str(e) for e in entry))
    # Write to csv file
    pct_csv_file = self.get_output_csv_filename()
    self.fiopctdataframe.to_csv(pct_csv_file, index=None)
    print("Created CSV percentile stats file: %s" % pct_csv_file)
