#!/usr/bin/python3

import os
import sys
import json
import time
import fioplotcommon as common

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
      date = time.strftime("%m_%d_%Y", time.localtime())
      ltime = time.strftime("%I_%M_%S%p", time.localtime())
      timestamp = date + "_" + ltime
      self.csvfilename = self.baseopcsvfn + timestamp + ".csv"
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
    self._50TH = '50.000000'
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
    keys = [self._50TH, self._95TH, self._99TH, self._995TH, self._999TH, \
            self._9995TH, self._9999TH]
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

