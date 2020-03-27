#!/usr/bin/python3

import os
import sys
import json
import time

class Fiostatsparser():
  def __init__(self, ctx):
    self.fiojsonfiles = []
    self.fiocsvfiles = []
    self.getfiofiles(ctx.srcdir, ctx.ftype)
    # Output CSV filea
    self.basecsvfile = '/client_csv_data_'
    self.baseopcsvfn = ctx.destdir + self.basecsvfile

  def getfiofiles(self, srcdir, ftype):
    files = [os.path.join(srcdir, fname) for fname in next(os.walk(srcdir))[2]]

    # Filter files based on passed filetype
    for f in files:
      if ftype in os.path.split(f)[1]:
        self.fiojsonfiles.append(f)
      else:
        self.fiocsvfiles.append(f)

  def get_output_csv_filename(self):
    date = time.strftime("%m-%d-%Y", time.localtime())
    ltime = time.strftime("%I:%M-%S%p", time.localtime())
    timestamp = date + "_" + ltime
    csvfilename = self.baseopcsvfn + timestamp + ".txt"
    return csvfilename

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
    self.fiobwdata = {}
    self.fiolatdata = {}
    self.fiopctdata = {}
    self.parse_json_data()

  def parse_json_data(self):
    for f in self.fiojsonfiles:
      with open(f, 'r') as json_file:
        f_info = os.fstat(json_file.fileno())
        if f_info.st_size == 0:
          print('JSON input file %s is empty'%f)
          sys.exit(1)

        # load json data
        json_data = json.load(json_file)
        # Extract latency specific data
        filestats = {}
        for data in json_data:
          if data == self.JOBS:
            op_data = json_data[self.JOBS][0][self.OPTYPE]
            fn = os.path.split(f)[1]
            self.fiobwdata[fn] = {}
            self.fiolatdata[fn] = {}
            self.fiopctdata[fn] = {}

            # Get bandwidth & iops data
            bwdata = self.parse_bw_stats(op_data)
            self.fiobwdata[fn].update(bwdata)
            # Get latency data
            latdata = self.parse_latency_stats(op_data)
            self.fiolatdata[fn].update(latdata)
            # Get percentile data
            pctdata = self.parse_percentile_stats(op_data)
            self.fiopctdata[fn].update(pctdata)

  def parse_bw_stats(self, data):
    bwstats = {}
    bwstats[self.BWBYTES] = float(data[self.BWBYTES])/self.MBYTE
    bwstats[self.IOPS] = float(data[self.IOPS])

    return bwstats

  def parse_latency_stats(self, data):
    stats = {}
    keys = [self.SLAT, self.CLAT, self.LAT]
    for key in keys:
      stats[key] =  float(data[key][self.MEAN])/self.MILLION

    return stats

  def parse_percentile_stats(self, data):
    clatpctstats = data[self.CLAT][self.PERCT]
    pstats = {}
    keys = [self._95TH, self._99TH, self._995TH, self._999TH, self._9995TH, self._9999TH]
    for key in keys:
      pstats[key] = float(clatpctstats[key])/self.MILLION

    return pstats

  def get_fio_bwdata(self):
    return self.fiobwdata

  def get_fio_latdata(self):
    return self.fiolatdata

  def get_fio_pctdata(self):
    return self.fiopctdata

  def dump_all_stats_in_csv(self):
    separator = ","
    jsonfiofiles = self.fiobwdata.keys()
    # Open csv file
    with open(self.get_output_csv_filename(), "w+") as f:
      # Build stats to write
      for fn in jsonfiofiles:
        csvdata = []
        csvdata.append(fn)
        for value in [*self.fiobwdata[fn].values()]:
          csvdata.append(value)
        for value in [*self.fiolatdata[fn].values()]:
          csvdata.append(value)
        for value in [*self.fiopctdata[fn].values()]:
          csvdata.append(value)
        csvstr = [str(data) for data in csvdata]
        csventry = separator.join(csvstr) + "\n"
        # Write to csv file
        f.write(csventry)

