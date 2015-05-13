#!/usr/bin/python
#
# is_regression.py - statistical test for performance throughput regression
# based on python scipy.stats.ttest_ind() function
#
# we input two sets of samples: 
#  the baseline sample set -- used as an indication of previously achieved level of performance
#  the current sample set -- used as an indication of the system currently being tested for performance regression
#
# command line inputs:
#  sample_type -- 'throughput' or 'response-time'
#  confidence_threshold -- min probability that two sample sets have a different mean 
#                           (e.g. 95 means that results differ with 95% probability)
#  max_pct_dev -- maximum percent deviation of either sample set, 100.0 x std.dev/mean 
#  base_sample -- file containing baseline performance throughput samples, 1 per line
#  current_sample -- file containing current performance throughput samples, 1 per line
#
# return status codes:
#  0 -- no regression, PASS
#  10 -- regression, FAIL
#  11 -- either sample set's variance too large
#         reject if the percent deviation for either baseline or current samples is > max_pct_dev
#  
# we declare a performance regression if base_set mean is worse than current_set mean and a T-test determines
# that the probability that the two sample sets have a different mean is greater than confidence_threshold
#
# the base sample set mean is "worse" than the current sample set mean if and only if:
#   the sample_type is 'throughput'    and the base mean > current mean
#   the sample type is 'response-time' and the base mean < current mean
#
# References: The Art of Computer Systems Perf. Analysis, Raj Jain
# see documentation for python scipy.stats.ttest_ind() function
#

import os
import sys
from sys import argv, exit
import math
import numpy
import scipy
from scipy.stats import ttest_ind
from numpy import array

# process status codes returned to shell
NOTOK=-1
PASS = 0
FAIL = 10
VARIANCE_TOO_HIGH=11
NOT_ENOUGH_SAMPLES=12

def usage(msg):
  print '\nERROR: ' + msg
  print 'usage: is_regression.py sample_type confidence_threshold max_pct_dev base_samples_file test_samples_file'
  print 'sample_type is either "throughput" or "response-time"'
  print 'confidence_threshold is probability that sample means differ expressed as a percentage'
  print 'max_pct_dev is maximum percent deviation allowed for either sample set'
  print 'samples files are text files with one floating-point sample value per line'
  sys.exit(NOTOK)

def read_samples_from_file( sample_filepath ):
  with open(sample_filepath, "r") as sample_file:
      samples = [ float(r.strip()) for r in sample_file.readlines() ]
  print '%d samples read from file %s'%(len(samples), sample_filepath)
  return array(samples)

def print_sample_stats(samples_name, samples_array):
    s = samples_array
    print 'sample stats for %s: min = %f, max = %f, mean = %f, sd = %f, pct.dev. = %5.2f'%\
            (samples_name, s.min(), s.max(), s.mean(), s.std(ddof=1), 100.0*s.std(ddof=1)/s.mean())

if len(argv) < 6:
  usage('not enough command line arguments')

sample_type = argv[1]
confidence_threshold = float(argv[2])
max_pct_dev = float(argv[3])

# read in and acknowledge command line arguments

print 'sample type = %s , confidence_threshold = %6.2f %%, max. pct. deviation = %6.2f %%'%\
        (sample_type, confidence_threshold, max_pct_dev)

baseline_sample_array = read_samples_from_file(argv[4])
print_sample_stats('baseline', baseline_sample_array)

current_sample_array = read_samples_from_file(argv[5])
print_sample_stats('current', current_sample_array)

# reject invalid inputs

if len(current_sample_array) < 3:
  print 'ERROR: not enough current samples'
  exit(NOT_ENOUGH_SAMPLES)

if len(baseline_sample_array) < 3:
  print 'ERROR: not enough baseline samples'
  exit(NOT_ENOUGH_SAMPLES)

# flunk the test if standard deviation is too high for either sample test

baseline_pct_dev = 100.0 * baseline_sample_array.std(ddof=1) / baseline_sample_array.mean()
current_pct_dev = 100.0 * current_sample_array.std(ddof=1) / current_sample_array.mean()

if baseline_pct_dev > max_pct_dev:
  print 'ERROR: pct. deviation of %5.2f is too high for baseline samples'%baseline_pct_dev
  exit(VARIANCE_TOO_HIGH)
if current_pct_dev > max_pct_dev:
  print 'ERROR: pct. deviation of %5.2f is too high for current samples'%current_pct_dev
  exit(VARIANCE_TOO_HIGH)

# FAIL the test if sample sets are accurate enough and 
# current sample set is statistically worse than baseline sample set

(t, same_mean_probability) = ttest_ind(baseline_sample_array, current_sample_array)
print 't-test t-statistic = %f probability = %f'%(t,same_mean_probability)
print 't-test says that mean of two sample sets differs with probability %6.2f%%'%\
        ((1.0-same_mean_probability)*100.0)

pb_threshold = (100.0 - confidence_threshold)/100.0
print 'same_mean_prob %f pb_threshold %f'%(same_mean_probability, pb_threshold)
if same_mean_probability < pb_threshold:
   # the two samples do not have the same mean
   # fail if current sample is worse than baseline sample as defined above
   if (sample_type == 'throughput'):
     if (baseline_sample_array.mean() > current_sample_array.mean()):
       print 'declaring a performance regression test FAILURE because of lower throughput'
       exit(FAIL)
   elif (sample_type == 'response-time'):
     if (baseline_sample_array.mean() < current_sample_array.mean()):
       print 'declaring a performance regression test FAILURE because of higher response time'
       exit(FAIL)
   else: usage('sample_type must either be "throughput" or "response-time"')
   print 'current sample set is statistically better than baseline sample set'
else:
   print 'sample sets are statistically indistinguishable for specified confidence level'
exit(PASS)  # no regression found
