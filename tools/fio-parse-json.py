#!/usr/bin/python
#
# fio-json-prs.py - example script to parse distributed workload generation result 
# produced by fio in JSON format
#
# input parameters:
#  1 - JSON file - file containing fio JSON output
#  2 - JSON path - path through JSON tree to a leaf node
#
# assumption: json output of non-leaf nodes consists of either
# - dictionary - key field selects sub-value
# - sequence - key field syntax is name=value, where
#              name is a dictionary key of sequence elements, and
#              value is the desired value to select a sequence element
# example:
# python fio-parse-json.py r.fiojob.json.log 'jobs/jobname=randread/read/iops'
# 

import os, sys
from pprint import pprint
import json

NOTOK=1

if len(sys.argv) < 3:
  print('usage: fio-parse-json.py fio-json.log path-to-leaf')
  print('path-to-leaf is a slash-separated list of key names in JSON tree')
  print('see instructions at top of this script')
  sys.exit(NOTOK)


def filter_json_node(next_branch, node_list_in):
  #print next_branch, json.dumps(node, indent=4)
  #print ''
  #sys.stdout.flush()
  next_node_list = []
  for n in node_list_in:
	dotlist = next_branch.split('=')
	if len(dotlist) > 2:
		print('unrecognized syntax at %s'%str(node))
		sys.exit(NOTOK)
	elif len(dotlist) == 1:
		next_node_list.append(n[next_branch])
		assert(isinstance(n, dict))
	else: # must be a sequence, take any element with key matching value
		select_key = dotlist[0]
		select_value = dotlist[1]
		for e in n:  # node is a seq
			#print 'select with key %s value %s sequence element %s'%(select_key, select_value, e)
			if select_value == '*':
				next_node_list.append(e)
			else:
				v = e[select_key]
				if v == select_value:
					next_node_list.append(e)
			
		if len(next_node_list) == 0:
			print('no list member in %s has key %s value %s'%(str(node), select_key, select_value))
			sys.exit(NOTOK)
  return next_node_list


fn = sys.argv[1]
json_tree_path = sys.argv[2].split('/')
with open(fn, 'r') as json_data:

  # check for empty file

  f_info = os.fstat(json_data.fileno())
  if f_info.st_size == 0:
    print('JSON input file %s is empty'%fn)
    sys.exit(NOTOK)

  # find start of JSON object and position file handle right before that

  lines = json_data.readlines()
  start_of_json_data=0
  for l in lines:
    if l[0] == '{': break
    start_of_json_data += 1
  json_data.seek(0, os.SEEK_SET)
  for j in range(0,start_of_json_data):
    l = json_data.readline()

  # parse the JSON object

  node = json.load(json_data)
  current_branch = None
  next_node_list = [node]
  for next_branch in json_tree_path:
	next_node_list = filter_json_node(next_branch, next_node_list)
  for n in next_node_list: print(n)

