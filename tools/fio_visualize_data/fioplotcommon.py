#!/usr/bin/python3

import re

def sort_map_data_by_key(data):
    sorteddata = {}
    # Sort data dictionary based on key
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    sorted_keys = sorted(data.keys(), key=alphanum_key)
    for key in sorted_keys:
      sorteddata[key] = data[key]
    return sorteddata

