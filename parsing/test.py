#!/usr/bin/python

import argparse
import os, fnmatch
import numpy
import hashlib
import database
from htmlgenerator import HTMLGenerator

def mkhash(values):
    value_string = ''.join([str(i) for i in values])
    return hashlib.sha256(value_string).hexdigest()

def parse_args():
    parser = argparse.ArgumentParser(description='get fio averages.')
    parser.add_argument(
        'input_directory',
        help = 'Directory to search.',
        )

    args = parser.parse_args()
    return args

def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result

def splits(s,d1,d2):
    l,_,r = s.partition(d1)
    m,_,r = r.partition(d2)
    return m

def getbw(s):
    if "GB/s" in s:
        return float(s[:-4])*1024
    if "MB/s" in s:
        return float(s[:-4])
    if "KB/s" in s:
        return float(s[:-4])/1024

if __name__ == '__main__':
    ctx = parse_args()
    database.create_db()

    files = find('output.*', ctx.input_directory)
    totals = {}
    for inputname in files:
        # strip off the input directory
        params = inputname[len(ctx.input_directory):].split("/")[3:-1]
        # make readahead into an int
        params[3] = int(params[3][7:])

        # Make op_size into an int
        params[4] = int(params[4][8:])

        # Make cprocs into an int
        params[5] = int(params[5][17:])

        # Make io_depth int an int
        params[6] = int(params[6][9:])

        params_hash = mkhash(params)
        params = [params_hash] + params
        params.extend([0,0])
        database.insert(params)

        for line in open(inputname):
            if "aggrb" in line:
                 bw = getbw(splits(line, 'aggrb=', ','))
                 if "READ" in line:
                     database.update_readbw(params_hash, bw)
                 elif "WRITE" in line:
                     database.update_writebw(params_hash, bw)
    html = HTMLGenerator()
    html.add_html(html.read_file('/home/nhm/src/cbt/include/html/table.html'))
    html.add_style(html.read_file('/home/nhm/src/cbt/include/css/table.css'))
    html.add_script(html.read_file('/home/nhm/src/cbt/include/js/jsxcompressor.min.js'))
    html.add_script(html.read_file('/home/nhm/src/cbt/include/js/d3.js'))
    html.add_script(html.read_file('/home/nhm/src/cbt/include/js/d3var.js'))
    html.add_script(html.format_data(database.fetch_table(['opsize', 'testtype'])))
    html.add_script(html.read_file('/home/nhm/src/cbt/include/js/table.js'))

    print '<meta charset="utf-8">'
    print '<title>D3 Table Test </title>'
    print '<html>'
    print html.to_string()
    print '</html>'
#    print database.fetch_table(['opsize', 'testtype'])

#    get_section(['opsize', 'testtype'])

#    write_html()
#    write_data(['opsize', 'testtype'])
#    write_style()
#    write_js()

