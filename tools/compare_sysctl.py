#!/usr/bin/python3

import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("FILE", help="sysctl output files to parse", nargs="+")
    args = parser.parse_args()

    return args

def compare_items(foo, files):
   # Write the header
   print('"Attribute",', end=' ')
   for fn in files:
       print(('"%s",' % fn), end=' ')
   print()

   for attribute,items in sorted(foo.items()):
       if len(items) < len(files) or not all_same(list(items.values())):
           print('"%s",' % attribute, end=' ')
           for fn in files:
               if fn in items:
                   print(('"%s",' % items[fn]), end=' ')
               else:
                   print('"",', end=' ')
           print()
           
def all_same(items):
    return all(x == items[0] for x in items)
               
if __name__ == '__main__':
    kvdict = {}
    ctx = parse_args()
    for fn in ctx.FILE:
       f = open(fn, 'r')
       for line in f:
           (key, value) = line.rstrip('\r\n').rsplit(' = ')
           kvdict.setdefault(key, {}).update({fn: value})
    compare_items(kvdict, ctx.FILE)
 
