#!/bin/sh
python /root/cbt/cbt.py -a=/home/cbt/series/CL1000 cbt-jobfiles/series/ceph3.2_20_4mk_rwrw.yml
python /root/cbt/cbt.py -a=/home/cbt/series/CL100-4M cbt-jobfiles/series/ceph3.2_40_4mk_rwrw.yml
python /root/cbt/cbt.py -a=/home/cbt/series/CL200-4M cbt-jobfiles/series/ceph3.2_60_4mk_rwrw.yml
python /root/cbt/cbt.py -a=/home/cbt/series/CL300-4M cbt-jobfiles/series/ceph3.2_80_4mk_rwrw.yml
