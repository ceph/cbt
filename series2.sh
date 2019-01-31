#!/bin/sh
./cbt.py -a=/home/cbt/series/RDB cbt/cbt-jobfiles/rados_high_lg0-3.yml
./cbt.py -a=/home/cbt/series/CL800 cbt-jobfiles/series/ceph3.2_40_4mk_rwrw.yml
./cbt.py -a=/home/cbt/series/CL200-4M cbt-jobfiles/series/ceph3.2_20_4mk_rwrw.yml
./cbt.py -a=/home/cbt/series/CL300-4M cbt-jobfiles/series/ceph3.2_60_4mk_rwrw.yml
./cbt.py -a=/home/cbt/series/CL400-4M cbt-jobfiles/series/ceph3.2_80_4mk_rwrw.yml
