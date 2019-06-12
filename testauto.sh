#!/bin/sh

/root/cbt/cbt.py -a=/home/bal_baseline/4m/120_$(date -u "+%F-%T") /root/cbt/cbt-jobfiles/bal_yamls/baseline_4M/20_4m.yml
/root/cbt/cbt.py -a=/home/bal_baseline/4m/120_$(date -u "+%F-%T") /root/cbt/cbt-jobfiles/bal_yamls/baseline_4M/50_4m.yml
/root/cbt/cbt.py -a=/home/bal_baseline/4m/120_$(date -u "+%F-%T") /root/cbt/cbt-jobfiles/bal_yamls/baseline_4M/100_4m.yml
/root/cbt/cbt.py -a=/home/bal_baseline/4m/120_$(date -u "+%F-%T") /root/cbt/cbt-jobfiles/bal_yamls/baseline_4M/120_4m.yml
