#!/usr/bin/sh

/root/cbt/cbt.py -a=/home/cbt/HIGH_seq/fio_rand_4k_600clients /root/cbt/my-yamls/high_performance_block/fio_rand_4k_600clients.yml
sleep 10s
/root/cbt/cbt.py -a=/home/cbt/HIGH_seq/fio_rand_4k_700clients /root/cbt/my-yamls/high_performance_block/fio_rand_4k_700clients.yml
#sleep 10s
#/root/cbt/cbt.py -a=/home/cbt/HIGHb/fio_rand_4k_8qd /root/cbt/my-yamls/high_performance_block/fio_rand_4k_qd8.yml
#sleep 10s
#/root/cbt/cbt.py -a=/home/cbt/HIGHb/fio_rand_4k_16qd /root/cbt/my-yamls/high_performance_block/fio_rand_4k_qd16.yml
