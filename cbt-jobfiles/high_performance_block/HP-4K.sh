#!/usr/bin/sh

/root/cbt/cbt.py -a=/home/cbt/HIGH/fio_rand_4k_100clients /root/cbt/my-yamls/high_performance_block/fio_rand_4k_100clients.yml
sleep 10s
/root/cbt/cbt.py -a=/home/cbt/HIGH/fio_rand_4k_200clients /root/cbt/my-yamls/high_performance_block/fio_rand_4k_200clients.yml
sleep 10s
/root/cbt/cbt.py -a=/home/cbt/HIGH/fio_rand_4k_300clients /root/cbt/my-yamls/high_performance_block/fio_rand_4k_300clients.yml
sleep 10s
/root/cbt/cbt.py -a=/home/cbt/HIGH/fio_rand_4k_400clients /root/cbt/my-yamls/high_performance_block/fio_rand_4k_400clients.yml
