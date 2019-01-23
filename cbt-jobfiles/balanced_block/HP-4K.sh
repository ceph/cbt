#!/usr/bin/sh

/root/cbt/cbt.py -a=/home/cbt/BALANCED_4k/fio_rand_4k_700clients_new /root/cbt/my-yamls/balanced_block/fio_rand_4k_700clients_new.yml
sleep 10s
/root/cbt/cbt.py -a=/home/cbt/BALANCED_4k/fio_rand_4k_800clients_new /root/cbt/my-yamls/balanced_block/fio_rand_4k_800clients_new.yml
sleep 10s
/root/cbt/cbt.py -a=/home/cbt/BALANCED_4k/fio_rand_4k_900clients_new /root/cbt/my-yamls/balanced_block/fio_rand_4k_900clients_new.yml
#sleep 10s
#/root/cbt/cbt.py -a=/home/cbt/BALANCED/fio_rand_4k_400clients /root/cbt/my-yamls/balanced_block/fio_rand_4k_400clients.yml
#sleep 10s
#/root/cbt/cbt.py -a=/home/cbt/BALANCED/fio_rand_4k_400clients /root/cbt/my-yamls/balanced_block/fio_rand_4k_400clients.yml

