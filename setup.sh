#!/bin/bash

# script to install CBT dependencies and tools for active benchmarking

sudo yum -y install deltarpm 
sudo yum check-update
sudo yum -y update
sudo yum install -y psmisc util-linux coreutils xfsprogs e2fsprogs findutils \
  git wget bzip2 make automake gcc gcc-c++ kernel-devel perf blktrace lsof \
  redhat-lsb sysstat screen python-yaml ipmitool dstat zlib-devel ntp

MIRROR="http://mirror.hmc.edu/fedora/linux/releases/22/Everything/x86_64/os/Packages"

wget ${MIRROR}/p/pdsh-2.31-3.fc22.x86_64.rpm
wget ${MIRROR}/p/pdsh-2.31-3.fc22.x86_64.rpm
wget ${MIRROR}/p/pdsh-rcmd-ssh-2.31-3.fc22.x86_64.rpm
wget ${MIRROR}/c/collectl-4.0.0-1.fc22.noarch.rpm
wget ${MIRROR}/i/iftop-1.0-0.9.pre4.fc22.x86_64.rpm
wget ${MIRROR}/i/iperf3-3.0.10-1.fc22.x86_64.rpm

sudo yum localinstall -y *.rpm

git clone https://github.com/axboe/fio.git
git clone https://github.com/andikleen/pmu-tools.git
git clone https://github.com/brendangregg/FlameGraph

cd ${HOME}/fio
./configure
make

# wget < Red Hat Ceph Storage ISO URL >
# sudo mount -o loop Ceph-*-dvd.iso /mnt
sudo yum localinstall -y /mnt/{MON,OSD}/*.rpm
sudo yum localinstall -y /mnt/Installer/ceph-deploy-*.rpm

sudo sed -i 's/Defaults    requiretty/#Defaults    requiretty/g' /etc/sudoers
sudo setenforce 0
( awk '!/SELINUX=/' /etc/selinux/config ; echo "SELINUX=disabled" ) > /tmp/x
sudo mv /tmp/x /etc/selinux/config
rpm -qa firewalld | grep firewalld && sudo systemctl stop firewalld && sudo systemctl disable firewalld
sudo systemctl stop irqbalance
sudo systemctl disable irqbalance
sudo systemctl start ntpd.service
sudo systemctl enable ntpd.service
