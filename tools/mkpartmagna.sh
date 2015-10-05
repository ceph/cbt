#!/bin/bash

SYSPART=`df | grep "/$" | cut -d" " -f1 | cut -d"/" -f3`
if [[ $SYSPART=="mapper" ]]
then
        echo "System disk is on an LVM - determining underlying block device..."
        SYSPART=`pvscan | grep -i root | awk -F " " '{print $2}' | awk -F "/" '{print $3}' | cut -c1,2,3`
fi
diskid='wwn'
echo "System on $SYSPART"

failed()
{
  sleep 2 # Wait for the kernel to stop whining
  echo "Hrm, that didn't work.  Calling for help."
#  sudo ipmitool chassis identify force
  echo "RAID Config failed: ${1}"
  while [ 1 ]; do sleep 10; done
  exit 1;
}

fakefailed()
{
  echo "ignoring megacli errors and forging on: ${1}"
}

echo "Making label on OSD devices"

# Data 
i=0
for DEV in `ls -al /dev/disk/by-id | grep $diskid | grep -v part | cut -f3 -d"/" | tr '\n' ' '`
do
  if [[ ! $SYSPART =~ $DEV ]]
  then
    sudo parted -s -a optimal /dev/$DEV mklabel gpt || failed "mklabel $DEV"
    echo "Creating osd device $i data label"
    echo "sudo parted -s -a optimal /dev/$DEV mkpart osd-device-$i-data $sp% $ep%"
    sudo parted -s -a optimal /dev/$DEV mkpart osd-device-$i-journal 0% 1000M || failed "mkpart $i-journal"
    sudo parted -s -a optimal /dev/$DEV mkpart osd-device-$i-data 1000M 100% || failed "mkpart $i-data"
    let "i++"
  fi
done
