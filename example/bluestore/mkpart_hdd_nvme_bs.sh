#!/bin/bash

JPD=8

failed()
{
  sleep 2 # Wait for the kernel to stop whining
  echo "Hrm, that didn't work.  Calling for help."
#  sudo ipmitool chassis identify force
  echo "RAID Config failed: ${1}"
  while [ 1 ]; do sleep 10; done
  exit 1;
}

# First, look for the system disk so we avoid touching it.
SYSPART=`df | grep "/$" | cut -d" " -f1 | cut -d"/" -f3`
#SYSPART=`sudo pvs | grep "/dev/" | cut -f3 -d" " | sed -e 's/[0-9]*$//g'`
echo "System on $SYSPART"

# Remove the partition label symlinks
sudo rm /dev/disk/by-partlabel/osd-device*

echo "Making label on OSD devices"
i=0

# Next, Make the OSD data partitions.  In this case we search for the seagate disks in the node.
for DEV in `ls -al /dev/disk/by-id/ata-ST9* | grep -v "part" | cut -f7 -d"/" | tr '\n' ' '`
do
  if [[ ! $SYSPART =~ $DEV ]] && [ $i -lt 37 ]
  then
    sudo parted -s -a optimal /dev/$DEV mklabel gpt || failed "mklabel $DEV"
    echo "Creating osd device $i data label"
    sudo parted -s -a optimal /dev/$DEV mkpart osd-device-$i-data 0G 10G || failed "mkpart $i-data"
    sudo parted -s -a optimal /dev/$DEV mkpart osd-device-$i-block 10G 100% || failed "mkpart $i-block"
    let "i++"
  fi
done

j=0;
for DEV in `ls -al /dev/nvme*n1 | cut -f3 -d"/" | tr '\n' ' '`
do
  sudo parted -s -a optimal /dev/$DEV mklabel gpt || failed "mklabel $DEV"
  for ((k=0; k < $JPD; k++ ))
  do
    if [[ ! $SYSPART =~ $DEV ]] && [ $j -lt $i ]
      then
        echo "Creating osd device $j journal label"
        sudo parted -s -a optimal /dev/$DEV mkpart osd-device-$j-wal $(( 10 * $k ))G $(( 10 * $(($k)) + 2))G || failed "mkpart $j-wal"
        sudo parted -s -a optimal /dev/$DEV mkpart osd-device-$j-db $(( 10 * $(($k)) + 2 ))G $(( 10 * $(($k + 1)) ))G || failed "mkpart $j-db"
        let "j++"
    fi
  done
done
