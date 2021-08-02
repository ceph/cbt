#!/bin/bash
set -e
TOP_DIR=$(cd $(dirname "$0") && pwd)

# configurations
RESULT_DIR="$TOP_DIR/results"
BUILD_DIR="~/ceph/build/"
POOL_NAME="test-pool"
START_ROUND=1
TOTAL_ROUND=5

# Note: currently only support single OSD to measure write amplification
# correctly.
mkdir -p $RESULT_DIR
cd $BUILD_DIR
CURRENT_ROUND=$START_ROUND
TARGET_ROUND=$(( CURRENT_ROUND + TOTAL_ROUND ))
while [ $CURRENT_ROUND -lt $TARGET_ROUND ]
do
  echo "start round $CURRENT_ROUND ..."
  CEPH_DEV=1 ./bin/ceph tell osd.0 dump_metrics 2>&1 | tee $RESULT_DIR/result_${CURRENT_ROUND}_metrics_start.log
  CEPH_DEV=1 ./bin/rados bench -p $POOL_NAME 5 write -b 4096 --no-cleanup 2>&1 | tee $RESULT_DIR/result_${CURRENT_ROUND}_bench.log
  CEPH_DEV=1 ./bin/ceph tell osd.0 dump_metrics 2>&1 | tee $RESULT_DIR/result_${CURRENT_ROUND}_metrics_end.log
  echo "finish round $CURRENT_ROUND"
  echo
  (( ++CURRENT_ROUND ))
  sleep 2
done
echo "done!"
cd $TOP_DIR
