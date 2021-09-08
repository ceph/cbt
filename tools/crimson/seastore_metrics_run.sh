#!/bin/bash
set -e
TOP_DIR=$(cd $(dirname "$0") && pwd)

# configurations
RESULT_DIR="$TOP_DIR/results"
BUILD_DIR="~/ceph/build/"
TOTAL_ROUND=10
BENCH_SECONDS=1

# Note: currently only support single OSD to measure write amplification
# correctly.
if [ -e $RESULT_DIR ]; then
  echo "'$RESULT_DIR' dir already exists, remove it or select a different one"
  exit 1
fi

mkdir -p $RESULT_DIR
cd $BUILD_DIR
CURRENT_ROUND=0
TARGET_ROUND=$(( CURRENT_ROUND + TOTAL_ROUND ))

CURRENT_MS=$(($(date +%s%N)/1000000))
CEPH_DEV=1 ./bin/ceph tell osd.0 dump_metrics 2>&1 | tee $RESULT_DIR/result_${CURRENT_ROUND}_metrics_${CURRENT_MS}.log
while [ $CURRENT_ROUND -lt $TARGET_ROUND ]
do
  (( ++CURRENT_ROUND ))
  echo "start round $CURRENT_ROUND ..."
  sleep $ROUND_SECONDS
  CURRENT_MS=$(($(date +%s%N)/1000000))
  CEPH_DEV=1 ./bin/ceph tell osd.0 dump_metrics 2>&1 | tee $RESULT_DIR/result_${CURRENT_ROUND}_metrics_${CURRENT_MS}.log
  echo "finish round $CURRENT_ROUND"
  echo
done
echo "done!"
cd $TOP_DIR
