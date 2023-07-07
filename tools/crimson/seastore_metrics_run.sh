#!/bin/bash
set -e
TOP_DIR=$(cd $(dirname "$0") && pwd)

# configurations
RESULT_DIR="$TOP_DIR/results"
BUILD_DIR="~/ceph/build/"

TOTAL_ROUND=10
ROUND_SECONDS=1

WITH_RADOS_BENCH=true
BENCH_POOL="pool-name"
BENCH_IODEPTH=64
BENCH_TIME=$(( ($TOTAL_ROUND - 1) * $ROUND_SECONDS -
               ($ROUND_SECONDS > 120 ? 120 : $ROUND_SECONDS) ))

METRICS_ENABLE=true

# require nvme and iostat, interval > 180s
STATS_ENABLE=true
STATS_DEV="/dev/dev-name"

collect_metrics() {
  if ! $METRICS_ENABLE; then
    return
  fi
  local current_round=$1
  local current_ms=$2
  local file_name=result_${current_round}_metrics_${current_ms}.log
  echo "start collect metrics to $file_name ..."
  CEPH_DEV=1 ./bin/ceph tell osd.0 dump_metrics 2>&1 | tee $RESULT_DIR/$file_name > /dev/null
  echo "finish collect metrics"
}

collect_stats() {
  if ! $STATS_ENABLE; then
    return
  fi
  local current_round=$1
  local current_ms=$2
  local file_name=result_${current_round}_stats_${current_ms}.log
  echo "start collect stats to $file_name ..."
  if [ `iostat -k -d $STATS_DEV | awk 'NR == 3 {print $5}'` = "kB_dscd/s" ]; then
    local read_wrtn_dscd_kb=( `iostat -k -d $STATS_DEV | awk 'NR == 4 {print $6, $7, $8}'` )
  elif [ `iostat -k -d $STATS_DEV | awk 'NR == 3 {print $5}'` = "kB_read" ]; then
    local read_wrtn_dscd_kb=( `iostat -k -d $STATS_DEV | awk 'NR == 4 {print $5, $6}'` )
    read_wrtn_dscd_kb[2]=0
  else
    echo "Warning! The parameter is incorrect. Modify the parameter according to the actual output of the iostat commmand"
    exit 1
  fi
  local nand_host_sectors=( `nvme intel smart-log-add $STATS_DEV | awk 'NR == 14 || NR == 15 {print $5}'` )
  if [ ${#nand_host_sectors[@]} -le 2 ]; then
    echo "Error! getting parameters, please try to execute command: nvme intel smart-log-add /dev/dev-name"
    exit 1
  fi  
  tee $RESULT_DIR/$file_name > /dev/null << EOT
{
  "read_kb": {
    "value": ${read_wrtn_dscd_kb[0]}
  },
  "wrtn_kb": {
    "value": ${read_wrtn_dscd_kb[1]}
  },
  "dscd_kb": {
    "value": ${read_wrtn_dscd_kb[2]}
  },
  "nand_sect": {
    "value": ${nand_host_sectors[0]}
  },
  "host_sect": {
    "value": ${nand_host_sectors[1]}
  }
}
EOT
  echo "finish collect stats"
}

run_rados_bench() {
  if ! $WITH_RADOS_BENCH; then
    return
  fi
  local bench_cmd="CEPH_DEV=1 ./bin/rados bench -p $BENCH_POOL $BENCH_TIME write -b 4096 --concurrent-ios=$BENCH_IODEPTH --no-cleanup"
  local file_name=result_0_radosbench.log
  echo "start rados bench $BENCH_TIME seconds to $file_name ..."
  CEPH_DEV=1 ./bin/rados bench -p $BENCH_POOL $BENCH_TIME write -b 4096 --concurrent-ios=$BENCH_IODEPTH --no-cleanup | tee $RESULT_DIR/$file_name &
}

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
collect_metrics $CURRENT_ROUND $CURRENT_MS
collect_stats $CURRENT_ROUND $CURRENT_MS
while [ $CURRENT_ROUND -lt $TARGET_ROUND ]
do
  (( ++CURRENT_ROUND ))
  echo "start round $CURRENT_ROUND of $TARGET_ROUND for ${ROUND_SECONDS}s ..."
  sleep $ROUND_SECONDS
  CURRENT_MS=$(($(date +%s%N)/1000000))
  collect_metrics $CURRENT_ROUND $CURRENT_MS
  collect_stats $CURRENT_ROUND $CURRENT_MS
  echo "finish round $CURRENT_ROUND"
  echo
  if [ $CURRENT_ROUND -eq 1 ]; then
    run_rados_bench
  fi
done
echo "done!"
cd $TOP_DIR
