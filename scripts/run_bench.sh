#! /bin/bash -e

if [[ $(basename $PWD) != "build" ]]; then
  echo 'run this script in "build"'
  exit
fi

NUMA_AFFINITY=0
APP_NAME=sec_idx_bench

sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

sleep 1s

numactl --membind=${NUMA_AFFINITY} --cpunodebind=${NUMA_AFFINITY} \
./${APP_NAME} $@
