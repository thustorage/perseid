#! /bin/bash -e

DB_WORKSPACE="/mnt/lsm_eval/pebblesdb"
PM_WORKSPACE="/mnt/pmem0/sec_idx"

echo "clear DIR $DB_WORKSPACE & $PM_WORKSPACE"

sudo rm -rf $DB_WORKSPACE
sudo rm -rf $PM_WORKSPACE

mkdir -p $DB_WORKSPACE
mkdir -p $PM_WORKSPACE

