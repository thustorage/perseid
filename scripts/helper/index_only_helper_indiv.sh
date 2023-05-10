#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

if [[ $# < 5 ]]; then
  echo "Usage: $0 <fig> <sec_idx_type> <pre_load> <trace> <topk> [<range>] [<threads>]"
  exit
fi

APP_NAME=sec_idx_bench
FIG_NAME=$1
SEC_IDX_TYPE=$2
PRELOAD=$3
TRACE=$4
TOPK=$5

if [[ $# > 5 && ($6 == "true" || $6 == "others") ]]; then
  RANGE=true
else
  RANGE=false
fi

if [[ $# > 6 ]]; then
  THREADS=$7
else
  THREADS=1
fi

IDX_ARR=(
  ""
  lsmsi
  lsmsi-pm
  pmasstree-perseid
  pmasstree-composite
  pmasstree-log
  fastfair-perseid
  fastfair-composite
  fastfair-log
)
IDX_NAME=${IDX_ARR[${SEC_IDX_TYPE}]}
RES_DIR="../results"

# build
mkdir -p ../build
cd ../build
cmake .. -DCMAKE_BUILD_TYPE=Release \
-DSEC_IDX_TYPE=${SEC_IDX_TYPE} \
-DVALIDATE=true \
-DPRUNE_SEQ=true \
-DPAL_PDB=false \
-DIDX_ONLY=true
make ${APP_NAME} -j


if [[ $2 == 3 || $2 == 6 ]]; then
  echo "eval perseid: ${IDX_NAME}"
else
  echo "eval others: ${IDX_NAME}"
  RES_DIR="../results/others"
fi
mkdir -p ${RES_DIR}

# run
../scripts/clean_db.sh
if [[ ${THREADS} > 1 ]]; then
  RES_FILE=${RES_DIR}/${FIG_NAME}_${IDX_NAME}_t${THREADS}.txt
else
  RES_FILE=${RES_DIR}/${FIG_NAME}_${IDX_NAME}.txt
fi
../scripts/run_bench.sh --preload="${PRELOAD}" --trace="${TRACE}" --topk="${TOPK}" --range_search=${RANGE} --threads=${THREADS} | tee ${RES_FILE}
