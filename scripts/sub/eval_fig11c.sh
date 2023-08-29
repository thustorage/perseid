#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

APP_NAME=sec_idx_bench
FIG_NAME=fig11c
PRELOAD="/mnt/lsm_eval/trace/insert_80M.txt"
TRACE="/mnt/lsm_eval/trace/new_mixed/read_heavy.txt"

ulimit -n 65535

mkdir -p ../build
cd ../build
if [[ $# < 1 || $1 != "others" ]]; then
  RES_DIR="../results"
  # pmasstree-perseid
  SEC_IDX_TYPE_ARR=(3)
  SEC_IDX_NAME_ARR=(perseid)
else
  RES_DIR="../results/others"
  # lsmsi-pm, pmasstree-composite, pmasstree-log
  SEC_IDX_TYPE_ARR=(2 4 5)
  SEC_IDX_NAME_ARR=(lsmsi-pm pmasstree-composite pmasstree-log)
fi

mkdir -p ${RES_DIR}


for ((i=0; i<${#SEC_IDX_TYPE_ARR[@]}; i++)); do
  cmake .. -DCMAKE_BUILD_TYPE=Release \
  -DSEC_IDX_TYPE=${SEC_IDX_TYPE_ARR[$i]} \
  -DVALIDATE=true \
  -DPRUNE_SEQ=true \
  -DPAL_PDB=true \
  -DIDX_ONLY=false
  make ${APP_NAME} -j

  ../scripts/clean_db.sh
  ../scripts/run_bench.sh --preload="${PRELOAD}" --trace="${TRACE}" --topk="10" | tee ${RES_DIR}/${FIG_NAME}_${SEC_IDX_NAME_ARR[$i]}.txt

  sleep 30s
done
