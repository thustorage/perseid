#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

APP_NAME=sec_idx_bench
FIG_NAME=fig9ad
PRELOAD=/mnt/lsm_eval/trace/insert_100M.txt
TRACE=/mnt/lsm_eval/trace/uniform_get_sec.txt

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
  # lsmsi-pm, pmasstree-composite
  SEC_IDX_TYPE_ARR=(2 4)
  SEC_IDX_NAME_ARR=(lsmsi-pm pmasstree-composite)
fi

mkdir -p ${RES_DIR}

for ((i=0; i<${#SEC_IDX_TYPE_ARR[@]}; i++)); do
  cmake .. -DCMAKE_BUILD_TYPE=Release \
  -DSEC_IDX_TYPE=${SEC_IDX_TYPE_ARR[$i]} \
  -DVALIDATE=true \
  -DPRUNE_SEQ=false \
  -DPAL_PDB=false \
  -DIDX_ONLY=false
  make ${APP_NAME} -j

  ../scripts/clean_db.sh
  if [[ ${SEC_IDX_TYPE_ARR[$i]} < 3 ]]; then
    ../scripts/run_bench.sh --preload="${PRELOAD}"
    ../scripts/run_bench.sh --use_existing_db=true --trace="${TRACE}" --topk="10,200" | tee ${RES_DIR}/${FIG_NAME}_${SEC_IDX_NAME_ARR[$i]}.txt
  else
    ../scripts/run_bench.sh --preload="${PRELOAD}" --trace="${TRACE}" --topk="10,200" | tee ${RES_DIR}/${FIG_NAME}_${SEC_IDX_NAME_ARR[$i]}.txt
  fi

  sleep 30s

  cmake .. -DCMAKE_BUILD_TYPE=Release \
  -DSEC_IDX_TYPE=${SEC_IDX_TYPE_ARR[$i]} \
  -DVALIDATE=true \
  -DPRUNE_SEQ=true \
  -DPAL_PDB=false \
  -DIDX_ONLY=false
  make ${APP_NAME} -j

  if [[ ${SEC_IDX_TYPE_ARR[$i]} < 3 ]]; then
    ../scripts/run_bench.sh --use_existing_db=true --trace="${TRACE}" --topk="10,200" | tee ${RES_DIR}/${FIG_NAME}_${SEC_IDX_NAME_ARR[$i]}-seq.txt
  else
    ../scripts/clean_db.sh
    ../scripts/run_bench.sh --preload="${PRELOAD}" --trace="${TRACE}" --topk="10,200" | tee ${RES_DIR}/${FIG_NAME}_${SEC_IDX_NAME_ARR[$i]}-seq.txt
  fi

  sleep 30s

  cmake .. -DCMAKE_BUILD_TYPE=Release \
  -DSEC_IDX_TYPE=${SEC_IDX_TYPE_ARR[$i]} \
  -DVALIDATE=true \
  -DPRUNE_SEQ=true \
  -DPAL_PDB=true \
  -DIDX_ONLY=false
  make ${APP_NAME} -j

  if [[ ${SEC_IDX_TYPE_ARR[$i]} < 3 ]]; then
    ../scripts/run_bench.sh --use_existing_db=true --trace="${TRACE}" --topk="10,200" | tee ${RES_DIR}/${FIG_NAME}_${SEC_IDX_NAME_ARR[$i]}-seq-par.txt
  else
    ../scripts/clean_db.sh
    ../scripts/run_bench.sh --preload="${PRELOAD}" --trace="${TRACE}" --topk="10,200" | tee ${RES_DIR}/${FIG_NAME}_${SEC_IDX_NAME_ARR[$i]}-seq-par.txt
  fi

  sleep 30s
done
