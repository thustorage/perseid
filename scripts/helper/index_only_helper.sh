#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

if [[ $# != 5 ]]; then
  echo "Usage: $0 <fig> <perseid_or_others> <pre_load> <trace> <topk>"
  exit
fi

APP_NAME=sec_idx_bench
FIG_NAME=$1
PRELOAD=$3
TRACE=$4
TOPK=$5

if [[ $2 != "others" ]]; then
  ### perseid
  # FAST&FAIR-perseid, P-Masstree-perseid
  SEC_IDX_TYPE_ARR=(6 3)
else
  ### others
  # LSMSI, LSMSI-PM, FAST&FAIR-composite, FAST&FAIR-log, P-Masstree-composite, P-Masstree-log
  SEC_IDX_TYPE_ARR=(1 2 7 8 4 5)
fi

for i in ${SEC_IDX_TYPE_ARR[@]}; do
  ./helper/index_only_helper_indiv.sh "${FIG_NAME}" "${i}" "${PRELOAD}" "${TRACE}" "${TOPK}"

  sleep 30s
done
