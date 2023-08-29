#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

FIG_NAME=fig10cd
TOPK=200
THREADS=(4 8 16 24 32)

for t in ${THREADS[@]}; do
  PRELOAD="/mnt/lsm_eval/trace/th${t}/skew_sec_upsert"
  TRACE="/mnt/lsm_eval/trace/th${t}/skew_sec_get"

  if [[ $# < 1 || $1 != "others" ]]; then
    # pmasstree-perseid
    ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 3 "${PRELOAD}" "${TRACE}" "${TOPK}" "false" "${t}"
  else
    # lsmsi-pm
    ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 2 "${PRELOAD}" "${TRACE}" "${TOPK}" "false" "${t}"
    # pmasstree-composite
    ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 4 "${PRELOAD}" "${TRACE}" "${TOPK}" "false" "${t}"
    # pmasstree-log
    ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 5 "${PRELOAD}" "${TRACE}" "${TOPK}" "false" "${t}"
  fi

  sleep 10s
done


