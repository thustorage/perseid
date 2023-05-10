#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

FIG_NAME=fig10ab

THREADS=(4 8 16 24 32)

for t in ${THREADS[@]}; do
  PRELOAD="/mnt/lsm_eval/trace/th${t}/skew_pri_upsert"
  TRACE="/mnt/lsm_eval/trace/th${t}/get_sec"

  if [[ $# < 1 || $1 != "others" ]]; then
    # pmasstree-perseid
    ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 3 "${PRELOAD}" "${TRACE}" "10" "false" "${t}"
  else
    # lsmsi-pm
    ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 2 "${PRELOAD}" "${TRACE}" "10" "false" "${t}"
    # pmasstree-composite
    ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 4 "${PRELOAD}" "${TRACE}" "10" "false" "${t}"
  fi

  sleep 10s
done


