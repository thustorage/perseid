#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

FIG_NAME=fig8d
PRELOAD=/mnt/lsm_eval/trace/skew_sec_upsert_200M.txt
TRACE=/mnt/lsm_eval/trace/skew_sec_get.txt

if [[ $# < 1 || $1 != "others" ]]; then
  # pmasstree-perseid
  ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 3 "${PRELOAD}" "${TRACE}" "10" "true"
else
  # lsmsi-pm
  ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 2 "${PRELOAD}" "${TRACE}" "10" "true"
  # pmasstree-composite
  ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 4 "${PRELOAD}" "${TRACE}" "10" "true"
  # pmasstree-log
  ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 5 "${PRELOAD}" "${TRACE}" "10" "true"
fi
