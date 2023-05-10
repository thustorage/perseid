#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

FIG_NAME=fig8a
PRELOAD=/mnt/lsm_eval/trace/insert_100M.txt
TRACE=/mnt/lsm_eval/trace/uniform_get_sec.txt

if [[ $# < 1 || $1 != "others" ]]; then
  # pmasstree-perseid
  ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 3 "${PRELOAD}" "${TRACE}" "10" "true"  
else
  # lsmsi-pm
  ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 2 "${PRELOAD}" "${TRACE}" "10" "true" 
  # pmasstree-composite
  ./helper/index_only_helper_indiv.sh "${FIG_NAME}" 4 "${PRELOAD}" "${TRACE}" "10" "true" 
fi
