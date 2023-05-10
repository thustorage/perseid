#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

FIG_NAME=fig6a
PRELOAD=/mnt/lsm_eval/trace/insert_100M.txt

if [[ $# < 1 || $1 != "others" ]]; then
  TYPE=perseid
else
  TYPE=others
fi

./helper/index_only_helper.sh "${FIG_NAME}" "${TYPE}" "${PRELOAD}" "" ""
