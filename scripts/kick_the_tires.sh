#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

FIG_NAME=kick
PRELOAD=/mnt/lsm_eval/trace/test/insert_100K.txt
TRACE=/mnt/lsm_eval/trace/test/uniform_get_sec_100K.txt


for ((i=1; i<=8; i++)) do
  ./helper/index_only_helper_indiv.sh "${FIG_NAME}" "${i}" "${PRELOAD}" "${TRACE}" "${TOPK}"
done

rm ../results/${FIG_NAME}*
rm ../results/others/${FIG_NAME}*


echo
echo "Kick-the-tires passed!"
