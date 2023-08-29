#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

./sub/eval_fig9ad.sh $@
./sub/eval_fig9be.sh $@
./sub/eval_fig9cf.sh $@
