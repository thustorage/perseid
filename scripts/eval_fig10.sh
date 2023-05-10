#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

# ./sub/eval_fig10a.sh $@
./sub/eval_fig10ab.sh $@
