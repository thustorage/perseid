#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

./sub/eval_fig11a.sh $@
./sub/eval_fig11b.sh $@
./sub/eval_fig11c.sh $@
