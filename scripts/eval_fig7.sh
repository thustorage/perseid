#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

ulimit -n 65535

./sub/eval_fig7a.sh $@
./sub/eval_fig7b.sh $@
./sub/eval_fig7c.sh $@
./sub/eval_fig7d.sh $@
