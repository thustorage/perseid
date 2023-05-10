#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

./sub/eval_fig6a.sh $@
./sub/eval_fig6b.sh $@
./sub/eval_fig6c.sh $@
