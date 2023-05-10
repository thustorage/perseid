#! /bin/bash -e

if [[ $(basename $PWD) != "scripts" ]]; then
  echo 'run this script in "scripts"'
  exit
fi

./sub/eval_fig8a.sh $@
./sub/eval_fig8b.sh $@
./sub/eval_fig8c.sh $@
