#! /bin/bash -e

if [[ $(basename $PWD) != "build" ]]; then
  echo 'run this script in "build"'
  exit
fi

APP_NAME=sec_idx_bench

cmake .. -DCMAKE_BUILD_TYPE=build \
-DSEC_IDX_TYPE=3 \
-DVALIDATE=true \
-DPRUNE_SEQ=true \
-DPAL_PDB=false \
-DIDX_ONLY=true

make ${APP_NAME} -j
