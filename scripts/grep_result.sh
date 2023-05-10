#! /bin/bash -e

if [ -n "$1" ]; then
  if ! test -e $1; then
    echo "file does not exist"
    exit
  fi
else
    echo "missing argument"
    exit
fi

grep "Final Result:" $1
