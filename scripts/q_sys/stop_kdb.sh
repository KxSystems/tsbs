#!/usr/bin/env bash

set -euo pipefail

SCRIPTDIR=$(dirname $(realpath $0))

source ${SCRIPTDIR}/common.sh

killIfPortInUse () {
  local port=$1

  if nc -z 127.0.0.1 "$port" &>/dev/null; then
    ${QEXEC} <<< "@[\`::$port; (exit; 0); ()]"
  fi
}

if [[ $DATAMEDIA == "ondisk" ]]; then
  echo "Stopping HDBs"
  if [[ $LOADBALANCER == "kdblb" ]]; then
    for i in $(seq $HDBNR); do
      killIfPortInUse $((QUERYPORT + i))
    done
    killall $LOADBALANCER # TODO: implement a robust solution that has no side effect
  else
    for i in $(seq $HDBNR); do
      killIfPortInUse $QUERYPORT
      sleep 0.05
    done
  fi
  echo "Stopping writer"
  killIfPortInUse ${WRITERPORT}
elif [[ $DATAMEDIA == "inmemory" ]]; then
  echo "Stopping RDB"
  killIfPortInUse ${WRITERPORT}
else
  echo "unknown data media"
fi

if [[ $BRIDGED == "true" ]];  then
  echo "Stopping bridges"
  for i in $(seq 0 $((${BRIDGENR}-1))); do
    killIfPortInUse ${INGESTPORT}
  done
fi