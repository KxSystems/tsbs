#!/usr/bin/env bash

set -euo pipefail

readonly USAGE="Usage: $0 [-u|--usecase iot|devops|cpu-only] [--skip-load] [-h|--help]"



#######################################
# Functions
#######################################

show_help() {
  cat <<EOF
$USAGE

Options:
  -h, --help              Show this help message
  -u, --usecase USECASE  Usecase: iot|devops|cpu-only (default cpu-only)
  --skip-load             Skip starting loading components (e.g. writer)

Examples:
  $0 --usecase iot
  $0 -u cpu-only --skip-load
EOF
  exit 0
}

check_port() {
  local port=$1

  if nc -z 127.0.0.1 "$port" &>/dev/null; then
    echo "Write port ${port} is pingable. Maybe leftover kdb+ processes are running.";
    error_exit "Run ./stop_kdb.sh and try again." 3
  fi
}

#######################################
# Main Script Execution
#######################################

SCRIPTDIR=$(dirname $(realpath $0))
source ${SCRIPTDIR}/common.sh

SKIPLOAD=false
DOMAIN="cpu-only"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      show_help
      ;;
    -u|--usecase)
      if [[ ! "$2" =~ ^(iot|devops|cpu-only)$ ]]; then
        error_exit "Invalid usecase: $2 (must be iot', 'devops' or 'cpu-only'" 2
      fi
      DOMAIN="$2"
      shift 2
      ;;
    --skip-load)
      SKIPLOAD=true
      shift
      ;;
    *)
      echo "Unknown parameter: $1"
      echo "$USAGE"
      exit 1
      ;;
  esac
done

SCHEMAFILE=schema/${DOMAIN}.q

LOGDIR=${SCRIPTDIR}/log/$(date +%m%d_%H%M%S)
mkdir -p ${LOGDIR}

check_port ${WRITERPORT}
check_port ${QUERYPORT}

LBADMINPORT=$((QUERYPORT-1))

if [[ $LOADBALANCER == "kdblb" ]]; then
  for port in seq $((QUERYPORT + 1)) $((QUERYPORT + HDBNR)); do
    check_port ${port}
  done
  check_port ${LBADMINPORT}
fi


if command -v numactl &>/dev/null; then
  NUMANODES=$(lscpu | grep "NUMA node(s)" | cut -d":" -f 2 | xargs)
  export NUMANODES
fi

get_numa_config() {
    if [[ -z "${NUMANODES:-}" || "${NUMANODES}" -le 1 || "${NUMA}" != "roundrobin" ]]; then
        echo ""
        return
    fi

    local process_id=$1
    local numa_node=$(( (process_id - 1) % NUMANODES ))
    echo "numactl -N ${numa_node} -m ${numa_node}"
}

cd ${SCRIPTDIR}/src

if [[ $DATAMEDIA == "ondisk" ]]; then
  KDBPOOLNR=$HDBNR
  mkdir -p ${DBDIR}
  if [[ ${SKIPLOAD} == true ]]; then
    WRITERPARAM=""
  else
    echo "Starting Writer"
    ${QEXEC} writer.q -db ${DBDIR} -schema ${SCHEMAFILE} -s ${WRITETHREADNR} -p ${WRITERPORT} > ${LOGDIR}/writer.log 2>&1 &
    WRITERPARAM="-writer ${WRITERPORT}"
    KDBPOOLNR=$((KDBPOOLNR + 1))
  fi

  if [[ ${HDBNR} == 1 ]]; then
    echo "Starting HDB"
    NUMAPREFIX=$(get_numa_config 0)
    ${NUMAPREFIX} ${QEXEC} hdb.q -db ${DBDIR} -query query/${DOMAIN}/hdb.q ${WRITERPARAM} -s ${HDBTHREADNR} -p ${QUERYPORT} > ${LOGDIR}/hdb.log 2>&1 &
    sleep 0.1  # this is needed for many kdb+ processes connect to license server at the same time
               # you can uncomment with host-hardcoded license file
  elif [[ $LOADBALANCER =~ "kdblb*" ]]; then
    for i in $(seq ${HDBNR}); do
      NUMAPREFIX=$(get_numa_config $i)
      ${NUMAPREFIX} ${QEXEC} hdb.q -db ${DBDIR} -query query/${DOMAIN}/hdb.q ${WRITERPARAM} -s ${HDBTHREADNR} -p $((QUERYPORT + i)) > ${LOGDIR}/hdb_$i.log 2>&1 &
      sleep 0.1
    done
    KDBLB_SERVERS_PORT_BASE=$((QUERYPORT+1)) KDBLB_SERVERS_COUNT=${HDBNR} KDBLB_SERVER_PORT=${QUERYPORT} KDBLB_ADMIN_PORT=${LBADMINPORT} $LOADBALANCER > ${LOGDIR}/kdblb.log 2>&1 &
    KDBPOOLNR=$((KDBPOOLNR + 1))
  else
    for i in $(seq 0 $((${HDBNR}-1))); do
      ${QEXEC} hdb.q -db ${DBDIR} -query query/${DOMAIN}/hdb.q ${WRITERPARAM} -s ${HDBTHREADNR} -p rp,${QUERYPORT} > ${LOGDIR}/hdb_$i.log 2>&1 &
      sleep 0.1
    done
  fi

  if [[ $(jobs -p|wc -w) -ne $KDBPOOLNR ]]; then
     echo "Not all kdb+ processes could start. Check logs at ${LOGDIR}/hdb_*.log and ${LOGDIR}/writer.log"
     exit 5
  fi

elif [[ $DATAMEDIA == "inmemory" ]]; then
  echo "Starting RDB"
  ${QEXEC} rdb.q ${SCHEMAFILE} query/${DOMAIN}/rdb.q -s ${RDBTHREADNR} -p ${WRITERPORT} > ${LOGDIR}/rdb.log 2>&1 &
  RDB_PID=$!
  echo "RDB started with pid ${RDB_PID}"
else
  echo "unknown data media"
fi

if [[ ${SKIPLOAD} == "false" && $BRIDGED == "true" ]];  then
  sleep 0.5
  for i in $(seq 0 $((${BRIDGENR}-1))); do
    echo "Starting bridge ${i}"
    ${QEXEC} bridge.q ${WRITERPORT} ${SCHEMAFILE} -p rp,${INGESTPORT} > ${LOGDIR}/bridge_${i}.log 2>&1 &
    sleep 0.1
  done
fi

cd -