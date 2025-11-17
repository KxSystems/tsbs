export QUERYPORT=5010
export INGESTPORT=5000  # this is hardcoded in the TSBS publisher

if [[ $BRIDGED == "true" ]]; then WRITERPORT=6020; else WRITERPORT=${INGESTPORT}; fi

error_exit() {
    echo "ERROR: $1" >&2
    exit "${2:-1}"
}

if [[ -z "${QEXEC:-}" ]]; then
  echo "The kdb+ binary path is not set by environment variable QEXEC"
  error_exit "You need to set QEXEC in config/kdbenv, then do 'source config/kdbenv'" 3
fi

