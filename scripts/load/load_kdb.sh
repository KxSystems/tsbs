#!/bin/bash
#
# TODO: This script does not work as yet.
#

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_kdb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_kdb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-kdb-data.gz}
DATABASE_PORT=${DATABASE_PORT:-5000}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

until curl http://${DATABASE_HOST}:${DATABASE_PORT}/ping 2>/dev/null; do
    echo "Waiting for kdb+"
    sleep 1
done

# Load new data
echo "Loading new data: $EXE_FILE_NAME, $DATABASE_NAME"
echo "Variables:"
echo "DATA_FILE: $DATA_FILE"
echo "BACKOFF_SECS: $BACKOFF_SECS"
echo "NUM_WORKERS: $NUM_WORKERS"
echo "BATCH_SIZE: $BATCH_SIZE"
echo "REPORTING_PERIOD: $REPORTING_PERIOD"
echo "DATABASE_HOST: $DATABASE_HOST"
echo "DATABASE_PORT: $DATABASE_PORT"

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --db-name=${DATABASE_NAME} \
                                --backoff=${BACKOFF_SECS} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} 
                                #--urls=http://${DATABASE_HOST}:${DATABASE_PORT}
