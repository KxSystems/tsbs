#!/usr/bin/env bash

set -euo pipefail

SCRIPTDIR=$(dirname $(realpath $0))
source ${SCRIPTDIR}/env

if [ -f "${DBDIR}/par.txt" ]; then
    for dbdir in $(cat ${DBDIR}/par.txt); do
        if [ -d ${dbdir} ]; then
            rm -rf ${dbdir}
            mkdir -p ${dbdir}
        fi
    done
    rm -f ${DBDIR}/sym
else
    echo "Deleting kdb+ data at ${DBDIR}"
    rm -rf ${DBDIR}
fi