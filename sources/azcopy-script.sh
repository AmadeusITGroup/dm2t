#!/usr/bin/bash

PROCESSING_TIME=$(date +%Y-%m-%d_%H-%M-%S)

# Initialize variables
SOURCE_CONTAINER=""
DESTINATION_CONTAINER=""
SRC_PATH=""
DST_PATH=""
STORAGE_ACCOUNT=""
LOG_LEVEL=""
CONCURRENCY_VALUE=4000

# Function to display help
show_help() {
  echo "Usage: $0 --src-container <source> --dst-container <destination> --src-path <src-path> --dst-path <dst-path> --src-storage-account <src-account> --dst-storage-account <dst-account> --log-level <level> --concurrency-value <value>"
  echo
  echo "Options:"
  echo "  --src-container       Source container"
  echo "  --dst-container       Destination container"
  echo "  --src-path            Source path"
  echo "  --dst-path            Destination path"
  echo "  --src-storage-account Source Storage account"
  echo "  --dst-storage-account Destination Storage account"
  echo "  --log-level           Log level"
  echo "  --concurrency-value   Concurrency value (default: 4000)"
  echo "  --help                Display this help message"
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
  --src-container)
    SRC_CONTAINER="$2"
    shift 2
    ;;
  --dst-container)
    DST_CONTAINER="$2"
    shift 2
    ;;
  --src-path)
    SRC_PATH="$2"
    shift 2
    ;;
  --dst-path)
    DST_PATH="$2"
    shift 2
    ;;
  --src-storage-account)
    SRC_STORAGE_ACCOUNT="$2"
    shift 2
    ;;
  --dst-storage-account)
    DST_STORAGE_ACCOUNT="$2"
    shift 2
    ;;
  --log-level)
    LOG_LEVEL="$2"
    shift 2
    ;;
  --concurrency-value)
    CONCURRENCY_VALUE="$2"
    shift 2
    ;;
  --help)
    show_help
    exit 0
    ;;
  *)
    echo "Invalid option: $1"
    show_help
    exit 1
    ;;
  esac
done

# Check for mandatory options
if [[ -z "$SRC_CONTAINER" || -z "$DST_CONTAINER" || -z "$SRC_PATH" || -z "$DST_PATH" || -z "$SRC_STORAGE_ACCOUNT" || -z "$DST_STORAGE_ACCOUNT" ]]; then
  echo "Error: Missing mandatory options."
  show_help
  exit 1
fi

echo "*******************************************************************************************"
echo "*                              DATA MESH TO DATA MESH COPY                                *"
echo "*******************************************************************************************"
echo " "
echo " "
echo "SRC_CONTAINER= [${SRC_CONTAINER}]"
echo "DST_CONTAINER= [${DST_CONTAINER}]"
echo "SRC_PATH= [${SRC_PATH}]"
echo "DST_PATH= [${DST_PATH}]"
echo "SRC_STORAGE_ACCOUNT= [${SRC_STORAGE_ACCOUNT}]"
echo "DST_STORAGE_ACCOUNT= [${DST_STORAGE_ACCOUNT}]"
echo "LOG_LEVEL= [${LOG_LEVEL}]"
echo "PROCESSING_TIME= [${PROCESSING_TIME}]"
echo "CONCURRENCY_VALUE= [${CONCURRENCY_VALUE}]"
echo " "
echo "*******************************************************************************************"

export AZCOPY_CONCURRENCY_VALUE=${CONCURRENCY_VALUE}

SOURCE_PATH="https://${SRC_STORAGE_ACCOUNT}.blob.core.windows.net/${SRC_CONTAINER}/${SRC_PATH}"
DESTINATION_PATH="https://${DST_STORAGE_ACCOUNT}.blob.core.windows.net/${DST_CONTAINER}/${DST_PATH}/"

azcopy login --identity

ROOT_LOGS="azcopy-logs"
mkdir -p ${ROOT_LOGS}

FORMATTED_SRC_PATH=$(echo ${SRC_PATH} | sed -E "s/\//_/g")
LOG_FILE="${ROOT_LOGS}/logs-${SRC_CONTAINER}-${FORMATTED_SRC_PATH}-${PROCESSING_TIME}.log"

echo "Launching sync from ${SOURCE_PATH} to ${DESTINATION_PATH}"
echo "Logs available in ${LOG_FILE}"
azcopy sync --recursive --log-level=${LOG_LEVEL} ${SOURCE_PATH} ${DESTINATION_PATH} >${LOG_FILE} 2>&1
RESULT_CODE=$?

grep "Elapsed Time (Minutes):" ${LOG_FILE}
grep "Final Job Status:" ${LOG_FILE}

exit $RESULT_CODE
