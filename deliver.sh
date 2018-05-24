#!/bin/bash -x
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# get args
PRODUCT_NAME="$1"
S3_URL="$2"
PROD_PATH="$3"
SNS_ARN="$4"
PURPOSE="$5"
BROWSE_IMAGE_NAME="filt_topophase.unw.geo.browse_small.png"

if [[ $PRODUCT_NAME = \[* ]] ; then
   PRODUCT_NAME=${PRODUCT_NAME:1:${#PRODUCT_NAME}-2}
fi

if [[ $S3_URL = \[* ]] ; then
   S3_URL=${S3_URL:1:${#S3_URL}-2}
fi

if [[ $PROD_PATH = \[* ]] ; then
   PROD_PATH=${PROD_PATH:1:${#PROD_PATH}-2}
fi

# source environment
source $HOME/verdi/bin/activate

# delivery date
dt=`date -u +"%Y%m%dT%H%M%SZ"`

# delivery product
deliv="delivery-${dt}-${PROD}"

echo "##########################################" 1>&2
echo -n "Merge met and dataset file: " 1>&2
date 1>&2
${BASE_PATH}/merge_metadata.py ${PROD_PATH}/${PRODUCT_NAME}.met.json ${PROD_PATH}/${PRODUCT_NAME}.dataset.json ${PRODUCT_NAME}_delivery.dataset.json ${S3_URL} 1>&2
STATUS=$?
echo -n "Finished merging met and dataset file: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to merge met and dataset file." 1>&2
  echo "{}"
  exit $STATUS
fi

echo "##########################################" 1>&2
echo -n "Queueing delivery message to SNS: " 1>&2
date 1>&2
${BASE_PATH}/sns_signal.py ${PRODUCT_NAME} ${S3_URL} ${SNS_ARN} ${PURPOSE} 1>&2
STATUS=$?
echo -n "Finished queuing delivery message to SNS: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to queue delivery message to SNS." 1>&2
  echo "{}"
  exit $STATUS
fi

# cleanup
if [[ $PRODUCT_NAME = \"* ]] ; then
   PRODUCT_NAME=${PRODUCT_NAME:1:${#PRODUCT_NAME}-2}
fi
rm -rf ${PRODUCT_NAME}
rm -rf ${PRODUCT_NAME}_delivery.dataset.json
