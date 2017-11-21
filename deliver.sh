#!/bin/bash -x
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# get args
PRODUCT_NAME="$1"
S3_URL="$2"
PROD_PATH="$3"
SNS_ARN="$4"
AWS_PROFILE="$5"
BROWSE_IMAGE_NAME="filt_topophase.unw.geo.browse_small.png"

# source environment
source $HOME/verdi/bin/activate

# delivery date
dt=`date -u +"%Y%m%dT%H%M%SZ"`

# delivery product
deliv="delivery-${dt}-${PROD}"

echo "##########################################" 1>&2
echo -n "Merge met and dataset file: " 1>&2
date 1>&2
${BASE_PATH}/merge_metadata.py ${PROD_PATH}[0]/${PRODUCT_NAME}[0].met.json ${PROD_PATH}[0]/${PRODUCT_NAME}[0].dataset.json ${PRODUCT_NAME}[0]_delivery.dataset.json ${S3_URL}[0] 1>&2
STATUS=$?
echo -n "Finished merging met and dataset file: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to merge met and dataset file." 1>&2
  echo "{}"
  exit $STATUS
fi

#echo "##########################################" 1>&2
#echo -n "Copying out browse image(${BROWSE_IMAGE_NAME}): " 1>&2
#date 1>&2
#cp ${PROD}/${BROWSE_IMAGE_NAME} ${BROWSE_IMAGE_NAME} 1>&2
#STATUS=$?
#echo -n "Finished copy of browse image: " 1>&2
#date 1>&2
#if [ $STATUS -ne 0 ]; then
#  echo "Failed to copy browse image(${BROWSE_IMAGE_NAME})." 1>&2
#  echo "{}"
#  exit $STATUS
#fi

#echo "##########################################" 1>&2
#echo -n "Zipping product: " 1>&2
#date 1>&2
#zip -r ${PROD}.zip $PROD 1>&2
#STATUS=$?
#echo -n "Finished zipping product: " 1>&2
#date 1>&2
#if [ $STATUS -ne 0 ]; then
#  echo "Failed to zip product." 1>&2
#  echo "{}"
#  exit $STATUS
#fi

#echo "##########################################" 1>&2
#echo -n "Delivering zipped product: " 1>&2
#date 1>&2
#aws --profile $AWS_PROFILE s3 cp ${PROD}.zip ${DEST_BUCKET}/${deliv}/${PROD}.zip 1>&2
#STATUS=$?
#echo -n "Finished delivering zipped product: " 1>&2
#date 1>&2
#if [ $STATUS -ne 0 ]; then
#  echo "Failed to deliver zipped product." 1>&2
#  echo "{}"
#  exit $STATUS
#fi

#echo "##########################################" 1>&2
#echo -n "Delivering zipped product metadata: " 1>&2
#date 1>&2
#aws --profile $AWS_PROFILE s3 cp ${PROD}.dataset.json ${DEST_BUCKET}/${deliv}/${deliv}.dataset.json 1>&2
#STATUS=$?
#echo -n "Finished delivering zipped product metadata: " 1>&2
#date 1>&2
#if [ $STATUS -ne 0 ]; then
#  echo "Failed to deliver zipped product metadata." 1>&2
#  echo "{}"
#  exit $STATUS
#fi

#echo "##########################################" 1>&2
#echo -n "Delivering browse image: " 1>&2
#date 1>&2
#aws --profile $AWS_PROFILE s3 cp ${BROWSE_IMAGE_NAME} ${DEST_BUCKET}/${deliv}/${BROWSE_IMAGE_NAME} 1>&2
#STATUS=$?
#echo -n "Finished delivering browse image: " 1>&2
#date 1>&2
#if [ $STATUS -ne 0 ]; then
#  echo "Failed to deliver browse image." 1>&2
#  echo "{}"
#  exit $STATUS
#fi

echo "##########################################" 1>&2
echo -n "Queueing delivery message to SNS: " 1>&2
date 1>&2
${BASE_PATH}/sns_signal.py ${PRODUCT_NAME}[0] ${PROD_PATH}[0] ${AWS_PROFILE} ${SNS_ARN}  1>&2
STATUS=$?
echo -n "Finished queuing delivery message to SNS: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to queue delivery message to SNS." 1>&2
  echo "{}"
  exit $STATUS
fi

# cleanup
#rm -rf ${PROD} ${PROD}.zip
