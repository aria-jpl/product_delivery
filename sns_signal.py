#!/usr/bin/env python
import os, sys, json, boto3
from urlparse import urlparse
import datetime

# get args
product_name = sys.argv[1]
bucket_url =  sys.argv[2]
profile = sys.argv[3]
sns_arn = sys.argv[4]
purpose = sys.argv[5]

product_list = []
BROWSE_IMAGE_NAME="filt_topophase.unw.geo.browse_small.png"

if product_name.startswith('"'):
    product_name = product_name.replace('"','')

if bucket_url.startswith('"'):
    bucket_url = bucket_url.replace('"','')

#time stamp the message
delivery_time = str(datetime.datetime.utcnow().isoformat()) 
# get bucket
#bucket = urlparse(bucket_url).netloc
datasets_pos = bucket_url.find("/datasets")
bucket_pos = bucket_url.rfind('/', 0, datasets_pos)
bucket_name = bucket_url[bucket_pos+1:datasets_pos]
path = bucket_url[datasets_pos+1:]

#get browse_path and metadata_path
browse_path = path+'/'+BROWSE_IMAGE_NAME
metadata_path = path+'/'+product_name+"_delivery.dataset.json"

region = boto3.session.Session().region_name
topic_arn = None

sns = boto3.client('sns')
topic_list = sns.list_topics()

for topic in topic_list['Topics']:
    if purpose == "test":
        if topic['TopicArn'].endswith(":product-notification-test"):
            topic_arn = topic['TopicArn']
    else:
        if topic['TopicArn'].endswith(":product-notification"):
            topic_arn = topic['TopicArn']

s3 = boto3.client('s3')
result = s3.list_objects(Bucket=bucket_name, Prefix= path)
for res in result['Contents']:
    key =  res["Key"]
    start_pos = key.rfind(product_name)
    product_start = key.find('/', start_pos)
    if key[product_start+1:].startswith(path)==False:
        product_list.append(key[product_start+1:])
    else:
        new_start_pos = key[product_start+1:].find(product_name)
        new_prod_start = key.find('/', new_start_pos)
        product_list.append(key[new_prod_start+1:])

body = {
  "ResponseTopic": {
    "Region": region,
    "Arn": topic_arn
    },
  "ProductName": product_name,
  "DeliveryTime": delivery_time,
  "Browse": {
    "Bucket": bucket_name,
    "Key": browse_path
    },
  "Metadata": {
    "Bucket": bucket_name,
    "Key": metadata_path
   },  
  "ProductFiles": {
    "Keys": product_list,
    "Bucket": bucket_name,
    "Prefix": path 
   }
}

#printing out the sns message for log
print json.dumps(body)

session = boto3.session.Session(profile_name=profile)
sns = session.client('sns')
sns.publish(TopicArn=sns_arn, Message=json.dumps(body))
