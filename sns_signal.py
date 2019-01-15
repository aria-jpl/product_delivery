#!/usr/bin/env python
import os, sys, json, boto3, re
from urlparse import urlparse
import datetime

S3_RE = re.compile(r's3://.+?/(.+?)/(.+)$')


# get args
product_name = sys.argv[1]
bucket_url =  sys.argv[2]
pub_sns_arn = sys.argv[3]
callback_sns_arn = sys.argv[4]

if product_name.startswith('"'):
    product_name = product_name.replace('"','')

if bucket_url.startswith('"'):
    bucket_url = bucket_url.replace('"','')

#time stamp the message
delivery_time = str(datetime.datetime.utcnow().isoformat()) 
# get bucket
#bucket = urlparse(bucket_url).netloc
#datasets_pos = bucket_url.find("/datasets")
#bucket_pos = bucket_url.rfind('/', 0, datasets_pos)
#bucket_name = bucket_url[bucket_pos+1:datasets_pos]
#path = bucket_url[datasets_pos+1:]
bucket_name, path = S3_RE.search(bucket_url).groups()

#get browse_path, metadata_path, and product_path
browse_path = path+'/'+product_name+".interferogram.browse_coarse.png"
metadata_path = path+'/'+product_name+"_delivery.dataset.json"
product_path = path+'/'+product_name+".nc"

my_region = boto3.session.Session().region_name

body = {
  "ResponseTopic": {
    "Region": my_region,
    "Arn": callback_sns_arn
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
  "Product": {
    "Bucket": bucket_name,
    "Key": product_path
   }
}

#printing out the sns message for log
print json.dumps(body)

region_start_pos = pub_sns_arn.find(':us-')
region_end_pos = pub_sns_arn.find(':',region_start_pos+1)

region = pub_sns_arn[region_start_pos+1:region_end_pos]

session = boto3.session.Session()
sns = session.client('sns', region_name = region)
sns.publish(TopicArn=pub_sns_arn, Message=json.dumps(body))
