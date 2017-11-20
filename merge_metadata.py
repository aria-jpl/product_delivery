#!/usr/bin/env python
import os, sys, json, logging, traceback
import boto3

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('merge_metadata')


def merge(met_file, ds_file, merged_file):
    """Merge met and dataset JSON file."""

    with open(ds_file) as f:
        ds = json.load(f)
    logger.info("dataset: {}".format(json.dumps(ds, indent=2)))

    with open(met_file) as f:
        ds['metadata'] = json.load(f)
    logger.info("met: {}".format(json.dumps(ds['metadata'], indent=2)))

    with open(merged_file, 'w') as f:
        json.dump(ds, f, indent=2, sort_keys=True)
    logger.info("merged: {}".format(json.dumps(ds, indent=2)))
    logger.info("written to: {}".format(merged_file))

def putFile(merged_file, bucket_name, key_name):
    """Put merged metadata file on s3 bucket"""
    s3 = boto3.client('s3')
    s3.upload_file(merged_file, bucket_name, key_name)

if __name__ == '__main__':
    try: 
        status = merge(sys.argv[1], sys.argv[2], sys.argv[3])
        key = sys.argv[3]
        datasets_pos = key.find("/datasets")
        bucket_pos = key.rfind('/', 0, datasets_pos)
        bucket_name = key[bucket_pos+1:datasets_pos]
        putFile(sys.argv[3], bucket_name, key)
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
