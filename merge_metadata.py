#!/usr/bin/env python
import os, sys, json, logging, traceback
import boto3

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger('merge_metadata')


def merge(met_file, ds_file, merged_file):
    """Merge met and dataset JSON file."""
    if met_file.startswith('"'):
        met_file = met_file[1:-1]
    if ds_file.startswith('"'):
        ds_file = ds_file[1:-1]
    if merged_file.startswith('"'):
        merged_file = merged_file[1:-1]

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
    key_name = key_name + merged_file
    s3 = boto3.client('s3')
    s3.upload_file(merged_file, bucket_name, key_name)

if __name__ == '__main__':
    try:
        met_file = sys.argv[1]
        ds_file = sys.argv[2]
        merged_file = sys.argv[3]

        if met_file.startswith('"'):
            met_file = met_file.replace('"','')

        if ds_file.startswith('"'):
            ds_file = ds_file.replace('"','')

        if merged_file.startswith('"'):
            merged_file = merged_file.replace('"','')

        status = merge(met_file, ds_file, merged_file)

        key = sys.argv[4]
        if key.startswith('"'):
            key = key.replace('"','')
 
        datasets_pos = key.find("/datasets")
        bucket_pos = key.rfind('/', 0, datasets_pos)
        bucket_name = key[bucket_pos+1:datasets_pos]

        key = key[datasets_pos+1:]
        putFile(merged_file, bucket_name, key)
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
