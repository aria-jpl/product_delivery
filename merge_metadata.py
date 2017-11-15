#!/usr/bin/env python
import os, sys, json, logging, traceback


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


if __name__ == '__main__':
    try: status = merge(sys.argv[1], sys.argv[2], sys.argv[3])
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
