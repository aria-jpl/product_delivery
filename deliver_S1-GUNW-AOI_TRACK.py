import json
from hysds_commons.job_utils import submit_mozart_job


QUEUE = "asf-job_worker-large"
JOB_TYPE = "job-product-delivery"

def submit_job(params, job_type, tag, rule, product_id):
    """

    :param params:
    :param job_type:
    :param tag:
    :param rule:
    :param product_id:
    :return:
    """
    print('submitting jobs with params:')
    print(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    mozart_job_id = submit_mozart_job({}, rule,
                                      hysdsio={
                                          "id": "internal-temporary-wiring",
                                          "params": params,
                                          "job-specification": "{}:{}".format(job_type, tag)
                                      },
                                      job_name='%s-%s-%s' % (job_type, product_id, tag)
                                      )

    print("For {} , Product Delivery Job ID: {}".format(product_id, mozart_job_id))


def get_localize_json(file_extension, product_name, s3_url):
    info = dict()
    info["local_path"] = "{}/{}.{}".format(product_name, product_name, file_extension)
    info["url"] = "{}/{}.{}".format(s3_url, product_name, file_extension)
    return info


def submit_prod_deliv(product_name, s3_url, pub_sns, callback_sns, release_version, list_name):
    """
    Submit product delivery job for a give product
    :param product_name:
    :param s3_url:
    :param pub_sns:
    :param callback_sns:
    :param release_version:
    :param list_name:
    :return:
    """
    localize_url = list()
    localize_url.append("met.json", product_name, s3_url)
    localize_url.append("dataset.json", product_name, s3_url)

    params = [
        {
            "name": "localize_url",
            "from": "value",
            "value": localize_url
        },
        {
            "name": "product_name",
            "from": "value",
            "value": product_name
        },
        {
            "name": "s3_url",
            "from": "value",
            "value": s3_url
        },
        {
            "name": "path",
            "from": "value",
            "value": product_name
        },
        {
            "name": "pub_sns_arn",
            "from": "value",
            "value": pub_sns
        },
        {
            "name": "callback_sns_arn",
            "from": "value",
            "value": callback_sns
        }
    ]

    rule = {
        "rule_name": "product-delivery_{}".format(list_name),
        "queue": QUEUE,
        "priority": '5',
        "kwargs": '{}'
    }

    submit_job(params, job_type=JOB_TYPE, tag=release_version, rule=rule, product_id=product_name)


if __name__ == '__main__':
    ctx = json.loads(open("_context.json", "r"))
    aoi_track_list_name = ctx.get("product_name")
    track_list_metadata = ctx.get("product_metadata")
    pub_sns_arn = ctx.get("pub_sns_arn")
    callback_sns_arn = ctx.get("callback_sns_arn")
    tag = ctx.get("container_specification").get("version")

    # iterate over every product in AOI track list and submit prod delivery jobs
    product_ids = track_list_metadata.get("s1-gunw-ids")
    product_urls = track_list_metadata.get("s1-gunw_urls")

    for (prod_id, prod_url) in zip(product_ids, product_urls):
        submit_prod_deliv(product_name=prod_id, s3_url=prod_url, pub_sns=pub_sns_arn, callback_sns=callback_sns_arn,
                          release_version=tag, list_name=aoi_track_list_name)
