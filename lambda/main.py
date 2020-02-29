import json
import logging
import os

import boto3
from amazon_kinesis_utils import baikonur_logging
from amazon_kinesis_utils import kinesis
from aws_xray_sdk.core import patch
from aws_xray_sdk.core import xray_recorder

# set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Loading function')

# patch boto3 with X-Ray
libraries = ('boto3', 'botocore')
patch(libraries)

# global client instances
s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')

# configure with env vars
FAILED_LOG_S3_PREFIX: str = os.environ['FAILED_LOG_S3_PREFIX']
FAILED_LOG_S3_BUCKET: str = os.environ['FAILED_LOG_S3_BUCKET']

LOG_ID_FIELD: str = os.environ['LOG_ID_FIELD']
LOG_TYPE_FIELD: str = os.environ['LOG_TYPE_FIELD']
LOG_TIMESTAMP_FIELD: str = os.environ['LOG_TIMESTAMP_FIELD']
LOG_TYPE_UNKNOWN_PREFIX: str = os.environ['LOG_TYPE_UNKNOWN_PREFIX']

LOG_TYPE_FIELD_WHITELIST_TMP: list = str(os.environ['LOG_TYPE_WHITELIST']).split(',')
if len(LOG_TYPE_FIELD_WHITELIST_TMP) == 0:
    LOG_TYPE_FIELD_WHITELIST = set()
else:
    LOG_TYPE_FIELD_WHITELIST = set(LOG_TYPE_FIELD_WHITELIST_TMP)

TARGET_STREAM_NAME: str = os.environ['TARGET_STREAM_NAME']
KINESIS_MAX_RETRIES: int = int(os.environ['KINESIS_MAX_RETRIES'])


def handler(event, context):
    raw_records = event['Records']
    logger.debug(raw_records)

    log_dict = dict()
    failed_dict = dict()

    xray_recorder.begin_subsegment('parse')
    for payload in kinesis.parse_records(raw_records):
        try:
            payload_parsed = json.loads(payload)
        except json.JSONDecodeError:
            logger.debug(f"Ignoring non-JSON data: {payload}")
            continue

        baikonur_logging.parse_payload_to_log_dict(
            payload_parsed,
            log_dict,
            failed_dict,
            LOG_TYPE_FIELD,
            LOG_TIMESTAMP_FIELD,
            LOG_ID_FIELD,
            LOG_TYPE_UNKNOWN_PREFIX,
            LOG_TYPE_FIELD_WHITELIST,
        )
    xray_recorder.end_subsegment()

    xray_recorder.begin_subsegment('kinesis PutRecords')
    for key in log_dict:
        logger.info(f"Processing log type {key}: {len(log_dict[key]['records'])} records")
        records_json = [json.dumps(x) for x in log_dict[key]['records']]
        kinesis.put_records_batch(kinesis_client, TARGET_STREAM_NAME, records_json, KINESIS_MAX_RETRIES)
    xray_recorder.end_subsegment()

    xray_recorder.begin_subsegment('s3 upload')
    baikonur_logging.save_json_logs_to_s3(s3_client, failed_dict, reason="Failed logs")
    xray_recorder.end_subsegment()

    logger.info("Finished")
