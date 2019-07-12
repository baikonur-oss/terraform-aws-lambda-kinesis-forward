import base64
import datetime
import gzip
import io
import json
import logging
import os
import random
import string
import time
import uuid
from json import JSONDecodeError

import boto3
import dateutil.parser
from aws_kinesis_agg.deaggregator import iter_deaggregate_records
from aws_xray_sdk.core import patch
from aws_xray_sdk.core import xray_recorder
from boto3.exceptions import S3UploadFailedError

# set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Loading function')

# when debug logging is needed, uncomment following lines:
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)

# patch boto3 with X-Ray
libraries = ('boto3', 'botocore')
patch(libraries)

# global client instances
s3 = boto3.client('s3')
kinesis_client = boto3.client('kinesis', region_name='ap-northeast-1')

# consts
RANDOM_ALPHANUMERICAL = string.ascii_lowercase + string.ascii_uppercase + string.digits

# configure with env vars
FAILED_LOG_S3_PREFIX: str = os.environ['FAILED_LOG_S3_PREFIX']
FAILED_LOG_S3_BUCKET: str = os.environ['FAILED_LOG_S3_BUCKET']

LOG_ID_FIELD: str = os.environ['LOG_ID_FIELD']
LOG_TYPE_FIELD: str = os.environ['LOG_TYPE_FIELD']
LOG_TIMESTAMP_FIELD: str = os.environ['LOG_TIMESTAMP_FIELD']
LOG_TYPE_FIELD_WHITELIST: list = str(os.environ['LOG_TYPE_WHITELIST']).split(',')
LOG_TYPE_UNKNOWN_PREFIX: str = os.environ['LOG_TYPE_UNKNOWN_PREFIX']

TARGET_STREAM_NAME: str = os.environ['TARGET_STREAM_NAME']


def append_to_dict(dictionary: dict, log_type: str, log_data: object, log_timestamp=None, log_id=None):
    if log_type not in dictionary:
        # we've got first record for this type, initialize value for type

        # first record timestamp to use in file path
        if log_timestamp:
            try:
                log_timestamp = dateutil.parser.parse(log_timestamp)
            except TypeError:
                logger.error(f"Bad timestamp: {log_timestamp}")
                logger.info(f"Falling back to current time for type \"{log_type}\"")
                log_timestamp = datetime.datetime.now()
        else:
            logger.info(f"No timestamp for first record")
            logger.info(f"Falling back to current time for type \"{log_type}\"")
            log_timestamp = datetime.datetime.now()

        # first record log_id field to use as filename suffix to prevent duplicate files
        if log_id:
            logger.info(f"Using first log record ID as filename suffix: {log_id}")
        else:
            log_id = str(uuid.uuid4())
            logger.info(f"First log record ID is not available, using random ID as filename suffix instead: {log_id}")

        dictionary[log_type] = {
            'records':         list(),
            'first_timestamp': log_timestamp,
            'first_id':        log_id,
        }

    dictionary[log_type]['records'].append(log_data)


def normalize_kinesis_payload(payload: dict):
    # Normalize messages from CloudWatch (subscription filters) and pass through anything else
    # https://docs.aws.amazon.com/ja_jp/AmazonCloudWatch/latest/logs/SubscriptionFilters.html

    logger.debug(f"normalizer input: {payload}")

    payloads = []

    if len(payload) < 1:
        logger.error(f"Got weird record: \"{payload}\", skipping")
        return payloads

    # check if data is JSON and parse
    try:
        payload = json.loads(payload)

    except JSONDecodeError:
        logger.error(f"Non-JSON data found: {payload}, skipping")
        return payloads

    if 'messageType' in payload:
        logger.debug(f"Got payload looking like CloudWatch Logs via subscription filters: "
                     f"{payload}")

        if payload['messageType'] == "DATA_MESSAGE":
            if 'logEvents' in payload:
                for event in payload['logEvents']:
                    # check if data is JSON and parse
                    try:
                        logger.debug(f"message: {event['message']}")
                        payload_parsed = json.loads(event['message'])
                        logger.debug(f"parsed payload: {payload_parsed}")

                    except JSONDecodeError:
                        logger.debug(f"Non-JSON data found inside CWL message: {event}, giving up")
                        continue

                    payloads.append(payload_parsed)

            else:
                logger.error(f"Got DATA_MESSAGE from CloudWatch but logEvents are not present, "
                             f"skipping payload: {payload}")

        elif payload['messageType'] == "CONTROL_MESSAGE":
            logger.info(f"Got CONTROL_MESSAGE from CloudWatch: {payload}, skipping")
            return payloads

        else:
            logger.error(f"Got unknown messageType, shutting down")
            raise ValueError(f"Unknown messageType: {payload}")
    else:
        payloads.append(payload)
        
    return payloads


def decode_validate(raw_records: list):
    xray_recorder.begin_subsegment('decode and validate')

    log_dict = dict()

    processed_records = 0

    for record in iter_deaggregate_records(raw_records):
        logger.debug(f"raw Kinesis record: {record}")
        # Kinesis data is base64 encoded
        decoded_data = base64.b64decode(record['kinesis']['data'])

        # check if base64 contents is gzip
        # gzip magic number 0x1f 0x8b
        if decoded_data[0] == 0x1f and decoded_data[1] == 0x8b:
            decoded_data = gzip.decompress(decoded_data)

        decoded_data = decoded_data.decode()
        normalized_payloads = normalize_kinesis_payload(decoded_data)
        logger.debug(f"Normalized payloads: {normalized_payloads}")

        for normalized_payload in normalized_payloads:
            logger.debug(f"Parsing normalized payload: {normalized_payload}")

            processed_records += 1

            # check if log type field is available
            try:
                log_type = normalized_payload[LOG_TYPE_FIELD]

            except KeyError:
                logger.error(f"Cannot retrieve necessary field \"{LOG_TYPE_FIELD}\" "
                             f"from payload: {normalized_payload}")
                log_type = f"{LOG_TYPE_UNKNOWN_PREFIX}/unknown_type"
                logger.error(f"Marking as {log_type}")

            # check if timestamp is present
            try:
                timestamp = normalized_payload[LOG_TIMESTAMP_FIELD]

            except KeyError:
                logger.error(f"Cannot retrieve recommended field \"{LOG_TIMESTAMP_FIELD}\" "
                             f"from payload: {normalized_payload}")
                timestamp = None

            try:
                log_id = normalized_payload[LOG_ID_FIELD]
            except KeyError:
                logger.error(f"Cannot retrieve recommended field \"{LOG_ID_FIELD}\" "
                             f"from payload: {normalized_payload}")
                log_id = None

            # valid data
            append_to_dict(log_dict, log_type, normalized_payload, log_timestamp=timestamp, log_id=log_id)

    logger.info(f"Processed {processed_records} records from Kinesis")
    xray_recorder.end_subsegment()
    return log_dict


def apply_whitelist(log_dict: dict, whitelist: list):
    retval = dict()
    if len(whitelist) == 0:
        for key in log_dict.keys():
            if not key.startswith(LOG_TYPE_UNKNOWN_PREFIX):
                retval[key] = log_dict[key]
        return retval

    for entry in whitelist:
        if entry in log_dict:
            retval[entry] = log_dict[entry]
    return retval


def kinesis_put(log_records: list):
    failed = list()
    for record in log_records:
        failed_records = []
        print(record)

        data_blob = json.dumps(record).encode('utf-8')

        success = False
        while not success:

            try:
                partition_key: str = ''.join(random.choices(RANDOM_ALPHANUMERICAL, k=20))
                response = kinesis_client.put_record(
                    StreamName=TARGET_STREAM_NAME,
                    Data=data_blob,
                    PartitionKey=partition_key,
                )

            except kinesis_client.exceptions.ResourceNotFoundException as e:
                logger.error(e)
                raise e

            except kinesis_client.exceptions.ProvisionedThroughputExceededException as e:
                logger.warning(f"Provisioned throughput is exceeded, sleeping for a second and trying again: {e}")
                time.sleep(1)
            except kinesis_client.exceptions.InternalFailureException as e:
                logger.error(f"Something bad happened: {e}")
                failed_records += data_blob
            else:
                logger.debug(f"> Response from Kinesis: {response}")
                success = True

    return failed


def save_failed(log_dict: dict):
    for log_type in log_dict:
        if log_type.startswith(LOG_TYPE_UNKNOWN_PREFIX):
            xray_recorder.begin_subsegment(f"bad data upload: {log_type}")

            data = log_dict[log_type]['records']
            logger.error(f"Got {len(data)} failed Kinesis records ({log_type})")

            timestamp = log_type['first_timestamp']
            key = FAILED_LOG_S3_PREFIX + '/' + timestamp.strftime("%Y-%m/%d/%Y-%m-%d-%H:%M:%S-")
            key += log_type['first_id'] + ".gz"

            logger.info(f"Saving failed records to S3: s3://{FAILED_LOG_S3_BUCKET}/{key}")
            data = '\n'.join(str(f) for f in data)
            put_to_s3_gzip(key, FAILED_LOG_S3_BUCKET, data)

            xray_recorder.end_subsegment()


def put_to_s3_gzip(key: str, bucket: str, data: str):
    # gzip and put data to s3 in-memory
    xray_recorder.begin_subsegment('gzip compress')
    data_gz = gzip.compress(data.encode(), compresslevel=9)
    xray_recorder.end_subsegment()

    xray_recorder.begin_subsegment('s3 upload')
    try:
        with io.BytesIO(data_gz) as data_gz_fileobj:
            s3_results = s3.upload_fileobj(data_gz_fileobj, bucket, key)

        logger.info(f"S3 upload errors: {s3_results}")

    except S3UploadFailedError as e:
        logger.error("Upload failed. Error:")
        logger.error(e)
        import traceback
        traceback.print_stack()
        raise
    xray_recorder.end_subsegment()


def handler(event, context):
    # check if stream exists:
    response = kinesis_client.describe_stream(
        StreamName=TARGET_STREAM_NAME,
    )

    if not response:
        raise kinesis_client.exceptions.ResourceNotFoundException()

    raw_records = event['Records']

    logger.debug(raw_records)

    log_dict: dict = decode_validate(raw_records)
    save_failed(log_dict)
    log_dict_filtered: dict = apply_whitelist(log_dict, LOG_TYPE_FIELD_WHITELIST)
    logger.debug(log_dict_filtered)
    for key in log_dict_filtered:
        logger.info(f"Processing log type {key}, {len(log_dict_filtered[key]['records'])} records")
        kinesis_put(log_dict_filtered[key]['records'])

    logger.info("Finished")
