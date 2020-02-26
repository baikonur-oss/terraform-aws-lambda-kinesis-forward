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
kinesis_client = boto3.client('kinesis')

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
KINESIS_MAX_RETRIES: int = int(os.environ['KINESIS_MAX_RETRIES'])


class KinesisException(Exception):
    pass


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


def normalize_kinesis_payload(p: dict):
    # Normalize messages from CloudWatch (subscription filters) and pass through anything else
    # https://docs.aws.amazon.com/ja_jp/AmazonCloudWatch/latest/logs/SubscriptionFilters.html

    logger.debug(f"normalizer input: {p}")

    if len(p) < 1:
        logger.error(f"Got weird record, skipping: {p}")
        return []

    # check if data is JSON and parse
    try:
        payload = json.loads(p)
        if type(payload) is not dict:
            logger.error(f"Top-level JSON data is not an object, giving up: {payload}")
            return []

    except JSONDecodeError:
        logger.error(f"Non-JSON data found: {p}, giving up")
        return []

    if 'messageType' not in payload:
        return [payload]

    # messageType is present in payload, must be coming from CloudWatch
    logger.debug(f"Got payload looking like CloudWatch Logs via subscription filters: {payload}")

    return extract_data_from_cwl_message(payload)


def extract_data_from_cwl_message(payload):
    # see: https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/SubscriptionFilters.html
    if payload['messageType'] == "CONTROL_MESSAGE":
        logger.info(f"Got CONTROL_MESSAGE from CloudWatch: {payload}, skipping")
        return []

    elif payload['messageType'] == "DATA_MESSAGE":
        payloads = []

        if 'logEvents' not in payload:
            logger.error(f"Got DATA_MESSAGE from CloudWatch Logs but logEvents are not present, "
                         f"skipping payload: {payload}")
            return []

        events = payload['logEvents']

        for event in events:
            # check if data is JSON and parse
            try:
                logger.debug(f"message: {event['message']}")
                payload_parsed = json.loads(event['message'])
                logger.debug(f"parsed payload: {payload_parsed}")

                if type(payload_parsed) is not dict:
                    logger.error(f"Top-level JSON data in CloudWatch Logs payload is not an object, skipping: "
                                 f"{payload_parsed}")
                    continue

            except JSONDecodeError as e:
                logger.debug(e)
                logger.debug(f"Non-JSON data found inside CloudWatch Logs message: {event}, skipping")
                continue

            payloads.append(payload_parsed)

        return payloads

    else:
        logger.error(f"Got unknown messageType: {payload['messageType']} , skipping")
        return []


def dict_get_default(dictionary, key, default, verbose=False):
    if key not in dictionary:
        logger.warning(f"Cannot retrieve field \"{key}\" "
                       f"from data: {dictionary}")
        if verbose:
            logger.warning(f"Falling back to default value: {default}")
        return default

    else:
        return dictionary[key]


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


def split_list(l, size):
    for i in range(0, len(l), size):
        yield l[i:i+size]


def kinesis_put(log_records: list):
    xray_recorder.begin_subsegment(f"kinesis put records")

    retry_list = []
    failed_list = []

    # Each PutRecords request can support up to 500 records
    # see: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.put_records

    for batch_index, batch in enumerate(split_list(log_records, 500)):
        records = []
        for record in batch:
            data_blob = json.dumps(record).encode('utf-8')
            partition_key: str = ''.join(random.choices(RANDOM_ALPHANUMERICAL, k=20)) # max 256 chars
            records.append({
                'Data':            data_blob,
                'PartitionKey':    partition_key,
            })

        logger.debug(records)

        retry_count = 0
        while len(records) > 0:
            subsegment = xray_recorder.begin_subsegment(f"put records batch {batch_index} retry {retry_count}")
            response = kinesis_client.put_records(
                Records=records,
                StreamName=TARGET_STREAM_NAME,
            )
            subsegment.put_annotation("records", len(records))
            subsegment.put_annotation("failed", response['FailedRecordCount'])
            xray_recorder.end_subsegment()

            if response['FailedRecordCount'] == 0:
                xray_recorder.end_subsegment()
                break
            else:
                retry_count += 1
                subsegment.put_annotation("failed_records", response['FailedRecordCount'])
                for index, record in enumerate(response['Records']):
                    if 'ErrorCode' in record:
                        if record['ErrorCode'] == 'ProvisionedThroughputExceededException':
                            retry_list.append(records[index])
                        elif record['ErrorCode'] == 'InternalFailure':
                            failed_list.append(records[index])

                records = retry_list
                retry_list = []

                if len(retry_list) > 0:
                    logger.info(f"Waiting 1 second for capacity")
                    time.sleep(1)
            xray_recorder.end_subsegment()

    xray_recorder.end_subsegment()
    return failed_list


def save_failed(log_dict: dict):
    for log_type in log_dict:
        if log_type.startswith(LOG_TYPE_UNKNOWN_PREFIX):
            xray_recorder.begin_subsegment(f"bad data upload: {log_type}")

            data = log_dict[log_type]['records']
            logger.error(f"Got {len(data)} failed Kinesis records ({log_type})")

            timestamp = log_dict[log_type]['first_timestamp']
            key = FAILED_LOG_S3_PREFIX + '/' + timestamp.strftime("%Y-%m/%d/%Y-%m-%d-%H:%M:%S-")
            key += log_dict[log_type]['first_id'] + ".gz"

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
        failed_records = kinesis_put(log_dict_filtered[key]['records'])
        if len(failed_records) > 0:
            logger.error(f"Got failed records from Kinesis: {failed_records}")

    logger.info("Finished")
