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
from typing import List, Any, Tuple, Dict

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


class KinesisException(Exception):
    pass


def append_to_log_dict(dictionary: dict, log_type: str, log_data: object, log_timestamp=None, log_id=None):
    if log_type not in dictionary:
        # we've got first record for this type, initialize value for type

        # first record timestamp to use in file path
        if log_timestamp is None:
            logger.info(f"No timestamp for first record")
            logger.info(f"Falling back to current time for type \"{log_type}\"")
            log_timestamp = datetime.datetime.now()
        else:
            try:
                log_timestamp = dateutil.parser.parse(log_timestamp)
            except TypeError:
                logger.error(f"Bad timestamp: {log_timestamp}")
                logger.info(f"Falling back to current time for type \"{log_type}\"")
                log_timestamp = datetime.datetime.now()

        # first record log_id field to use as filename suffix to prevent duplicate files
        if log_id is None:
            log_id = str(uuid.uuid4())
            logger.info(f"First log record ID is not available, using random ID as filename suffix instead: {log_id}")
        else:
            logger.info(f"Using first log record ID as filename suffix: {log_id}")

        dictionary[log_type] = {
            'records':         list(),
            'first_timestamp': log_timestamp,
            'first_id':        log_id,
        }

    dictionary[log_type]['records'].append(log_data)


def normalize_kinesis_payload(p: dict) -> List[dict]:
    # Normalize messages from CloudWatch (subscription filters) and pass through anything else
    # https://docs.aws.amazon.com/ja_jp/AmazonCloudWatch/latest/logs/SubscriptionFilters.html

    logger.debug(f"Normalizer input: {p}")

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

    return extract_json_data_from_cwl_message(payload)


def extract_json_data_from_cwl_message(payload: dict) -> List[dict]:
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


def dict_get_default(dictionary: dict, key: str, default: any, verbose: bool = False) -> Any:
    """
    Get key from dictionary if key is in dictionary, default value otherwise

    :param dictionary: dictionary to retrieve key from
    :param key: key name in dictionary
    :param default: value to return if key is not in dictionary
    :param verbose: output detailed warning message when returning default value
    :return: value for key if key is in dictionary, default value otherwise
    """
    if key not in dictionary:
        if verbose:
            logger.warning(f"Cannot retrieve field \"{key}\" from data: {dictionary}, "
                           f"falling back to default value: {default}")
        return default, True

    else:
        return dictionary[key], False


def parse_json_logs(raw_kinesis_records: list) -> List[dict]:
    """
    Deaggregates, decodes, decompresses Kinesis Records and parses them as JSON
    events by log_type.

    :param raw_kinesis_records: Raw Kinesis records (usually event['Records'] in Lambda handler function)
    :return:
    """
    parent_segment = xray_recorder.begin_subsegment('parse_json_logs')

    all_payloads = list()

    processed_records = 0

    for record in iter_deaggregate_records(raw_kinesis_records):
        processed_records += 1

        logger.debug(f"Raw Kinesis record: {record}")

        # Kinesis data is base64 encoded
        raw_data = base64.b64decode(record['kinesis']['data'])

        # decompress data if raw data is gzip (log data from CloudWatch Logs subscription filters comes gzipped)
        # gzip magic number: 0x1f 0x8b
        if raw_data[0] == 0x1f and raw_data[1] == 0x8b:
            raw_data = gzip.decompress(raw_data)

        data = raw_data.decode()
        payloads = normalize_kinesis_payload(data)
        logger.debug(f"Normalized payloads: {payloads}")

        for payload in payloads:
            all_payloads.append(payload)

    logger.info(f"Processed {processed_records} records from Kinesis")
    parent_segment.end_subsegment()

    return all_payloads


def parse_payloads_to_log_dict(payloads,
                               log_id_key,
                               log_timestamp_key,
                               log_type_key,
                               log_type_whitelist) -> Tuple[Dict[str, dict], Dict[str, dict]]:

    log_dict = dict()
    failed_dict = dict()

    for payload in payloads:
        target_dict = log_dict

        logger.debug(f"Parsing normalized payload: {payload}")

        log_type_unknown = f"{LOG_TYPE_UNKNOWN_PREFIX}/unknown_type"

        log_type, log_type_missing = dict_get_default(
            payload,
            key=log_type_key,
            default=log_type_unknown,
            verbose=True,
        )

        if log_type_missing:
            target_dict = failed_dict
        else:
            if (log_type_whitelist is not None) and (log_type not in log_type_whitelist):
                continue

        timestamp, _ = dict_get_default(
            payload,
            key=log_timestamp_key,
            default=None,
        )

        log_id, _ = dict_get_default(
            payload,
            key=log_id_key,
            default=None,
        )

        # valid data
        append_to_log_dict(target_dict, log_type, payload, log_timestamp=timestamp, log_id=log_id)

    return log_dict, failed_dict


def split_list(lst: list, size: int) -> List[list]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def kinesis_put_batch_json(client, records: list, max_retries: int) -> None or List[dict]:
    """
    Put multiple records to Kinesis Data Streams using PutRecords API.


    :param client: Kinesis API client (e.g. boto3.client('kinesis') )
    :param records: list of records to send. Records will be dumped with json.dumps
    :param max_retries: Maximum retries for resending failed records
    :return: Records failed to put in Kinesis Data Stream after all retries
    """
    parent_segment = xray_recorder.begin_subsegment(f"kinesis_put_batch_json")

    retry_list = []

    # Each PutRecords API request can support up to 500 records:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.put_records

    for batch_index, batch in enumerate(split_list(records, 500)):
        records_to_send = create_kinesis_records_json(batch)
        retries_left = max_retries

        while len(records_to_send) > 0:
            subsegment = xray_recorder.begin_subsegment(f"kinesis_put_batch_json try")
            kinesis_response = client.put_records(
                Records=records_to_send,
                StreamName=TARGET_STREAM_NAME,
            )
            subsegment.put_annotation("batch_index", batch_index)
            subsegment.put_annotation("records", len(records_to_send))
            subsegment.put_annotation("records_failed", kinesis_response['FailedRecordCount'])
            subsegment.end_subsegment()

            if kinesis_response['FailedRecordCount'] == 0:
                break
            else:
                index: int
                record: dict
                for index, record in enumerate(kinesis_response['Records']):
                    if 'ErrorCode' in record:
                        # original records list and response record list have same order, guaranteed:
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.put_records
                        logger.error(f"A record failed with error: {record['ErrorCode']} {record['ErrorMessage']}")
                        retry_list.append(records_to_send[index])

                records_to_send = retry_list
                retry_list = []

                if retries_left == 0:
                    error_msg = f"No retries left, giving up on records: {records_to_send}"
                    logger.error(error_msg)
                    return records_to_send

                retries_left -= 1

                logger.info(f"Waiting 500 ms before retrying")
                time.sleep(0.5)

    parent_segment.end_subsegment()

    return None


def create_kinesis_records_json(batch: List[dict]) -> List[dict]:
    random_alphanumerical = string.ascii_lowercase + string.ascii_uppercase + string.digits

    records = []
    record: str
    for record in batch:
        data_blob = json.dumps(record).encode('utf-8')
        partition_key: str = ''.join(random.choices(random_alphanumerical, k=20))  # max 256 chars
        records.append({
            'Data':         data_blob,
            'PartitionKey': partition_key,
        })
    logger.debug(f"Formed Kinesis Records batch for PutRecords API: {records}")
    return records


def save_json_logs_to_s3(client, log_dict: dict, reason: str = "not specified"):
    logger.info(f"Saving logs to S3. Reason: {reason}")

    xray_recorder.begin_subsegment(f"s3 upload")

    for log_type in log_dict:
        xray_recorder.begin_subsegment(f"s3 upload: {log_type}")

        timestamp = log_dict[log_type]['first_timestamp']
        key = FAILED_LOG_S3_PREFIX + '/' + timestamp.strftime("%Y-%m/%d/%Y-%m-%d-%H:%M:%S-")

        key += log_dict[log_type]['first_id'] + ".gz"

        data = log_dict[log_type]['records']
        data = '\n'.join(str(f) for f in data)

        logger.info(f"Saving logs to S3: s3://{FAILED_LOG_S3_BUCKET}/{key}")
        put_to_s3(client, FAILED_LOG_S3_BUCKET, key, data, gzip_compress=True)

        xray_recorder.end_subsegment()

    xray_recorder.end_subsegment()


def put_to_s3(client, bucket: str, key: str, data: str, gzip_compress: bool = False):
    if gzip_compress:
        # gzip and put data to s3 in-memory
        xray_recorder.begin_subsegment('gzip compress')
        data_p = gzip.compress(data.encode(), compresslevel=9)
        xray_recorder.end_subsegment()
    else:
        data_p = data

    xray_recorder.begin_subsegment('s3 upload')
    try:
        with io.BytesIO(data_p) as fileobj:
            s3_results = client.upload_fileobj(fileobj, bucket, key)

        logger.info(f"S3 upload errors: {s3_results}")

    except S3UploadFailedError as e:
        logger.error("Upload failed. Error:")
        logger.error(e)
        import traceback
        traceback.print_stack()
        raise
    xray_recorder.end_subsegment()


def handler(event, context):
    raw_records = event['Records']
    logger.debug(raw_records)

    log_dict: dict
    failed_dict: dict

    payloads = parse_json_logs(raw_records)

    log_dict, failed_dict = parse_payloads_to_log_dict(
        payloads,
        LOG_TYPE_FIELD,
        LOG_TIMESTAMP_FIELD,
        LOG_ID_FIELD,
        LOG_TYPE_FIELD_WHITELIST,
    )

    for key in log_dict:
        logger.info(f"Processing log type {key}: {len(log_dict[key]['records'])} records")
        kinesis_put_batch_json(kinesis_client, log_dict[key]['records'], KINESIS_MAX_RETRIES)

    save_json_logs_to_s3(s3, failed_dict, reason="Failed logs")

    logger.info("Finished")
