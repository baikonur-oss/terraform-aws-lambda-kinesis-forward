import base64
import gzip
import json
import logging
import random
import string
import time
from json import JSONDecodeError
from typing import List

from aws_kinesis_agg.deaggregator import iter_deaggregate_records

from .misc import split_list

logger = logging.getLogger('root')
logger.setLevel(logging.INFO)


class KinesisException(Exception):
    pass


def normalize_payload(p: dict) -> List[dict]:
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


def create_json_records(data_list: list) -> List[dict]:
    """
    Create Kinesis Records for use with PutRecords API

    :param data_list: List of data to convert
    :return: List of Kinesis Records for PutRecords API
    """
    random_alphanumerical = string.ascii_lowercase + string.ascii_uppercase + string.digits

    records = []
    record: str
    for record in data_list:
        data_blob = json.dumps(record).encode('utf-8')
        partition_key: str = ''.join(random.choices(random_alphanumerical, k=20))  # max 256 chars
        records.append({
            'Data': data_blob,
            'PartitionKey': partition_key,
        })
    logger.debug(f"Formed Kinesis Records batch for PutRecords API: {records}")
    return records


def put_batch_json(client, stream_name: str, records: list, max_retries: int) -> None or List[dict]:
    """
    Put multiple records to Kinesis Data Streams using PutRecords API.

    :param client: Kinesis API client (e.g. boto3.client('kinesis') )
    :param stream_name: Kinesis Data Streams stream name
    :param records: list of records to send. Records will be dumped with json.dumps
    :param max_retries: Maximum retries for resending failed records
    :return: Records failed to put in Kinesis Data Stream after all retries
    """

    retry_list = []

    # Each PutRecords API request can support up to 500 records:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html#Kinesis.Client.put_records

    for batch_index, batch in enumerate(split_list(records, 500)):
        records_to_send = create_json_records(batch)
        retries_left = max_retries

        while len(records_to_send) > 0:
            kinesis_response = client.put_records(
                Records=records_to_send,
                StreamName=stream_name,
            )

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

    return None


def parse_json_logs(raw_records: list) -> dict:
    """
    Generator that de-aggregates, decodes, decompresses Kinesis Records and parses them as JSON.

    :param raw_records: Raw Kinesis records (usually event['Records'] in Lambda handler function)
    :return:
    """
    for record in iter_deaggregate_records(raw_records):
        logger.debug(f"Raw Kinesis record: {record}")

        # Kinesis data is base64 encoded
        raw_data = base64.b64decode(record['kinesis']['data'])

        # decompress data if raw data is gzip (log data from CloudWatch Logs subscription filters comes gzipped)
        # gzip magic number: 0x1f 0x8b
        if raw_data[0] == 0x1f and raw_data[1] == 0x8b:
            raw_data = gzip.decompress(raw_data)

        data = raw_data.decode()
        payloads = normalize_payload(data)
        logger.debug(f"Normalized payloads: {payloads}")

        for payload in payloads:
            yield payload
