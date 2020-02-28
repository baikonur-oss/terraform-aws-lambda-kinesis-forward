# Baikonur Kinesis/Lambda Logging specific methods
import datetime
import logging
import uuid
from typing import Any

import dateutil.parser

from . import s3

logger = logging.getLogger('root')


def parse_payload_to_log_dict(payload,
                              log_dict,
                              failed_dict,
                              log_id_key,
                              log_timestamp_key,
                              log_type_key,
                              log_type_unknown_prefix,
                              log_type_whitelist=None):
    logger.debug(f"Parsing normalized payload: {payload}")

    log_type_unknown = f"{log_type_unknown_prefix}/unknown_type"

    log_type, log_type_missing = dict_get_default(
        payload,
        key=log_type_key,
        default=log_type_unknown,
        verbose=True,
    )

    if log_type_missing:
        target_dict = failed_dict
    else:
        target_dict = log_dict
        if (log_type_whitelist is not None) and (log_type not in log_type_whitelist):
            return

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

    return


logger.setLevel(logging.INFO)


def save_json_logs_to_s3(client, log_dict: dict, reason: str = "not specified", gzip_compress: bool = True,
                         key_prefix: str = ""):
    logger.info(f"Saving logs to S3. Reason: {reason}")

    for log_type in log_dict:
        timestamp = log_dict[log_type]['first_timestamp']
        key = key_prefix + '/' + timestamp.strftime("%Y-%m/%d/%Y-%m-%d-%H:%M:%S-")

        key += log_dict[log_type]['first_id'] + ".gz"

        data = log_dict[log_type]['records']
        data = '\n'.join(str(f) for f in data)

        logger.info(f"Saving logs to S3: s3://{key_prefix}/{key}")
        s3.put_str_data(client, key_prefix, key, data, gzip_compress)


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
            'records': list(),
            'first_timestamp': log_timestamp,
            'first_id': log_id,
        }

    dictionary[log_type]['records'].append(log_data)


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
