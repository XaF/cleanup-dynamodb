#!/usr/bin/env python

import argparse
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
import errno
import logging
import OpenSSL
import os
from socket import error as SocketError
import sys
import time


class Namespace(object):
    """from dynamodb-bnr"""
    def __init__(self, adict = None):
        if adict is not None:
            self.__dict__.update(adict)

    def update(self, adict):
        self.__dict__.update(adict)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __str__(self):
        return str(self.__dict__)


_global_aws = {
    'client': {},
    'resource': {},
}

_global_session = None

const_parameters = Namespace({
    'throughputexceeded_sleeptime':
        int(os.getenv('DYNAMODB_BNR_THROUGHPUTEXCEEDED_SLEEPTIME', 10)),
    'throughputexceeded_maxretry':
        int(os.getenv('DYNAMODB_BNR_THROUGHPUTEXCEEDED_MAXRETRY', 5)),
    'connresetbypeer_sleeptime':
        int(os.getenv('DYNAMODB_BNR_ECONNRESET_MAXRETRY', 15)),
    'connresetbypeer_maxretry':
        int(os.getenv('DYNAMODB_BNR_ECONNRESET_MAXRETRY', 5)),
    'opensslerror_maxretry':
        int(os.getenv('DYNAMODB_BNR_OPENSSLERROR_MAXRETRY', 5)),
    'dynamodb_max_batch_write':
        int(os.getenv('DYNAMODB_BNR_MAX_BATCH_WRITE', 25)),
})


def get_session_aws():
    global _global_session
    if _global_session is None:
        if parameters.profile:
            _global_session = boto3.Session(profile_name=parameters.profile)
        else:
            _global_session = boto3.Session(
                region_name=parameters.region,
                aws_access_key_id=parameters.access_key,
                aws_secret_access_key=parameters.secret_key,
            )
    return _global_session


def get_client_aws(service):
    global _global_aws
    if service not in _global_aws['client']:
        _global_aws['client'][service] = get_session_aws().client(
            service_name=service
        )
    return _global_aws['client'][service]


def get_resource_aws(service):
    global _global_aws
    if service not in _global_aws['resource']:
        _global_aws['resource'][service] = get_session_aws().resource(
            service_name=service
        )
    return _global_aws['resource'][service]


def get_client_dynamodb():
    return get_client_aws('dynamodb')


def get_resource_dynamodb():
    return get_resource_aws('dynamodb')


def manage_db_scan(table, **kwargs):
    """from dynamodb-bnr"""
    items_list = None
    throughputexceeded_currentretry = 0
    connresetbypeer_currentretry = 0
    while items_list is None:
        try:
            items_list = table.scan(**kwargs)
        except SocketError as e:
            if e.errno != errno.ECONNRESET or \
                    connresetbypeer_currentretry >= const_parameters.connresetbypeer_maxretry:
                raise

            connresetbypeer_currentretry += 1
            sleeptime = const_parameters.connresetbypeer_sleeptime
            sleeptime *= connresetbypeer_currentretry
            logger.info(('Got \'Connection reset by peer\', '
                         'waiting {} seconds before retry').format(sleeptime))
            time.sleep(sleeptime)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ProvisionedThroughputExceededException' or \
                    throughputexceeded_currentretry >= const_parameters.throughputexceeded_maxretry:
                raise

            throughputexceeded_currentretry += 1
            sleeptime = const_parameters.throughputexceeded_sleeptime
            sleeptime *= throughputexceeded_currentretry
            logger.info(('Got \'ProvisionedThroughputExceededException\', '
                         'waiting {} seconds before retry').format(sleeptime))
            time.sleep(sleeptime)

    return items_list


def table_batch_write(client, table_name, items, request_type='PutRequest'):
    """from dynamodb-bnr"""
    if request_type == 'PutRequest':
        request_type_key = 'Item'
    elif request_type == 'DeleteRequest':
        request_type_key = 'Key'
    else:
        raise RuntimeError('Unknown request type: {}'.format(request_type))

    requests = []
    for item in items[:const_parameters.dynamodb_max_batch_write]:
        requests.append({
            request_type: {
                request_type_key: item
            }
        })

    request_items = {
        'RequestItems': {
            table_name: requests,
        }
    }

    response = None
    throughputexceeded_currentretry = 0
    while response is None:
        try:
            response = client.batch_write_item(**request_items)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ProvisionedThroughputExceededException' or \
                    throughputexceeded_currentretry >= const_parameters.throughputexceeded_maxretry:
                raise e

            throughputexceeded_currentretry += 1
            sleeptime = const_parameters.throughputexceeded_sleeptime
            sleeptime *= throughputexceeded_currentretry
            logger.info(('Got \'ProvisionedThroughputExceededException\', '
                         'waiting {} seconds before retry').format(sleeptime))
            time.sleep(sleeptime)

    returnedItems = []
    if 'UnprocessedItems' in response and response['UnprocessedItems']:
        for put_request in response['UnprocessedItems'][table_name]:
            item = put_request[request_type][request_type_key]
            returnedItems.append(item)

    items[:const_parameters.dynamodb_max_batch_write] = returnedItems

    if returnedItems:
        nreturnedItems = len(returnedItems)
        sleeptime = nreturnedItems * 2
        message = ('Table \'{0}\': {1} item(s) returned during '
                   'batch write; sleeping {2} second(s) to avoid '
                   'congestion').format(
            table_name, nreturnedItems, sleeptime)
        logger.info(message)
        time.sleep(sleeptime)

    return items


def parser_error(errmsg):
    parser.print_usage()
    print('{}: error: {}'.format(
        os.path.basename(__file__), errmsg))
    sys.exit(1)


def run():
    # Check that AWS EC2 configuration is available
    if parameters.profile is None and \
            (parameters.access_key is None or
             parameters.secret_key is None or
             parameters.region is None):
        parser_error(('AWS EC2 configuration is incomplete '
                      '(access key? {}, secret key? {}, region? {}'
                      ') or profile? {})').format(
            parameters.access_key is not None,
            parameters.secret_key is not None,
            parameters.region is not None,
            parameters.profile is not None))

    # Preparing connection
    client_ddb = get_client_dynamodb()
    resource_ddb = get_resource_dynamodb()

    # Getting schema to identify primary key
    logger.info('Getting schema from table \'{}\''.format(parameters.table))
    table_schema = None
    current_retry = 0
    while table_schema is None:
        try:
            table_schema = client_ddb.describe_table(TableName=parameters.table)
            table_schema = table_schema['Table']
        except (OpenSSL.SSL.SysCallError, OpenSSL.SSL.Error) as e:
            if current_retry >= const_parameters.opensslerror_maxretry:
                raise

            logger.warning("Got OpenSSL error, retrying...")
            current_retry += 1

    if 'KeySchema' not in table_schema or not table_schema['KeySchema']:
        raise RuntimeError('No key found for table {}'.format(parameters.table))

    table_keys = [x['AttributeName'] for x in table_schema['KeySchema']]
    comparison_attribute = Attr(parameters.field)

    if not hasattr(comparison_attribute, parameters.comparison_operator):
        parser_error('Invalid operator: {}'.format(
            parameters.comparison_operator))

    comparison = getattr(
        comparison_attribute,
        parameters.comparison_operator)(parameters.comparison_value)

    more_items = 1
    items = []

    table = resource_ddb.Table(parameters.table)

    items_list = manage_db_scan(
        table,
        FilterExpression=comparison
    )

    if items_list['ScannedCount'] == 0:
        logger.info('No item in the table.')
        return

    nbItemsDeleted = 0
    nbItemsScanned = 0

    while more_items and items_list['ScannedCount'] > 0:
        logger.info(('Reading items from table'
                     ' \'{}\' ({}) - {}/{} found').format(
                    parameters.table,
                    more_items,
                    items_list['Count'],
                    items_list['ScannedCount']))

        items.extend(
            dict((name, {'S': item[name]}) for name in table_keys)
            for item in items_list['Items'])
        nbItemsDeleted += items_list['Count']
        nbItemsScanned += items_list['ScannedCount']

        if 'LastEvaluatedKey' in items_list:
            items_list = manage_db_scan(
                table,
                ExclusiveStartKey=items_list['LastEvaluatedKey'],
                FilterExpression=comparison)
            more_items += 1
        else:
            more_items = False

        if items:
            logger.info('{} items to be deleted...'.format(len(items)))
            last_info = int(len(items) / parameters.print_period)
            while len(items) >= const_parameters.dynamodb_max_batch_write or \
                    (not more_items and len(items) > 0):
                logger.debug('Current number of items to delete: '.format(len(items)))

                if last_info != int(len(items) / parameters.print_period):
                    last_info = int(len(items) / parameters.print_period)
                    logger.info(('{} items left to delete '
                                 'in current scan...').format(len(items)))

                items = table_batch_write(
                    client_ddb,
                    parameters.table,
                    items,
                    'DeleteRequest')

    if nbItemsDeleted == 0:
        logger.info('No item to delete for {} items scanned.'.format(
            nbItemsScanned))
        return

    logger.info('{} items deleted for {} items scanned.'.format(
        nbItemsDeleted, nbItemsScanned))


parser = None
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Python script to cleanup a DynamoDB table that matches '
                    'criteria for a given field')

    # Logging options
    parser.add_argument(
        '-d', '--debug',
        dest='loglevel',
        default='INFO',
        action='store_const', const='DEBUG',
        help='Activate debug output')
    parser.add_argument(
        '--loglevel',
        dest='loglevel',
        default='INFO',
        choices=('NOTSET', 'DEBUG', 'INFO',
                 'WARNING', 'ERROR', 'CRITICAL'),
        help='Define the specific log level')
    parser.add_argument(
        '--print-period',
        type=int,
        default=100,
        help='The number of elements to delete before printing '
             'an information about the number of elements left to delete')

    # AWS general options
    parser.add_argument(
        '--profile',
        default=os.getenv('AWS_DEFAULT_PROFILE', None),
        help='Define the AWS profile name (defaults to the environment '
             'variable AWS_DEFAULT_PROFILE)')
    parser.add_argument(
        '--access-key', '--accessKey',
        default=os.getenv('AWS_ACCESS_KEY_ID', None),
        help='Define the AWS default access key (defaults to the '
             'environment variable AWS_ACCESS_KEY_ID)')
    parser.add_argument(
        '--secret-key', '--secretKey',
        default=os.getenv('AWS_SECRET_ACCESS_KEY', None),
        help='Define the AWS default secret key (defaults to the '
             'environment variable AWS_SECRET_ACCESS_KEY)')
    parser.add_argument(
        '--region',
        default=os.getenv('REGION', None),
        help='Define the AWS default region (defaults to the environment '
             'variable REGION)')

    parser.add_argument(
        '--table',
        default=None,
        required=True,
        help='The table to clean up')
    parser.add_argument(
        '--field',
        default='expire',
        help='The field to use for the clean up')
    parser.add_argument(
        '--comparison-operator', '--operator',
        default='lt',
        help='The comparison operator to use')
    parser.add_argument(
        '--comparison-type', '--type',
        default='int',
        help='The type to use for the comparison value')
    parser.add_argument(
        '--comparison-value', '--value',
        default=int(time.time() - 86400),
        help='The comparison operator to use')

    parameters = parser.parse_args()

    parameters.comparison_type = getattr(
        __builtins__, parameters.comparison_type)
    parameters.comparison_value = parameters.comparison_type(
        parameters.comparison_value)

    logging.basicConfig(
        format='%(asctime)s::%(name)s::%(processName)s'
               '::%(levelname)s::%(message)s',
        level=getattr(logging, parameters.loglevel))

    if parameters.loglevel != 'DEBUG':
        for name, logger in logging.root.manager.loggerDict.items():
            logger.disabled = True

    logger = logging.getLogger(os.path.basename(__file__))

    run()
