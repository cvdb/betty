import os
import math
import datetime
from decimal import Decimal
import time
import json
import pytz
from pprint import pprint
import boto3
from botocore.exceptions import ClientError


import betfairlightweight
from betfairlightweight import filters
from betfairlightweight.resources.bettingresources import (
    PriceSize,
    MarketBook
)


# create trading instance
# in this case we expect cert to be in same location as this file.
dir_path = os.path.dirname(os.path.realpath(__file__))
my_username = "???"
my_password = "???"
my_app_key = "???"
trading = betfairlightweight.APIClient(username=my_username,
                                       password=my_password,
                                       app_key=my_app_key,
                                       certs=dir_path)

trading.login()

def as_decimal(val):
    if not val:
        return Decimal('0')
    if isinstance(val, str):
        if val.isdecimal():
            return Decimal(val)
        else:
            return Decimal('0')
    if math.isnan(val):
        return Decimal('0')
    return Decimal(str(val))

def utc2local(utc):
    tz = pytz.timezone('Australia/Sydney')
    return pytz.utc.localize(utc).astimezone(tz)

# find the ACTIVE runner with the lowest BSP
# NEAR price estimate is more accurate so use that.
# NOTE: ACTUAL SP is only calculated once market goes IN_PLAY
# and we can only beT on BSP PRE_PLAY.
def get_fav_runner(runners):
    # NOTE: near & far price data is only available with the LIVE KEY.
    # For testing we will look for ACTUAL SP after the market has gone IN_PLAY.
    # We will try place bets at BSP but these may not be matched.
    best_runner = None;
    for runner in runners:
        if not best_runner:
            best_runner = runner
        else:            
            if (runner and runner.status == 'ACTIVE' 
                    and runner.sp
                    # and runner.sp.near_price
                    # and runner.sp.near_price < best_runner.sp.near_price):
                    and runner.sp.actual_sp
                    and runner.sp.actual_sp < best_runner.sp.actual_sp):
                best_runner = runner
    return best_runner

def format_market_book(mbook):
    mb = {}
    # Only include data that can change here
    best_runner = get_fav_runner(mbook.runners)
    if best_runner:
        mb['runner_selection_id'] = best_runner.selection_id
        mb['runner_near_bsp'] = best_runner.sp.near_price
        mb['runner_far_bsp'] = best_runner.sp.far_price
        mb['runner_bsp'] = best_runner.sp.actual_sp
    else:
        mb['runner_selection_id'] = None
        mb['runner_near_bsp'] = None
        mb['runner_far_bsp'] = None
        mb['runner_bsp'] = None
    mb['market_id'] = mbook.market_id 
    mb['market_status'] = mbook.status 
    mb['bsp_reconciled'] = mbook.bsp_reconciled
    mb['inplay'] = mbook.inplay
    mb['total_matched'] = mbook.total_matched
    return mb


def get_market_books(marketids):
    price_filter = betfairlightweight.filters.price_projection(price_data=['EX_BEST_OFFERS', 'SP_AVAILABLE', 'SP_TRADED'])
    # Please note: Separate requests should be made for OPEN & CLOSED markets. 
    # Request that include both OPEN & CLOSED markets will only return those markets that are OPEN.
    # So if we don't get back a market here assume its now closed and fetch separately.
    mbooks = trading.betting.list_market_book(
        market_ids=marketids,
        price_projection=price_filter,
        order_projection='ALL'
    )
    # extract orders placed against the runner here    
    return list(map(format_market_book, mbooks))

def should_bet_market_book(market_book):
    market_data = ('\n' + str(market_book.get('market_id')) 
            + ', BSP reconciled: ' + str(market_book.get('bsp_reconciled'))
            + ', BSP: ' + str(market_book.get('runner_near_bsp'))
            + ', status: ' + str(market_book.get('market_status')))

    # NOTE: for testing bet on BSP IN_PLAY
    # if market_book.get('bsp_reconciled'):
    #     print('skipping market, BSP reconciled:' + market_data)
    #     return False

    if not market_book.get('bsp_reconciled'):
        print('skipping market, BSP note yet reconciled:' + market_data)
        return False

    if market_book.get('market_status') == 'CLOSED':
        print('skipping market, CLOSED:' + market_data)
        return False

    # For testing we use actual BSP
    #if as_decimal(market_book.get('runner_near_bsp')) > 2:
    #    print('skipping market, BSP too high:' + market_data)
    #    return False

    if as_decimal(market_book.get('runner_bsp')) > 2 or as_decimal(market_book.get('runner_bsp')) <= 0:
        print('skipping market, BSP too high or ZERO:' + market_data)
        return False
        
    # Try best as close to START as possible here....
        
    print('BETTING market:' + market_data)
    return True


def put_market_book(record, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table('market_books')
    response = table.put_item(
       Item={
            'market_id': record['market_id']['S'],
            'market_start_time_utc': record['market_start_time_utc']['S'],
            'market_start_time': record['market_start_time']['S'],
            'market_status': record['market_status']['S'],
            'bsp_reconciled': record['bsp_reconciled']['BOOL'],
            'inplay': record['inplay']['BOOL'],
            'total_matched': as_decimal(record['total_matched']['N']),
            'runner_selection_id': str(record['runner_selection_id']['S']),
            'runner_near_bsp': as_decimal(record['runner_near_bsp']['N']),
            'runner_far_bsp': as_decimal(record['runner_far_bsp']['N']),
            'runner_bsp': as_decimal(record['runner_bsp']['N']),
            'updated_at': record['updated_at']['S']
        }
    )
    return response

def put_bet_market_book(record, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table('bet_market_books')
    
    # Here we need to ensure we only ever insert 1 row in this table per bet
    # because it triggers placing a bet.
    response = table.put_item(
        ConditionExpression='attribute_not_exists(market_id)',
        Item={
            'market_id': record['market_id']['S'],
            'market_start_time_utc': record['market_start_time_utc']['S'],
            'market_start_time': record['market_start_time']['S'],
            'market_status': record['market_status']['S'],
            'bsp_reconciled': record['bsp_reconciled']['BOOL'],
            'inplay': record['inplay']['BOOL'],
            'in_progress': True,
            'total_matched': as_decimal(record['total_matched']['N']),
            'runner_selection_id': str(record['runner_selection_id']['S']),
            'runner_near_bsp': as_decimal(record['runner_near_bsp']['N']),
            'runner_far_bsp': as_decimal(record['runner_far_bsp']['N']),
            'runner_bsp': as_decimal(record['runner_bsp']['N']),
            'updated_at': record['updated_at']['S']
        }
    )
    return response

# This is only for local testing
def create_market_book_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
        TableName='market_books',
        KeySchema=[
            {
                'AttributeName': 'market_id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'market_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    return table

def get_bet_market_book(market_id, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table('bet_market_books')
    try:
        response = table.get_item(Key={'market_id': market_id})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response.get('Item')


# This is only for local testing
def get_market_book(market_id, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table('market_books')
    try:
        response = table.get_item(Key={'market_id': market_id})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response.get('Item')


def merge_changed_data(rec, mbook):
    print('MERGING:' + json.dumps(mbook))
    rec['runner_selection_id']['S'] = mbook.get('runner_selection_id')
    rec['runner_near_bsp']['N']= mbook.get('runner_near_bsp')
    rec['runner_far_bsp']['N']= mbook.get('runner_far_bsp')
    rec['runner_bsp']['N']= mbook.get('runner_bsp')
    rec['market_status']['S'] = mbook.get('market_status')
    rec['bsp_reconciled']['BOOL'] = mbook.get('bsp_reconciled')
    rec['inplay']['BOOL'] = mbook.get('inplay')
    rec['total_matched']['N'] = mbook.get('total_matched')
    rec['updated_at'] = {}
    rec['updated_at']['S'] = datetime.datetime.utcnow().isoformat()
    return rec


def lambda_handler(event, context):
    # based on the list of market books in the event
    # fetch the latest data for each and save to the Dynamo DB table if changed.
    # Check each market mook to see if we want to bet on it.
    # If we want to bet, insert a record into the bet_market_books table.
    try:
        dynamodb = boto3.resource('dynamodb',region_name='ap-southeast-2')
        marketids = list(map(lambda rec: rec['dynamodb']['Keys']['market_id']['S'], event['Records']))
        records = list(map(lambda rec: rec['dynamodb']['NewImage'], event['Records']))
        mbooks = get_market_books(marketids)
        for rec in records:
            mbook = next(mb for mb in mbooks if mb['market_id'] == rec['market_id']['S'])
            if mbook:
                # skip CLOSED markets
                if mbook.get('market_status') == 'CLOSED':
                    print('STOP TRACKING')
                    continue
                rec = merge_changed_data(rec, mbook)
                if should_bet_market_book(mbook):
                    print('BETTING:' + json.dumps(rec))
                    put_market_book(rec, dynamodb)
                    put_bet_market_book(rec, dynamodb)
                else:
                    print('PUTTING:' + json.dumps(rec))
                    put_market_book(rec, dynamodb)
            else:
                print('NOT FOUND:' + json.dumps(rec))

        return { "statusCode": 200 }
    except Exception as e:
        print(e)
        return { "statusCode": 500 }


 

# if __name__ == '__main__':
    # create_market_book_table()
    # lambda_handler(event, None)
    # put_market_book({ 
    #     'market_id': '12345', 
    #     'market_start_time_utc': '12345', 
    #     'market_start_time': '12345', 
    #     'market_status': 'CLOSED', 
    #     'bsp_reconciled': False, 
    #     'inplay': False, 
    #     'total_matched': Decimal(str(999.99)), 
    #     'runner_selection_id': '3333333.444', 
    #     'runner_bsp': Decimal(str(123.99))
    #     })
    # pprint(get_market_book('12345'), sort_dicts=False)









