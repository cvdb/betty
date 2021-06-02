import os
import math
import datetime
from decimal import Decimal
import time
import json
import pandas as pd
import numpy as np
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

def utc2local(utc):
    tz = pytz.timezone('Australia/Sydney')
    return pytz.utc.localize(utc).astimezone(tz)

def get_todays_events(in_play):
    thoroughbreds_event_filter = betfairlightweight.filters.market_filter(
        event_type_ids=[get_horse_racing_event_type_id()],
        market_countries=['AU'],
        in_play_only=in_play,
        bsp_only=True,
        market_start_time={
            'to': (datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime("%Y-%m-%dT%TZ")
        }
    )
    # Get a list of all thoroughbred events as objects
    aus_thoroughbred_events = trading.betting.list_events(
        filter=thoroughbreds_event_filter
    )
    # Create a DataFrame with all the events by iterating over each event object
    aus_thoroughbred_events_today = pd.DataFrame({
        'event_name': [event_object.event.name for event_object in aus_thoroughbred_events],
        'event_id': [event_object.event.id for event_object in aus_thoroughbred_events],
        'event_venue': [event_object.event.venue for event_object in aus_thoroughbred_events],
        'country_code': [event_object.event.country_code for event_object in aus_thoroughbred_events],
        'time_zone': [event_object.event.time_zone for event_object in aus_thoroughbred_events],
        'open_date_utc': [event_object.event.open_date for event_object in aus_thoroughbred_events],
        'open_date': [utc2local(event_object.event.open_date) for event_object in aus_thoroughbred_events],
        'market_count': [event_object.market_count for event_object in aus_thoroughbred_events]
    })
    return aus_thoroughbred_events_today

def is_target_market(market_catalogue):
    return (market_catalogue.description.bsp_market 
        and market_catalogue.description.turn_in_play_enabled
        and market_catalogue.description.market_type=='WIN')

# returns up to 10 markets, 
# first to start, matching filter criteria
def get_market_catalogues():
    market_catalogue_filter = betfairlightweight.filters.market_filter(
        event_type_ids=['7'],
        market_countries=['AU'],
        # in_play_only=True,
        bsp_only=True
    )
    market_catalogues = trading.betting.list_market_catalogue(
        filter=market_catalogue_filter,
        market_projection=["MARKET_START_TIME", "MARKET_DESCRIPTION", "EVENT"],
        max_results='5',
        sort='FIRST_TO_START'
    )

    venue_names = []
    event_names = []
    market_names = []
    market_ids = []
    market_start_times_utc = []
    market_start_times = []
    totals_matched = []

    for market_cat_object in market_catalogues:
        if not is_target_market(market_cat_object):
            continue
        # print(market_cat_object.json())
        venue_names.append(market_cat_object.event.venue)
        event_names.append(market_cat_object.event.name)
        market_names.append(market_cat_object.market_name)
        market_ids.append(market_cat_object.market_id)
        market_start_times_utc.append(market_cat_object.market_start_time)
        market_start_times.append(utc2local(market_cat_object.market_start_time))
        totals_matched.append(market_cat_object.total_matched)

    # Create a DataFrame for each market catalogue
    df = pd.DataFrame({
        'venue_name': venue_names,
        'event_name': event_names,
        'market_name': market_names,
        'market_id': market_ids,
        'market_start_time_utc': market_start_times_utc,
        'market_start_time': market_start_times,
        'total_matched': totals_matched,
    })
    df.sort_values(by=['market_start_time'], inplace=True)
    return df


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


def get_market_books(df_market_catalogues):
    price_filter = betfairlightweight.filters.price_projection(price_data=['EX_BEST_OFFERS', 'SP_AVAILABLE', 'SP_TRADED'])
    data = [[] for i in range(11)]

    # Request market books each on their own
    # tis is done because we need to fetch OPEN & CLOSED
    # markets separately.
    for row in df_market_catalogues.itertuples():
        # Request market books
        mbooks = trading.betting.list_market_book(
            market_ids=[row.market_id],
            price_projection=price_filter,
            order_projection='ALL'
        )
        mb = mbooks[0]

        # print(mb.json())
        data[0].append(mb.market_id)
        data[1].append(row.market_start_time_utc)
        data[2].append(row.market_start_time)
        data[3].append(mb.status)
        data[4].append(mb.bsp_reconciled)
        data[5].append(mb.inplay)
        data[6].append(mb.total_matched)
        best_runner = get_fav_runner(mb.runners)
        if best_runner and best_runner.status == 'ACTIVE':
            data[7].append(best_runner.selection_id)
            data[8].append(best_runner.sp.near_price)
            data[9].append(best_runner.sp.far_price)
            data[10].append(best_runner.sp.actual_sp)
        else:
            data[7].append(None)
            data[8].append(None)
            data[9].append(None)
            data[10].append(None)

    # extract orders placed against the runner here    

    df = pd.DataFrame({
        'market_id': data[0],
        'market_start_time_utc': data[1],
        'market_start_time': data[2],
        'market_status': data[3],
        'bsp_reconciled': data[4],
        'inplay': data[5],
        'total_matched': data[6],
        'runner_selection_id': data[7],
        'runner_near_bsp': data[8],
        'runner_far_bsp': data[9],
        'runner_bsp': data[10],
    })
    return df

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


def should_load_market_book(market_book):
    market_data = ('\n' + str(market_book.get('market_id')) 
            + ', now UTC:' + datetime.datetime.utcnow().isoformat()
            + ', starts at:' + market_book.get('market_start_time_utc').isoformat()
            + ', BSP reconciled: ' + str(market_book.get('bsp_reconciled'))
            + ', BSP: ' + str(market_book.get('runner_bsp'))
            + ', status: ' + str(market_book.get('market_status')))

    # only look at markets that start in LESS THAN 2 minutes
    # skip markets that only start in 5 minutes for example.
    soonest_start_dt_utc = datetime.datetime.utcnow() + datetime.timedelta(minutes=2)
    if market_book.get('market_start_time_utc') > soonest_start_dt_utc:
        print('skipping market, not starting soon:' + market_data)
        return False

    print('LOADING market:' + market_data)
    return True
    
    
def put_market_book(market_book, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table('market_books')

    # In this case we only want to insert if not exists
    current = get_market_book(market_book.get('market_id'), dynamodb)
    if current:
        return

    if not should_load_market_book(market_book):
        return
    
    response = table.put_item(
       Item={
            'market_id': market_book.get('market_id'),
            'market_start_time_utc': market_book.get('market_start_time_utc').isoformat(),
            'market_start_time': market_book.get('market_start_time').isoformat(),
            'market_status': market_book.get('market_status'),
            'bsp_reconciled': market_book.get('bsp_reconciled'),
            'inplay': market_book.get('inplay'),
            'total_matched': as_decimal(market_book.get('total_matched')),
            'runner_selection_id': str(market_book.get('runner_selection_id')),
            'runner_near_bsp': as_decimal(market_book.get('runner_near_bsp')),
            'runner_far_bsp': as_decimal(market_book.get('runner_far_bsp')),
            'runner_bsp': as_decimal(market_book.get('runner_bsp'))
        }
    )
    return response


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

def lambda_handler(event, context):
    try:
        df_market_catalogues = get_market_catalogues()
        df_market_books = get_market_books(df_market_catalogues)
        dynamodb = boto3.resource('dynamodb',region_name='ap-southeast-2')
        # dynamodb = None
        mbooks_dict = df_market_books.to_dict(orient='records')
        for mbook in mbooks_dict:
            # put_market_book(mbook, dynamodb)
            pprint(mbook)
        return { "statusCode": 200 }
    except Exception as e:
        print(e)
        return { "statusCode": 500 }




if __name__ == '__main__':
    # create_market_book_table()
    lambda_handler(None, None)
    # mb = {
    #     'market_id': '12345', 
    #     'market_start_time_utc': datetime.datetime.utcnow(), 
    #     'market_start_time': datetime.datetime.utcnow(), 
    #     'market_status': 'CLOSED', 
    #     'bsp_reconciled': False, 
    #     'inplay': False, 
    #     'total_matched': None, 
    #     'runner_selection_id': '3333333.444', 
    #     'runner_bsp': float('33.55')
    #     }
    # put_market_book({
    #     'market_id': mb['market_id'], 
    #     'market_start_time_utc': mb['market_start_time_utc'], 
    #     'market_start_time': mb['market_start_time'], 
    #     'market_status': mb['market_status'], 
    #     'bsp_reconciled': mb['bsp_reconciled'], 
    #     'inplay': mb['inplay'], 
    #     'total_matched': Decimal(str((mb['total_matched'] or 0))), 
    #     'runner_selection_id': mb['runner_selection_id'], 
    #     'runner_bsp': Decimal(str((mb['runner_bsp'] or 0)))
    #     })
    # pprint(get_market_book('12345'), sort_dicts=False)








