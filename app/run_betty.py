import argparse
import os
import datetime
import time
import json
import pandas as pd
import numpy as np
import pytz

import betfairlightweight
from betfairlightweight import filters
from betfairlightweight.resources.bettingresources import (
    PriceSize,
    MarketBook
)


# create trading instance
certs_path = r'/home/cvdb/projects/betty/betty/app/key/'
my_username = "???"
my_password = "???"
my_app_key = "???"
trading = betfairlightweight.APIClient(username=my_username,
                                       password=my_password,
                                       app_key=my_app_key,
                                       certs=certs_path)

trading.login()

def get_horse_racing_event_type_id():
    # Filter for just horse racing
    horse_racing_filter = betfairlightweight.filters.market_filter(text_query='Horse Racing')
    # This returns a list
    horse_racing_event_type = trading.betting.list_event_types(
        filter=horse_racing_filter)
    # Get the first element of the list
    horse_racing_event_type = horse_racing_event_type[0]
    horse_racing_event_type_id = horse_racing_event_type.event_type.id
    # print(f"The event type id for horse racing is {horse_racing_event_type_id}")
    return horse_racing_event_type_id

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
        event_type_ids=[get_horse_racing_event_type_id()],
        market_countries=['AU'],
        # in_play_only=True,
        bsp_only=True
    )
    market_catalogues = trading.betting.list_market_catalogue(
        filter=market_catalogue_filter,
        market_projection=["MARKET_START_TIME", "MARKET_DESCRIPTION", "EVENT"],
        max_results='10',
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
        market_start_times_utc.append(market_cat_object.market_start_time.strftime("%Y-%m-%dT%TZ"))
        market_start_times.append(utc2local(market_cat_object.market_start_time).strftime("%Y-%m-%dT%TZ"))
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

# find the ACTIVE runner with the lowest BSP
def get_fav_runner(runners):
    lowest_runner = runners[0];
    for runner in runners:
        print(runner.status)
        if runner.status == 'ACTIVE' and runner.sp.actual_sp < lowest_runner.sp.actual_sp:
            lowest_runner = runner
    return lowest_runner

def get_market_books(marketids):
    price_filter = betfairlightweight.filters.price_projection(price_data=['EX_BEST_OFFERS', 'SP_AVAILABLE', 'SP_TRADED'])
    market_books = []
    # Request market books each on their own
    # tis is done because we need to fetch OPEN & CLOSED
    # markets separately.
    for mid in marketids:
        # Request market books
        mbooks = trading.betting.list_market_book(
            market_ids=[mid],
            price_projection=price_filter,
            order_projection='ALL'
        )
        market_books.extend(mbooks)
    
    data = [[] for i in range(7)]

    for mb in market_books:
        # print(mb.json())
        data[0].append(mb.market_id)
        data[1].append(mb.status)
        data[2].append(mb.bsp_reconciled)
        data[3].append(mb.inplay)
        data[4].append(mb.total_matched)
        best_runner = None
        if mb.bsp_reconciled:
            best_runner = get_fav_runner(mb.runners)
        if best_runner and best_runner.status == 'ACTIVE':
            data[5].append(best_runner.selection_id)
            data[6].append(best_runner.sp.actual_sp)
        else:
            data[5].append(None)
            data[6].append(None)

    # extract orders placed against the runner here    

    df = pd.DataFrame({
        'market_id': data[0],
        'market_status': data[1],
        'bsp_reconciled': data[2],
        'inplay': data[3],
        'total_matched': data[4],
        'runner_selection_id': data[5],
        'runner_bsp': data[6],
    })
    return df

def place_bets():
    print('placing bets')
    df_market_catalogues = get_market_catalogues()
    print('\nmarket catalogues: \n' + str(df_market_catalogues))
    # now get the market book for each market_catalogue
    df_market_books = get_market_books(df_market_catalogues['market_id'].tolist())
    # df_market_books = get_market_books(['1.183229073'])
    print('\nmarket books: \n' + str(df_market_books))

def ProcessArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--command', help='run a command of some type')
    return parser.parse_args()

if __name__ == '__main__':
    args = ProcessArgs()

    if args.command == 'place_bets':
        place_bets()
    else:
        print('invalid selection')




