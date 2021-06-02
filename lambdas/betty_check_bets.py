import os
import traceback
import json
import uuid
import boto3
from pprint import pprint
from db import put_item
from db import get_item
from db import delete_item
from db import update_item
from db import get_items
from db import create_tables
from trading import get_trading
from utils import clean_market_book_record
from utils import as_decimal
import betfairlightweight

def get_test_order(current_bets):
    cleared_orders = { '_data': { 'cleared_orders': [] } }
    for cb in current_bets:
        cleared_orders['_data']['cleared_orders'].append({
            'run_id': cb['run_id'],
            'bet_id': cb['bet_id'], 
            'market_id': cb['market_id'], 
            'profit': cb['amt_bet'],
            'bet_outcome': 'WON'})
    return cleared_orders

def get_settled_bets(dynamodb, trading):
    current_bets = get_items({ 'table_name': 'current_bets' }, dynamodb)
    if not current_bets:
        print('no current bets')
        return []
    betids = list(map(lambda cb: cb.get('bet_id'), current_bets))
    cleared_orders = trading.betting.list_cleared_orders(bet_status="SETTLED", bet_ids=betids)
    lapsed_orders = trading.betting.list_cleared_orders(bet_status="LAPSED", bet_ids=betids)
    print('CLEARED: ' + cleared_orders.json())
    print('LAPSED: ' + lapsed_orders.json())
    if cleared_orders:
        for order in cleared_orders.orders:
            cbet = next(cb for cb in current_bets if cb['market_id'] == order.market_id)
            if cbet:
                cbet['bet_outcome'] = order.bet_outcome
                cbet['profit'] = order.profit
                cbet['settled'] = True
    if lapsed_orders:
        for order in lapsed_orders.orders:
            cbet = next(cb for cb in current_bets if cb['market_id'] == order.market_id)
            if cbet:
                cbet['bet_outcome'] = 'LAPSED'
                cbet['profit'] = 0
                cbet['settled'] = True
    return current_bets

def update_bet_run(bet_run, dynamodb):
    request = {
            'table_name': 'bet_runs',
            'key': { 'run_id': bet_run['run_id'] },
            'update_expression': 'set locked = :locked, amt_lost = :amt_lost',
            'condition_expression': 'locked <> :locked',
            'expression_attribute_values': { ':locked': False, ':amt_lost': bet_run['amt_lost'] }
            }
    return update_item(request, dynamodb)

def update_bet_mbook(mbook, dynamodb):
    request = {
            'table_name': 'bet_market_books',
            'key': { 'market_id': mbook.get('market_id') },
            'update_expression': 'set win_loose = :win_loose',
            'condition_expression': 'attribute_exists(market_id)', # dummy
            'expression_attribute_values': { ':win_loose': as_decimal(mbook['win_loose']) }
            }
    return update_item(request, dynamodb)

# Reset the bet run.
# update the 'bet_market_books' record
# delete the current_bets record
def process_lapsed(order, dynamodb):
    bet_run = get_item({ 'table_name': 'bet_runs', 'key': { 'run_id': order['run_id'] } }, dynamodb)
    # Leave the amount lost AS-IS
    bet_run['locked'] = False
    update_bet_run(bet_run, dynamodb)
    mbook = {
            'market_id': order['market_id'],
            'win_loose': order['profit'] 
            }
    update_bet_mbook(mbook, dynamodb)
    delete_item({ 'table_name': 'current_bets', 'key': { 'market_id': order['market_id'] } }, dynamodb)

# Reset the bet run.
# update the 'bet_market_books' record
# delete the current_bets record
def process_win(order, dynamodb):
    bet_run = get_item({ 'table_name': 'bet_runs', 'key': { 'run_id': order['run_id'] } }, dynamodb)
    bet_run['amt_lost'] = 0
    bet_run['locked'] = False
    update_bet_run(bet_run, dynamodb)
    mbook = {
            'market_id': order['market_id'],
            'win_loose': order['profit'] 
            }
    update_bet_mbook(mbook, dynamodb)
    delete_item({ 'table_name': 'current_bets', 'key': { 'market_id': order['market_id'] } }, dynamodb)

# Add the most recent loss to the bet run.
# update the 'bet_market_books' record
# delete the current_bets record
def process_loose(order, dynamodb):
    bet_run = get_item({ 'table_name': 'bet_runs', 'key': { 'run_id': order['run_id'] } }, dynamodb)
    # NOTE: profit is negative here, so amount lost will be negative
    bet_run['amt_lost'] = bet_run['amt_lost'] + order['profit']
    bet_run['locked'] = False
    update_bet_run(bet_run, dynamodb)
    mbook = {
            'market_id': order['market_id'],
            'win_loose': order['profit'] 
            }
    update_bet_mbook(mbook, dynamodb)
    delete_item({ 'table_name': 'current_bets', 'key': { 'market_id': order['market_id'] } }, dynamodb)


def process_orders(cleared_orders, dynamodb):
    for co in cleared_orders:
        if co['bet_outcome'] == 'WON':
            process_win(co, dynamodb)
        if co['bet_outcome'] == 'LOST':
            process_loose(co, dynamodb)
        if co['bet_outcome'] == 'LAPSED':
            process_lapsed(co, dynamodb)
        
def lambda_handler(event, context):
    # for each new bet.....
    # get free bet run, if none start new, lock it
    # calc amount to bet
    # place the bet
    # save bet_id and run_id to the bet_market_books record
    try:
        dynamodb = boto3.resource('dynamodb',region_name='ap-southeast-2')
        trading = get_trading()
        # cleared orders are "current bets" adjusted based on WIN_LOOSE
        settled_orders = get_settled_bets(dynamodb, trading)
        process_orders(settled_orders, dynamodb)

        return { "statusCode": 200 }
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return { "statusCode": 500 }


 


#if __name__ == '__main__':
#    lambda_handler(None, None)
#    mbooks = get_items({ 'table_name': 'bet_market_books' }, None)
#    pprint(mbooks)
#    print('\n')
#    mbooks = get_items({ 'table_name': 'bet_runs' }, None)
#    pprint(mbooks)
#    print('\n')










