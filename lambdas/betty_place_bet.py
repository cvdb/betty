import os
import traceback
import json
import uuid
import boto3
from pprint import pprint
from db import put_item
from db import get_item
from db import update_item
from db import get_items
from db import create_tables
from trading import get_trading
from utils import clean_market_book_record
import betfairlightweight



def get_unlocked_bet_run(dynamodb):
    bet_runs = get_items({ 'table_name': 'bet_runs' }, dynamodb)
    unlocked_run = None
    for br in bet_runs:
        if not br['locked']:
            unlocked_run = br
    return unlocked_run

def lock_bet_run(bet_run, dynamodb):
    request = {
            'table_name': 'bet_runs',
            'key': { 'run_id': bet_run.run_id },
            'update_expression': 'set locked = :locked',
            'condition_expression': 'locked <> :locked',
            'expression_attribute_values': { ':locked': True }
            }
    return update_item(request, dynamodb)

def insert_new_run(dynamodb):
    new_run = {
            'run_id': str(uuid.uuid4()),
            'locked': True,
            'win_target': 10,
            'amt_lost': 0
            }
    request = { 'table_name': 'bet_runs', 'item': new_run  }
    return put_item(request, dynamodb)

def get_free_bet_run(dynamodb):
    attempt = 0
    locked_run = None
    while attempt <= 10 and not locked_run:
        try:
            attempt = attempt + 1
            unlocked_run = get_unlocked_bet_run(dynamodb)
            if not unlocked_run:
                locked_run = insert_new_run(dynamodb)
            else:
                locked_run = lock_bet_run(unlocked_run, dynamodb)
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            print('failed to lock run, attempt:' + str(attempt))
    return locked_run

def get_bet_ammout(mbook, bet_run):
    # Calc how much to bet to make target
    # we base this on the amount lost before a final win
    # NOTE: for TEST we user the 'runner_bsp', for LIVE we use the 'runner_near_bsp'
    if mbook['runner_bsp'] == 0:
        return 0
    amtbet = ((-1 * bet_run['amt_lost']) + bet_run['win_target']) / mbook['runner_bsp']
    return amtbet

# here we bet directly on BSP based on 'NEAR PROCE' for LIVE PRE_PLAY
# but for TEST we place a normal bet IN_PLAY at the BSP
def place_bet(amt_bet, mbook, trading):
    # Define a limit order filter
    limit_order_filter = betfairlightweight.filters.limit_order(
        size=amt_bet, 
        price=float(mbook['runner_bsp']),
        persistence_type='PERSIST'
    )
    # Define an instructions filter
    instructions_filter = betfairlightweight.filters.place_instruction(
        selection_id=str(mbook['runner_selection_id']),
        order_type="LIMIT",
        side="BACK",
        limit_order=limit_order_filter
    )
    # Place the order
    order = trading.betting.place_orders(
        market_id=mbook['market_id'],
        instructions=[instructions_filter]
    )
    return order['_data']['instructionReports'][0]['betId'] 

def update_mbook(mbook, run_id, bet_id, dynamodb):
    mbook['run_id'] = run_id
    mbook['bet_id'] = bet_id
    mbook['win_loose'] = 0 # ZERO means PENDING
    request = { 'table_name': 'bet_market_books', 'item': mbook  }
    return put_item(request, dynamodb)
        
def lambda_handler(event, context):
    # for each new bet.....
    # get free bet run, if none start new, lock it
    # calc amount to bet
    # place the bet
    # save bet_id and run_id to the bet_market_books record
    try:
        # dynamodb = boto3.resource('dynamodb',region_name='ap-southeast-2')
        dynamodb = None
        trading = get_trading()
        records = list(map(lambda rec: rec['dynamodb']['NewImage'], event['Records']))
        for rec in records:
            mbook = clean_market_book_record(rec)
            bet_run = get_free_bet_run(dynamodb)
            amt_bet = get_bet_ammout(mbook, bet_run) 
            bet_id = place_bet(amt_bet, mbook, trading)
            update_mbook(mbook, bet_run['run_id'], bet_id, dynamodb)

        return { "statusCode": 200 }
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return { "statusCode": 500 }


 


if __name__ == '__main__':
    trading = get_trading()
    mbook = {'bsp_reconciled': False,
            'inplay': False,
            'market_id': '1.183529542',
            'market_status': 'OPEN',
            'runner_bsp': 2.0,
            'runner_far_bsp': None,
            'runner_near_bsp': None,
            'runner_selection_id': 33751150,
            'total_matched': 7.29}
    place_bet(10, mbook, trading)
    # create_tables(None)
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    # with open(dir_path + '/sample_event_data/market_book_event.json') as f:
    #     event = json.load(f)
    #     lambda_handler(event, None)
    #     mbooks = get_items({ 'table_name': 'bet_market_books' }, None)
    #     pprint(mbooks)
    #     runs = get_items({ 'table_name': 'bet_runs' }, None)
    #     pprint(runs)









