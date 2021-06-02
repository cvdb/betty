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
from utils import as_decimal
import betfairlightweight



def get_unlocked_bet_run(dynamodb):
    bet_runs = get_items({ 'table_name': 'bet_runs' }, dynamodb)
    unlocked_run = None
    # here we should sort by last used and pick oldest
    for br in bet_runs:
        if not br['locked']:
            unlocked_run = br
            break
    return unlocked_run

def lock_bet_run(bet_run, dynamodb):
    request = {
            'table_name': 'bet_runs',
            'key': { 'run_id': bet_run['run_id'] },
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
        print('ZERO BSP, bet ZERO')
        return 0
    print('calculate amount to bet based on amt_lost:' + str(bet_run['amt_lost'])
        + ', win_target:' + str(bet_run['win_target'])
        + ', runner_bsp:' + str(mbook['runner_bsp']))
    amtbet = ((-1 * bet_run['amt_lost']) + bet_run['win_target']) / mbook['runner_bsp']
    amtbet = round(amtbet, 2)
    print('amount to bet for bet_run:' + bet_run['run_id'] + ' is:' + str(amtbet))
    return amtbet

# here we bet directly on BSP based on 'NEAR PROCE' for LIVE PRE_PLAY
# but for TEST we place a normal bet IN_PLAY at the BSP
def place_bet(amt_bet, mbook, trading):
    # Define a limit order filter
    limit_order_filter = betfairlightweight.filters.limit_order(
        size=float(amt_bet), 
        price=round(float(mbook['runner_bsp']), 2),
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
    pprint(order.json())
    return order.place_instruction_reports[0].bet_id

# This table is our long term storage
def update_bet_mbook(mbook, run_id, bet_id, amt_bet, dynamodb):
    mbook['run_id'] = run_id
    mbook['bet_id'] = bet_id
    mbook['amt_bet'] = as_decimal(amt_bet)
    mbook['win_loose'] = 0 # ZERO means PENDING
    mbook['in_progress'] = False
    # We need to be careful here because this
    # update will trigger another call to this lambda
    # and we must not place duplicate bets
    request = { 
        'table_name': 'bet_market_books', 
        'condition_expression': 'in_progress <> :in_progress',
        'expression_attribute_values': { ':in_progress': False },
        'item': mbook  }
    return put_item(request, dynamodb)

# This is a temp working table used to store bets
# untill they are fully processed.
def put_current_bet(mbook, dynamodb):
    current_bet = {
            'market_id': mbook.get('market_id'),
            'runner_selection_id': mbook.get('runner_selection_id'),
            'runner_bsp': mbook.get('runner_bsp'),
            'run_id': mbook.get('run_id'),
            'amt_bet': mbook.get('amt_bet'),
            'run_id': mbook.get('run_id'),
            'bet_id': mbook.get('bet_id'),
            'bet_outcome': '',
            'proffit': 0,
            'settled': False
            }
    request = { 'table_name': 'current_bets', 'item': current_bet  }
    return put_item(request, dynamodb)

def extract_new_image(record):
    if record and record['dynamodb'] and record['dynamodb']['NewImage']:
        return record['dynamodb']['NewImage']
    else:
        print('record missing NewImage:' + json.dumps(record))
        return None
    
def lambda_handler(event, context):
    # for each new bet.....
    # get free bet run, if none start new, lock it
    # calc amount to bet
    # place the bet
    # save bet_id and run_id to the bet_market_books record
    try:
        dynamodb = boto3.resource('dynamodb',region_name='ap-southeast-2')
        trading = get_trading()
        records = list(map(extract_new_image, event['Records']))
        for rec in records:
            if not rec:
                continue
            mbook = clean_market_book_record(rec)
            if not mbook['in_progress']:
                print('skipping mbook NOT in progress:' + mbook['market_id'])
                continue
            print('placing bet on market: ' + mbook.get('market_id'))
            bet_run = get_free_bet_run(dynamodb)
            print('using bet_run: ' + bet_run['run_id'] + ' for market: ' + mbook.get('market_id'))
            amt_bet = get_bet_ammout(mbook, bet_run) 
            bet_id = place_bet(amt_bet, mbook, trading)
            print('placed bet: ' + bet_id + ' using bet_run: ' + bet_run['run_id'] + ' for market: ' + mbook.get('market_id'))
            mbook = update_bet_mbook(mbook, bet_run['run_id'], bet_id, amt_bet, dynamodb)
            put_current_bet(mbook, dynamodb)

        return { "statusCode": 200 }
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return { "statusCode": 500 }


 


# if __name__ == '__main__':
    # trading = get_trading()
    # mbook = {'bsp_reconciled': False,
    #         'inplay': False,
    #         'market_id': '1.183529542',
    #         'market_status': 'OPEN',
    #         'runner_bsp': 2.0,
    #         'runner_far_bsp': None,
    #         'runner_near_bsp': None,
    #         'runner_selection_id': 33751150,
    #         'total_matched': 7.29}
    # place_bet(10, mbook, trading)
    #
    # create_tables(None)
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    # with open(dir_path + '/sample_event_data/market_book_event.json') as f:
    #     event = json.load(f)
    #     lambda_handler(event, None)
    #     mbooks = get_items({ 'table_name': 'bet_market_books' }, None)
    #     pprint(mbooks)
    #     print('\n')
    #     runs = get_items({ 'table_name': 'bet_runs' }, None)
    #     pprint(runs)
    #     print('\n')
    #     runs = get_items({ 'table_name': 'current_bets' }, None)
    #     pprint(runs)










