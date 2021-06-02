import math
from decimal import Decimal
import pytz


def as_decimal(val):
    if not val:
        return Decimal('0')
    if isinstance(val, str):
        try:
            return Decimal(val)
        except ValueError:
            return Decimal('0')
    if math.isnan(val):
        return Decimal('0')
    return Decimal(str(val))

def utc2local(utc):
    tz = pytz.timezone('Australia/Sydney')
    return pytz.utc.localize(utc).astimezone(tz)

def clean_market_book_record(record):
    get_s = lambda x: x.get('S') if x else None
    get_n = lambda x: x.get('N') if x else None
    get_bool = lambda x: x.get('BOOL') if x else None
    as_decimal(get_n(record.get('runner_bsp')))
    return {
            'market_id': get_s(record.get('market_id')),
            'market_start_time_utc': get_s(record.get('market_start_time_utc')),
            'market_start_time': get_s(record.get('market_start_time')),
            'market_status': get_s(record.get('market_status')),
            'bsp_reconciled': get_bool(record.get('bsp_reconciled')),
            'inplay': get_bool(record.get('inplay')),
            'total_matched': as_decimal(get_n(record.get('total_matched'))),
            'runner_selection_id': get_s(record.get('runner_selection_id')),
            'runner_near_bsp': as_decimal(get_n(record.get('runner_near_bsp'))),
            'runner_far_bsp': as_decimal(get_n(record.get('runner_far_bsp'))),
            'runner_bsp': as_decimal(get_n(record.get('runner_bsp'))),
            'run_id': get_s(record.get('run_id')),
            'bet_id': get_s(record.get('bet_id')),
            'amt_bet': as_decimal(get_n(record.get('amt_bet'))),
            'win_loose': as_decimal(get_n(record.get('win_loose'))),
            'updated_at': get_s(record.get('updated_at'))
            }
