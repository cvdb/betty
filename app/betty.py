import argparse
import os
import pandas as pd
from data import load_horse_racing
from horse_racing import get_race_data
from horse_racing import get_11_perc_race_data


def show_horse_racing():
    df = load_horse_racing()
    print(get_race_data(df).head(200))

def show_horse_racing_ave_win_bsp_perc():
    df = load_horse_racing()
    race_data = get_race_data(df)
    # any winning BSP PERC above 25% is a fluke
    winners_bsp_below_25 = race_data[race_data['BSP_WIN_PERC'] < 25]
    print(winners_bsp_below_25["BSP_WIN_PERC"].mean())

def show_horse_racing_bsp_ratio():
    df = load_horse_racing()
    print('average MIN_MAX:' + str(get_race_data(df)["BSP_MIN_MAX_RATIO"].mean()))
    print('max MIN_MAX:' + str(get_race_data(df)["BSP_MIN_MAX_RATIO"].max()))
    print('min MIN_MAX:' + str(get_race_data(df)["BSP_MIN_MAX_RATIO"].min()))

def show_win_bsp_bins():
    df = load_horse_racing()
    winners = df[df['WIN_LOSE'] == 1]
    bins = pd.cut(winners['BSP'], list(range(100)))
    results = winners.groupby(bins)['BSP'].agg(['count'])
    print(results)

def show_loose_bsp_bins():
    df = load_horse_racing()
    loosers = df[df['WIN_LOSE'] != 1]
    bins = pd.cut(loosers['BSP'], list(range(100)))
    results = loosers.groupby(bins)['BSP'].agg(['count'])
    print(results)

def show_bsp_list():
    df = get_race_data(load_horse_racing())
    df = df[(df['BSP_MIN'] > 1) & (df['BSP_MIN'] < 2)]
    df = df[['BSP_WIN', 'BSP_LIST_ROUNDED']]
    print(df.head(100))

def show_bsp_2_3_odds():
    df = get_race_data(load_horse_racing())
    df = df[(df['BSP_MIN'] > 2) & (df['BSP_MIN'] < 3)]
    df_win = df[(df['BSP_WIN'] >= 2) & (df['BSP_WIN'] <= 3)]
    df_loose = df[(df['BSP_WIN'] < 2) | (df['BSP_WIN'] > 3)]
    print('win: ' + str(len(df_win)))
    print('loose: ' + str(len(df_loose)))
    
def show_bsp_1_2_odds():
    df = get_race_data(load_horse_racing())
    df = df[(df['BSP_MIN'] > 1) & (df['BSP_MIN'] < 2)]
    df_win = df[(df['BSP_WIN'] >= 1) & (df['BSP_WIN'] <= 2)]
    df_loose = df[(df['BSP_WIN'] < 1) | (df['BSP_WIN'] > 2)]
    print('win: ' + str(len(df_win)))
    print('loose: ' + str(len(df_loose)))


# given the starting balance and the var used to track
# the lowest balance apply cost & winnings to balance to 
# produce a running total
def show_betting_ledger(running_bal, iswin, bsp, bets_placed):
    # We are getting the results here, but we assume that we bet
    # blind just before the race.

    # Calc how much to bet to make $10
    # we base this on the amount lost before a final win
    amtbet = ((-1 * running_bal['amtlost']) + running_bal['wintarget']) / bsp

    # calc win loose amount
    winlooseamt = 0;
    if iswin:
        winlooseamt = amtbet * bsp 
    else:
        winlooseamt = -1 * amtbet

    # Now we adjust the running balance by win loose
    running_bal['balance'] = running_bal['balance'] + winlooseamt
    # print('prev amtlost: ' + str(running_bal['amtlost']) + ', amtbet: ' + str(amtbet) + ', bsp: ' + str(bsp) + ', winloose: ' + str(winlooseamt))

    # we need to keep a running total of loose amounts
    # so that we know how much to bet above to recover 
    # previous losses.
    if iswin:
        running_bal['amtlost'] = 0
    else:
        running_bal['amtlost'] = running_bal['amtlost'] + winlooseamt

    # adjust other running totals
    if running_bal['balance'] < running_bal['lowest_balance']:
        running_bal['lowest_balance'] = running_bal['balance']
    if running_bal['balance'] > running_bal['max_balance']:
        running_bal['max_balance'] = running_bal['balance']


def test_bsp_perc_betting():
    print('Testing BSP 11% betting')
    df = get_race_data(load_horse_racing())
    df_near_11 = get_11_perc_race_data(df)[["BSP_WIN", "BSP_WIN_PERC", "BSP_NEAR_11", "BSP_LIST_ROUNDED"]]
    # df_near_11 = get_11_perc_race_data(df)[["BSP_WIN", "BSP_LIST_ROUNDED", "BSP_WIN_PERC", "BSP_NEAR_11"]]
    print('total races: ' + str(len(df_near_11)))
    # filter out those we did not bet on
    df_near_11 = df_near_11[df_near_11['BSP_NEAR_11'].notnull()]
    print('total races bet: ' + str(len(df_near_11)))
    df_near_11['WINNING_BET'] = df_near_11.apply(lambda x: str(x['BSP_WIN']) in x['BSP_NEAR_11'].split(','), axis=1)
    winners = df_near_11[df_near_11['WINNING_BET'] == True]
    loosers = df_near_11[df_near_11['WINNING_BET'] == False]
    print('WINNERS: \n' + str(winners.head(20)))
    print('LOOSERS: \n' + str(loosers.head(20)))
    print('number of winning bets: ' + str(len(winners)))
    print('average winning BSP: ' + str(winners['BSP_WIN'].mean()))
    print('average loosing BSP: ' + str(loosers['BSP_WIN'].mean()))
    print('average winning BSP_PERC: ' + str(winners['BSP_WIN_PERC'].mean()))
    print('average loosing BSP_PERC: ' + str(loosers['BSP_WIN_PERC'].mean()))

    running_bal = { 'balance': 0, 'lowest_balance': 0, 'max_balance': 0, 'wintarget': 10, 'amtlost': 0 }
    # NOTE: we shuffle the rows into a random order first
    # here we get the number of bets placed by splitting the BSP_NEAR_11 column
    df_near_11.sample(frac=1).apply(lambda x: show_betting_ledger(running_bal, x['WINNING_BET'], x['BSP_WIN'], len(x['BSP_NEAR_11'].split(','))), axis=1)
    print('lowest balance: ' + str(running_bal['lowest_balance']))
    print('max balance: ' + str(running_bal['max_balance']))
    print('final balance: ' + str(running_bal['balance']))


def test_lowest_bsp_betting():
    print('Testing lowest BSP betting')
    df = get_race_data(load_horse_racing())
    df = df[(df['BSP_MIN'] >= 1) & (df['BSP_MIN'] <= 2)]
    df = df[["BSP_WIN", "BSP_MIN", "BSP_LIST_ROUNDED"]]
    winning = df[df['BSP_WIN'] == df['BSP_MIN']]
    loosing = df[df['BSP_WIN'] != df['BSP_MIN']]
    print('WINNERS: \n' + str(winning.head(20)))
    print('LOOSERS: \n' + str(loosing.head(20)))
    print('total races: ' + str(len(df)))
    print('total winning races: ' + str(len(winning)))
    print('total loosing races: ' + str(len(loosing)))
    running_bal = { 'balance': 0, 'lowest_balance': 0, 'max_balance': 0, 'wintarget': 35, 'amtlost': 0 }
    # NOTE: we shuffle the rows into a random order first
    df.sample(frac=1).apply(lambda x: show_betting_ledger(running_bal, x['BSP_WIN'] == x['BSP_MIN'], x['BSP_MIN'], 1), axis=1)
    print('lowest balance: ' + str(running_bal['lowest_balance']))
    print('max balance: ' + str(running_bal['max_balance']))
    print('final balance: ' + str(running_bal['balance']))



def ProcessArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--show', help='show data of some type')
    return parser.parse_args()

if __name__ == '__main__':
    args = ProcessArgs()

    if args.show == 'horse_racing':
        show_horse_racing()
    elif args.show == 'horse_racing_ave_win_bsp_perc':
        show_horse_racing_ave_win_bsp_perc()
    elif args.show == 'horse_racing_bsp_ratio':
        show_horse_racing_bsp_ratio()
    elif args.show == 'test_bsp_perc_betting':
        test_bsp_perc_betting()
    elif args.show == 'test_lowest_bsp_betting':
        test_lowest_bsp_betting()
    elif args.show == 'show_win_bsp_bins':
        show_win_bsp_bins()
    elif args.show == 'show_loose_bsp_bins':
        show_loose_bsp_bins()
    elif args.show == 'show_bsp_list':
        show_bsp_list()
    elif args.show == 'show_bsp_2_3_odds':
        show_bsp_2_3_odds()
    elif args.show == 'show_bsp_1_2_odds':
        show_bsp_1_2_odds()
    else:
        print('invalid selection')




