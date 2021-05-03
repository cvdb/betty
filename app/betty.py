import argparse
import os
from data import load_horse_racing
from horse_racing import get_race_data
from horse_racing import get_11_perc_race_data


def show_horse_racing():
    df = load_horse_racing()
    print(get_race_data(df).head(200))

def show_horse_racing_ave_win_bsp_perc():
    df = load_horse_racing()
    print(get_race_data(df)["BSP_WIN_PERC"].mean())

def show_horse_racing_bsp_ratio():
    df = load_horse_racing()
    print('average MIN_MAX:' + str(get_race_data(df)["BSP_MIN_MAX_RATIO"].mean()))
    print('max MIN_MAX:' + str(get_race_data(df)["BSP_MIN_MAX_RATIO"].max()))
    print('min MIN_MAX:' + str(get_race_data(df)["BSP_MIN_MAX_RATIO"].min()))

def show_betting_ledger(balance, amtbet, lowest_balance):
    def show_betting_ledger_inner(bsp_win, bsp_win_perc, bsp_near_11):
        nonlocal balance
        nonlocal lowest_balance
        balance = balance - amtbet
        if bsp_win_perc == bsp_near_11:
            balance = balance + (amtbet * bsp_win)
        if balance < lowest_balance:
            lowest_balance = balance
        print('balance: ' + str(balance) + ', lowest_balance: ' + str(lowest_balance))

    return show_betting_ledger_inner


def test_bsp_perc_betting():
    print('Testing BSP 11% betting')
    df = get_race_data(load_horse_racing())
    df_near_11 = get_11_perc_race_data(df)[["BSP_WIN", "BSP_WIN_PERC", "BSP_NEAR_11"]]
    print('total races: ' + str(len(df_near_11)))
    # filter out those we did not bet on
    df_near_11 = df_near_11[df_near_11['BSP_NEAR_11'].notnull()]
    print('total races bet: ' + str(len(df_near_11)))
    winners = df_near_11[df_near_11['BSP_WIN_PERC'] == df_near_11['BSP_NEAR_11']]
    loosers = df_near_11[df_near_11['BSP_WIN_PERC'] != df_near_11['BSP_NEAR_11']]
    print('WINNERS: \n' + str(winners.head(20)))
    print('LOOSERS: \n' + str(loosers.head(20)))
    print('number of winning bets: ' + str(winners.count()))
    print('average winning BSP: ' + str(winners['BSP_WIN'].mean()))
    print('average loosing BSP: ' + str(loosers['BSP_WIN'].mean()))
    print('average winning BSP_PERC: ' + str(winners['BSP_WIN_PERC'].mean()))
    print('average loosing BSP_PERC: ' + str(loosers['BSP_WIN_PERC'].mean()))
    print('average winning CLOSEST_BSP_PERC: ' + str(winners['BSP_NEAR_11'].mean()))
    print('average loosing CLOSEST_BSP_PERC: ' + str(loosers['BSP_NEAR_11'].mean()))

    # Now work out a running total betting $10 per race
    # start with a ZERO balance
    balance = 0
    lowest_balance = 0
    show_ledger = show_betting_ledger(balance, 50, lowest_balance)
    df_near_11.apply(lambda x: show_ledger(x['BSP_WIN'], x['BSP_WIN_PERC'], x['BSP_NEAR_11']), axis=1)

    amtbet = len(df_near_11) * 50
    amtwon = len(winners) * winners['BSP_WIN'].mean() * 50
    print('amount bet:' + str(amtbet))
    print('amount won:' + str(amtwon))
    print('proffit:' + str(amtwon - amtbet))




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
    else:
        print('invalid selection')



