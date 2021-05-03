import os
import pandas as pd
import numpy as np

"""""""""""""""""""""""""""""""""""""""""""""
Process horse racing data loaded from CSV.
"""""""""""""""""""""""""""""""""""""""""""""

"""
Odds per hourse calculated by people who know a lot about racing
already provide much insight. Try to align odds with win_lose data.
"""

def __add_bsp_perc(df):
    df['BSP_PERC'] = 1 / df['BSP'] * 100


# Here we want 1 row per race
# with the winning hourse ID and the list of BSP's for all runners
def get_race_data(df):
    # Get 1 row per event ID with min and max BSP
    grouped = df.groupby('EVENT_ID').agg(
            BSP_MIN=pd.NamedAgg('BSP', 'min'),
            BSP_MAX=pd.NamedAgg('BSP', 'max'),
            BSP_COUNT=pd.NamedAgg('BSP', 'count'),
            BSP_LIST=pd.NamedAgg('BSP', lambda x: ','.join(map(str, x))))

    # Now get winning BSP and selection ID
    winners = df[df['WIN_LOSE'] == 1]
    winners_bsp = winners[['EVENT_ID', 'BSP', 'SELECTION_ID']]
    winners_bsp_renamed = winners_bsp.rename(
        columns={
            "BSP": "BSP_WIN",
            "SELECTION_ID": "WIN_SELECTION_ID"
            }
        )

    # Merge winning data with average data
    final_df = pd.merge(grouped, winners_bsp_renamed, how="left", on="EVENT_ID")
    final_df = final_df[final_df['BSP_WIN'].notnull()]
    final_df['WIN_SELECTION_ID'] = final_df['WIN_SELECTION_ID'].fillna(-1).astype(np.int64)

    # Now, calculate the BSP_PERC.
    # if min BSP was 1 % and max BSP was 100%
    # what percent is the winning BSP.

    # Divide the max and the winning BSP by the MIN
    # for example [2, 6, 8] -> [1, 3, 4]
    # then to get PERC: 3/4 * 100
    final_df['BSP_MAX_OVER_MIN'] = final_df['BSP_MAX'] / final_df['BSP_MIN']
    final_df['BSP_WIN_OVER_MIN'] = final_df['BSP_WIN'] / final_df['BSP_MIN']
    final_df['BSP_WIN_PERC'] = final_df['BSP_WIN_OVER_MIN'] / final_df['BSP_MAX_OVER_MIN'] * 100
    final_df['BSP_MIN_MAX_RATIO'] = final_df['BSP_MIN'] / final_df['BSP_MAX']

    return final_df


def find_bsp_near_11(bsp_list, bsp_min, bsp_max):
    bsp_percs = []
    for bsp in bsp_list.split(','):
        bsp_max_over_min = bsp_max / bsp_min
        bsp_over_min = float(bsp) / bsp_min
        bsp_perc = bsp_over_min / bsp_max_over_min * 100
        bsp_percs.append(bsp_perc)
    bsp_near_11 = min(bsp_percs, key=lambda x:abs(x-11))
    return bsp_near_11

def get_11_perc_race_data(df):
    # Find the BSP per race that is the closest to 11%
    df['BSP_NEAR_11'] = df.apply(lambda x: find_bsp_near_11(x['BSP_LIST'], x['BSP_MIN'], x['BSP_MAX']), axis=1)
    return df 
            





