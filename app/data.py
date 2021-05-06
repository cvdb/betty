import os
import tarfile
import zipfile
import bz2
import glob
import pandas as pd

horse_racing_folders = [
    '../../data/racing_data'
]

year_filter = '.'

# Set default options
pd.set_option('display.max_rows', None)


def load_horse_racing():
    print('loading horse racing data')
    df = __load_csv(horse_racing_folders)
    # print('')
    # print(df.info())
    # print('')
    # print(df[['EVENT_ID', 'EVENT_DT','EVENT_NAME','SELECTION_NAME', 'BSP']].head(200))
    # print('')
    df.sort_values(by=['EVENT_ID', 'BSP'], inplace=True)
    return df


def __load_csv(file_paths):
    dfs = []
    for file_path in file_paths:
        if os.path.isdir(file_path):
            print('loading CSV data from folder: ' + file_path)
            for path in glob.iglob(file_path + '**/**/*.csv', recursive=True):
                if year_filter in path:
                    dfs.append(pd.read_csv(path, parse_dates=True))
        elif os.path.isfile(file_path):
            if year_filter in file_path:
                ext = os.path.splitext(file_path)[1]
                if ext == '.csv':
                    print('loading CSV data from file: ' + file_path)
                    dfs.append(pd.read_csv(path, parse_dates=True))
        
    return pd.concat(dfs, axis=0)

if __name__ == '__main__':
    load_horse_racing()
