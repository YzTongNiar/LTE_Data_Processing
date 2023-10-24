import re
import csv
import os
import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import pickle

def read_airscope_csv_file(file_path):
    # dtype_dict = {'tstamp': 'float', ' plmnid': 'int', ' tac': 'int', ' cellID': 'str', ' rsrp': 'int',
    # ' rsrq': 'int', ' pl': 'int', ' snr': 'int', ' proc': 'str', ' nof_ue': 'int',
    # ' nof_cce': 'int', ' dl_bps': 'int', ' dl_mcs': 'int', ' dl_prb': 'str', ' dl_time': 'str',
    # ' dl_mimo': 'int', ' ul_bps': 'int', ' ul_mcs': 'int', ' ul_prb': 'str', ' ul_time': 'str',
    # ' ul_retx': 'str'}
    df = pd.read_csv(file_path, sep=';', low_memory=False)
    for idx, row in enumerate(df['tstamp']):
        str_time = datetime.datetime.fromtimestamp(row / 1000000.0).strftime('%H:%M:%S.%f')
        data_time = datetime.datetime.strptime(str_time,'%H:%M:%S.%f')
        df.loc[idx,'tstamp'] = data_time
    return df.loc[:,['tstamp',' dl_prb',' ul_prb']]

def read_app_timelog_list(file_path):
    app_timelog = open(file_path,'r')
    app_timelog_list = []
    for line in app_timelog:
        line = line.split('    ')
        start_time = datetime.datetime.strptime(line[1][11:], '%H:%M:%S')
        end_time = datetime.datetime.strptime(line[0][11:], '%H:%M:%S')
        time_periods = (start_time, end_time)
        app_timelog_list.append(time_periods)
    return app_timelog_list

def prb_map(prb_data, timelog_list):
    prb_map_list = []
    for periods in timelog_list:
        prb_within_periods = prb_data.loc[(prb_data['tstamp']>=periods[0])&(prb_data['tstamp']<=periods[1])]
        prb_within_periods = prb_within_periods.reset_index()
        prb_map_list.append(prb_within_periods)
    return prb_map_list

def perct_to_flt(x):
    if x == 0:
        return 0
    else:
        return float(x.strip('%')) / 100

def map_mean_prb(mean_prb_list):
    bins = [0, 0.2, 0.4, 0.6, 0.8, 1.1]
    labels = [0, 1, 2, 3, 4]
    mapped_values = pd.cut(mean_prb_list, bins=bins, labels=labels, include_lowest=True)
    mapped_values_list = mapped_values.to_list()
    return mapped_values_list

def insert_prb(encoded_data_path, label_path, prb_list, save_path):
    encoded_data = pd.read_csv(encoded_data_path, sep=',')
    label = pd.read_csv(label_path, sep=',')
    last_index = encoded_data['series_id'][len(encoded_data)-1] # in general should be 300
    encoded_data.insert(encoded_data.shape[1]-1,'dl_prb',0) # insert two new columns for prb values
    encoded_data.insert(encoded_data.shape[1]-1,'ul_prb',0)
    mean_prb_list = [[],[]]

    for idx in range (int(last_index)+1):
        df_len_dl = encoded_data.loc[encoded_data['series_id'] == idx, 'dl_prb'].shape[0]
        df_len_ul = encoded_data.loc[encoded_data['series_id'] == idx, 'ul_prb'].shape[0]
        dl_prb_item = prb_list[idx][' dl_prb'].tolist()
        ul_prb_item = prb_list[idx][' ul_prb'].tolist()
        if len(dl_prb_item) < df_len_dl:
            dl_prb_item.extend([0] * (df_len_dl - len(dl_prb_item)))
            ul_prb_item.extend([0] * (df_len_ul - len(ul_prb_item)))
        encoded_data.loc[encoded_data['series_id'] == idx, 'dl_prb'] = dl_prb_item[:df_len_dl]
        encoded_data.loc[encoded_data['series_id'] == idx, 'ul_prb'] = ul_prb_item[:df_len_ul]
        mean_prb_list[0].append(np.mean(prb_list[idx][' dl_prb'][:df_len_dl].apply(perct_to_flt)))
        mean_prb_list[1].append(np.mean(prb_list[idx][' ul_prb'][:df_len_ul].apply(perct_to_flt)))
    try:
        encoded_data['dl_prb'] = encoded_data['dl_prb'].apply(perct_to_flt)
        encoded_data['ul_prb'] = encoded_data['ul_prb'].apply(perct_to_flt)
    except:
        print(encoded_data.loc[encoded_data['dl_prb'].apply(lambda x: isinstance(x, float)), 'dl_prb'])
    encoded_data.to_csv(save_path, sep=',', index=False)

    # with open(save_path, 'wb') as ff:
    #     pickle.dump(encoded_data, ff)
    #     ff.close()

    label['mean_prb_dl'] = map_mean_prb(mean_prb_list[0])
    label['mean_prb_ul'] = map_mean_prb(mean_prb_list[1])
    label.to_csv(label_path, sep=',', index=False)
    return encoded_data, label

def series_mean_prb(prb_list, encoded_data):
    mean_prb_df = pd.DataFrame(columns=['dl_mean_prb','ul_mean_prb'])
    for idx, prb in enumerate(prb_list):
        df_len_dl = encoded_data.loc[encoded_data['series_id'] == idx, 'dl_prb'].shape[0]
        df_len_ul = encoded_data.loc[encoded_data['series_id'] == idx, 'ul_prb'].shape[0]
        prb_data = pd.DataFrame([[np.mean(prb[' dl_prb'][:df_len_dl].apply(lambda x: float(x.strip('%')) / 100)), \
                               np.mean(prb[' ul_prb'][:df_len_ul].apply(lambda x: float(x.strip('%')) / 100))]], columns=['dl_mean_prb','ul_mean_prb'])
        mean_prb_df = pd.concat([mean_prb_df, prb_data])
    return mean_prb_df

def mean_prb_sta(mean_prb_df, appname):
    mean_prb_sta = pd.cut(mean_prb_df['dl_mean_prb'], bins=[0,0.2,0.4,0.6,0.8,1.2],right=False)
    dl_counts = pd.value_counts(mean_prb_sta, sort=False)
    dl_plot = plt.bar(dl_counts.index.astype(str), dl_counts)
    plt.bar_label(dl_plot, dl_counts)
    plt.title(f'{appname} Downlink Mean Prb Statistics')
    plt.show()

    mean_prb_sta = pd.cut(mean_prb_df['ul_mean_prb'], bins=[0,0.2,0.4,0.6,0.8,1.2],right=False)
    ul_counts = pd.value_counts(mean_prb_sta, sort=False)
    ul_plot = plt.bar(ul_counts.index.astype(str), ul_counts)
    plt.bar_label(ul_plot, ul_counts)
    plt.title(f'{appname} Uplink Mean Prb Statistics')
    plt.show()

    plt.plot(range(len(mean_prb_df['dl_mean_prb'])), mean_prb_df['dl_mean_prb'])
    plt.plot(range(len(mean_prb_df['ul_mean_prb'])), mean_prb_df['ul_mean_prb'])
    plt.legend(['dl_mean_prb', 'ul_mean_prb'])
    plt.title(f'{appname} Instantaneous Prb Figure')
    plt.show()

def prb_mapping(airscope_csv_path, app_timelog_path, save_encoded_path, save_encoded_with_prb_path, label_path):
    prb_data = read_airscope_csv_file(airscope_csv_path)
    app_timelog_list = read_app_timelog_list(app_timelog_path)
    prb_list = prb_map(prb_data, app_timelog_list)
    encoded_data = insert_prb(save_encoded_path, label_path, prb_list, save_encoded_with_prb_path)
    return encoded_data