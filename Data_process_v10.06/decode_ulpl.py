import re
import csv
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm


def read_start_time(log_path):
    log_file = log_path+'/airscope.log'
    input_data = open(log_file,'r')
    for line in input_data:
        line = line.split(' ')
        if line[0] != '\n':
            return line[0]


def rnti_filter(rnti_path,log_path):
    input_data = open(rnti_path, 'r')
    rnti_list = [[],[]]
    rnti_final = [[],[]]
    pattern = r'\b(\d+\.\d+)\b'
    start_time = read_start_time(log_path)
    start_time = datetime.strptime(start_time, '%H:%M:%S.%f')
    for line in input_data:
        if 'Time' in line:
            line = next(input_data)
            match = re.search(pattern, line)
            if match:
                time_value = float(match.group(1))
                time_value = timedelta(seconds=time_value)+start_time
                rnti_list[0].append(str(time_value.time()))
            else:
                print("Time not found in the data line.")
        if "RNTI=" in line:
            index1 = line.find("=")
            index2 = line.find(')')
            rnti_list[1].append(str(hex(int(line[index1+1:index2]))))
    for idx, rnti in enumerate(rnti_list[1]):
        if rnti not in '0xfffe':
            rnti_final[0].append(rnti_list[0][idx])
            rnti_final[1].append(rnti)
    return rnti_final


def time_period_judge(RNTI_time, RNTI_list, RRC_time_idx):
    '''
    RNTI_time: the time of the reading RNTI
    RNTI_list: the target RNTI list with time
    RRC_time_idx: the index of the target RNTI
    '''
    RNTI_time = datetime.strptime(RNTI_time, '%H:%M:%S.%f')
    RRC_time = datetime.strptime(RNTI_list[0][RRC_time_idx], '%H:%M:%S.%f')
    if RRC_time_idx == len(RNTI_list[1])-1:
        next_RRC_time = None
    else:
        next_RRC_time = datetime.strptime(RNTI_list[0][RRC_time_idx+1], '%H:%M:%S.%f')
    if next_RRC_time == None:
        if RNTI_time > RRC_time:
            return True
    else:
        if RRC_time < RNTI_time < next_RRC_time:
            return True
    return False


def filter_data(log_path, csv_write_down, csv_write_up, rntilist):
    input_data = open(log_path, 'r')
    for line in input_data:
        line = line.split(' ')
        if '[I]' not in line:
            continue
        for idx, rnti in enumerate(rntilist[1]):
            if 'rnti=%s,' % rnti in line:
                if '[MAC' in line or '[HI]' in line:
                    break
                else:
                    if time_period_judge(line[0], rntilist, idx):
                        tbs_string = [s for s in line if "tbs" in s]
                        rnti_string = [s for s in line if "rnti" in s]
                        uldl_string = [s for s in line if "DL" in s or 'UL' in s]
                        tbs_match = [line[0], rnti_string[0][5:], uldl_string[0], re.findall(r"\d+", tbs_string[0])[0]]
                        if '[DL]' in uldl_string:
                            csv_write_down.writerow(tbs_match)
                        else:
                            csv_write_up.writerow(tbs_match)


def zero_complement(df, link, appname):
    """
    Check time difference between adjacent time points
    if time difference is larger than 1 second
    insert a new row [time+=1, same rnti, same link, tbs=0]
    """
    empty_df = pd.DataFrame(columns=df.columns)
    for row_id in tqdm(range(1, df.shape[0]), desc=f'zero_complement {appname}_{link}'):
        str_time = datetime.strptime(df.loc[row_id - 1, 'time'], '%H:%M:%S.%f')
        end_time = datetime.strptime(df.loc[row_id, 'time'], '%H:%M:%S.%f')
        if end_time - str_time > timedelta(seconds=1):
            time_interval = timedelta(seconds=1)
            current_time = str_time
            new_row = df.loc[row_id - 1].copy()
            while current_time <= end_time - time_interval:
                current_time += time_interval
                new_row['time'] = current_time.strftime("%H:%M:%S.%f")
                new_row['tbs_dl'] = 0
                empty_df.loc[len(empty_df)] = new_row
    df_new = pd.concat([df, empty_df], ignore_index=True)
    df_new = df_new.sort_values(by='time').reset_index(drop=True)
    return df_new


def cut_off(df_DL, df_UL, log_path, start_id, data_file_path, appname):
    """
    Cut off the data set by specified time periods
    When cut off, only check the integer part of time
    """
    df_cut_off = pd.DataFrame({'rnti': [], 'tbs_dl': [], 'tbs_ul': []})
    file = open(log_path)
    index = start_id


    for i, line in tqdm(enumerate(file), desc=f'The series is being extracted.{appname}'):


        log = line.split(' ')
        time1 = log[1]
        time2 = log[-5]
        id1 = max(df_DL.index[df_DL.time.str.startswith(time1)])
        id2 = min(df_DL.index[df_DL.time.str.startswith(time2)])
        id3 = max(df_UL.index[df_UL.time.str.startswith(time1)])
        id4 = min(df_UL.index[df_UL.time.str.startswith(time2)])

        """
        Cut off uplink and downlink tbs series from the row rbs series
        Generate the index(th) tbs series as '[time, rnti, tbs_dl, tbs_ul]' 
        """
        df_index_DL = df_DL.loc[df_DL.index[id2:id1], ['time', 'rnti', 'tbs_dl']]
        df_index_DL = df_index_DL.set_index('time')
        df_index_UL_tbs = df_UL.loc[df_UL.index[id4:id3], ['time', 'tbs_ul']]
        df_index_UL_rnti = df_UL.loc[df_UL.index[id4:id3], ['time', 'rnti']]
        df_index_UL_tbs = df_index_UL_tbs.set_index('time')
        df_index_UL_rnti = df_index_UL_rnti.set_index('time')
        df_index = pd.concat([df_index_DL, df_index_UL_rnti], axis=0)
        df_index = df_index.join(df_index_UL_tbs)
        df_index = df_index.fillna(0)
        # Add series index
        df_index['series_id'] = np.ones(df_index.shape[0])*index
        # Delete duplicated samples
        duplicated_idx = df_index.index.duplicated()
        df_index = df_index[~duplicated_idx]
        # Sort samples by time
        df_index = df_index.sort_index(level=0)
        # df_cut_off = pd.concat([df_cut_off, df_index])
        if i == 0:
            df_index.to_csv(data_file_path, index_label='time', mode='a')
        else:
            df_index.to_csv(data_file_path, index_label='time', mode='a', header=None)
        index = index + 1
    return df_cut_off, index


def generate_traffic_zero_complement_file(dl_path, ul_path, dl_aft_zerocom, ul_aft_zerocom, appname):

    df_dl = pd.read_csv(dl_path, dtype={'time': str, 'rnti': str, 'link': str, f'tbs_dl': int})
    df_dl_val = df_dl
    while True:
        l1 = len(df_dl_val)
        df_new_down = zero_complement(df_dl_val, 'dl', appname)
        df_dl_val = df_new_down
        if len(df_new_down) == l1:
            df_new_down.to_csv(dl_aft_zerocom, index=False)
            break

    df_ul = pd.read_csv(ul_path, dtype={'time': str, 'rnti': str, 'link': str, f'tbs_ul': int})
    df_ul_val = df_ul
    while True:
        l1 = len(df_ul_val)
        df_new_up = zero_complement(df_ul_val, 'ul', appname)
        df_ul_val = df_new_up
        if len(df_new_up) == l1:
            df_new_up.to_csv(ul_aft_zerocom, index=False)
            break



def generate_data_file(dl_path, ul_path, log_path, data_file_path, start_index, appname):
    """
    :param dl_path: the file path to the downlink traffic block size (tbs_dl)
    :param ul_path: the file path to the uplink traffic block size (tbs_ul)
    :param log_path: the file path to the app usage log
    :param data_file_path: the file path to save the final tbs series
    :param start_index: the start index of the tbs series
    """
    df_new_down = pd.read_csv(dl_path, dtype={'time': str, 'rnti': str, 'link': str, 'tbs_dl': int})
    df_new_up = pd.read_csv(ul_path, dtype={'time': str, 'rnti': str, 'link': str, 'tbs_ul': int})
    a, id_str = cut_off(df_new_down, df_new_up, log_path, start_index, data_file_path, appname)
    return id_str


def generate_traffic_files(dl_path, ul_path, RNTIlist, file_path, total_log_num, appname ):
    """
    :param dl_path: the file path to save downlink traffic block size (tbs_dl)
    :param ul_path: the file path to save uplink traffic block size (tbs_ul)
    :param RNTIlist: inout path of the RNTI list for targe UE
    :param file_path: the path of the log file from airscope
    :param total_log_num: the total number of log files
    """
    f1 = open(dl_path, 'w', newline='')
    write_down = csv.writer(f1)
    write_down.writerow(['time', 'rnti', 'link', 'tbs_dl'])

    f2 = open(ul_path, 'w', newline='')
    write_up = csv.writer(f2)
    write_up.writerow(['time', 'rnti', 'link', 'tbs_ul'])

    for i in tqdm(range(0,  total_log_num), desc=f'read the {appname} log file'):

        try:
            if i == 0:
                path = file_path + "/airscope.log"
                filter_data(path, write_down, write_up, RNTIlist)
            else:
                path = file_path + "/airscope.log.%d" % i
                filter_data(path, write_down, write_up, RNTIlist)
        except:
            print("An exception occurred")
            break
