import matplotlib.pyplot as plt
import re
import csv
import numpy as np
import pandas as pd
import datetime

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
    start_time = datetime.datetime.strptime(start_time,'%H:%M:%S.%f')
    for line in input_data:
        if 'Time' in line:
            line = next(input_data)
            match = re.search(pattern, line)
            if match:
                time_value = float(match.group(1))
                time_value = datetime.timedelta(seconds=time_value)+start_time
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
    RNTI_time = datetime.datetime.strptime(RNTI_time,'%H:%M:%S.%f')
    RRC_time = datetime.datetime.strptime(RNTI_list[0][RRC_time_idx],'%H:%M:%S.%f')
    if RRC_time_idx == len(RNTI_list[1])-1:
        next_RRC_time = None
    else:
        next_RRC_time = datetime.datetime.strptime(RNTI_list[0][RRC_time_idx+1],'%H:%M:%S.%f')

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
        for idx, rnti in enumerate(rntilist[1]):
            if 'rnti=%s,' % rnti in line:
                if '[MAC' in line or '[HI]' in line:
                    break

                else:
                    if time_period_judge(line[0],rntilist,idx):
                        tbs_string = [s for s in line if "tbs" in s]
                        rnti_string = [s for s in line if "rnti" in s]
                        uldl_string = [s for s in line if "DL" in s or 'UL' in s]
                        tbs_match = [line[0], rnti_string[0][5:], uldl_string[0], re.findall(r"\d+", tbs_string[0])[0]]
                        if '[DL]' in uldl_string:
                            csv_write_down.writerow(tbs_match)
                        else:
                            csv_write_up.writerow(tbs_match)

def time_difference(t1: str, t2: str) -> float:
    """
    Return the difference between two time points by seconds
    """
    t1_list = list(map(float, t1.split(':')))
    t2_list = list(map(float, t2.split(':')))
    time_diff = 3600 * (t2_list[0] - t1_list[0]) + 60 * (t2_list[1] - t1_list[1]) + ((t2_list[2]) - (t1_list[2]))
    return time_diff


def time_plus_one(time: str) -> str:
    """
    Add 1 second at current time point
    """
    time_list = list(map(str, time.split(':')))
    'Check carry-out of seconds'
    if time_list[2].split('.')[0] == '59':
        time_list[2] = '00.000000'
        'Check carry-out of minutes'
        if time_list[1] == '59':
            time_list[1] = '00'
            'Check carry-out of hours'
            if time_list[0] == '23':
                time_list[0] = '00'
            else:
                time_list[0] = str(int(time_list[0]) + 1)
                'Formatting 0X'
                if len(time_list[0]) == 1:
                    time_list[0] = '0' + time_list[0]
        else:
            time_list[1] = str(int(time_list[1]) + 1)
            'Formatting 0X'
            if len(time_list[1]) == 1:
                time_list[1] = '0' + time_list[1]
    else:
        temp = str(int(time_list[2].split('.')[0]) + 1)
        if len(temp) == 1:
            temp = '0' + temp
        time_list[2] = temp + '.000000'
    time_str = ':'.join(time_list)
    return time_str


def zero_complement(df, link):
    """
    Check time difference between adjacent time points
    if time difference is larger than 1 second
    insert a new row [time+=1, same rnti, same link, tbs=0]
    """
    for row_id in range(1, df.shape[0]):
        t1 = df.loc[row_id - 1, 'time']
        t2 = df.loc[row_id, 'time']
        if time_difference(t1, t2) > 1:
            new_row = df.loc[row_id - 1].copy()
            new_row['time'] = time_plus_one(new_row['time'])
            new_row[f'tbs_{link}'] = '0'
            df.loc[row_id - 0.5] = new_row
            df = df.sort_index().reset_index(drop=True)
    return df


def multiple_zero_complement(data_path: str, link) -> pd.DataFrame:
    """
    Due to the change of data size led by inserting zeros
    Multiple zero complements are required to complement
    the entire data set
    """
    df = pd.read_csv(data_path, dtype={'time': str, 'rnti': str, 'link': str, 'tbs_ul': int})
    while True:
        l1 = len(df)
        df = zero_complement(df, link)
        if len(df) == l1:
            return df


def find_time(df: pd.DataFrame, time: str) -> int:
    """
    find time point by the integer part
    lead to a small portion of redundant data in the data set
    """
    for index, row in df.iterrows():
        if row['time'][:8] == time[:8]:
            return index


def cut_off(df_DL, df_UL, log_path, start_id , data_file_path):
    """
    Cut off the data set by specified time periods
    When cut off, only check the integer part of time
    """
    df_cut_off = pd.DataFrame({'rnti': [], 'tbs_dl': [], 'tbs_ul': []})
    file = open(log_path)
    index = start_id
    for line in file:
        print('The {id}-th series is being extracted.'.format(id=index))
        log = line.split(' ')
        time1 = log[1] + '.000000'
        # time2 = log[-1][:-1] + '.000000'
        time2 = log[-5] + '.000000'
        # print(time1,time2)
        id1 = find_time(df_DL, time1)
        id2 = find_time(df_DL, time2) + 1
        # print(time1,time2)
        id3 = find_time(df_UL, time1)
        id4 = find_time(df_UL, time2) + 1
        # print(id1,id2)
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
        idx = np.ones(df_index.shape[0])*index
        idx = idx.astype(int)
        df_index['series_id'] = idx
        # Delete duplicated samples
        duplicated_idx = df_index.index.duplicated()
        df_index = df_index[~duplicated_idx]
        # Sort samples by time
        df_index = df_index.sort_index(level=0)
        df_cut_off = pd.concat([df_cut_off, df_index])
        # df_index.to_csv(data_file_path, index_label='time', mode='a')
        index = index + 1
    return df_cut_off


def generate_traffic_zero_complement_file(dl_path, ul_path):
    df_new_down = multiple_zero_complement(dl_path, 'dl')
    df_new_down.to_csv(dl_path, index=False)
    df_new_up = multiple_zero_complement(ul_path, 'ul')
    df_new_up.to_csv(ul_path, index=False)


def generate_data_file(dl_path, ul_path, log_path, data_file_path, start_index):
    """
    :param dl_path: the file path to the downlink traffic block size (tbs_dl)
    :param ul_path: the file path to the uplink traffic block size (tbs_ul)
    :param log_path: the file path to the app usage log
    :param data_file_path: the file path to save the final tbs series
    :param start_index: the start index of the tbs series
    """
    df_new_down = pd.read_csv(dl_path, dtype={'time': str, 'rnti': str, 'link': str, 'tbs_dl': str})
    df_new_up = pd.read_csv(ul_path, dtype={'time': str, 'rnti': str, 'link': str, 'tbs_ul': str})
    df_cut_off = cut_off(df_new_down, df_new_up, log_path, start_index, data_file_path)
    # delete the duplicated samples
    # duplicated_idx = df_cut_off.index.duplicated()
    # df_cut_off = df_cut_off[~duplicated_idx]
    df_cut_off.to_csv(data_file_path, index_label='time', mode='a')


def generate_traffic_files(dl_path, ul_path, RNTIlist, file_path, total_log_num ):
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

    for i in range(0,  total_log_num+1):
        print('read the %dth file' % i)
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




