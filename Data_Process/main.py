import decode_ulpl
import concat_excels
import encoding_data

if __name__ == '__main__':
    # load file path
    appname = 'wechat_videochat'
    date = '2023.9.24'
    total_log_num = 240
    id_str = 0  # start sample index of the class

    day = date[5:].replace('.', '_')
    log_name = f'log_{appname}.txt'
    Data_file_location = f'F:/airscope/data/{date}_{appname}'
    # save file path
    data_folder = f'../Data/data_{day}'
    # X_train_path = 'C:/Users/Jarvis/Desktop/Academic/Dissertation/Data/all_data.csv'
    X_train_path = data_folder + f'/Data_{appname}.csv'
    y_train_path = '../Data/label.csv'  # Label file
    save_path = f'encoded_feature_{day}_{appname}.pkl'  # Encoded data pkl file
    save_encoded_path = f'encoded_feature_{day}_{appname}.csv'  # Encoded data csv file

    downlink_path = 'downlink_' + appname + '.csv'  # the path for downlink traffic without cutting off
    uplink_path = 'uplink_' + appname + '.csv'  # the path for uplink traffic without cutting off
    series_data_path = data_folder + '/Data_' + appname + '.csv'  # the path for the final result
    app_timelog_path = f'F:/airscope/data/{date}_{appname}/' + log_name  # the path for the time stamp
    RNTI_path = Data_file_location + '/RNTI.txt'  # the path for the '.pcap' file

    RNTI_list = decode_ulpl.rnti_filter(RNTI_path, Data_file_location)
    decode_ulpl.generate_traffic_files(downlink_path, uplink_path, RNTI_list, Data_file_location, total_log_num)
    decode_ulpl.generate_traffic_zero_complement_file(downlink_path, uplink_path)
    decode_ulpl.generate_data_file(downlink_path, uplink_path, app_timelog_path, series_data_path, id_str)

    # concat_excels.merge(data_folder, X_train_path)
    encoded_data = encoding_data.load_encoded_data(X_train_path, y_train_path, save_path, save_encoded_path)