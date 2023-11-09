import decode_ulpl
import concat_excels
import encoding_data
import os
import pandas as pd
import argparse
import prb_map
import push_up

parser = argparse.ArgumentParser()
parser.add_argument('--raw_data_folder_path', default='F:/airscope_data/validation_data3/', type=str, help='The folder to store airscope.log, data folder in this folder should be like 2023.10.24_tiktok ')
parser.add_argument('--filtered_data_folder', default='./Processed_data/Filtered_data/', type=str, help='The folder to store the dl or ul tbs data')
parser.add_argument('--data_for_encoding', default='./Processed_data/Apps_data/', type=str, help='The folder to store the data after cutting of')
parser.add_argument('--X_train_path', default='./Processed_data/data_all_apps.csv', type=str, help='The path to store data of all apps')
parser.add_argument('--Y_train_path', default='./Processed_data/Encoded_data/', type=str, help='The path to store label')
parser.add_argument('--save_path', default='./Processed_data/Encoded_data/encoded_feature_', type=str, help='The path to store encoded data')
args = parser.parse_args()


def label_save(appname, sample_ind, id_last, label_file_path):
    label_class = {'a_streamingvideo': ['tiktok', 'netflix', 'youtube','amazonprime'],
                   'b_streamingmusic': ['spotify', 'youtubemusic', 'applemusic'],
                   'c_textchat': ['whatsapptext', 'wechattext', 'telegramtext'],
                   'd_videochat': ['whatsappvideo', 'wechatvideo', 'telegramvideo'],
                   'e_game': ['pubg', 'legends', 'genshin'],
                   'f_socialnetworking': ['ins', 'facebook', 'x'],
                   'g_shopping': ['shopee', 'lazada', 'carousell']}
    for ikey, ivalue in label_class.items():
        if appname in ivalue:
            app = [appname] * (sample_ind - id_last)
            traffic_service = [ikey] * (sample_ind - id_last)
            sample_id = list(range(id_last, sample_ind))
            df_label = pd.DataFrame({'service': traffic_service, 'app': app, 'sample_ind': sample_id})
            break

    if id_last == 0:
        df_label.to_csv(label_file_path, index=False, mode='a')
    else:
        df_label.to_csv(label_file_path, header=None, index=False, mode='a')


if __name__ == '__main__':
    # load file path
    raw_data_folder_path = args.raw_data_folder_path
    raw_filter_data_folder = args.filtered_data_folder
    data_for_encoding = args.data_for_encoding
    X_train_path = args.X_train_path
    Y_train_path = args.Y_train_path
    app_list = os.listdir(raw_data_folder_path)

    MongoDB = push_up.MongoBase('traffic_with_prb')

    for i, app_str in enumerate(app_list):
        id_str = 0
        id_str_last = 0
        app_data_path = raw_data_folder_path + app_str
        rnti_file_path = app_data_path + '/RNTI.txt'

        app_data_list = os.listdir(app_data_path)
        log_file_list = [x for x in app_data_list if 'airscope.log' in x]
        record_time_filename = [x for x in app_data_list if 'log_' in x]
        record_time_path = app_data_path + '/' + record_time_filename[0]
        airscp_csv_filename = [x for x in app_data_list if 'airscope.csv' in x]
        airscp_csv_path = app_data_path + '/' + airscp_csv_filename[0]
        total_log_number = len(log_file_list)

        '''
        Path for the saved_files
        '''
        app_name = app_str.split('_')[1]
        downlink_path = raw_filter_data_folder + 'downlink_' + app_name + '.csv'
        uplink_path = raw_filter_data_folder + 'uplink_' + app_name + '.csv'
        dl_aft_zerocom = raw_filter_data_folder + 'downlink_' + app_name + '_aft_zerocom' + '.csv'
        ul_aft_zerocom = raw_filter_data_folder + 'uplink_' + app_name + '_aft_zerocom' + '.csv'
        series_data_path = data_for_encoding + 'Data_' + app_name + '.csv'
        label_path = args.Y_train_path + app_name+'_label.csv'
        save_path = args.save_path + app_name + '.pkl'
        save_encoded_path = args.save_path + app_name + '.csv'
        save_encoded_with_prb_path = args.save_path + app_name +'_with_prb.csv'
        '''
        Data_process for each app
        '''

        '''
        RNTI list of the experiment mobile phone
        '''
        RNTI_list = decode_ulpl.rnti_filter(rnti_file_path, app_data_path)
        '''
        Filter the ul dl tbs from the airscope.log files
        '''
        decode_ulpl.generate_traffic_files(downlink_path, uplink_path, RNTI_list, app_data_path, total_log_number,
                                           app_name)
        '''
        complement zeros for the time when there are no valid ul or dl tbs data (for cut of)
        '''
        decode_ulpl.generate_traffic_zero_complement_file(downlink_path, uplink_path, dl_aft_zerocom,
                                                          ul_aft_zerocom, app_name)
        '''
        cut the data with the log of running app
        '''

        id_str = decode_ulpl.generate_data_file(dl_aft_zerocom, ul_aft_zerocom, record_time_path, series_data_path,
                                                id_str, app_name)

        '''
        generate the label file
        '''
        label_save(app_name, id_str, id_str_last, label_path)

        '''
        add prb value
        '''
        encoding_data.load_encoded_data(series_data_path, label_path, save_path, save_encoded_path)
        '''
        calculate mean prb and add to label
        '''
        encoded_data_with_prb = prb_map.prb_mapping(airscp_csv_path, record_time_path, save_encoded_path, save_encoded_with_prb_path, label_path)
        '''
        push to MongoDB
        '''
        # MongoDB.saveDatatoDB(save_encoded_with_prb_path, label_path)