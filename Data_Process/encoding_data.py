import pandas as pd
from sklearn.preprocessing import LabelEncoder
import pickle
import preprocessing
import numpy as np
import random


def encoding_feature(Data, Label):
    label_encoder = LabelEncoder()
    encoded_labels = label_encoder.fit_transform(Label.apps)
    Label["label"] = encoded_labels
    Feature_Columns = Data.columns.tolist()[0:4]
    sequences = []
    for iseries_id, group in Data.groupby("series_id"):
        try:
            isequence_features = group[Feature_Columns]
            ilabel = Label[Label.series_id == iseries_id].iloc[0].label

            sequences.append((isequence_features, ilabel))
        except:
            print(f'An error happen at series {iseries_id}')

    encoded_feature_sequences, features_encoded = preprocessing.feature_encoder(sequences)
    return encoded_feature_sequences, features_encoded


def load_encoded_data(X_train_path, y_train_path, save_path, save_encoded_path):

    X_train = pd.read_csv(X_train_path)  # Data file
    y_train = pd.read_csv(y_train_path)  # Label file

    encoded_data, encoded_feature = encoding_feature(X_train, y_train)
    encoded_feature.to_csv(save_encoded_path, index=False)
    with open(save_path, 'wb') as ff:
        pickle.dump(encoded_data, ff)
        ff.close()

    return encoded_data


def time_slice(encoding_data, time_len):
    time_sliced_data1 = []
    series_length = []
    for i in encoding_data:
        ifeature = i[0]
        time_max_index = np.argmin(abs(ifeature.iloc[:, 0].to_numpy() - (ifeature.iloc[-1, 0] - time_len)))
        if time_max_index == 0:
            index1 = 0
        else:
            index1 = np.random.randint(time_max_index)
        index2 = np.argmin(abs(ifeature.iloc[:, 0].to_numpy() - (ifeature.iloc[index1, 0] + time_len)))
        sliced_feature = ifeature.iloc[index1:index2+1, :]
        series_length.append(len(sliced_feature))
        time_sliced_data1.append((sliced_feature, i[1]))
    return time_sliced_data1, max(series_length)


def val_index(encoded_data):
    label = 0
    num = 0
    for index, data in enumerate(encoded_data):
        if data[1] == label:
            num += 1
        else:
            print(num)
            num = 1
        val = label - data[1]
        label = data[1]
        if val not in [0, -1]:
            print('Wrong')
    print('pass')


if __name__ == '__main__':
    '''
    FOR FIRST TIME TO ENCODE DATA
    '''

    X_train_path = "C:/Users/yongming001/Desktop/LTE project/DataSetMaker_Interpolation/all_data.csv" # Data file
    y_train_path = "C:/Users/yongming001/Desktop/LTE project/DataSetMaker_Interpolation/label.csv"  # Label file
    save_path = 'encoded_feature_9_14.pkl'  # Encoded data pkl file
    save_encoded_path = 'encoded_feature_9_14.csv'  # Encoded data csv file
    encoding_data = load_encoded_data(X_train_path, y_train_path, save_path, save_encoded_path  )

    '''
    SECOND TIME TO SLICE DATA AND PICK TEST DATA
    '''
    #
    # save_path = 'encoded_feature.pkl'  # Encoded data pkl file
    # with open(save_path, 'rb') as ff:
    #     encoded_data = pickle.load(ff)
    #     ff.close()
    # val_index(encoded_data)
    #
    # slice_time = 20
    #
    # time_sliced_data, series_length = time_slice(encoded_data, slice_time)
    #
    # time_sliced_data = time_sliced_data
    #
    # test_rand = random.sample(range(1, 19), 10)
    # test_rand.sort(reverse=False)
    # test_index = []
    # for i in [0, 100, 200, 300, 400, 500, 520, 540, 560, 580, 600, 620, 640, 660, 680, 700, 720, 740, 760, 780]:
    #     for j in test_rand:
    #         test_index.append(j + i)
    #
    # test_data = [time_sliced_data[i] for i in test_index]
    # val_index(test_data)
    #
    # test_index.sort(reverse=True)
    # all_index = [number for number in range(0, 800)]
    # for i in test_index:
    #     del all_index[i]
    #
    # train_data = [time_sliced_data[i] for i in all_index]
    # val_index(train_data)
    # save_path = 'encoded__train_data.pkl'  # Encoded data pkl file
    # with open(save_path, 'wb') as ff:
    #     pickle.dump(train_data, ff)
    #     ff.close()
    #
    # save_path = 'encoded__test_data.pkl'  # Encoded data pkl file
    # with open(save_path, 'wb') as ff:
    #     pickle.dump(test_data, ff)
    #     ff.close()
