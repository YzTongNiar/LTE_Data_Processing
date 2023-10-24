import pandas as pd
from sklearn.preprocessing import LabelEncoder
import pickle
import preprocessing
import numpy as np
import random


def encoding_feature(Data, Label):
    label_encoder = LabelEncoder()
    encoded_labels = label_encoder.fit_transform(Label.service)
    Label["label"] = encoded_labels
    Feature_Columns = Data.columns.tolist()[0:4]
    sequences = []
    for iseries_id, group in Data.groupby("series_id"):
        try:
            isequence_features = group[Feature_Columns]
            ilabel = Label[Label.sample_ind == iseries_id].iloc[0].label
            sequences.append((isequence_features, ilabel))
        except:
            print(f'An error happen at series {iseries_id}')

    encoded_feature_sequences, features_encoded = preprocessing.feature_encoder(sequences)
    return encoded_feature_sequences, features_encoded


def load_encoded_data(X_train_path, y_train_path, save_path, save_encoded_path):

    X_train = pd.read_csv(X_train_path, dtype={'time': str, 'rnti': str,
                                               'link': str, 'tbs_dl': int,
                                               'tbs_ul': int, 'series_id': int})  # Data file

    y_train = pd.read_csv(y_train_path, dtype={'service': str, 'app': str, 'series_id': int})  # Label file

    encoded_data, encoded_feature = encoding_feature(X_train, y_train)

    encoded_feature.to_csv(save_encoded_path, index=False)

    # with open(save_path, 'wb') as ff:
    #     pickle.dump(encoded_data, ff)
    #     ff.close()

    return encoded_feature







