import numpy as np
import pandas as pd
from tqdm import tqdm


def time_difference(time1, time2, com):
    list1 = list(map(float, time1.split(":")))
    list2 = list(map(float, time2.split(":")))
    if (list2[com] - list1[com]) >= 2:
        time_diff = (list2[com + 1] - list1[com + 1])-10
    else:
        time_diff = 60 * (list2[com] - list1[com]) + (list2[com + 1] - list1[com + 1])
    return time_diff


def feature_encoder(feature_sequences):
    encoded_feature_sequence = []
    feature_after_encoding = pd.DataFrame({'time': [],
                                           'mean_tbs_dl': [], 'mean_tbs_ul': [],
                                           'max_tbs_dl': [], 'max_tbs_ul': [],
                                           'total_dl': [], 'total_ul': [],
                                           'num_of_packets_dl': [], 'num_of_packets_ul': []})
    print("Start data encoding")
    for index, feature in enumerate(tqdm(feature_sequences)):
        ifeature_after_encoding = pd.DataFrame({'time': [],
                                                'mean_tbs_dl': [], 'mean_tbs_ul': [],
                                                'max_tbs_dl': [], 'max_tbs_ul': [],
                                                'total_dl': [], 'total_ul': [],
                                                'num_of_packets_dl': [], 'num_of_packets_ul': []})
        ifeature = feature[0]
        itime = ifeature[['time']]
        index1 = ifeature.index.tolist()[0]
        time_slice = 0.1
        time1 = itime.loc[index1]
        ''''
        Compensation of the time
        '''
        if index < 500:
            com = 1
        else:
            com = 0

        for index2, time2 in itime.iterrows():

            if time_difference(time1[0], time2[0], com) > 0.1:

                d = {'time': [time_slice],
                     'mean_tbs_dl': [ifeature.loc[index1:index2, ['tbs_dl']].mean()[0]],
                     'mean_tbs_ul': [ifeature.loc[index1:index2, ['tbs_ul']].mean()[0]],
                     'max_tbs_dl': [ifeature.loc[index1:index2, ['tbs_dl']].max()[0]],
                     'max_tbs_ul': [ifeature.loc[index1:index2, ['tbs_ul']].max()[0]],
                     'total_dl': [ifeature.loc[index1:index2, ['tbs_dl']].sum()[0]],
                     'total_ul': [ifeature.loc[index1:index2, ['tbs_ul']].sum()[0]],
                     'num_of_packets_dl': [
                         (ifeature.loc[index1:index2, ['tbs_dl']] != 0).astype(int).sum(axis=0)[0]],
                     'num_of_packets_ul': [
                         (ifeature.loc[index1:index2, ['tbs_ul']] != 0).astype(int).sum(axis=0)[0]]}
                d1 = pd.DataFrame(d)
                # d1 = d1.replace([np.inf, -np.inf], 0)
                d1 = d1.fillna(0)

                if ifeature_after_encoding['time'].empty:
                    pass
                elif round(d1['time'].values[-1] - ifeature_after_encoding['time'].values[-1], 1) >= 0.2:

                    intertime = (d1['time'].values[-1] - ifeature_after_encoding['time'].values[-1]) / 0.1
                    D_inter = pd.DataFrame({'time': [], 'mean_tbs_dl': [], 'mean_tbs_ul': [],
                                            'max_tbs_dl': [], 'max_tbs_ul': [], 'total_dl': [], 'total_ul': [],
                                            'num_of_packets_dl': [], 'num_of_packets_ul': []})
                    for i in range(round(intertime) - 1):
                        d_inter = {'time': [ifeature_after_encoding['time'].values[-1] + (i + 1) * 0.1],
                                   'mean_tbs_dl': [0], 'mean_tbs_ul': [0],
                                   'max_tbs_dl': [0], 'max_tbs_ul': [0], 'total_dl': [0], 'total_ul': [0],
                                   'num_of_packets_dl': [0], 'num_of_packets_ul': [0]}

                        d_inter = pd.DataFrame(d_inter)

                        D_inter = pd.concat([D_inter, d_inter])

                    ifeature_after_encoding = pd.concat([ifeature_after_encoding, D_inter])

                ifeature_after_encoding = pd.concat([ifeature_after_encoding, d1])
                time_slice = time_slice + round(time_difference(time1[0], time2[0], com), 1)
                index1 = index2
                time1 = itime.loc[index1]

        encoded_feature_sequence.append((ifeature_after_encoding, feature[1]))
        ifeature_after_encoding['series_id'] = np.ones(ifeature_after_encoding.shape[0]) * index
        feature_after_encoding = pd.concat([feature_after_encoding, ifeature_after_encoding])
    return encoded_feature_sequence, feature_after_encoding
