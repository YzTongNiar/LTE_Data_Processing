import pickle
import matplotlib.pyplot as plt
import pandas as pd
import os

if __name__ == '__main__':
    save_path = 'encoded_feature.pkl'  # Encoded data pkl file
    with open(save_path, 'rb') as ff:
        encoded_data = pickle.load(ff)
        ff.close()
    y_train = pd.read_csv('Label.csv', dtype={'service': str, 'app': str, 'series_id': int})
    apps = y_train['app'].unique()
    for i, appname in enumerate(apps):
        index_list = y_train.sample_ind.loc[y_train['app'] == appname]
        ifig = 0
        for dataindex in index_list.tolist():
            if ifig == 0:
                fig, ax = plt.subplots(10, figsize=(8, 15), layout='constrained')
            idata = encoded_data[dataindex]
            dl_ul = idata[0]

            ax[ifig].plot(dl_ul.time, dl_ul.total_ul, label='ul')  # Plot some data on the axes.
            ax[ifig].plot(dl_ul.time, dl_ul.total_dl, label='dl')
            ax[ifig].set_title(appname + str(dataindex))
            ax[ifig].legend()
            ifig += 1
            if ifig == 10:
                fig.show()
                fig.savefig(f'./fig/spotify/{dataindex}_4')
                ifig = 0

        os.system('pause')
