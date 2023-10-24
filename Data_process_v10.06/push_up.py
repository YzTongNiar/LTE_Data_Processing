from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pickle
import pandas as pd

class MongoBase:
    def __init__(self, collec):
        self.collection = collec
        self.openDB()

    def openDB(self):
        uri = "mongodb+srv://cellulartraffic:Record_123@cellulartraffic.li8kini.mongodb.net/?retryWrites=true&w=majority"
        self.client = MongoClient(uri, server_api=ServerApi('1'))
        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
        self.db = self.client['test']
        self.collection = self.db[self.collection]
    def closeDB(self):
        self.client.close()

    def saveDatatoDB(self, save_path, label_path):
        encoded_data = pd.read_csv(save_path, sep=',')
        sub_dataframes = [group for _, group in encoded_data.groupby('series_id')]
        y = pd.read_csv(label_path, sep=',')
        for index, data in enumerate(sub_dataframes):
            self.collection.insert_one({'traffic_service': y['service'].loc[index],
                                           'app': y['app'].loc[index],
                                           'dl_prb': str(y['mean_prb_dl'].loc[index]),
                                           'ul_prb': str(y['mean_prb_ul'].loc[index]),
                                           'data': data.iloc[:, 0:9].to_dict(orient='records')})

# if __name__ == "__main__":
#
#     MongoDB = MongoBase('traffic')