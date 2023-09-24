import os, pandas as pd


def get_files(path):
    fs = []
    for root, dirs, files in os.walk(path):
        for file in files:
            fs.append(os.path.join(root, file))
    return fs


def merge(data_folder, output_file_path):
    files = get_files(data_folder)
    arr = []
    for i in files:
      arr.append(pd.read_csv(i))

    concat_data = pd.concat(arr)
    concat_data = concat_data.sort_values(by=["series_id", 'time'], ascending=True)
    concat_data.to_csv(output_file_path, index=False)
    return concat_data


# if __name__ == '__main__':
#     data_folder = 'C:/Users/yongming001/Desktop/LTE project/DataSetMaker_Interpolation/data_9_14'
#     output_file_path = 'C:/Users/yongming001/Desktop/LTE project/DataSetMaker_Interpolation/all_data.csv'
#     merge(data_folder, output_file_path)
