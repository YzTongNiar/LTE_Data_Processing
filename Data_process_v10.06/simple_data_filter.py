import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
import numpy as np

# Function to plot data samples
def plot_data_samples(samples):
    fig, ax = plt.subplots(batch_size, figsize=(20, 5 * batch_size), layout='constrained')
    for i, sample in enumerate(samples):
        time = [point['time'] for point in sample['data']]
        total_dl = [point['total_dl'] for point in sample['data']]
        total_ul = [point['total_ul'] for point in sample['data']]

        ax[i].plot(time, total_ul, label='Upload bps', linewidth=2)
        ax[i].plot(time, total_dl, label='Download bps', linewidth=2)
        ax[i].set_title(f'sample {i}')
        ax[i].legend(fontsize="15")
    fig.show()

# Function to validate and save data
def validate_and_save_data(sample, is_valid):
    if is_valid:
        valid_collection.insert_one(sample)
    else:
        invalid_collection.insert_one(sample)

# Connect to MongoDB
url = "mongodb+srv://cellulartraffic:Record_123@cellulartraffic.li8kini.mongodb.net/?retryWrites=true&w=majority"
db_name = 'test'
client = MongoClient(url)
db = client[db_name]
collection = db["traffic_with_prb"]

# Create valid and invalid collections
valid_collection = db["valid_data"]
invalid_collection = db["invalid_data"]

# Query and process data in batches of 5
batch_size = 5
skip = 0

while True:
    samples = list(collection.find().skip(skip).limit(batch_size))
    if not samples:
        break  # No more data to process

    # Plot data samples
    plot_data_samples(samples)

    # Get user input for which samples to keep
    user_input = input("Enter the indices of valid samples (0-4), 'a' for all valid, 'n' for all invalid, or 'q' to quit: ")

    if user_input == 'q':
        break  # Quit the program
    if user_input == 'a':
        valid_indices = list(range(len(samples)))  # Mark all samples as valid
    elif user_input == 'n':
        valid_indices = []  # Mark all samples as invalid
    else:
        try:
            valid_indices = [int(idx) for idx in user_input.split()]
        except ValueError:
            print("Invalid input. Please try again.")
            continue

    # Validate and save data
    for i, sample in enumerate(samples):
        validate_and_save_data(sample, i in valid_indices)

    skip += batch_size

print("Data validation and saving completed.")