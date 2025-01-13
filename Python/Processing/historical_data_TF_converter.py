import pandas as pd
from datetime import datetime, timedelta
import os

# This script converts 1-minute candle data from a CSV file into various custom timeframes.
# It prompts the user to input the folder path containing the 1-minute candle CSV file.
# The script then resamples the data into the specified custom timeframes and saves the resulting data as new CSV files in the same folder.

# Default path (change this to your actual path). You can put it here or enter it when prompted.
# The default path I've put here is just a guess because I don't know what date you downloaded the data, so it guesses today:
default_folder_path = os.path.join("..", "..", "Data", "SOLUSDT", "Candles", datetime.now().strftime('%Y-%m-%d'))

# Define the custom timeframes
timeframes = ['3m', '4m', '5m', '6m', '8m', '9m', '10m', '12m', '15m', '16m', '18m', '20m', '24m', '30m', '32m', '40m', '45m', '48m', '72m', '80m', '90m', '96m', '144m', '160m', '288m', '2h', '3h', '4h', '6h', '8h', '12h', '1D', '2D', '3D', '4D', '5D', '6D', '7D', '8D', '9D', '10D', '11D', '12D', '13D', '14D', '15D', '16D', '17D', '18D', '19D', '20D', '21D', '22D', '23D', '24D', '25D', '26D', '27D', '28D', '29D', '30D', '31D', '32D', '33D', '34D', '35D', '36D']

# **************************************************************************************************
# Prompt for the folder path
folder_path = input(f"\n\rEnter the folder path containing the 1-minute candle CSV file (default: {default_folder_path}): ") or default_folder_path

# Find the input file in the folder
input_file = None
for file in os.listdir(folder_path):
    if file.endswith("_1m.csv"):
        input_file = os.path.join(folder_path, file)
        break

if input_file is None:
    raise FileNotFoundError("No file ending in '_1m.csv' found in the specified folder.")

# Load the 1-minute candle data
df = pd.read_csv(input_file)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)

# Get the datestamp from the first record
first_record_date = df.index[0].strftime('%Y%m%d')

# Function to resample the data
def resample_data(df, rule):
    return df.resample(rule).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

# Function to handle year-end rollover
def handle_year_end_rollover(df, rule):
    years = list(range(df.index.year.min(), df.index.year.max() + 1))
    result = []
    for year in years:
        year_start = pd.Timestamp(f'{year}-01-01')
        year_end = pd.Timestamp(f'{year}-12-31 23:59:59')
        year_df = df[(df.index >= year_start) & (df.index <= year_end)]
        if not year_df.empty:
            resampled_year_df = resample_data(year_df, rule)
            result.append(resampled_year_df)
    return pd.concat(result)

# Ensure the folder exists
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Resample and save the data for each custom timeframe
for tf in timeframes:
    rule = tf.replace('m', 'min').replace('h', 'h').replace('D', 'D')
    combined_df = handle_year_end_rollover(df, rule)
    
    # Save the combined data for each timeframe
    file_name = f"{folder_path}/solusdt_binance_{first_record_date}_{tf}.csv"
    combined_df.to_csv(file_name, index=True)

    print(f"Data for timeframe {tf} saved to {file_name}")

print('Custom timeframe data creation complete!')
