#!/usr/bin/env python
import ccxt
import pandas as pd
from datetime import datetime, timedelta
import os
from tqdm import tqdm

# This script downloads historical data for any trading pair (eg. SOLUSDT) from Binance Futures
# and saves it as CSV files for different timeframes.

# Usage:
# 1. Ensure you have a virtual environment set up and activated.
# 2. Install the required packages using:
#    pip install ccxt pandas tqdm
# 3. Run the script:
#    python download_binance_historical_data.py
# 4. Enter the folder path where you want to save the CSV files when prompted.
#    The default folder path is set to '..\..\Data'.
default_folder_path = os.path.join("..", "..", "Data")

#    The program will create a subfolder in the specified path named {symbol}
#    and a subfolder inside that one named "Candles"
#    and a subfolder inside that one named {end_date}.
#    If you don't want it to do that, turn off this flag:
create_subfolders = True
# The data will be saved in CSV format with filenames in the format:
# {symbol}_binance_{start_date}_{end_date}_{timeframe}.csv
# It is important in later scripts that the file name ends in _{timeframe}.csv

# You can change the following variables as desired:
# - symbol: The trading pair to download data for. Default is 'SOLUSDT'.
symbol = 'SOLUSDT'

# - timeframes: The list of timeframes to download data for. 
# If you want a basic set, use the default: ['1M', '1w', '3d', '1d', '12h', '8h', '6h', '4h', '2h', '1h', '30m', '15m', '5m', '3m', '1m'].
# HOWEVER, I recommend just downloading the 1m timeframe as any other timeframes can be generated from it.
# To do so, comment out this line (add a # character to the front of it) and then uncomment the next line by removing the '#' character. 
timeframes = ['1M', '1w', '3d', '1d', '12h', '8h', '6h', '4h', '2h', '1h', '30m', '15m', '5m', '3m', '1m']
#timeframes = ['1m']

# - start_date: The start date for the data download. In SOLUSDT's case, the earliest data available starts at 2020-09-14
# so as long as your start date is earlier it should be fine.  Of course you can pick a later one, too.
start_date = '2020-01-01'

# - end_date: The end date for the data download. Default is the current date.  Change this if you want a specific window.
end_date = datetime.now().strftime('%Y-%m-%d')

# **************************************************************************************************
# Initialize the Binance futures client
exchange = ccxt.binance({
    'options': {
        'defaultType': 'future',
    },
})

# Ensure the folder exists
folder_path = input(f"\n\rEnter the folder path to save the CSV files (default: {default_folder_path}): ") or default_folder_path
if create_subfolders:
    folder_path = os.path.join(folder_path, symbol, "Candles", end_date)

if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Convert start and end dates to milliseconds
start_timestamp = exchange.parse8601(start_date + 'T00:00:00Z')
end_timestamp = exchange.parse8601(end_date + 'T00:00:00Z')

# Progress bar for timeframes
timeframe_pbar = tqdm(total=len(timeframes), desc='Overall Progress', unit='timeframe')

for timeframe in timeframes:
    print(f'\nDownloading data for timeframe: {timeframe}')
    all_candles = []
    current_timestamp = start_timestamp
    #first_record = True

    # Calculate the total minutes for the progress bar
    total_minutes = (end_timestamp - start_timestamp) // 60000
    pbar = tqdm(total=total_minutes, desc=f'Timeframe: {timeframe}', unit='min')

    while current_timestamp < end_timestamp:
        candles = exchange.fetch_ohlcv(symbol, timeframe, since=current_timestamp, limit=1500)
        if len(candles) == 0:
            break
        current_timestamp = candles[-1][0] + exchange.parse_timeframe(timeframe) * 1000
        all_candles.extend(candles)

        # Update the progress bar
        pbar.update(len(candles))

    # Jump the progress bar to 100% - this is done because we don't know the exact start date of the data until after the progress bar is declared.
    pbar.n = pbar.total
    pbar.refresh()
    pbar.close()

    # Convert data to a Pandas DataFrame
    df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Generate the filename
    start_date_formatted = start_date.replace('-', '')
    end_date_formatted = end_date.replace('-', '')
    filename = f'{symbol}_binance_{start_date_formatted}_{end_date_formatted}_{timeframe}.csv'
    file_path = os.path.join(folder_path, filename)

    # Save to a CSV file
    df.to_csv(file_path, index=False)
    print(f'Data saved to {file_path}')

    # Update the overall progress bar
    timeframe_pbar.update(1)

timeframe_pbar.close()
print('Data download complete!')
