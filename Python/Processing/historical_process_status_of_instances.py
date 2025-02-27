# This script processes instances using the 1m candle data as a price reference.
# It calculates whether specific price targets or Fibonacci levels are reached and updates the instance's status accordingly.
# The processed files are saved to an output folder, and the original files can be optionally deleted.
# This "single core" version of the file will do all processing in one thread, which may take a long time.
# For faster processing, use the "multicore" version of the file.

import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime

# Default paths (change these to your actual paths). You can put them here or enter them when prompted.
# Updated to match the new structure used by the download_binance_historical_data.py script:
price_data_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Candles')
instances_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Unprocessed')
default_output_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Processed', 'CompleteSet')

# Flag to control whether the old file is deleted after saving the output
delete_unprocessed_when_done = True  

# **************************************************************************************************
# Subroutine to find the exact date when a target price is reached
def find_target_date(one_minute_df, target_price, direction, start_date, end_date=None):
    # Filter the 1-minute data starting from the start_date
    if end_date is not None:
        one_minute_data_filtered = one_minute_df[(one_minute_df.index >= start_date) & (one_minute_df.index <= end_date)]
    else:
        one_minute_data_filtered = one_minute_df[one_minute_df.index >= start_date]
    
    # Check if the target price is reached in the 1-minute data
    if direction == 'up':
        target_reached = one_minute_data_filtered[one_minute_data_filtered['high'] >= target_price]
    else:
        target_reached = one_minute_data_filtered[one_minute_data_filtered['low'] <= target_price]
    
    if not target_reached.empty:
        exact_date = target_reached.index[0]
        return exact_date.replace(second=0, microsecond=0)

    return None

# Function to update the status and active date
def update_status(instance_df, one_minute_df, timeframe):
    # Append the necessary columns to the instance dataframe if they don't exist
    if 'Status' not in instance_df.columns:
        instance_df['Status'] = 'Pending'
    if 'Active Date' not in instance_df.columns:
        instance_df['Active Date'] = None
    if 'Reached0.5' not in instance_df.columns:
        instance_df['Reached0.5'] = 0
    if 'Reached0.0' not in instance_df.columns:
        instance_df['Reached0.0'] = 0
    if 'Reached-0.5' not in instance_df.columns:
        instance_df['Reached-0.5'] = 0
    if 'Reached-1.0' not in instance_df.columns:
        instance_df['Reached-1.0'] = 0
    if 'Completed Date' not in instance_df.columns:
        instance_df['Completed Date'] = None
    if 'DateReached0.5' not in instance_df.columns:
        instance_df['DateReached0.5'] = None
    if 'DateReached0.0' not in instance_df.columns:
        instance_df['DateReached0.0'] = None
    if 'DateReached-0.5' not in instance_df.columns:
        instance_df['DateReached-0.5'] = None
    if 'DateReached-1.0' not in instance_df.columns:
        instance_df['DateReached-1.0'] = None
    if 'MaxDrawdown' not in instance_df.columns:
        instance_df['MaxDrawdown'] = None
    if 'MaxDrawdown Date' not in instance_df.columns:
        instance_df['MaxDrawdown Date'] = None
    if 'MaxFib' not in instance_df.columns:
        instance_df['MaxFib'] = None

    # Create a progress bar for the row processing with the timeframe included
    instance_pbar = tqdm(total=len(instance_df), desc=f'Processing {timeframe} instances', unit='instance', leave=False)

    for idx, instance in instance_df.iterrows():
        # Determine the direction for different targets
        entry_direction = 'down' if instance['direction'] == 'long' else 'up'
        target_direction = 'up' if instance['direction'] == 'long' else 'down'
        
        # Check if entry target was reached
        active_date = find_target_date(one_minute_df, instance['entry'], entry_direction, instance['confirm_date'])
        if active_date is None:
            instance_pbar.update(1)
            continue  # Stop processing the record if no active date is found

        instance_df.at[idx, 'Active Date'] = active_date
        instance_df.at[idx, 'Status'] = 'Active'

        # Check if the target was reached
        completed_date = find_target_date(one_minute_df, instance['target'], target_direction, active_date)
        if completed_date is None:
            instance_pbar.update(1)
            continue  # Stop processing the record if no completed date is found

        instance_df.at[idx, 'Completed Date'] = completed_date
        instance_df.at[idx, 'Status'] = 'Completed'

        # Calculate MaxDrawdown and MaxDrawdown Date
        drawdown_period = one_minute_df.loc[active_date:completed_date]
        
        if instance['direction'] == 'short':
            max_drawdown_row = drawdown_period.loc[drawdown_period['high'].idxmax()] if not drawdown_period.empty else pd.Series()
            max_drawdown = max_drawdown_row.get('high', None)
        else:  # direction == 'long'
            max_drawdown_row = drawdown_period.loc[drawdown_period['low'].idxmin()] if not drawdown_period.empty else pd.Series()
            max_drawdown = max_drawdown_row.get('low', None)

        max_drawdown_date = max_drawdown_row.name
        instance_df.at[idx, 'MaxDrawdown'] = max_drawdown
        instance_df.at[idx, 'MaxDrawdown Date'] = max_drawdown_date

        # Check Fibonacci levels in order between active and completed dates
        fib_levels = [
            ('fib0.5', 'Reached0.5', 'DateReached0.5'),
            ('fib0.0', 'Reached0.0', 'DateReached0.0'),
            ('fib-0.5', 'Reached-0.5', 'DateReached-0.5'),
            ('fib-1.0', 'Reached-1.0', 'DateReached-1.0')
        ]

        for fib, reach_key, date_key in fib_levels:
            if (instance['direction'] == 'long' and max_drawdown <= instance[fib]) or (instance['direction'] == 'short' and max_drawdown >= instance[fib]):
                fib_date = find_target_date(one_minute_df, instance[fib], entry_direction, active_date, completed_date)
                if fib_date:
                    instance_df.at[idx, reach_key] = 1
                    instance_df.at[idx, date_key] = fib_date
                    active_date = fib_date  # Update active_date to the fib_date for the next fib level check
                else:
                    break
            else:
                break

        instance_pbar.update(1)

    instance_pbar.close()
    return instance_df

# Prompt for the instances folder, the candle data folder, and the output folder
instances_folder = input(f"\n\rEnter the folder path containing the instance CSV files (default: {instances_folder}): ") or instances_folder
price_data_folder = input(f"\n\rEnter the folder path containing the price data CSV files (default: {price_data_folder}): ") or price_data_folder
output_folder = input(f"\n\rEnter the output folder path to save the updated instance CSV files (default: {default_output_folder}): ") or default_output_folder

# Create the output directory if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Load the 1-minute data
one_minute_csv = next(f for f in os.listdir(price_data_folder) if f.endswith('_1m.csv'))
one_minute_csv = os.path.join(price_data_folder, one_minute_csv)
one_minute_data = pd.read_csv(one_minute_csv, parse_dates=['timestamp'])
one_minute_data.set_index('timestamp', inplace=True)

files = [f for f in os.listdir(instances_folder) if f.endswith('.csv')]
total_files = len(files)

# Create a progress bar for the file processing
total_size_mb = sum(os.path.getsize(os.path.join(instances_folder, f)) for f in files) / (1024 * 1024)
total_size_mb = round(total_size_mb, 2)
file_pbar = tqdm(total=total_size_mb, desc=f'Processing file 0 of {total_files}', unit='MB', leave=True)

for idx, filename in enumerate(files, start=1):
    file_pbar.set_description(f'Processing file {idx} of {total_files}')
    filepath = os.path.join(instances_folder, filename)
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    file_size_mb = round(file_size_mb, 2)
    df = pd.read_csv(filepath, parse_dates=['confirm_date'])

    # Ensure 'confirm_date' is properly converted to datetime
    df['confirm_date'] = pd.to_datetime(df['confirm_date'])

    # Extract the timeframe from the filename (e.g., '12h' from 'instances_solusdt_binance_20200101_20241109_12h.csv')
    timeframe = filename.split('_')[-1].replace('.csv', '')

    # Update the status and active date
    updated_instance_df = update_status(df, one_minute_data, timeframe)

    # Save the updated instance CSV to the output folder
    updated_instance_filepath = os.path.join(output_folder, filename)
    updated_instance_df.to_csv(updated_instance_filepath, index=False)
    print(f'Updated instances saved to {updated_instance_filepath}')

    # Remove the processed file from the input folder if the flag is set
    if delete_unprocessed_when_done:
        os.remove(filepath)

    file_pbar.update(file_size_mb)

file_pbar.close()

print('Processing complete!')
