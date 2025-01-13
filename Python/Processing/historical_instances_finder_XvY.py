# This script processes historical candle data to find instances of XvY candle breaks.
# It calculates some Fibonacci retrace levels for further analysis and saves the results to CSV files.
# Below, you can set the default threshold for opportunity size and the default input/output paths.

import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime

# Configurable variable for the minimum percent size of opportunity.  Set this to whatever you want.
min_diff_percent = 0.004  # 0.4%

# Configurable variable for the maximum number of breaking candles.
#  Set to 0 for no limit.
#  Set to 1 to generate Xv1 data.
max_y = 1  # Maximum number of breaking candles

# Default paths (change these to your actual paths). You can put them here or enter them when prompted.
# The default input path I've put here is just a guess because I don't know what date you downloaded the data, so it guesses today:
default_input_path = os.path.join("..", "..", "Data", "SOLUSDT", "Candles", datetime.now().strftime('%Y-%m-%d'))
default_output_path = os.path.join("..", "..", "Data", "SOLUSDT", "Instances", "Xv1", "Unprocessed")

# **************************************************************************************************
# Function to find instances and calculate Fibonacci extension levels
def find_instances(df, timeframe):
    instances = []

    # Create a progress bar for the file processing
    candle_pbar = tqdm(total=len(df) - 1, desc=f'Processing candles for {timeframe}', unit='candle', leave=False)

    i = 0
    while i < len(df) - 1:  # Adjust loop to ensure there's a following candle
        curr_candle = df.iloc[i]

        # Determine if the current candle is bullish or bearish
        if curr_candle['close'] > curr_candle['open']:
            direction = 'bullish'
        else:
            direction = 'bearish'

        # Initialize the series of candles
        series_start_index = i
        series_high = curr_candle['high']
        series_low = curr_candle['low']

        # Check subsequent candles to form a series
        j = i + 1
        while j < len(df):
            next_candle = df.iloc[j]

            # Check if the next candle continues in the same direction
            if (direction == 'bullish' and next_candle['close'] > next_candle['open']) or \
               (direction == 'bearish' and next_candle['close'] < next_candle['open']):
                series_high = max(series_high, next_candle['high'])
                series_low = min(series_low, next_candle['low'])
                j += 1
            else:
                break

        # Skip recording 1v1 situations
        if j - i == 1:
            i = j  # Move to the next candle
            candle_pbar.update(1)
            continue

        # Check if the series is broken by up to max_y candles
        k = j
        while k < len(df) and (max_y == 0 or k < j + max_y):
            breaking_candle = df.iloc[k]

            if direction == 'bullish':
                if breaking_candle['close'] < breaking_candle['open'] and \
                   breaking_candle['open'] > curr_candle['close'] and \
                   breaking_candle['close'] < curr_candle['open']:
                    fib_base = series_high - curr_candle['open']
                    target = curr_candle['open'] - fib_base * 0.618
                    fib0_5 = curr_candle['open'] + fib_base * 0.5
                    fib0_0 = series_high
                    fibN0_5 = curr_candle['open'] + fib_base * 1.5
                    fibN1_0 = curr_candle['open'] + fib_base * 2.0
                    situation = f"{j - i}v{k - j + 1}"

                    # Ensure x >= y before recording the instance
                    if (j - i) >= (k - j + 1) and abs(target - curr_candle['open']) / curr_candle['open'] >= min_diff_percent:
                        confirm_candle = df.iloc[k + 1] if k + 1 < len(df) else breaking_candle
                        instance_id = f"{confirm_candle.name.strftime('%Y-%m-%d %H:%M:%S')}_{timeframe}_{situation}_short"
                        instances.append({
                            'instance_id': instance_id,
                            'situation': situation,
                            'timeframe': timeframe,
                            'confirm_date': confirm_candle.name,
                            'direction': 'short',
                            'target': target,
                            'entry': curr_candle['open'],
                            'fib0.5': fib0_5,
                            'fib0.0': fib0_0,
                            'fib-0.5': fibN0_5,
                            'fib-1.0': fibN1_0
                        })
                    break  # Stop after finding a valid breaking candle
            elif direction == 'bearish':
                if breaking_candle['close'] > breaking_candle['open'] and \
                   breaking_candle['open'] < curr_candle['close'] and \
                   breaking_candle['close'] > curr_candle['open']:
                    fib_base = curr_candle['open'] - series_low
                    target = curr_candle['open'] + fib_base * 0.618
                    fib0_5 = curr_candle['open'] - fib_base * 0.5
                    fib0_0 = series_low
                    fibN0_5 = curr_candle['open'] - fib_base * 1.5
                    fibN1_0 = curr_candle['open'] - fib_base * 2.0
                    situation = f"{j - i}v{k - j + 1}"

                    # Ensure x >= y before recording the instance
                    if (j - i) >= (k - j + 1) and abs(target - curr_candle['open']) / curr_candle['open'] >= min_diff_percent:
                        confirm_candle = df.iloc[k + 1] if k + 1 < len(df) else breaking_candle
                        instance_id = f"{confirm_candle.name.strftime('%Y-%m-%d %H:%M:%S')}_{timeframe}_{situation}_long"
                        instances.append({
                            'instance_id': instance_id,
                            'situation': situation,
                            'timeframe': timeframe,
                            'confirm_date': confirm_candle.name,
                            'direction': 'long',
                            'target': target,
                            'entry': curr_candle['open'],
                            'fib0.5': fib0_5,
                            'fib0.0': fib0_0,
                            'fib-0.5': fibN0_5,
                            'fib-1.0': fibN1_0
                        })
                    break  # Stop after finding a valid breaking candle

            k += 1

        # Update the progress bar with the correct number of rows processed
        candle_pbar.update(j - i)
        i = j  # Move to the next candle

    candle_pbar.close()

    return pd.DataFrame(instances)

# Prompt for the input and output paths
input_folder = input(f"\n\rEnter the input folder path containing the timeframe CSV files (default: {default_input_path}): ") or default_input_path
output_folder = input(f"\n\rEnter the output folder path to save the instance CSV files (default: {default_output_path}): ") or default_output_path

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Get the total size of all files in the folder
total_size_mb = sum(os.path.getsize(os.path.join(input_folder, f)) for f in os.listdir(input_folder) if f.endswith('.csv')) / (1024 * 1024)
total_size_mb = round(total_size_mb, 2)

# Process each file in the input folder
files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
total_files = len(files)

# Create a progress bar for the file processing
file_pbar = tqdm(total=total_size_mb, desc=f'Processing file 0 of {total_files}', unit='MB')

multi_day_instances_file = os.path.join(output_folder, f'instances_XvY_multi-day.csv')

for idx, filename in enumerate(files, start=1):
    file_pbar.set_description(f'Processing file {idx} of {total_files}')
    filepath = os.path.join(input_folder, filename)
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    file_size_mb = round(file_size_mb, 2)
    df = pd.read_csv(filepath, parse_dates=['timestamp'])
    df.set_index('timestamp', inplace=True)

    timeframe = filename.split('_')[-1].replace('.csv', '')
    instances_df = find_instances(df, timeframe)

    if 'D' in timeframe and timeframe != '1D':
        if not os.path.exists(multi_day_instances_file):
            instances_df.to_csv(multi_day_instances_file, index=False)
        else:
            instances_df.to_csv(multi_day_instances_file, mode='a', header=False, index=False)
    else:
        output_filepath = os.path.join(output_folder, f'instances_XvY_{filename.split(".")[0]}.csv')
        instances_df.to_csv(output_filepath, index=False)
        tqdm.write(f'Instances saved to {output_filepath}')

    file_pbar.update(file_size_mb)

file_pbar.close()

print('Instance extraction complete!')
