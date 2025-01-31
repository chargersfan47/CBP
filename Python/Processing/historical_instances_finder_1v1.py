# This script processes historical candle data to find instances of 1v1 candle breaks.
# It calculates some Fibonacci retrace levels for further analysis and saves the results to CSV files.
# Below, you can set the default threshold for opportunity size and the default input/output paths.
# Optimized by x13pixels, thank you!

import pandas as pd
from datetime import datetime, timedelta
import os
from tqdm import tqdm

# Configurable variable for the minimum percent size of opportunity.  Set this to whatever you want.
min_diff_percent = 0.004  # 0.4%

# This is just some text.  This is not a setting.
situation = '1v1'

# Default paths (change these to your actual paths). You can put them here or enter them when prompted.
# The default input path I've put here is just a guess because I don't know what date you downloaded the data, so it guesses today:
default_input_path = os.path.join("..", "..", "Data", "SOLUSDT", "Candles", datetime.now().strftime('%Y-%m-%d'))
default_output_path = os.path.join("..", "..", "Data", "SOLUSDT", "Instances", situation, "Unprocessed")

# **************************************************************************************************
# Function to find instances and calculate Fibonacci extension levels
def find_instances(df, timeframe):
    instances = []

    # Create a progress bar for the file processing
    candle_pbar = tqdm(total=len(df) - 2, desc=f'Processing candles for {timeframe}', unit='candle', leave=False)

    prev_candle = df.iloc[0]
    curr_candle = df.iloc[1]
    next_candle = df.iloc[2]

    for i in range(1, len(df) - 2):  # Adjust loop to ensure there's a following candle
        prev_candle_open = prev_candle['open']
        prev_candle_close = prev_candle['close']
        curr_candle_open = curr_candle['open']
        curr_candle_close = curr_candle['close']

        prev_body = abs(prev_candle_close - prev_candle_open)
        curr_body = abs(curr_candle_close - curr_candle_open)

        # Check for bullish followed by bearish or vice versa with larger body
        if ((prev_candle_close > prev_candle_open and curr_candle_close < curr_candle_open) or
            (prev_candle_close < prev_candle_open and curr_candle_close > curr_candle_open)) and curr_body > prev_body:

            # Calculate Fibonacci extension levels
            if prev_candle_close > prev_candle_open:
                # Bullish followed by bearish
                direction = 'short'
                prev_candle_high = prev_candle['high']
                fib_base = prev_candle_high - prev_candle_open
                target = prev_candle_open - fib_base * 0.618
                entry = prev_candle_open
                fib0_5 = prev_candle_open + fib_base * 0.5
                fib0_0 = prev_candle_high
                fib_neg0_5 = prev_candle_open + fib_base * 1.5
                fib_neg1_0 = prev_candle_open + fib_base * 2
            else:
                # Bearish followed by bullish
                direction = 'long'
                prev_candle_low = prev_candle['low']
                fib_base = prev_candle_open - prev_candle_low
                target = prev_candle_open + fib_base * 0.618
                entry = prev_candle_open
                fib0_5 = prev_candle_open - fib_base * 0.5
                fib0_0 = prev_candle_low
                fib_neg0_5 = prev_candle_open - fib_base * 1.5
                fib_neg1_0 = prev_candle_open - fib_base * 2

            instance_id = f"{next_candle.name.strftime('%Y-%m-%d %H:%M:%S')}_{timeframe}_{situation}_{direction}"

            instance = {
                'instance_id': instance_id,
                'situation': situation,
                'timeframe': timeframe,
                'confirm_date': next_candle.name,
                'direction': direction,
                'target': target,
                'entry': entry,
                'fib0.5': fib0_5,
                'fib0.0': fib0_0,
                'fib-0.5': fib_neg0_5,
                'fib-1.0': fib_neg1_0
            }
            instances.append(instance)

        candle_pbar.update(1)

        prev_candle = curr_candle
        curr_candle = next_candle
        next_candle = df.iloc[i + 1]  # The following candle

    candle_pbar.close()
    return pd.DataFrame(instances)

# Prompt for the input and output paths
input_path = input(f"\n\rEnter the input folder path containing the timeframe CSV files (default: {default_input_path}): ") or default_input_path
output_path = input(f"\n\rEnter the output folder path to save the instance CSV files (default: {default_output_path}): ") or default_output_path

# Ensure the output folder exists
if not os.path.exists(output_path):
    os.makedirs(output_path)

# Calculate the total size of the files in MB
total_size_mb = sum(os.path.getsize(os.path.join(input_path, f)) for f in os.listdir(input_path) if f.endswith('.csv')) / (1024 * 1024)
total_size_mb = round(total_size_mb, 2)

# Process each file in the input folder
files = [f for f in os.listdir(input_path) if f.endswith('.csv')]
total_files = len(files)

# Create a progress bar for the file processing
file_pbar = tqdm(total=total_size_mb, desc=f'Processing file 0 of {total_files}', unit='MB')

multi_day_instances_file = os.path.join(output_path, f'instances_{situation}_multi-day.csv')

for idx, filename in enumerate(files, start=1):
    file_pbar.set_description(f'Processing file {idx} of {total_files}')
    filepath = os.path.join(input_path, filename)
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
        output_filepath = os.path.join(output_path, f'instances_{situation}_{filename.split(".")[0]}.csv')
        instances_df.to_csv(output_filepath, index=False)
        tqdm.write(f'Instances saved to {output_filepath}')

    file_pbar.update(file_size_mb)

file_pbar.close()

print('Instance extraction complete!')
