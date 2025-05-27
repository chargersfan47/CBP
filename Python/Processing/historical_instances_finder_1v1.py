# This script processes historical candle data to find instances of 1v1 candle breaks.
# It calculates some Fibonacci retrace levels for further analysis and saves the results to CSV files.
# Below, you can set the default threshold for opportunity size and the default input/output paths.
# Optimized by x13pixels, thank you!

import pandas as pd
from datetime import datetime, timedelta
import os
from tqdm import tqdm

# Configurable variable for the minimum percent size of opportunity.  Set this to whatever you want.
min_diff_percent = 0.001  # 0.1%

# This is just some text.  This is not a setting.
situation = '1v1'

# Default paths (change these to your actual paths). You can put them here or enter them when prompted.
# Updated to match the new structure used by the download_binance_historical_data.py script:
default_input_path = os.path.join("..", "..", "Data", "SOLUSDT-BINANCE", "Candles")
default_output_path = os.path.join("..", "..", "Data", "SOLUSDT-BINANCE", "Instances-0.1", situation, "Unprocessed")

# **************************************************************************************************
# Function to find instances and calculate Fibonacci extension levels
def find_instances(df, timeframe, progress_callback=None):
    instances = []
    total_candles = len(df) - 2
    
    # Create a progress bar for the file processing with clean number formatting
    candle_pbar = tqdm(
        total=total_candles,
        desc=f'Processing candles for {timeframe}',
        unit='candle',
        leave=False,
        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
    )
    
    # Initialize progress tracking
    last_update = 0
    update_interval = max(100, total_candles // 20)  # Update at most 20 times per file

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
        
        # Calculate the duration of each candle by measuring time to next candle
        # For previous candle, measure time between prev_candle and curr_candle
        prev_candle_duration = (curr_candle.name - prev_candle.name).total_seconds()
        
        # For current candle, measure time between curr_candle and next_candle
        curr_candle_duration = (next_candle.name - curr_candle.name).total_seconds()

        # Check for bullish followed by bearish or vice versa with larger body
        # Also validate that the current candle's duration is <= previous candle's duration
        if (((prev_candle_close > prev_candle_open and curr_candle_close < curr_candle_open) or
            (prev_candle_close < prev_candle_open and curr_candle_close > curr_candle_open)) and 
            curr_body > prev_body and curr_candle_duration <= prev_candle_duration):

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

            # Check if the difference between entry and target meets minimum percentage requirement
            diff_percent = abs(target - entry) / entry
            if diff_percent >= min_diff_percent:
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
                    'fib-1.0': fib_neg1_0,
                    'move_size': diff_percent
                }
                instances.append(instance)

        # Only update progress bar every N candles to reduce overhead
        if (i + 1) % 100 == 0 or i == total_candles - 1:
            candle_pbar.update((i + 1) - candle_pbar.n)  # Update with the actual number of candles processed
            if progress_callback and (i + 1) % 1000 == 0:  # Only call progress callback every 1000 candles
                progress = (i + 1) / total_candles  # Current progress as a fraction (0 to 1)
                progress_callback(progress)
        elif i == 0:  # Always update on first candle to show progress has started
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

class ProgressUpdater:
    def __init__(self, file_pbar, processed_mb, file_size_mb):
        self.file_pbar = file_pbar
        self.processed_mb = round(processed_mb, 1)
        self.file_size_mb = round(file_size_mb, 1)
    
    def update_progress(self, progress):
        # Only update if we've made at least 0.1MB of progress to avoid too many updates
        current_mb = round(self.processed_mb + (progress * self.file_size_mb), 1)
        if current_mb > self.file_pbar.n + 0.1 or progress >= 1.0:
            self.file_pbar.n = current_mb
            self.file_pbar.refresh()

# Process each file in the input folder, sorted for consistent ordering
files = sorted([f for f in os.listdir(input_path) if f.endswith('.csv')])
total_files = len(files)

# Create a progress bar for the file processing with clean number formatting
file_pbar = tqdm(
    total=round(total_size_mb, 1),
    desc=f'Processing file 0 of {total_files}',
    unit='MB',
    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
)

# Track the total MB processed so far
processed_mb = 0.0

multi_day_instances_file = os.path.join(output_path, f'instances_{situation}_multi-day.csv')

for idx, filename in enumerate(files, start=1):
    filepath = os.path.join(input_path, filename)
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    
    # Create a progress updater instance with rounded values
    progress_updater = ProgressUpdater(file_pbar, round(processed_mb, 1), round(file_size_mb, 1))
    
    # Update the progress bar with current file info
    file_pbar.set_description(f'Processing file {idx} of {total_files}')
    
    df = pd.read_csv(filepath, parse_dates=['timestamp'])
    df.set_index('timestamp', inplace=True)
    
    timeframe = filename.split('_')[-1].replace('.csv', '')
    
    # Pass the update callback to find_instances
    instances_df = find_instances(df, timeframe, progress_updater.update_progress)
    
    # Save the results
    if 'D' in timeframe and timeframe != '1D':
        if not os.path.exists(multi_day_instances_file):
            instances_df.to_csv(multi_day_instances_file, index=False)
            tqdm.write(f'{multi_day_instances_file} created with {len(instances_df)} instances')
        else:
            instances_df.to_csv(multi_day_instances_file, mode='a', header=False, index=False)
            tqdm.write(f'{multi_day_instances_file} updated with {len(instances_df)} {timeframe} instances')
    else:
        output_filepath = os.path.join(output_path, f'instances_{situation}_{filename.split(".")[0]}.csv')
        instances_df.to_csv(output_filepath, index=False)
        tqdm.write(f'{output_filepath} created with {len(instances_df)} instances')
    
    # Update the total processed MB (round to 1 decimal place)
    processed_mb = round(processed_mb + file_size_mb, 1)
    file_pbar.n = processed_mb
    file_pbar.refresh()
    
    # Update the progress bar description to show completed files
    file_pbar.set_description(f'Processed {idx} of {total_files} files')

file_pbar.close()

print('Instance extraction complete!')
