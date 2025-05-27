# This script processes historical candle data to find instances of XvY candle breaks.
# It calculates some Fibonacci retrace levels for further analysis and saves the results to CSV files.
# Below, you can set the default threshold for opportunity size and the default input/output paths.

import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime

# Configurable variable for the minimum percent size of opportunity.  Set this to whatever you want.
min_diff_percent = 0.001  # 0.1%

# Configurable variable for the maximum number of breaking candles.
#  Set to 0 for no limit.
#  Set to 1 to generate Xv1 data.
max_y = 1  # Maximum number of breaking candles

# Default paths (change these to your actual paths). You can put them here or enter them when prompted.
# Updated to match the new structure used by the download_binance_historical_data.py script:
default_input_path = os.path.join("..", "..", "Data", "SOLUSDT-BINANCE", "Candles")
default_output_path = os.path.join("..", "..", "Data", "SOLUSDT-BINANCE", "Instances-Xv1-0.1", "XvY", "Unprocessed")

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

# **************************************************************************************************
# Function to find instances and calculate Fibonacci extension levels
def find_instances(df, timeframe, progress_callback=None):
    instances = []
    total_candles = len(df) - 1
    
    # Create a progress bar for the file processing
    candle_pbar = tqdm(
        total=total_candles,
        desc=f'Processing candles for {timeframe}',
        unit='candle',
        leave=False,
        bar_format='{desc} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
    )
    
    # Initialize progress tracking
    last_update = 0
    update_interval = max(100, total_candles // 20)  # Update at most 20 times per file

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
                    diff_percent = abs(target - curr_candle['open']) / curr_candle['open']
                    if (j - i) >= (k - j + 1) and diff_percent >= min_diff_percent:
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
                            'fib-1.0': fibN1_0,
                            'move_size': diff_percent
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
                    diff_percent = abs(target - curr_candle['open']) / curr_candle['open']
                    if (j - i) >= (k - j + 1) and diff_percent >= min_diff_percent:
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
                            'fib-1.0': fibN1_0,
                            'move_size': diff_percent
                        })
                    break  # Stop after finding a valid breaking candle

            k += 1

        # Update the progress bar with the correct number of rows processed
        rows_processed = j - i
        candle_pbar.update(rows_processed)
        
        # Update progress callback if provided
        if progress_callback and (i % update_interval == 0 or i == total_candles - 1):
            progress = i / total_candles
            progress_callback(progress)
            
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

# Process each file in the input folder, sorted for consistent ordering
files = sorted([f for f in os.listdir(input_folder) if f.endswith('.csv')])
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

multi_day_instances_file = os.path.join(output_folder, 'instances_XvY_multi-day.csv')

for idx, filename in enumerate(files, 1):
    filepath = os.path.join(input_folder, filename)
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    
    # Create a progress updater instance with rounded values
    progress_updater = ProgressUpdater(file_pbar, processed_mb, file_size_mb)
    
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
        output_filepath = os.path.join(output_folder, f'instances_XvY_{filename.split(".")[0]}.csv')
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
