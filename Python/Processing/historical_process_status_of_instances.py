# This script processes instances using the 1m candle data as a price reference.
# It calculates whether specific price targets or Fibonacci levels are reached and updates the instance's status accordingly.
# The processed files are saved to an output folder, and the original files can be optionally deleted.
# This "single core" version of the file will do all processing in one thread, which may take a long time.
# For faster processing, use the "multicore" version of the file.

import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime, timedelta
import subprocess
import time
import re

# Default paths (change these to your actual paths). You can put them here or enter them when prompted.
# Updated to match the new structure used by the download_binance_historical_data.py script:
price_data_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Candles')
instances_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Unprocessed')
default_output_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Processed', 'CompleteSet')

# Flag to control whether the old file is deleted after saving the output
delete_unprocessed_when_done = True  

# Dictionary to store loaded timeframe data
timeframe_data = {}

# Dictionary to cache 1-second sample data
sample_data_cache = {}

# Function to determine when to shift timeframes
def can_shift_up(timestamp, higher_timeframe):
    """Determine if we can shift up to a higher timeframe based on timestamp"""
    if higher_timeframe == '30m':
        # For 30m, we can shift up if we're at the start of a 30-minute interval (00 or 30)
        return timestamp.minute in [0, 30] and timestamp.second == 0
    elif higher_timeframe == '1D':
        # For 1D, we can shift up if we're at the start of a day (00:00)
        return timestamp.hour == 0 and timestamp.minute == 0 and timestamp.second == 0
    return False

# Function to find the next timeframe shift point
def next_shift_point(timestamp, target_timeframe):
    """Find the next timestamp where we can shift to the target timeframe"""
    if target_timeframe == '30m':
        if timestamp.minute < 30:
            # Next 30-minute mark is at the same hour, minute 30
            return timestamp.replace(minute=30, second=0, microsecond=0)
        else:
            # Next 30-minute mark is at the next hour, minute 0
            return (timestamp.replace(second=0, microsecond=0) + timedelta(hours=1)).replace(minute=0)
    elif target_timeframe == '1D':
        # Next day start
        next_day = timestamp.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return next_day
    return timestamp  # Default - no shift

# Function to check if 1s sample data exists for a specific date and time
def check_for_1s_sample(timestamp, symbol, exchange):
    """
    Check if 1-second sample data exists for a specific minute.
    
    Args:
        timestamp: The timestamp to check (pd.Timestamp)
        symbol: Trading symbol
        exchange: Exchange name
        
    Returns:
        str: Path to the sample file if it exists, None otherwise
    """
    # Format the date and time for the filename
    date_str = timestamp.strftime('%Y%m%d')
    time_str = timestamp.strftime('%H%M')
    
    # Generate expected filename
    filename = f"{symbol}-{exchange.upper()}_sample_{date_str}_{time_str}_1s.csv"
    
    # Generate expected path - Sample files are in the Candles/Samples folder
    sample_path = os.path.join(
        price_data_folder,  # This is already pointing to the Candles folder
        "Samples",
        filename
    )
    
    if os.path.exists(sample_path):
        return sample_path
    
    return None

# Function to download 1s sample data for a specific minute
def download_1s_sample(timestamp, symbol, exchange):
    """
    Download 1-second sample data for a specific minute by calling the downloader script.
    Will retry indefinitely until successful.
    
    Args:
        timestamp: The timestamp to download (pd.Timestamp)
        symbol: Trading symbol
        exchange: Exchange name
        
    Returns:
        str: Path to the downloaded sample file if successful
    """
    # Format the date for command line argument
    date_str = timestamp.strftime('%Y-%m-%d')
    time_str = timestamp.strftime('%H:%M')
    
    # Get the path to the downloader script
    downloader_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'download_binance_historical_data.py')
    
    # Build the command
    cmd = [
        'python', 
        downloader_script,
        '--symbol', symbol,
        '--start-date', date_str,
        '--sample', exchange.lower(), time_str
    ]
    
    tqdm.write(f"\nDownloading 1s data for {symbol} on {exchange} at {date_str} {time_str}...")
    
    # Keep trying until we succeed
    attempt = 1
    while True:
        try:
            # Execute the command
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Check if the file was created
            sample_path = check_for_1s_sample(timestamp, symbol, exchange)
            if sample_path:
                tqdm.write(f"Successfully downloaded 1s sample data on attempt {attempt}")
                return sample_path
            
            tqdm.write(f"Download completed but file not found, retrying...")
        except subprocess.CalledProcessError as e:
            tqdm.write(f"Error executing downloader script (attempt {attempt}): {str(e)}")
        
        # Determine wait time - start with 5 seconds, then use 30 seconds after the first attempt
        wait_time = 5 if attempt == 1 else 30
        tqdm.write(f"Waiting {wait_time} seconds before retry...")
        time.sleep(wait_time)
        attempt += 1

# Function to load 1s sample data
def load_1s_sample_data(file_path):
    """
    Load 1-second sample data from a CSV file.
    
    Args:
        file_path: Path to the 1-second sample data file
        
    Returns:
        pd.DataFrame: The loaded data, or None if loading failed
    """
    try:
        # Check if already in cache
        if file_path in sample_data_cache:
            return sample_data_cache[file_path]
        
        # Load the data with UTC timezone 
        df = pd.read_csv(file_path, parse_dates=['timestamp'])
        
        # Ensure the timestamp is localized to UTC to match the 1m data
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        
        # Set the timestamp as index
        df.set_index('timestamp', inplace=True)
        
        # Cache the data
        sample_data_cache[file_path] = df
        
        return df
    except Exception as e:
        tqdm.write(f"Error loading 1s sample data: {str(e)}")
        return None

# Function to analyze 1s data for accurate activation and completion
def analyze_1s_data(activation_price, completion_price, direction, df_1s):
    """
    Analyze 1-second data to determine exact activation and completion times.
    Ensures that completion only happens after activation.
    
    Args:
        activation_price: Price at which the instance is activated
        completion_price: Target price for completion
        direction: Trading direction ('long' or 'short')
        df_1s: DataFrame with 1-second data
        
    Returns:
        tuple: (activation_timestamp, completion_timestamp) or (activation_timestamp, None)
    """
    if direction == 'long':
        entry_direction = 'down'  # Price needs to go down to reach entry
        target_direction = 'up'   # Price needs to go up to reach target
    else:  # 'short'
        entry_direction = 'up'    # Price needs to go up to reach entry
        target_direction = 'down' # Price needs to go down to reach target
    
    # Determine price columns based on direction
    entry_col = 'low' if entry_direction == 'down' else 'high'
    target_col = 'high' if target_direction == 'up' else 'low'
    
    activation_timestamp = None
    completion_timestamp = None
    
    tqdm.write(f"Analyzing 1s data: Looking for {entry_direction} to {activation_price} then {target_direction} to {completion_price}")
    
    # Find activation point first
    for timestamp, candle in df_1s.iterrows():
        if (entry_direction == 'down' and candle[entry_col] <= activation_price) or \
           (entry_direction == 'up' and candle[entry_col] >= activation_price):
            activation_timestamp = timestamp
            tqdm.write(f"Found activation at {timestamp} with price {candle[entry_col]}")
            break
    
    # If activation found, look for completion AFTER activation
    if activation_timestamp is not None:
        # Only check data points after activation
        post_activation_data = df_1s[df_1s.index > activation_timestamp]
        
        if not post_activation_data.empty:
            tqdm.write(f"Checking {len(post_activation_data)} candles after activation for completion")
            
            for timestamp, candle in post_activation_data.iterrows():
                if (target_direction == 'up' and candle[target_col] >= completion_price) or \
                   (target_direction == 'down' and candle[target_col] <= completion_price):
                    completion_timestamp = timestamp
                    tqdm.write(f"Found completion at {timestamp} with price {candle[target_col]}")
                    break
            
            if completion_timestamp is None:
                tqdm.write(f"No completion found after activation within this 1-minute period")
        else:
            tqdm.write(f"No data available after activation timestamp")
    else:
        tqdm.write(f"Activation point not found in 1s data")
    
    return activation_timestamp, completion_timestamp

# Function to check if a price target has been reached within a candle.
def check_price_target(candle, target_price, direction, price_col):
    """
    Check if a price target has been reached within a candle.
    
    Args:
        candle: The price candle data
        target_price: Price target to find
        direction: 'up' or 'down' - the direction price needs to move to reach target
        price_col: Column to check ('high' for 'up', 'low' for 'down')
        
    Returns:
        Boolean: True if target is reached, False otherwise
    """
    if direction == 'up':
        return candle[price_col] >= target_price
    else:  # direction == 'down'
        return candle[price_col] <= target_price

def track_extreme_price(candle, direction, extreme_price, opposite_col):
    """
    Check if a new extreme price is found in the candle.
    
    Args:
        candle: The price candle data
        direction: 'up' or 'down' - the direction price needs to move to reach target
        extreme_price: Current extreme price
        opposite_col: Column to check ('low' for 'up', 'high' for 'down')
        
    Returns:
        Boolean: True if new extreme found, False otherwise
        Float: New extreme price value (or None if no new extreme)
    """
    if extreme_price is None:
        return True, candle[opposite_col]
    
    if direction == 'up' and candle[opposite_col] < extreme_price:
        return True, candle[opposite_col]
    elif direction == 'down' and candle[opposite_col] > extreme_price:
        return True, candle[opposite_col]
    
    return False, extreme_price

def search_1m_timeframe(target_price, direction, start_date, end_date, next_gear_shift, skip_extreme_checking):
    """
    Search for target price in 1m timeframe data.
    
    Args:
        target_price: Price target to find
        direction: 'up' or 'down' - direction price needs to move
        start_date: Starting date
        end_date: Ending date
        next_gear_shift: Next timestamp to shift timeframe
        skip_extreme_checking: Whether to skip tracking extremes
        
    Returns:
        Tuple of (target_date, extreme_price, extreme_date, current_date)
    """
    # Set price columns based on direction
    price_col = 'high' if direction == 'up' else 'low'
    opposite_col = 'low' if direction == 'up' else 'high'
    
    target_date = None
    extreme_price = None
    extreme_date = None
    current_date = start_date
    
    # Search using 1m timeframe until we can shift up or find the target
    while current_date <= end_date and target_date is None:
        if current_date >= next_gear_shift:
            break  # Time to shift up
        
        # Get the current candle (if it exists in data)
        if current_date in timeframe_data['1m'].index:
            candle = timeframe_data['1m'].loc[current_date]
            
            # Check if target is reached
            if check_price_target(candle, target_price, direction, price_col):
                target_date = current_date
                break
            
            # Track extreme price if not skipping
            if not skip_extreme_checking:
                new_extreme_found, new_extreme_price = track_extreme_price(candle, direction, extreme_price, opposite_col)
                if new_extreme_found:
                    extreme_price = new_extreme_price
                    extreme_date = current_date
        
        # Move to next minute
        current_date = current_date + timedelta(minutes=1)
        
        # Check if we've reached the end of our data
        if current_date > timeframe_data['1m'].index[-1]:
            break
    
    return target_date, extreme_price, extreme_date, current_date

def search_in_higher_timeframe(timeframe, target_price, direction, current_date, end_date, skip_extreme_checking):
    """
    Search for target price in a higher timeframe (30m or 1D).
    
    Args:
        timeframe: The timeframe to search in ('30m' or '1D')
        target_price: Price target to find
        direction: 'up' or 'down' - the direction price needs to move
        current_date: Starting date
        end_date: Ending date
        skip_extreme_checking: Whether to skip tracking extremes
        
    Returns:
        Tuple of (target_date, extreme_price, extreme_date, next_gear_shift)
    """
    price_col = 'high' if direction == 'up' else 'low'
    opposite_col = 'low' if direction == 'up' else 'high'
    
    target_date = None
    extreme_price = None
    extreme_date = None
    next_gear_shift = None
    
    # Get filtered data for this timeframe
    data_tf = timeframe_data[timeframe]
    data_tf_filtered = data_tf[(data_tf.index >= current_date) & (data_tf.index <= end_date)]
    
    # Set candle duration based on timeframe
    if timeframe == '30m':
        candle_duration = timedelta(minutes=30)
        higher_tf = '1D'
    else:  # timeframe == '1D'
        candle_duration = timedelta(days=1)
        higher_tf = None
    
    # Search through candles in this timeframe
    for timestamp, candle in data_tf_filtered.iterrows():
        # Check if target is reached in this candle
        if check_price_target(candle, target_price, direction, price_col):
            # Target reached in this candle - shift down to lower timeframe for precision
            end_of_candle = timestamp + candle_duration
            
            # Find the lower timeframe to shift down to
            if timeframe == '30m':
                lower_tf = '1m'
            else:  # timeframe == '1D'
                lower_tf = '30m'
            
            # Get lower timeframe data for this candle
            data_lower_in_candle = timeframe_data[lower_tf][
                (timeframe_data[lower_tf].index >= timestamp) & 
                (timeframe_data[lower_tf].index < end_of_candle)
            ]
            
            # If we're in 1D timeframe and shifting to 30m
            if timeframe == '1D' and lower_tf == '30m':
                found_in_lower_tf = False
                target_lower_tf = None
                
                # Find which lower timeframe candle reached the target
                for ts_lower, candle_lower in data_lower_in_candle.iterrows():
                    if check_price_target(candle_lower, target_price, direction, price_col):
                        found_in_lower_tf = True
                        target_lower_tf = ts_lower
                        break
                
                if found_in_lower_tf:
                    # Now shift down to 1m for precision
                    end_of_lower = target_lower_tf + timedelta(minutes=30)
                    # Get 1m data for this 30m candle
                    data_1m_in_lower = timeframe_data['1m'][
                        (timeframe_data['1m'].index >= target_lower_tf) & 
                        (timeframe_data['1m'].index < end_of_lower)
                    ]
                    
                    # Find exact 1m candle where target was reached
                    for ts_1m, candle_1m in data_1m_in_lower.iterrows():
                        if check_price_target(candle_1m, target_price, direction, price_col):
                            target_date = ts_1m
                            break
            else:
                # For 30m directly to 1m
                for ts_lower, candle_lower in data_lower_in_candle.iterrows():
                    if check_price_target(candle_lower, target_price, direction, price_col):
                        target_date = ts_lower
                        break
            
            if target_date is not None:
                break
        
        # Track extreme price if not skipping
        if not skip_extreme_checking:
            new_extreme_found, new_extreme_price = track_extreme_price(candle, direction, extreme_price, opposite_col)
            if new_extreme_found:
                # Potential new extreme in this candle - shift down to lower timeframe for precision
                end_of_candle = timestamp + candle_duration
                
                # Find the lower timeframe to shift down to
                if timeframe == '30m':
                    lower_tf = '1m'
                else:  # timeframe == '1D'
                    lower_tf = '30m'
                
                # Get lower timeframe data for this candle
                data_lower_in_candle = timeframe_data[lower_tf][
                    (timeframe_data[lower_tf].index >= timestamp) & 
                    (timeframe_data[lower_tf].index < end_of_candle)
                ]
                
                # If we're in 1D timeframe and need to go to 30m first
                if timeframe == '1D' and lower_tf == '30m':
                    extreme_lower_tf = None
                    extreme_lower_tf_ts = None
                    extreme_lower_tf_val = new_extreme_price
                    
                    # Find which lower timeframe candle has the extreme
                    for ts_lower, candle_lower in data_lower_in_candle.iterrows():
                        new_extreme_found_lower, new_extreme_lower = track_extreme_price(
                            candle_lower, direction, extreme_lower_tf_val, opposite_col)
                        
                        if new_extreme_found_lower:
                            extreme_lower_tf_val = new_extreme_lower
                            extreme_lower_tf_ts = ts_lower
                    
                    if extreme_lower_tf_ts is not None:
                        # Shift down to 1m for precision
                        end_of_lower = extreme_lower_tf_ts + timedelta(minutes=30)
                        # Get 1m data for this lower timeframe candle
                        data_1m_in_lower = timeframe_data['1m'][
                            (timeframe_data['1m'].index >= extreme_lower_tf_ts) & 
                            (timeframe_data['1m'].index < end_of_lower)
                        ]
                        
                        # Find exact 1m candle with extreme
                        for ts_1m, candle_1m in data_1m_in_lower.iterrows():
                            new_extreme_found_1m, new_extreme_1m = track_extreme_price(
                                candle_1m, direction, extreme_price, opposite_col)
                            
                            if new_extreme_found_1m:
                                extreme_price = new_extreme_1m
                                extreme_date = ts_1m
                else:
                    # For 30m directly to 1m
                    for ts_lower, candle_lower in data_lower_in_candle.iterrows():
                        new_extreme_found_lower, new_extreme_lower = track_extreme_price(
                            candle_lower, direction, extreme_price, opposite_col)
                        
                        if new_extreme_found_lower:
                            extreme_price = new_extreme_lower
                            extreme_date = ts_lower
        
        # Check if we can shift to an even higher timeframe (for 30m to 1D)
        if higher_tf and target_date is None and can_shift_up(timestamp, higher_tf):
            next_gear_shift = timestamp
            break  # Exit current timeframe loop to shift up
    
    return target_date, extreme_price, extreme_date, next_gear_shift

def find_target_date(target_price, direction, start_date, end_date=None, skip_extreme_checking=True):
    """
    Find when a target price is reached using a multi-timeframe approach.
    
    Args:
        target_price: Price target to find
        direction: 'up' or 'down' - the direction price needs to move to reach target
        start_date: Starting date to begin search
        end_date: Optional end date to stop search (default: end of data)
        skip_extreme_checking: Whether to skip tracking extreme prices (default: True)
        
    Returns:
        A tuple containing:
        - target_date: Date when target price was reached (or None)
        - extreme_price: Max or min price seen during search (based on opposite of direction)
        - extreme_date: Date when extreme price occurred
    """
    if end_date is None:
        end_date = timeframe_data['1m'].index[-1]
    
    # Ensure start_date is within data range
    if start_date > timeframe_data['1m'].index[-1] or start_date < timeframe_data['1m'].index[0]:
        return None, None, None
    
    # Find the next points to shift timeframe
    next_gear_shift = next_shift_point(start_date, '30m')
    
    # Step 1: Search using 1m timeframe
    target_date, extreme_price, extreme_date, current_date = search_1m_timeframe(
        target_price, direction, start_date, end_date, next_gear_shift, skip_extreme_checking
    )
    
    # If target found in 1m timeframe, return results
    if target_date is not None:
        return target_date, extreme_price, extreme_date
    
    # Step 2: If we can shift to 30m, do so
    if current_date <= end_date:
        target_date, extreme_price, extreme_date, next_gear_shift = search_in_higher_timeframe(
            '30m', target_price, direction, current_date, end_date, skip_extreme_checking
        )
        
        # If target found in 30m, return results
        if target_date is not None:
            return target_date, extreme_price, extreme_date
        
        # Step 3: If we can shift to 1D, do so
        if next_gear_shift is not None and next_gear_shift <= end_date and can_shift_up(next_gear_shift, '1D'):
            target_date, extreme_price, extreme_date, _ = search_in_higher_timeframe(
                '1D', target_price, direction, next_gear_shift, end_date, skip_extreme_checking
            )
    
    # Return the results
    return target_date, extreme_price, extreme_date

# Subroutine to process an instance 
def process_instance(instance, idx, instance_df):
    # Determine the direction for different targets
    if instance['direction'] == 'long':
        entry_direction = 'down'  # Price needs to go down to reach entry
        target_direction = 'up'   # Price needs to go up to reach target
    else:  # 'short'
        entry_direction = 'up'    # Price needs to go up to reach entry
        target_direction = 'down' # Price needs to go down to reach target

    # Initialize variables
    active_date = None
    completed_date = None
    max_drawdown = None
    max_drawdown_date = None
    used_1s_data = False  # Flag to track if we used 1-second data for activation

    # First pass: Find active date (when entry price is reached)
    active_date, _, _ = find_target_date(instance['entry'], entry_direction, instance['confirm_date'])
    
    # Skip if the instance never became active
    if active_date is None:
        return
    
    # Mark as active
    instance_df.at[idx, 'Active Date'] = active_date
    instance_df.at[idx, 'Status'] = 'Active'
    
    # Second pass: Find completed date (when target price is reached)
    # First try without offset to see if it's in the same minute
    temp_completed_date, max_drawdown, max_drawdown_date = find_target_date(
        instance['target'], target_direction, active_date, skip_extreme_checking=False)
    
    # Check if activation and completion are in the same minute
    if temp_completed_date is not None and temp_completed_date.replace(second=0, microsecond=0) == active_date.replace(second=0, microsecond=0):
        tqdm.write(f"\nFound activation and completion in the same minute: {active_date}")
        tqdm.write(f"Instance {idx}: {instance['entry']} to {instance['target']} ({instance['direction']})")
        
        # Only extract symbol and exchange when we need to download 1s data
        filename = next(iter(timeframe_files.values()))
        tqdm.write(f"Processing filename: {filename}")
        
        # Handle complex filename patterns like "instances_1v1_SOLUSDT_binance_1m.csv"
        # General pattern: anything_symbol_exchange_timeframe.csv
        
        try:
            # Split by underscore and remove the extension from the last part
            parts = filename.split('_')
            if len(parts) >= 3:  # Need at least 3 parts: symbol_exchange_timeframe.csv
                # Timeframe is the last part before .csv
                timeframe_part = parts[-1].split('.')[0]
                
                # Symbol is typically two parts before timeframe
                symbol = parts[-3]
                
                # Exchange is typically one part before timeframe
                exchange = parts[-2]
                
                tqdm.write(f"Extracted symbol: {symbol}, exchange: {exchange} from filename: {filename}")
                
                # Try to get 1s data for this minute
                sample_path = check_for_1s_sample(active_date, symbol, exchange)
                
                # If 1s data doesn't exist, try to download it
                if not sample_path:
                    sample_path = download_1s_sample(active_date, symbol, exchange)
                
                # If we have 1s data, analyze it
                if sample_path:
                    df_1s = load_1s_sample_data(sample_path)
                    if df_1s is not None:
                        tqdm.write(f"Analyzing 1s data to determine exact activation and completion times...")
                        precise_active_date, precise_completed_date = analyze_1s_data(
                            instance['entry'], instance['target'], instance['direction'], df_1s)
                        
                        if precise_active_date is not None:
                            # Update the activation date to the precise time
                            active_date = precise_active_date
                            instance_df.at[idx, 'Active Date'] = active_date
                            used_1s_data = True  # Flag that we used 1s data for accurate timing
                            
                            if precise_completed_date is not None:
                                # We found a precise completion date in the 1s data
                                completed_date = precise_completed_date
                                tqdm.write(f"Using 1s data, determined completion at {completed_date}")
                            else:
                                # No completion found in the 1s data, continue as normal
                                tqdm.write(f"No completion found in 1s data, checking next minute...")
                                # We have accurate activation time from 1s data, no need for offset
                                completed_date, _, _ = find_target_date(
                                    instance['target'], target_direction, active_date, skip_extreme_checking=False)
                        else:
                            tqdm.write(f"Could not determine precise activation time from 1s data, using original")
                            # Without precise 1s data, use original activation time
                            completed_date, _, _ = find_target_date(
                                instance['target'], target_direction, active_date, skip_extreme_checking=False)
                    else:
                        tqdm.write(f"Could not load 1s data, using standard approach")
                        # Without 1s data, use standard approach
                        completed_date, _, _ = find_target_date(
                            instance['target'], target_direction, active_date, skip_extreme_checking=False)
                else:
                    tqdm.write(f"Could not get 1s data, using standard approach")
                    # Without 1s data, use standard approach
                    completed_date, _, _ = find_target_date(
                        instance['target'], target_direction, active_date, skip_extreme_checking=False)
            else:
                tqdm.write(f"Filename format not recognized: {filename}. Using standard approach.")
                # Without 1s data, use standard approach
                completed_date, _, _ = find_target_date(
                    instance['target'], target_direction, active_date, skip_extreme_checking=False)
        except Exception as e:
            tqdm.write(f"Error extracting symbol and exchange: {str(e)}. Using standard approach.")
            # Without 1s data, use standard approach
            completed_date, _, _ = find_target_date(
                instance['target'], target_direction, active_date, skip_extreme_checking=False)
    else:
        # Normal case - activation and completion are not in the same minute
        # or completion wasn't found right away
        completed_date = temp_completed_date

    # If instance completed, record it
    if completed_date is not None:
        instance_df.at[idx, 'Completed Date'] = completed_date
        instance_df.at[idx, 'Status'] = 'Completed'

    # Record max drawdown - ALWAYS record this even if we don't find a completed date
    if max_drawdown is not None:
        instance_df.at[idx, 'MaxDrawdown'] = max_drawdown
        instance_df.at[idx, 'MaxDrawdown Date'] = max_drawdown_date
        
        # Calculate MaxFib
        fib1_price = instance['entry']  # This is our 1.0 level
        fib0_price = instance['fib0.0']  # This is our 0.0 level
        
        if fib1_price != fib0_price:  # Avoid division by zero
            max_fib = (max_drawdown - fib0_price) / (fib1_price - fib0_price)
            instance_df.at[idx, 'MaxFib'] = max_fib

    # Define fib levels in order
    fib_levels = [
        ('fib0.5', 'Reached0.5', 'DateReached0.5'),
        ('fib0.0', 'Reached0.0', 'DateReached0.0'),
        ('fib-0.5', 'Reached-0.5', 'DateReached-0.5'),
        ('fib-1.0', 'Reached-1.0', 'DateReached-1.0')
    ]

    # Set up variables for tracking fib levels
    reached_fibs = {
        'Reached0.5': 0,
        'Reached0.0': 0,
        'Reached-0.5': 0,
        'Reached-1.0': 0
    }
    fib_dates = {
        'DateReached0.5': None,
        'DateReached0.0': None, 
        'DateReached-0.5': None,
        'DateReached-1.0': None
    }

    # Third pass: Find fib levels - but don't use sequential time checks
    end_date_for_fibs = max_drawdown_date if max_drawdown_date else (
        completed_date if completed_date else timeframe_data['1m'].index[-1]
    )

    # Track previous fib date to start each next fib level search from
    prev_search_date = active_date
    
    # Check each Fibonacci level in sequence
    for fib_key, reach_key, date_key in fib_levels:
        # Only proceed to check this fib level if:
        # 1. It's the first level (fib0.5), OR
        # 2. The previous level was found (based on the pattern: 0.5 -> 0.0 -> -0.5 -> -1.0)
        should_check = True
        
        if fib_key == 'fib0.0' and reached_fibs.get('Reached0.5', 0) == 0:
            # Don't check fib0.0 if fib0.5 wasn't reached
            should_check = False
        elif fib_key == 'fib-0.5' and reached_fibs.get('Reached0.0', 0) == 0:
            # Don't check fib-0.5 if fib0.0 wasn't reached
            should_check = False
        elif fib_key == 'fib-1.0' and reached_fibs.get('Reached-0.5', 0) == 0:
            # Don't check fib-1.0 if fib-0.5 wasn't reached
            should_check = False
        
        if should_check:
            # First, check if this fib level was reached in the same 1-minute candle as activation
            # Get the 1m candle that contains the active_date
            active_minute = active_date.replace(second=0, microsecond=0)
            if active_minute in timeframe_data['1m'].index:
                active_candle = timeframe_data['1m'].loc[active_minute]
                
                # Check if fib level was reached in this candle
                fib_reached_in_candle = False
                if entry_direction == 'down':  # Long position, price went down
                    # Check if low price in candle is <= fib level
                    if active_candle['low'] <= instance[fib_key]:
                        fib_reached_in_candle = True
                        fib_date = active_minute
                else:  # entry_direction == 'up', Short position, price went up
                    # Check if high price in candle is >= fib level
                    if active_candle['high'] >= instance[fib_key]:
                        fib_reached_in_candle = True
                        fib_date = active_minute
                
                if fib_reached_in_candle:
                    reached_fibs[reach_key] = 1
                    fib_dates[date_key] = fib_date
                    # Update the previous search date for the next fib level
                    prev_search_date = fib_date
                    # Continue to the next fib level
                    continue
            
            # If not found in the same candle, search for this fib level after activation
            fib_date, _, _ = find_target_date(
                instance[fib_key], entry_direction, prev_search_date, end_date_for_fibs)
            
            if fib_date is not None:
                reached_fibs[reach_key] = 1
                fib_dates[date_key] = fib_date
                # Update the previous search date for the next fib level
                prev_search_date = fib_date
    
    # Update instance with fib dates and reached status
    for reach_key, value in reached_fibs.items():
        instance_df.at[idx, reach_key] = value

    for date_key, value in fib_dates.items():
        if value is not None:
            instance_df.at[idx, date_key] = value

    # Double-check fib levels against MaxFib for consistency
    if instance_df.at[idx, 'MaxFib'] is not None:
        max_fib_value = instance_df.at[idx, 'MaxFib']
        
        # Verify and correct reached fib levels based on MaxFib
        if max_fib_value <= 0.5 and instance_df.at[idx, 'Reached0.5'] == 0:
            instance_df.at[idx, 'Reached0.5'] = 1
        
        if max_fib_value <= 0.0 and instance_df.at[idx, 'Reached0.0'] == 0:
            instance_df.at[idx, 'Reached0.0'] = 1
            
        if max_fib_value <= -0.5 and instance_df.at[idx, 'Reached-0.5'] == 0:
            instance_df.at[idx, 'Reached-0.5'] = 1
            
        if max_fib_value <= -1.0 and instance_df.at[idx, 'Reached-1.0'] == 0:
            instance_df.at[idx, 'Reached-1.0'] = 1

# Function to update the status and active date
def update_status(instance_df, timeframe):
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
        # This code will skip instances that already have a completed date. 
        #if pd.notna(instance['Completed Date']):
        #    instance_pbar.update(1)
        #    continue  # Skip to next instance if this one is already completed
        
        # Process this instance
        process_instance(instance, idx, instance_df)
        
        instance_pbar.update(1)

    instance_pbar.close()
    return instance_df

# Prompt for the instances folder, the candle data folder, and the output folder
instances_folder = input(f"\n\rEnter the folder path containing the instance CSV files (default: {instances_folder}): ") or instances_folder
price_data_folder = input(f"\n\rEnter the folder path containing the price data CSV files (default: {price_data_folder}): ") or price_data_folder
output_folder = input(f"\n\rEnter the output folder path to save the updated instance CSV files (default: {default_output_folder}): ") or default_output_folder

# Create the output directory if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Load the timeframe data
tqdm.write("Loading price data for multiple timeframes...")
timeframe_files = {
    '1m': next(f for f in os.listdir(price_data_folder) if f.endswith('_1m.csv')),
    '30m': next(f for f in os.listdir(price_data_folder) if f.endswith('_30m.csv')),
    '1D': next(f for f in os.listdir(price_data_folder) if f.endswith('_1D.csv'))
}

for timeframe, filename in timeframe_files.items():
    filepath = os.path.join(price_data_folder, filename)
    tqdm.write(f"Loading {timeframe} data from {filepath}...")
    timeframe_data[timeframe] = pd.read_csv(filepath, parse_dates=['timestamp'])
    timeframe_data[timeframe].set_index('timestamp', inplace=True)
    tqdm.write(f"Loaded {len(timeframe_data[timeframe])} {timeframe} candles.")

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
  
    # Extract the timeframe from the filename
    timeframe = filename.split('_')[-1].split('.')[0]
    
    # Read the instances CSV file
    instances = pd.read_csv(filepath, parse_dates=['confirm_date'])
    
    # Update the status and active date for instances
    instances = update_status(instances, timeframe)
    
    # Save the updated file
    output_filepath = os.path.join(output_folder, filename)
    instances.to_csv(output_filepath, index=False)
    
    # Optionally delete the unprocessed file
    if delete_unprocessed_when_done:
        os.remove(filepath)
        
    file_pbar.update(file_size_mb)

file_pbar.close()
tqdm.write(f"\n\rProcessing complete. Updated files saved to {output_folder}")
