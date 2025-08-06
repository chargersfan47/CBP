#!/usr/bin/env python
# This script processes historical candle data to find instances of 1v1 and 1v1+1 candle patterns.
# It is optimized to only process new data since the last run and detect if an instance type changes.
# Based on historical_instances_finder_1v1.py and historical_instances_finder_1v1+1.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime
import os
import re
import time
import argparse
from tqdm import tqdm

# Configurable variables (note: these are multiplied by 100 in the calculation)
min_diff_percent = 0.1     # 0.1% - Minimum percent size of opportunity for 1v1
min_diff_percent_plus = 0.4  # 0.4% - Minimum percent size for 1v1+1

# Situation types
SITUATION_1V1 = '1v1'
SITUATION_1V1PLUS = '1v1+1'

# Default paths (change these to your actual paths or enter when prompted)
default_input_path = os.path.join("..", "..", "Data", "SOLUSDT-BINANCE", "Candles")
default_output_path = os.path.join("..", "..", "Data", "SOLUSDT-BINANCE", "Instances", "Unprocessed")

# Verbosity flag
verbose = False

# **************************************************************************************************
# Utility functions for optimized processing
# **************************************************************************************************

def read_last_n_lines(file_path, n):
    """Read last n lines from a file efficiently"""
    try:
        with open(file_path, 'rb') as f:
            # Move to end of file
            f.seek(0, 2)
            file_size = f.tell()
            
            # Initialize variables
            lines = []
            chars_back = 0
            
            # Read backwards until we have n lines or reach start of file
            while len(lines) < n and chars_back < file_size:
                # Move back one character at a time
                chars_back += 1
                f.seek(-chars_back, 2)
                
                # Read one character
                char = f.read(1)
                
                # If we hit a newline, we found a complete line
                if char == b'\n':
                    # Read the line
                    line = f.readline().decode('utf-8')
                    if line.strip():  # Only add non-empty lines
                        lines.append(line)
            
            # If we still need more lines and haven't reached start of file
            if len(lines) < n and chars_back < file_size:
                # Read first line if we haven't reached our target
                f.seek(0)
                line = f.readline().decode('utf-8')
                if line.strip():  # Only add non-empty lines
                    lines.append(line)
                    
        # Return last n lines in correct order
        return list(reversed(lines[-n:]))
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        return []

def read_candles_from_timestamp(file_path, start_timestamp, lookback_candles=5):
    """Read candle data backwards from end of file until we reach start_timestamp minus lookback buffer.
    
    Args:
        file_path: Path to the candle CSV file
        start_timestamp: Timestamp to start reading from (we'll read back further for lookback)
        lookback_candles: Number of extra candles to read before start_timestamp
        
    Returns:
        pandas DataFrame with candle data from start_timestamp onwards (plus lookback buffer)
    """
    try:
        # First, let's peek at the file to get the header and determine timestamp format
        with open(file_path, 'r', encoding='utf-8') as f:
            header_line = f.readline().strip()
            if not header_line:
                print(f"Empty file: {file_path}")
                return pd.DataFrame()
        
        # Parse the start timestamp if it's a string
        if isinstance(start_timestamp, str):
            start_timestamp = pd.to_datetime(start_timestamp)
        elif isinstance(start_timestamp, pd.Timestamp):
            pass  # Already a timestamp
        else:
            start_timestamp = pd.to_datetime(start_timestamp)
        
        # Calculate how far back we need to go (start_timestamp minus some buffer)
        # We'll read extra candles to ensure we have enough for lookback
        buffer_candles = lookback_candles + 10  # Extra buffer for safety
        
        if verbose:
            tqdm.write(f"Reading candles from {file_path} starting from {start_timestamp}...")
        
        # Read backwards from end of file to find our starting point
        lines_to_read = []
        found_start = False
        
        with open(file_path, 'rb') as f:
            # Move to end of file
            f.seek(0, 2)
            file_size = f.tell()
            
            if file_size == 0:
                print(f"Empty file: {file_path}")
                return pd.DataFrame()
            
            # Initialize variables
            lines = []
            chars_back = 0
            candles_found = 0
            
            # Read backwards line by line
            while chars_back < file_size:
                # Move back one character at a time to find newlines
                chars_back += 1
                f.seek(-chars_back, 2)
                
                # Read one character
                char = f.read(1)
                
                # If we hit a newline, we found a complete line
                if char == b'\n':
                    # Read the line
                    current_pos = f.tell()
                    line = f.readline().decode('utf-8').strip()
                    
                    if line and line != header_line:  # Skip empty lines and header
                        lines.append(line)
                        candles_found += 1
                        
                        # Try to parse the timestamp from this line
                        try:
                            # Assume timestamp is the first column
                            timestamp_str = line.split(',')[0]
                            line_timestamp = pd.to_datetime(timestamp_str)
                            
                            # Check if we've gone back far enough
                            if line_timestamp < start_timestamp:
                                # We've found enough data, but let's get a few more for the buffer
                                if candles_found >= buffer_candles:
                                    found_start = True
                                    break
                        except (ValueError, IndexError):
                            # Skip lines that don't parse correctly
                            continue
                    
                    # Reset file position for next iteration
                    f.seek(current_pos - 1)
            
            # If we didn't find enough data, read from the beginning
            if not found_start and chars_back >= file_size:
                # We've read the entire file, add the first line if we haven't
                f.seek(0)
                first_line = f.readline().decode('utf-8').strip()
                if first_line and first_line != header_line and first_line not in lines:
                    lines.append(first_line)
        
        if not lines:
            print(f"No candle data found in {file_path}")
            return pd.DataFrame()
        
        # Reverse the lines to get chronological order and add header
        lines.reverse()
        csv_content = header_line + '\n' + '\n'.join(lines)
        
        # Create DataFrame from the CSV content
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content), parse_dates=['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        if verbose:
            tqdm.write(f"Loaded {len(df)} candles from {df.index[0] if not df.empty else 'N/A'} to {df.index[-1] if not df.empty else 'N/A'}")
        
        return df
        
    except Exception as e:
        if verbose:
            tqdm.write(f"Error reading candle data from {file_path}: {str(e)}")
            tqdm.write("Falling back to reading entire file...")
        try:
            df = pd.read_csv(file_path, parse_dates=['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            # Filter to only data from start_timestamp onwards (with lookback buffer)
            if not df.empty and start_timestamp is not None:
                # Calculate buffer timestamp
                timeframe_minutes = 1  # Default to 1 minute, will be refined
                buffer_time = start_timestamp - pd.Timedelta(minutes=timeframe_minutes * (lookback_candles + 5))
                df = df[df.index >= buffer_time]
            
            return df
        except Exception as fallback_error:
            print(f"Fallback reading also failed: {str(fallback_error)}")
            return pd.DataFrame()

def timeframe_to_minutes(timeframe):
    """Convert timeframe to minutes for calculations"""
    timeframe = timeframe.lower()
    match = re.match(r'(\d+)([a-zA-Z]+)', timeframe)
    if not match:
        return 0
        
    value, unit = int(match.group(1)), match.group(2)
    
    if unit == 'm':
        return value
    elif unit == 'h':
        return value * 60
    elif unit == 'd':
        return value * 24 * 60
    elif unit == 'w':
        return value * 7 * 24 * 60
    elif unit == 'mo':
        return value * 30 * 24 * 60  # Approximate
    return 0

def get_last_processed_timestamp(output_path, timeframe):
    """Get the timestamp of the last processed candle for a specific timeframe"""
    # Use consistent file naming for all timeframes including multi-day
    filename = f'instances_SOLUSDT_binance_{timeframe}.csv'
    
    filepath = os.path.join(output_path, filename)
    
    if not os.path.exists(filepath):
        return None
    
    # Read the last line to get the timestamp
    last_line = read_last_n_lines(filepath, 1)
    if not last_line:
        return None
    
    try:
        # Parse the CSV line - skip the header if that's what we got
        fields = last_line[0].strip().split(',')
        if fields[0].lower() in ['timestamp', 'confirm_date']:
            # We got the header, try to read one more line
            last_line = read_last_n_lines(filepath, 2)
            if len(last_line) < 2:  # Only header in file
                return None
            fields = last_line[1].strip().split(',')
        
        # Get the timestamp from the first field
        return pd.to_datetime(fields[0])
    except Exception as e:
        print(f"Error parsing timestamp from {filepath}: {str(e)}")
        return None

def needs_update(candle_filepath, instance_filepath, timeframe):
    """Check if a timeframe needs updating by comparing timestamps"""
    if not os.path.exists(instance_filepath):
        # Instance file doesn't exist, we need to create it
        return True

    # Get last lines of instance file
    last_line_instance = read_last_n_lines(instance_filepath, 1)
    if not last_line_instance:
        # Instance file is empty, need to update
        return True
        
    # Parse the timestamp from the last instance
    try:
        last_instance_data = last_line_instance[0].split(',')
        last_instance_ts = pd.to_datetime(last_instance_data[0])  # assuming timestamp is first column
    except Exception as e:
        if verbose:
            print(f"Error parsing last instance timestamp: {str(e)}")
        return True

    # Get last line of candle file
    last_line_candle = read_last_n_lines(candle_filepath, 1)
    if not last_line_candle:
        # Candle file is empty but instance file exists? Something is wrong
        if verbose:
            print(f"Warning: Candle file {candle_filepath} appears empty but instance file exists")
        return False

    # Parse the timestamp from the last candle
    try:
        last_candle_data = last_line_candle[0].split(',')
        last_candle_ts = pd.to_datetime(last_candle_data[0])  # assuming timestamp is first column
    except Exception as e:
        if verbose:
            print(f"Error parsing last candle timestamp: {str(e)}")
        return True

    # Calculate timeframe in minutes for proper comparison
    timeframe_mins = timeframe_to_minutes(timeframe)
    
    # Need at least one more candle period after the last instance timestamp
    # for a new potential instance (need two candles: one to break, one to confirm)
    required_time_diff = timedelta(minutes=timeframe_mins * 2)
    
    # If there's new candle data after the last instance (plus required time),
    # we need to update the instance file
    return last_candle_ts > (last_instance_ts + required_time_diff)

def is_instance_in_file(instance_data, file_path):
    """Check if an instance with the same timestamp already exists in the file"""
    if not os.path.exists(file_path):
        return False
        
    # Read last N lines since instances are typically appended chronologically
    lines = read_last_n_lines(file_path, 100)  # Adjust number as needed
    timestamp_str = instance_data['confirm_date'].strftime('%Y-%m-%d %H:%M:%S')
    
    for line in lines:
        if timestamp_str in line:
            return True
    
    return False

# **************************************************************************************************
# Instance detection functions
# **************************************************************************************************

def find_instances(df, timeframe, start_index=0, progress_callback=None, lookback_candles=5, last_instance=None):
    """Find instances of candle patterns in the dataframe
    
    Args:
        df: DataFrame with candle data
        timeframe: The timeframe of the data (e.g., '1m', '5m', '1h')
        start_index: The index to start processing from (for incremental updates)
        progress_callback: Callback for updating progress
        lookback_candles: Number of candles to look back before start_index to catch patterns
                         that might have been in progress before the start_index
        last_instance: The last instance from the existing data that might need upgrading
    
    Returns:
        DataFrame containing all instances with their situation type
    """
    instances = []
    
    # We need at least 2 candles to check for patterns
    if len(df) < 2:
        print(f"Not enough candles: have {len(df)}, need at least 2")
        return pd.DataFrame(instances)
    
    # If we have a last instance that might need upgrading, check it first
    if last_instance is not None and last_instance['situation'] == SITUATION_1V1 and not df.empty:
        last_instance_time = pd.to_datetime(last_instance['confirm_date'])
        if verbose:
            tqdm.write(f"Checking for upgrade of 1v1 instance at {last_instance_time}")
        
        # Find the candle that would confirm the 1v1+1 pattern
        # We need to check the candle before the last instance's date to see if it confirms the pattern
        for i in range(len(df) - 1):
            curr_candle = df.iloc[i]
            next_candle = df.iloc[i + 1] if i + 1 < len(df) else None
            
            # Check if the next candle is the one that would confirm the 1v1+1 pattern
            if next_candle is not None and pd.to_datetime(next_candle.name) == last_instance_time:
                curr_candle_bullish = curr_candle['close'] > curr_candle['open']
                next_candle_bullish = next_candle['close'] > next_candle['open']
                
                if curr_candle_bullish == next_candle_bullish:
                    # Upgrade to 1v1+1
                    upgraded_instance = last_instance.copy()
                    upgraded_instance['situation'] = SITUATION_1V1PLUS
                    instances.append(upgraded_instance)
                    if verbose:
                        tqdm.write(f"Upgraded instance at {last_instance_time} to 1v1+1")
                    
                    # Update start_index to after the confirmation candle
                    start_index = i + 2
                    break
    
    # Adjust start_index to look back for patterns that might have started before the start_index
    # But make sure we don't go before the last instance's date
    if last_instance is not None:
        last_instance_time = pd.to_datetime(last_instance['confirm_date'])
        # Find the candle that's one period before the last instance
        for i in range(len(df)):
            if pd.to_datetime(df.index[i]) >= last_instance_time:
                start_index = max(0, i - 1)  # Start from one candle before the last instance
                break
    
    adjusted_start_index = max(0, start_index - lookback_candles)
    
    # Print processing range summary
    if verbose:
        tqdm.write(f"Processing {len(df)} candles from {df.index[0]} to {df.index[-1]}")
        if adjusted_start_index < start_index:
            tqdm.write(f"Looking back {start_index - adjusted_start_index} candles before start_index")
    
    total_candles = len(df) - adjusted_start_index - 1  # Total candles to process
    
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

    # Process each candle as the current candle
    for i in range(adjusted_start_index, len(df)):
        # Get current candle data
        curr_candle = df.iloc[i]
        curr_candle_open = curr_candle['open']
        curr_candle_close = curr_candle['close']
        
        # Get previous candle if available
        prev_candle = df.iloc[i-1] if i > start_index else None
        
        # Get next candle if available (for 1v1+1 pattern)
        next_candle = df.iloc[i+1] if i + 1 < len(df) else None
        
        # Skip first candle as we need a previous candle for comparison
        if prev_candle is None:
            continue
        # Get candle data
        prev_candle_open = prev_candle['open']
        prev_candle_close = prev_candle['close']
        
        # Calculate candle bodies and directions
        prev_body = abs(prev_candle_close - prev_candle_open)
        curr_body = abs(curr_candle_close - curr_candle_open)
        
        # Calculate candle durations with 10% tolerance
        curr_candle_duration = 0
        if next_candle is not None:
            curr_candle_duration = (next_candle.name - curr_candle.name).total_seconds()
        
        prev_candle_duration = (curr_candle.name - prev_candle.name).total_seconds()

        # Check for basic 1v1 pattern conditions
        is_opposite_direction = ((prev_candle_close > prev_candle_open and curr_candle_close < curr_candle_open) or
                               (prev_candle_close < prev_candle_open and curr_candle_close > curr_candle_open))
        
        curr_larger_than_prev = curr_body > prev_body
        curr_duration_valid = curr_candle_duration <= prev_candle_duration * 1.1  # Allow 10% tolerance
        
        # Common 1v1 pattern check
        if is_opposite_direction and curr_larger_than_prev and curr_duration_valid:
            # Calculate basic pattern data
            if prev_candle_close > prev_candle_open:
                # Bullish followed by bearish
                direction = 'short'
                prev_candle_high = prev_candle['high']
                fib_base = prev_candle_high - prev_candle_open
                target = prev_candle_open - fib_base * 0.618
                entry = prev_candle_open
                fib0_5 = prev_candle_open + fib_base * 0.5
                fib0_0 = prev_candle_high
                fib_0_5 = prev_candle_open + fib_base * 1.5
                fib_1_0 = prev_candle_open + fib_base * 2
                
                # For 1v1+1 check, we need a third candle in the same direction as current
                if next_candle is not None:
                    next_candle_bullish = next_candle['close'] > next_candle['open']
                    curr_candle_bullish = curr_candle_close > curr_candle_open
                    is_1v1plus = (next_candle_bullish == curr_candle_bullish)
                else:
                    is_1v1plus = False
            else:
                # Bearish followed by bullish
                direction = 'long'
                prev_candle_low = prev_candle['low']
                fib_base = prev_candle_open - prev_candle_low
                target = prev_candle_open + fib_base * 0.618
                entry = prev_candle_open
                fib0_5 = prev_candle_open - fib_base * 0.5
                fib0_0 = prev_candle_low
                fib_0_5 = prev_candle_open - fib_base * 1.5
                fib_1_0 = prev_candle_open - fib_base * 2
                
                # For 1v1+1 check, we need a third candle in the same direction as current
                if next_candle is not None:
                    next_candle_bullish = next_candle['close'] > next_candle['open']
                    curr_candle_bullish = curr_candle_close > curr_candle_open
                    is_1v1plus = (next_candle_bullish == curr_candle_bullish)
                else:
                    is_1v1plus = False
            
            # Check opportunity size
            diff_percent = abs(target - entry) / entry * 100
            
            # Add to instances if it meets size criteria
            if diff_percent >= min_diff_percent:
                # Determine if this is a 1v1 or 1v1+1 pattern
                situation = SITUATION_1V1PLUS if is_1v1plus and diff_percent >= min_diff_percent_plus else SITUATION_1V1
                
                # Calculate confirmation date as breaking candle's timestamp + 1 timeframe
                if 'mo' in timeframe:
                    # For monthly timeframes, use DateOffset with the exact number of months
                    match = re.match(r'(\d+)mo', timeframe.lower())
                    if match:
                        months = int(match.group(1))
                        confirm_date = curr_candle.name + pd.DateOffset(months=months)
                    else:
                        confirm_date = curr_candle.name + pd.DateOffset(months=1)
                else:
                    # For non-monthly timeframes, use minutes
                    timeframe_mins = timeframe_to_minutes(timeframe)
                    confirm_date = curr_candle.name + pd.Timedelta(minutes=timeframe_mins)
                
                # Create instance data
                instance_data = {
                    'confirm_date': confirm_date,  # Breaking candle's timestamp + 1 timeframe
                    'timeframe': timeframe,
                    'direction': direction,
                    'entry': round(entry, 4),
                    'target': round(target, 4),
                    'diff_percent': round(diff_percent, 4),
                    'fib0_0': round(fib0_0, 4),
                    'fib0_5': round(fib0_5, 4),
                    'fib_0_5': round(fib_0_5, 4),
                    'fib_1_0': round(fib_1_0, 4),
                    'situation': situation
                }
                
                # Add instance if we're at or after the start_index
                if i >= start_index:
                    instances.append(instance_data)
                    
                    # Only print debug info for the last few candles if verbose
                    if verbose and i >= len(df) - 3:
                        tqdm.write(f"\nProcessed candle at {curr_candle.name}")
                        tqdm.write(f"Pattern: {situation}")
                        tqdm.write(f"Prev: {prev_candle['open']:.4f}-{prev_candle['close']:.4f} ({'up' if prev_candle['close'] > prev_candle['open'] else 'down'})")
                        tqdm.write(f"Curr: {curr_candle['open']:.4f}-{curr_candle['close']:.4f} ({'up' if curr_candle['close'] > curr_candle['open'] else 'down'})")
                        if next_candle is not None:
                            tqdm.write(f"Next: {next_candle['open']:.4f}-{next_candle['close']:.4f} ({'up' if next_candle['close'] > next_candle['open'] else 'down'})")
                        else:
                            tqdm.write("No next candle (end of data)")

        # Update progress
        if (i - start_index) % update_interval == 0 or i == start_index + 1:
            candle_pbar.update(min(update_interval, i - last_update))
            last_update = i
            if progress_callback:
                progress_callback((i - start_index) / total_candles)
    
    candle_pbar.close()
    return pd.DataFrame(instances)

def update_instance_types(existing_instances, new_instances):
    """Update existing instances to new versions if they've been upgraded
    
    Args:
        existing_instances: DataFrame of existing instances
        new_instances: DataFrame of newly found instances
        
    Returns:
        int: Number of instances that were upgraded
    """
    upgraded_count = 0
    
    if existing_instances.empty or new_instances.empty:
        return upgraded_count
    
    # Get the last existing instance
    last_existing = existing_instances.iloc[-1]
    
    # Check if the last existing instance was a 1v1 that needs upgrading
    if last_existing['situation'] == SITUATION_1V1:
        # Look for a new instance with the same confirm_date that's a 1v1+1
        matching_new = new_instances[
            (new_instances['confirm_date'] == last_existing['confirm_date']) &
            (new_instances['situation'] == SITUATION_1V1PLUS)
        ]
        
        if not matching_new.empty:
            upgraded_count = 1
    
    return upgraded_count

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
# Main execution code
# **************************************************************************************************

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Process historical candle data to find and update trading pattern instances.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print verbose output')
    parser.add_argument('-i', '--input', help=f'Input folder path containing CSV files (default: {default_input_path})')
    parser.add_argument('-o', '--output', help=f'Output folder path for instance files (default: {default_output_path})')
    parser.add_argument('-np', '--no-prompt', action='store_true', help='Skip prompts and use default paths')
    return parser.parse_args()

def process_file(filepath, output_path, processed_mb, file_size_mb, file_pbar):
    """Process a single candle data file to find instances.
    
    The update process works as follows:
    1. Pull the last record out of the existing data and note its date
    2. If it's a 1v1, check if the next candle moves in the same direction
    3. If confirmed, replace just that last line with the updated 1v1+1 version
    4. Process any new candle data after the last record
    5. Append new instances to the file efficiently
    
    Returns:
        Tuple of (new_1v1_count, new_1v1plus_count, upgraded_count)
    """
    # Extract timeframe from filename
    timeframe = os.path.basename(filepath).split('_')[2].split('.')[0]
    
    # Initialize variables for determining what data we need to read
    start_timestamp = None
    
    # Check if we have existing instances to determine where to start reading
    output_filepath = os.path.join(output_path, f'instances_{os.path.basename(filepath)}')
    
    if os.path.exists(output_filepath) and os.path.getsize(output_filepath) > 0:
        try:
            # Read the last instance to determine our starting point
            last_lines = read_last_n_lines(output_filepath, 1)
            if last_lines:
                # Parse the last instance
                header_line = None
                with open(output_filepath, 'r') as f:
                    header_line = f.readline().strip()
                
                if header_line:
                    # Create a mini CSV to parse the last instance
                    from io import StringIO
                    mini_csv = header_line + '\n' + last_lines[0]
                    last_instance_df = pd.read_csv(StringIO(mini_csv), parse_dates=['confirm_date'])
                    if not last_instance_df.empty:
                        start_timestamp = last_instance_df.iloc[0]['confirm_date']
                        if verbose:
                            tqdm.write(f"Found last instance at {start_timestamp}, will read candles from around this time")
        except Exception as e:
            if verbose:
                tqdm.write(f"Error reading last instance: {str(e)}, will read entire candle file")
            start_timestamp = None
    
    # Read candle data using optimized reverse reading
    try:
        if start_timestamp is not None:
            # Use reverse reading to get only the data we need
            df = read_candles_from_timestamp(filepath, start_timestamp, lookback_candles=10)
        else:
            # No existing instances, read entire file (first run)
            if verbose:
                tqdm.write(f"No existing instances found, reading entire candle file: {os.path.basename(filepath)}")
            df = pd.read_csv(filepath, parse_dates=['timestamp'])
            df.set_index('timestamp', inplace=True)
            
        if df.empty:
            print(f"No candle data loaded from {os.path.basename(filepath)}")
            return 0, 0, 0
            
    except (ValueError, KeyError) as e:
        print(f"Error reading {os.path.basename(filepath)}: {str(e)}")
        print(f"Skipping corrupted file: {os.path.basename(filepath)}")
        return 0, 0, 0  # Return zero counts for skipped file
    
    # Validate that we have the expected columns
    expected_columns = ['open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        print(f"Missing columns {missing_columns} in {os.path.basename(filepath)}")
        print(f"Skipping file with incorrect structure: {os.path.basename(filepath)}")
        return 0, 0, 0  # Return zero counts for skipped file
    
    # Initialize counters and data structures
    new_1v1_count = 0
    new_1v1plus_count = 0
    upgraded_count = 0
    existing_instances = pd.DataFrame()  # Initialize empty DataFrame
    last_instance = None
    start_index = 0
    
    if os.path.exists(output_filepath) and os.path.getsize(output_filepath) > 0:
        # Read existing instances (we already determined start_timestamp above)
        existing_instances = pd.read_csv(output_filepath, parse_dates=['confirm_date'])
        
        if not existing_instances.empty:
            # Get the last instance
            last_instance = existing_instances.iloc[-1].to_dict()
            
            # Since we used reverse reading, our DataFrame should start around the last instance
            # Find the index of the last instance's confirm date in our loaded data
            try:
                last_instance_timestamp = pd.to_datetime(last_instance['confirm_date'])
                
                # Find the closest timestamp in our loaded data
                if not df.empty:
                    # Look for exact match first
                    if last_instance_timestamp in df.index:
                        last_instance_idx = df.index.get_loc(last_instance_timestamp)
                        start_index = last_instance_idx + 1
                    else:
                        # Find the closest timestamp after the last instance
                        future_timestamps = df.index[df.index > last_instance_timestamp]
                        if len(future_timestamps) > 0:
                            closest_timestamp = future_timestamps[0]
                            start_index = df.index.get_loc(closest_timestamp)
                        else:
                            # All our data is before the last instance, start from end
                            start_index = len(df)
                    
                    if verbose:
                        tqdm.write(f"Last instance was at {last_instance_timestamp}, starting processing from index {start_index}")
                    
                    # If the last instance was a 1v1, we might need to upgrade it
                    if last_instance['situation'] == SITUATION_1V1:
                        if verbose:
                            tqdm.write(f"Last instance was a 1v1, will check for upgrade opportunity")
                
            except (KeyError, ValueError) as e:
                if verbose:
                    tqdm.write(f"Error finding last instance in data: {str(e)}, starting from beginning of loaded data")
                start_index = 0
        else:
            if verbose:
                tqdm.write("Existing instance file is empty, starting from beginning")
    else:
        if verbose:
            tqdm.write("No existing instance file found, starting from beginning")
    
    # Find instances in the data
    progress_updater = ProgressUpdater(file_pbar, processed_mb, file_size_mb)
    new_instances = find_instances(
        df, timeframe, 
        start_index=start_index,
        progress_callback=progress_updater.update_progress,
        last_instance=last_instance
    )
    
    # Count new instances by type and check for upgrades
    if not new_instances.empty:
        # First check if we upgraded the last instance
        if not existing_instances.empty:
            last_existing = existing_instances.iloc[-1]
            if last_existing['situation'] == SITUATION_1V1:
                # Look for a new instance with the same confirm_date that's a 1v1+1
                matching_new = new_instances[
                    (pd.to_datetime(new_instances['confirm_date']) == pd.to_datetime(last_existing['confirm_date'])) &
                    (new_instances['situation'] == SITUATION_1V1PLUS)
                ]
                if not matching_new.empty:
                    upgraded_count = 1
                    if verbose:
                        tqdm.write(f"Found upgrade for instance at {last_existing['confirm_date']}")
        
        # Count new instances (excluding any that were upgrades)
        new_1v1_count = len(new_instances[new_instances['situation'] == SITUATION_1V1])
        new_1v1plus_count = len(new_instances[new_instances['situation'] == SITUATION_1V1PLUS]) - upgraded_count
    
    # Process instances to file if we have any new ones
    if not new_instances.empty:
        # If we have existing instances, we need to handle upgrades
        if os.path.exists(output_filepath) and os.path.getsize(output_filepath) > 0:
            # Read existing instances
            existing_instances = pd.read_csv(output_filepath, parse_dates=['confirm_date'])
            
            # If we upgraded the last instance, remove it from the existing instances
            if upgraded_count > 0 and not existing_instances.empty:
                existing_instances = existing_instances.iloc[:-1]
            
            # Combine with new instances
            all_instances = pd.concat([existing_instances, new_instances])
        else:
            # No existing instances, just use the new ones
            all_instances = new_instances
        
        # Ensure confirm_date is datetime and sort by it
        all_instances['confirm_date'] = pd.to_datetime(all_instances['confirm_date'])
        all_instances = all_instances.sort_values('confirm_date')
        
        # Remove any duplicates, keeping the last occurrence (which would be the upgraded version)
        all_instances = all_instances.drop_duplicates(
            subset=['confirm_date'], 
            keep='last'
        )
        
        # Save to file
        all_instances.to_csv(output_filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')
    
    return new_1v1_count, new_1v1plus_count, upgraded_count

def main():
    """Main function"""
    # Record start time
    start_time = datetime.datetime.now()
    args = parse_args()
    
    # Set verbosity
    global verbose
    verbose = args.verbose
    
    # Get input/output paths
    if args.no_prompt:
        # Use defaults or provided arguments, skip prompts
        input_path = args.input if args.input else default_input_path
        output_path = args.output if args.output else default_output_path
    else:
        # Use interactive prompts
        input_path = args.input if args.input else input(f"\n\rEnter the input folder path containing the timeframe CSV files (default: {default_input_path}): ") or default_input_path
        
        output_path = args.output if args.output else input(f"\n\rEnter the output folder path to save the instance CSV files (default: {default_output_path}): ") or default_output_path
    
    # Print folder usage information
    print(f"\nUsing input folder: {input_path}")
    print(f"Using output folder: {output_path}")
    print("Each timeframe will have a single file containing all instance types in the 'situation' column.\n")

    # Ensure the output folder exists
    os.makedirs(output_path, exist_ok=True)

    # Calculate the total size of the files in MB
    files = sorted([f for f in os.listdir(input_path) if f.endswith('.csv')])
    total_size_mb = sum(os.path.getsize(os.path.join(input_path, f)) for f in files) / (1024 * 1024)
    total_size_mb = round(total_size_mb, 2)
    total_files = len(files)

    # Create a progress bar for the file processing with clean number formatting
    # Round to 1 decimal place and ensure the value is not higher than the actual total to avoid frac warning
    rounded_size = min(round(total_size_mb, 1), total_size_mb)
    file_pbar = tqdm(
        total=rounded_size,
        desc=f'Processing file 0 of {total_files}',
        unit='MB',
        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
    )

    # Track the total MB processed so far
    processed_mb = 0.0
    total_1v1_instances = 0
    total_1v1plus_instances = 0
    total_upgraded_instances = 0
    updated_files = 0
    
    # Process each file
    for idx, filename in enumerate(files, start=1):
        filepath = os.path.join(input_path, filename)
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        
        # Update the progress bar with current file info
        file_pbar.set_description(f'Processing file {idx} of {total_files}')
        
        # Process the file
        new_1v1, new_1v1plus, upgraded = process_file(filepath, output_path, processed_mb, file_size_mb, file_pbar)
        
        # Show completion message for this file
        timeframe = filename.split('_')[2].split('.')[0]  # Extract timeframe from filename
        if new_1v1 > 0 or new_1v1plus > 0 or upgraded > 0:
            tqdm.write(f"[{timeframe}]: +{new_1v1} 1v1, +{new_1v1plus} 1v1+1, {upgraded} upgraded")
        else:
            tqdm.write(f"[{timeframe}]: no new instances")
        
        # Update counters
        if new_1v1 > 0 or new_1v1plus > 0 or upgraded > 0:
            updated_files += 1
            total_1v1_instances += new_1v1
            total_1v1plus_instances += new_1v1plus
            total_upgraded_instances += upgraded
        
        # Update the total processed MB
        processed_mb += file_size_mb
        file_pbar.n = processed_mb
        file_pbar.refresh()
        
        # Update the progress bar description
        file_pbar.set_description(f'Processed {idx} of {total_files} files')

    file_pbar.close()

    # Calculate runtime
    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    minutes, seconds = divmod(elapsed_time.seconds, 60)
    
    print(f'\nInstance extraction complete!')
    print(f'Files processed: {total_files}, Files updated: {updated_files}')
    print(f'New 1v1 instances found: {total_1v1_instances}')
    print(f'New 1v1+1 instances found: {total_1v1plus_instances}')
    print(f'Existing 1v1 instances upgraded to 1v1+1: {total_upgraded_instances}')
    print(f'\nTotal execution time: {minutes} minutes and {seconds} seconds')

if __name__ == "__main__":
    main()
