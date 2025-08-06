#!/usr/bin/env python
import os
import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
import argparse
import sys
import re
import time
import subprocess
from tqdm import tqdm

# This script downloads historical data for any trading pair (e.g., SOLUSDT) from Binance and saves it as CSV files for different timeframes.
# 
# Usage:
# python download_binance_historical_data.py [OPTIONS]
# 
# Main Modes:
# --some: Downloads all standard timeframes directly from Binance,
#         overwriting any existing candle data files. Useful for backtesting
#         on a small set of timeframes.
# --all: Downloads 1m data and launches the converter to create custom
#         timeframes. Ideal for comprehensive data analysis.
# --sample: Downloads 1-second data for a specified 1-minute period.
#           Requires --start-date and --symbol to be explicitly specified.
#           This mode is useful for high-resolution data analysis within a
#           specific minute.
# 
# Command Line Arguments:
# -s, --symbol: Trading pair symbol (default: SOLUSDT)
# -d, --directory: Data directory (default: ../../Data/{symbol}-{exchange}/Candles)
# --start-date: Start date (YYYY-MM-DD or YYYYMMDD)
# --end-date: End date (YYYY-MM-DD or YYYYMMDD)
# -v, --verbose: Print verbose output
# -k, --keep-incomplete: Keep incomplete periods
# -h, --help, -?, --?, /?: Show this help message and exit

# Default settings - you can change these if desired (or specify them as command line argu)
default_folder_path = os.path.join('..', '..', 'Data')
start_date = '2020-01-01'  # The start date for the data download
end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')  # The end date for the data download (default: current date)
default_symbol = "SOLUSDT"  # Default trading pair

# It is strongly recommended that you DON'T change this one unless you plan to heavily modify the code:
default_exchange = "binance"  # Default exchange (lowercase)

# Standard timeframes supported by Binance
# this list will be used with --some mode (see project README for more details) 
STANDARD_TIMEFRAMES = ['1m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1mo']

# Do not edit below this line unless you know what you're doing.
# **************************************************************************

# Global variables
folder_path = None  # Will be set in main()
verbose = False  # Global verbose flag
keep_incomplete = False  # Global keep_incomplete flag

# Global exchange ID (lowercase)
exchange = default_exchange.lower()

# Mapping between our timeframe format and exchange format
timeframe_map = {
    exchange: {
        '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
        '1h': '1h', '4h': '4h', '6h': '6h', '12h': '12h',
        '1d': '1d', '3d': '3d', '1w': '1w', '1mo': '1M'
    }
}

def get_exchange_timeframe(exchange_id, timeframe):
    """Map of exchange-specific timeframe formats"""
    return timeframe_map.get(exchange_id, {}).get(timeframe, timeframe)

def get_timeframe_components(timeframe):
    """Extract components from a timeframe string"""
    timeframe = timeframe.lower()
    match = re.match(r'(\d+)([a-zA-Z]+)', timeframe)
    if not match:
        raise ValueError(f"Invalid timeframe format: {timeframe}")
    return int(match.group(1)), match.group(2)

def get_available_bases(timeframes):
    """Get all available base timeframes from the list of timeframes"""
    # Sort timeframes by duration in minutes
    return sorted(timeframes, key=timeframe_to_minutes)

def find_best_base(timeframe, available_bases):
    """Find the best base timeframe for the given target timeframe"""
    if not available_bases:
        return None
        
    target_mins = timeframe_to_minutes(timeframe)
    best_base = None
    best_factor = 0
    
    for base in available_bases:
        if base == timeframe:  # Don't use self as base
            continue
            
        base_mins = timeframe_to_minutes(base)
        if base_mins == 0 or target_mins % base_mins != 0:
            continue  # Not a factor
            
        factor = target_mins // base_mins
        if factor > best_factor:
            best_factor = factor
            best_base = base
    
    return best_base

def get_base_timeframe(timeframe, available_timeframes=None):
    """Get the appropriate base timeframe to use for resampling
    
    Args:
        timeframe: The target timeframe (e.g., '15m', '4h')
        available_timeframes: List of all available timeframes to choose from
        
    Returns:
        str: The best base timeframe to use, or None if none found
    """
    if available_timeframes is None:
        # Default to standard bases if no timeframes provided
        return _get_default_base_timeframe(timeframe)
        
    # Make sure we have the base timeframes in the available list
    required_bases = {'1m', '1h', '1d'}
    available_bases = set(available_timeframes).union(required_bases)
    
    # Try to find the best base from available timeframes
    best_base = find_best_base(timeframe, available_bases)
    if best_base:
        return best_base
        
    # Fall back to default behavior if no suitable base found
    return _get_default_base_timeframe(timeframe)

def _get_default_base_timeframe(timeframe):
    """Get default base timeframe (original behavior)"""
    timeframe = timeframe.lower()
    number, unit = get_timeframe_components(timeframe)
    
    if unit == 'm' or timeframe == '1h':
        return "1m"
    elif (unit == 'h' and timeframe != '1h') or timeframe == '1d':
        return "1h"
    elif unit in ['d', 'w', 'mo'] and timeframe != '1d':
        return "1d"
    raise ValueError(f"Unsupported timeframe format: {timeframe}")

def get_third_last_line(file_path):
    """Get the third last line from a file, excluding empty lines"""
    lines = read_last_n_lines(file_path, 3)
    if len(lines) >= 3:
        return lines[0].strip()
    return None

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

def read_candles_from_timestamp(file_path, start_timestamp, lookback_buffer=10):
    """Read candle data backwards from end of file until we reach start_timestamp minus lookback buffer.
    
    Args:
        file_path: Path to the candle CSV file
        start_timestamp: Timestamp to start reading from (we'll read back further for buffer)
        lookback_buffer: Number of extra candles to read before start_timestamp
        
    Returns:
        pandas DataFrame with candle data from start_timestamp onwards (plus lookback buffer)
    """
    try:
        # First, let's peek at the file to get the header
        with open(file_path, 'r', encoding='utf-8') as f:
            header_line = f.readline().strip()
            if not header_line:
                if verbose:
                    print(f"Empty file: {file_path}")
                return pd.DataFrame()
        
        # Parse the start timestamp if it's a string
        if isinstance(start_timestamp, str):
            start_timestamp = pd.to_datetime(start_timestamp)
        elif isinstance(start_timestamp, pd.Timestamp):
            pass  # Already a timestamp
        else:
            start_timestamp = pd.to_datetime(start_timestamp)
        
        if verbose:
            print(f"Reading candles from {file_path} starting from {start_timestamp}...")
        
        # Read backwards from end of file to find our starting point
        lines_to_read = []
        found_start = False
        
        with open(file_path, 'rb') as f:
            # Move to end of file
            f.seek(0, 2)
            file_size = f.tell()
            
            if file_size == 0:
                if verbose:
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
                                if candles_found >= lookback_buffer:
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
            if verbose:
                print(f"No candle data found in {file_path}")
            return pd.DataFrame()
        
        # Reverse the lines to get chronological order and add header
        lines.reverse()
        csv_content = header_line + '\n' + '\n'.join(lines)
        
        # Create DataFrame from the CSV content
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content), dtype={'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        if verbose:
            print(f"Loaded {len(df)} candles from {df['timestamp'].iloc[0] if not df.empty else 'N/A'} to {df['timestamp'].iloc[-1] if not df.empty else 'N/A'}")
        
        return df
        
    except Exception as e:
        if verbose:
            print(f"Error reading candle data from {file_path}: {str(e)}")
            print("Falling back to reading entire file...")
        # Fallback to reading entire file if reverse reading fails
        try:
            df = pd.read_csv(file_path, dtype={'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Filter to only data from start_timestamp onwards (with lookback buffer)
            if not df.empty and start_timestamp is not None:
                # Calculate buffer timestamp - use a conservative 1-minute buffer
                buffer_time = start_timestamp - pd.Timedelta(minutes=lookback_buffer)
                df = df[df['timestamp'] >= buffer_time]
            
            return df
        except Exception as fallback_error:
            print(f"Fallback reading also failed: {str(fallback_error)}")
            return pd.DataFrame()

def timeframe_to_offset(timeframe):
    """Convert timeframe to pandas offset string"""
    # First handle the minute case (must be before any case conversion)
    if timeframe.endswith('m'):
        return timeframe[:-1] + 'min'
    
    # Convert D/W/M to uppercase as pandas expects
    tf = timeframe.upper() if timeframe[-1] in ['d', 'D', 'w', 'W', 'm', 'M'] else timeframe
    
    # Handle hours (must be lowercase)
    if tf.endswith('H'):
        return tf[:-1] + 'h'
    
    return tf

def resample_data(df, rule):
    """Function to resample the data with proper rollover handling"""
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    resampled = df.resample(rule, closed='left', label='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    # Ensure float type for all numeric columns
    for col in ['open', 'high', 'low', 'close']:
        resampled[col] = resampled[col].astype(float)
        
    # Trim volume to 3 decimal places
    resampled['volume'] = resampled['volume'].round(3)
        
    return resampled

def handle_year_end_rollover(df, rule):
    if verbose:
        print("Handling resampling using year-end rollover...")
        
    if 'D' in rule and rule != '1D':  
        # Handle multi-day timeframes with year-end rollover
        years = list(range(df.index.year.min(), df.index.year.max() + 1))
        result = []
        
        for year in years:
            year_start = pd.Timestamp(f'{year}-01-01')
            year_end = pd.Timestamp(f'{year}-12-31 23:59:59')
            year_df = df[(df.index >= year_start) & (df.index <= year_end)]
            
            if not year_df.empty:
                # Use resample_data but with proper year boundaries
                resampled = resample_data(year_df, rule)
                result.append(resampled)
        
        return pd.concat(result) if result else pd.DataFrame()
    
    # For 1D and other timeframes, use standard resampling
    return resample_data(df, rule)

def handle_midnight_rollover(df, rule):
    if verbose:
        print("Handling resampling using midnight UTC rollover...")

    """Function to handle midnight UTC rollover for sub-daily timeframes"""
    # Group by date to ensure rollover at midnight UTC
    dates = list(set(df.index.date))
    result = []
    for date in dates:
        date_start = pd.Timestamp(date)
        date_end = date_start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        date_df = df[(df.index >= date_start) & (df.index <= date_end)]
        if not date_df.empty:
            resampled_date_df = resample_data(date_df, rule)
            result.append(resampled_date_df)
    return pd.concat(result) if result else pd.DataFrame()

def handle_weekly_rollover(df, rule):
    if verbose:
        print("Handling resampling using weekly rollover...")

    """Function to handle weekly data (always start on Monday)"""
    # Extract the number of weeks from the timeframe
    weeks = int(''.join(filter(str.isdigit, rule)))
    
    # First resample to 1 week starting Monday
    df = df.resample('W-MON', closed='left', label='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    if weeks > 1:
        # Then resample to N weeks, using {weeks}W-MON to maintain Monday start
        df = df.resample(f'{weeks}W-MON', closed='left', label='left').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
    
    # Trim volume to 3 decimal places
    df['volume'] = df['volume'].round(3)
    
    return df

def handle_monthly_rollover(df, rule):
    if verbose:
        print("Handling resampling using monthly rollover...")

    """Function to handle monthly data (always start at month begin)"""
    # Extract the number of months from the timeframe
    months = int(''.join(filter(str.isdigit, rule)))
    
    # Handle multi-month periods by year to ensure Jan 1st rollover
    if months > 1:
        years = list(range(df.index.year.min(), df.index.year.max() + 1))
        result = []
        for year in years:
            year_start = pd.Timestamp(f'{year}-01-01')
            year_end = pd.Timestamp(f'{year}-12-31 23:59:59')
            year_df = df[(df.index >= year_start) & (df.index <= year_end)]
            if not year_df.empty:
                # First resample to months
                monthly_df = year_df.resample('MS', closed='left', label='left').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
                
                # Then resample to N months
                multi_month_df = monthly_df.resample(f'{months}MS', closed='left', label='left').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
                result.append(multi_month_df)
        return pd.concat(result) if result else pd.DataFrame()
    else:
        # Single month periods just need MS resampling
        df = df.resample('MS', closed='left', label='left').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
    
    # Trim volume to 3 decimal places
    df['volume'] = df['volume'].round(3)
    
    return df

def divides_evenly_into_day(tf):
    """Function to check if a timeframe divides evenly into a day"""
    # Extract the numeric value and unit from the timeframe
    value = int(''.join(filter(str.isdigit, tf)))
    unit = ''.join(filter(str.isalpha, tf))
    
    if unit == 'm':
        # For minutes, check if it divides evenly into 1440 (minutes in a day)
        return 1440 % value == 0
    elif unit == 'h':
        # For hours, check if it divides evenly into 24 (hours in a day)
        return 24 % value == 0
    return False

def resample_timeframe(df, timeframe):
    """Resample data to target timeframe"""
    # Convert timeframe to pandas offset string (only affects minute timeframes)
    offset = timeframe_to_offset(timeframe)
    
    # Ensure we have a copy to avoid modifying the original
    df = df.copy()
    
    # Set timestamp as index if it's not already
    if 'timestamp' in df.columns:
        df = df.set_index('timestamp')
    
    # For weekly data, ensure we start on Monday
    if timeframe.lower().endswith('w'):
        df = handle_weekly_rollover(df, timeframe)
    # For monthly data, ensure we start at month begin
    elif timeframe.lower().endswith('mo'):
        df = handle_monthly_rollover(df, timeframe)
    # For daily timeframes, handle year-end rollover
    elif timeframe.lower().endswith('d') and timeframe.lower() != '1d':
        df = handle_year_end_rollover(df, timeframe)
    # For sub-daily timeframes that don't divide evenly into a day, handle midnight rollover
    elif not divides_evenly_into_day(timeframe):
        df = handle_midnight_rollover(df, timeframe)
    else:
        # For all other timeframes
        df = resample_data(df, offset)
    
    # Reset index to get timestamp as a column
    return df.reset_index()

def update_file_with_overlap(target_file, df):
    """Update a file with new data, handling overlap correctly"""
    # If this is a new file, write it directly
    if not os.path.exists(target_file):
        df = df.sort_values('timestamp').reset_index(drop=True)
        df.to_csv(target_file, mode='w', header=True, date_format='%Y-%m-%d %H:%M:%S', index=False)
        return True
        
    if df.empty:
        return False
        
    # Sort the DataFrame before any operations:
    df = df.sort_values('timestamp').reset_index(drop=True)

    # Get the first new row for comparison
    first_new_row = df.iloc[0:1].to_csv(date_format='%Y-%m-%d %H:%M:%S', header=False, index=False).strip()
    
    # Compare with third last line
    last_line = get_third_last_line(target_file)
    if last_line and compare_csv_lines(last_line, first_new_row):
        # Read all lines except last two
        with open(target_file, 'r') as f:
            header = f.readline()  # Get header
            lines = f.readlines()[:-2]  # Get all lines except last two, excluding header
            
        # Write back header and all lines except last two
        with open(target_file, 'w', newline='') as f:
            f.write(header)
            f.writelines(lines)
            
            # Skip the first row of new data (it's the overlap row) and write the rest
            new_data = df.iloc[1:].to_csv(date_format='%Y-%m-%d %H:%M:%S', header=False, index=False)
            f.write(new_data)
        return True
    else:
        if verbose:
            print(f"No overlap found or data mismatch")
            print(f"Expected: {last_line}")
            print(f"Got: {first_new_row}")
            
            # Add more detailed comparison information
            try:
                parts1 = last_line.split(',')
                parts2 = first_new_row.split(',')
                
                # If timestamps match, show detailed differences for each field
                if parts1[0] == parts2[0]:
                    field_names = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    for i in range(1, min(len(parts1), len(parts2))):
                        field = field_names[i] if i < len(field_names) else f"field{i}"
                        try:
                            val1 = float(parts1[i])
                            val2 = float(parts2[i])
                            diff = abs(val1 - val2)
                            print(f"  {field}: {val1} vs {val2}, diff={diff:.9f}")
                        except (ValueError, IndexError):
                            pass
            except Exception:
                pass
        return False

def truncate_future_candles(file_path, end_date):
    """Truncate last line if we are still within its period (incomplete)"""
    # Don't do anything if we're keeping incomplete candles
    if keep_incomplete:
        return
        
    if not os.path.exists(file_path):
        return
    
    try:
        # Extract timeframe from filename
        timeframe = os.path.basename(file_path).split('_')[-1].replace('.csv', '')
        
        # Get last line efficiently
        last_line = read_last_n_lines(file_path, 1)
        if not last_line:
            return
        
        # Parse timestamp and ensure it's UTC
        last_ts = pd.to_datetime(last_line[0].split(',')[0]).tz_localize('UTC')
        
        # Calculate period start and end
        timeframe_mins = timeframe_to_minutes(timeframe)
        period_end = last_ts + timedelta(minutes=timeframe_mins)
        
        # A period is incomplete if our end_date falls within it
        # i.e., if end_date is after period start but before period end
        is_incomplete = last_ts <= end_date < period_end
        
        # Truncate if needed
        if is_incomplete:
            if verbose:
                print(f"Removing incomplete candle on {timeframe} at {last_ts}")
            
            # Convert timestamp to string format as it appears in the file
            timestamp_str = last_ts.strftime('%Y-%m-%d %H:%M:%S')
            
            with open(file_path, 'rb+') as f:
                # Start from end of file
                f.seek(0, os.SEEK_END)
                pos = f.tell()
                
                # Buffer to store characters as we read backwards
                buf = bytearray()
                
                # Read backwards until we find a newline followed by our timestamp
                while pos > 0:
                    # Move back one character
                    pos -= 1
                    f.seek(pos)
                    
                    # Read one character
                    char = f.read(1)
                    
                    # If we hit a newline
                    if char == b'\n':
                        # Convert buffer to string and check if it starts with our timestamp
                        line_start = buf.decode('utf-8')
                        if line_start.startswith(timestamp_str):
                            # Found the line to truncate from
                            f.seek(pos)
                            f.truncate()
                            break
                        # Clear buffer after each newline
                        buf = bytearray()
                    else:
                        # Add character to start of buffer
                        buf.insert(0, char[0])
                
    except Exception as e:
        print(f"Error truncating {file_path}: {str(e)}")

def update_timeframe_from_base(symbol, exchange_id, timeframe, folder_path):
    """Update a specific timeframe from its base timeframe data"""
    # Get base timeframe
    base_tf = get_base_timeframe(timeframe)
    if not base_tf:
        if verbose:
            print(f"No base timeframe found for {timeframe}")
        return
    
    # Get base file data
    base_file, base_tf = get_base_file_data(folder_path, symbol, timeframe, exchange_id)
    if base_file is None:
        if verbose:
            print(f"No base file found for {timeframe}")
        return
        
    # Get target file path
    target_file = os.path.join(folder_path, get_candle_filename(symbol, exchange_id, timeframe))
    
    # Get the third last line to find where to start
    if os.path.exists(target_file):
        last_line = get_third_last_line(target_file)
        if last_line:
            try:
                last_ts = pd.to_datetime(last_line.split(',')[0])
                if verbose:
                    print(f"Meshing at timestamp {last_ts}")
            except:
                last_ts = pd.Timestamp('2020-01-01 00:00:00')
        else:
            last_ts = pd.Timestamp('2020-01-01 00:00:00')
    else:
        last_ts = pd.Timestamp('2020-01-01 00:00:00')
    
    # Read base timeframe data using optimized reverse reading
    # Only load data from last_ts onwards instead of entire file
    df = read_candles_from_timestamp(base_file, last_ts, lookback_buffer=20)
    
    # Filter to ensure we only have data from last_ts onwards
    if not df.empty:
        df = df[df['timestamp'] >= last_ts]
    
    if df.empty:
        if verbose:
            print(f"No data to process in base file")
        return
    
    if verbose:
        print(f"Retrieved {len(df)} records from base file")
    
    # Resample to target timeframe
    resampled = resample_timeframe(df, timeframe)
    if resampled.empty:
        if verbose:
            print(f"No data after resampling")
        return
    
    # Update the file using the common update function
    if not update_file_with_overlap(target_file, resampled):
        print(f"No overlap found for {timeframe} - skipping update to avoid errors")
        return  # Don't append if no overlap found - this prevents duplicates

def get_base_file_data(folder_path, symbol, timeframe, exchange_id):
    """Get data from appropriate base file with fallbacks"""
    # Get base timeframe using our standard function
    base_tf = get_base_timeframe(timeframe)
    base_file = f"{symbol}_{exchange_id}_{base_tf}.csv"
    
    base_path = os.path.join(folder_path, base_file)
    if os.path.exists(base_path):
        return base_path, base_tf
    
    if not os.path.exists(base_path):
        print(f"Base file {base_file} not found")
        return None, None

def timeframe_to_minutes(timeframe):
    """Convert timeframe to minutes for sorting and calculations"""
    match = re.match(r'(\d+)([a-zA-Z]+)', timeframe)
    if not match:
        raise ValueError(f"Invalid timeframe format: {timeframe}")
    
    number = int(match.group(1))
    unit = match.group(2).lower()
    
    # Convert to minutes
    if unit == 's':
        return number / 60  # Convert seconds to fractional minutes
    elif unit == 'm':
        return number
    elif unit == 'h':
        return number * 60
    elif unit == 'd':
        return number * 24 * 60
    elif unit == 'w':
        return number * 7 * 24 * 60
    elif unit in ['mo', 'M']:  # Handle both 'mo' and 'M' for months
        return number * 30 * 24 * 60  # Approximate month as 30 days
    else:
        raise ValueError(f"Unknown timeframe unit: {unit}")

def needs_update(file_path, timeframe, current_time):
    """Check if a timeframe file needs updating based on the last candle and current time
    
    Args:
        file_path (str): Path to the timeframe file
        timeframe (str): Timeframe string (e.g., '1m', '5m', '1h')
        current_time (datetime): Current time in UTC
        
    Returns:
        bool: True if the file needs updating, False otherwise
    """
    if not os.path.exists(file_path):
        if verbose:
            print(f"File {file_path} doesn't exist, needs to be created")
        return True  # File doesn't exist, so it needs to be created
        
    # Get last line from file
    last_line = read_last_n_lines(file_path, 1)
    if not last_line:
        if verbose:
            print(f"File {file_path} is empty, needs update")
        return True  # Empty file, needs update
        
    try:
        # Parse timestamp from last line
        last_ts = pd.to_datetime(last_line[0].split(',')[0])
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize('UTC')
        
        # Calculate timeframe duration in minutes
        timeframe_mins = timeframe_to_minutes(timeframe)
        
        # Calculate when the next COMPLETED candle would be available
        # A candle is only complete after its period has ended
        next_complete_candle_time = last_ts + timedelta(minutes=timeframe_mins * 2)
        
        # If current time is past when the next complete candle would be available, we need to update
        needs_update = current_time >= next_complete_candle_time
        
        if verbose:
            if needs_update:
                print(f"Timeframe {timeframe} needs update: last candle at {last_ts}, next complete candle at {next_complete_candle_time}, current time {current_time}")
            else:
                print(f"Timeframe {timeframe} doesn't need update: last candle at {last_ts}, next complete candle at {next_complete_candle_time}, current time {current_time}")
            
        return needs_update
        
    except Exception as e:
        if verbose:
            print(f"Error checking if {timeframe} needs update: {str(e)}")
        return True  # If there's an error, assume we need to update

def get_existing_timeframes(folder_path):
    """Get dictionary of existing timeframe files in the folder"""
    existing_timeframes = {}
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            if file.endswith('.csv'):
                timeframe = file.split('_')[-1].replace('.csv', '')
                existing_timeframes[timeframe] = file
    return existing_timeframes

def get_candle_filename(symbol, exchange, timeframe):
    """Get filename for candle data"""
    return f"{symbol}_{exchange}_{timeframe}.csv"

def get_timeframe_ms(exchange, timeframe):
    """Get timeframe duration in milliseconds"""
    # Map timeframe to minutes
    minutes = timeframe_to_minutes(timeframe)
    
    # Convert to milliseconds
    return minutes * 60 * 1000

def download_historical_data(symbol, exchange_id, timeframe, start_time, end_time, data_dir, args):
    """Downloads historical candlestick data from the specified exchange"""
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    if args.some:
        print(f"Downloading {timeframe} TF from source {exchange_id}...")
    else:
        print(f"Updating {timeframe} TF from source {exchange_id}...")
    
    # Initialize exchange
    exchange_instance = init_exchange(exchange_id)
    
    # Convert timeframe to exchange format
    exchange_tf = get_exchange_timeframe(exchange_id, timeframe)
    if not exchange_tf:
        if verbose:
            print(f"Skipping unsupported timeframe: {timeframe}")
        return None
    
    # Convert timestamps to milliseconds
    if args.some:
        end_timestamp_ms = int(pd.Timestamp.now(tz='UTC').timestamp() * 1000)
    else:
        end_timestamp_ms = int(pd.to_datetime(end_time).timestamp() * 1000)
    since_ms = int(pd.to_datetime(start_time).timestamp() * 1000)
    current_timestamp = since_ms
    limit = 1000
    
    # Calculate timeframe duration in milliseconds
    tf_ms = get_timeframe_ms(exchange_id, timeframe)
    
    retry_count = 0
    max_retries = 3
    retry_delay = 10  # seconds
    
    # Calculate total number of candles for progress bar
    total_candles = (end_timestamp_ms - since_ms) // tf_ms
    if total_candles <= 0:
        if verbose:
            print("No new data to download")
        return None
    
    pbar = tqdm(desc=f'Downloading {symbol} {timeframe} candles', total=total_candles, unit='candles')
    all_candles = []
    
    while current_timestamp < end_timestamp_ms:
        try:
            candles = exchange_instance.fetch_ohlcv(
                timeframe=exchange_tf,
                symbol=symbol,
                since=int(current_timestamp),  # Ensure integer type for Binance API
                limit=limit
            )
            
            if not candles:
                break
                
            all_candles.extend(candles)
            
            # Update progress bar
            num_candles = len(candles)
            pbar.update(num_candles)
            
            if num_candles < limit:
                break
                
            # Get timestamp of last candle
            current_timestamp = candles[-1][0] + tf_ms
            
            # Rate limiting
            time.sleep(exchange_instance.rateLimit / 1000)  # Convert to seconds
            
            retry_count = 0  # Reset retry count after successful request
            
        except Exception as e:
            retry_count += 1
            if retry_count > max_retries:
                print(f"\nError downloading data: {str(e)}")
                print(f"Failed after {max_retries} retries")
                break
            print(f"\nRetry {retry_count}/{max_retries} after error: {str(e)}")
            time.sleep(retry_delay)
            continue
    
    pbar.close()
    
    if not all_candles:
        if verbose:
            print("No data downloaded")
        return None
        
    # Convert to DataFrame
    df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    
    # Convert timestamp to datetime with UTC timezone
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    
    # Set timestamp as index
    df.set_index('timestamp', inplace=True)
    
    # Sort by index
    df.sort_index(inplace=True)
    
    # Remove duplicates
    df = df[~df.index.duplicated(keep='first')]
    
    # Remove incomplete candles at the end if requested
    if not keep_incomplete:
        if args.some:
            now = pd.Timestamp.now(tz='UTC')
            # For monthly timeframe, use 30 days as an approximation
            if timeframe == '1mo':
                last_complete = now - pd.Timedelta(days=30)
            else:
                last_complete = now - pd.Timedelta(timeframe)
        else:
            # For default/--all mode, use the specified end_date
            now = pd.to_datetime(end_time, utc=True)
            # For monthly timeframe, use 30 days as an approximation
            if timeframe == '1mo':
                last_complete = now - pd.Timedelta(days=30)
            else:
                last_complete = now - pd.Timedelta(timeframe)
        df = df[df.index <= last_complete]
    
    return df

def init_exchange(exchange_id):
    """Initialize exchange"""
    exchange_class = getattr(ccxt, exchange_id.lower())
    exchange = exchange_class({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'spot'
        }
    })
    return exchange

def compare_csv_lines(line1, line2, tolerance=1e-3):
    """Compare two CSV lines with tolerance for floating point values"""
    try:
        parts1 = line1.split(',')
        parts2 = line2.split(',')
        
        # Compare timestamps exactly
        if parts1[0] != parts2[0]:
            return False
        
        # Compare numeric values with tolerance
        for i in range(1, len(parts1)):
            try:
                val1 = float(parts1[i])
                val2 = float(parts2[i])
                # Use a larger tolerance for volume (assumed to be the last value)
                current_tolerance = tolerance * 10 if i == 5 else tolerance
                if abs(val1 - val2) > current_tolerance:
                    return False
            except ValueError:
                if parts1[i] != parts2[i]:
                    return False
        return True
    except Exception:
        return False

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Download historical data from Binance')
    parser.add_argument('-s', '--symbol', type=str, default=default_symbol, help='Trading pair symbol (default: SOLUSDT)')
    parser.add_argument('-d', '--directory', type=str, help='Data directory (default: ../../Data/{symbol}-{exchange}/Candles)')
    parser.add_argument('--start-date', type=str, default=start_date, help='Start date (YYYY-MM-DD or YYYYMMDD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD or YYYYMMDD)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print verbose output')
    parser.add_argument('-k', '--keep-incomplete', action='store_true', help='Keep incomplete periods')
    parser.add_argument('--some', action='store_true', help='Download all standard timeframes')
    parser.add_argument('--all', action='store_true', help='Download 1m data and launch converter')
    parser.add_argument('--sample', nargs=2, metavar=('EXCHANGE', 'HH:MM'), help='Download 1s data for a 1-minute period. Requires --start-date and --symbol.')
    args = parser.parse_args()
    
    # Validate that modes are not used together
    if sum([args.some, args.all, bool(args.sample)]) > 1:
        parser.error("Cannot use --some, --all, or --sample arguments together. They represent different modes of operation.")
    
    # Validate sample mode requirements
    if args.sample and (not args.start_date or args.start_date == start_date or not args.symbol):
        parser.error("--sample mode requires both --start-date and --symbol to be explicitly specified.")
    
    return args

def download_sample_data(symbol, exchange_id, start_date, sample_time, data_dir=None):
    """
    Downloads 1-second candle data for a specified 1-minute period.
    
    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
        exchange_id (str): Exchange ID (e.g., 'binance')
        start_date (str): Start date in 'YYYY-MM-DD' format
        sample_time (str): Time in 'HH:MM' format for the 1-minute sample
        data_dir (str, optional): Directory where to save the data
    
    Returns:
        DataFrame: The downloaded data
    """
    # Initialize exchange
    exchange_instance = init_exchange(exchange_id)
    
    # Create sample time range (1 minute)
    start_datetime = pd.to_datetime(f"{start_date} {sample_time}:00", utc=True)
    end_datetime = start_datetime + pd.Timedelta(minutes=1)
    
    # Convert timestamps to milliseconds
    since = int(start_datetime.timestamp() * 1000)
    end_timestamp = int(end_datetime.timestamp() * 1000)
    
    print(f"Downloading 1s sample data for {symbol} on {exchange_id}")
    print(f"Time period: {start_datetime} to {end_datetime} UTC")
    
    # Use 1s timeframe
    timeframe = '1s'
    exchange_tf = timeframe  # Assuming 1s is supported directly
    
    # Calculate timeframe duration in milliseconds
    tf_ms = 1000  # 1 second = 1000 milliseconds
    
    retry_count = 0
    max_retries = 5
    retry_delay = 5  # seconds
    
    pbar = tqdm(desc=f'Downloading {symbol} 1s candles', total=60, unit='candles')
    all_candles = []
    current_timestamp = since
    
    while current_timestamp < end_timestamp:
        try:
            # Note: Some exchanges might not support 1s timeframe directly
            # In that case, we might need to use a smaller timeframe or tick data
            candles = exchange_instance.fetch_ohlcv(
                timeframe=exchange_tf,
                symbol=symbol,
                since=current_timestamp,  # Original integer format
                limit=100  # Get more than we need for the minute
            )
            
            if not candles:
                break
                
            # Filter to only include candles within our time range
            filtered_candles = [c for c in candles if since <= c[0] < end_timestamp]
            all_candles.extend(filtered_candles)
            
            # Update progress bar based on time coverage
            time_covered = min(end_timestamp, candles[-1][0] + tf_ms) - current_timestamp
            seconds_covered = time_covered // 1000
            pbar.update(seconds_covered)
            
            # Get timestamp of last candle
            current_timestamp = candles[-1][0] + tf_ms
            
            # Rate limiting
            time.sleep(exchange_instance.rateLimit / 1000)  # Convert to seconds
            
            retry_count = 0  # Reset retry count after successful request
            
            # If we've gone past our end time, we're done
            if current_timestamp >= end_timestamp:
                break
                
        except Exception as e:
            retry_count += 1
            if retry_count > max_retries:
                print(f"\nError downloading sample data: {str(e)}")
                print(f"Failed after {max_retries} retries")
                break
            print(f"\nRetry {retry_count}/{max_retries} after error: {str(e)}")
            time.sleep(retry_delay)
            continue
    
    pbar.close()
    
    if not all_candles:
        print("No sample data downloaded")
        return None
        
    # Convert to DataFrame
    df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    
    # Convert timestamp to datetime with UTC timezone
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    
    # Set timestamp as index
    df.set_index('timestamp', inplace=True)
    
    # Sort by index
    df.sort_index(inplace=True)
    
    # Remove duplicates
    df = df[~df.index.duplicated(keep='first')]
    
    return df

def timeframe_sort_key(tf, available_timeframes=None):
    """Sort key function for timeframes that ensures:
    1. Timeframes are grouped by their base timeframe
    2. Within each group, base timeframes come last
    3. Otherwise sorted by duration in minutes
    4. Special case: 1h comes last in the 1m group"""
    # Get base timeframe, considering available timeframes if provided
    base = get_base_timeframe(tf, available_timeframes) if available_timeframes else get_base_timeframe(tf)
    
    # Get base priorities (shorter timeframes have higher priority)
    base_priorities = {
        '1m': 300,  # Highest priority for minute-based
        '2m': 290,
        '3m': 280,
        '5m': 270,
        '10m': 260,
        '15m': 250,
        '30m': 240,
        '1h': 200,   # Hourly group
        '2h': 190,
        '3h': 180,
        '4h': 170,
        '6h': 160,
        '12h': 150,
        '1d': 100,   # Daily group
        '3d': 90,
        '1w': 80,
        '1mo': 70
    }
    
    # Default priority for unknown timeframes (sort by minutes)
    base_priority = base_priorities.get(base, 0)
    
    # Special handling for base timeframes (they should come last in their group)
    is_base = tf == base
    
    # Get duration in minutes for secondary sorting
    minutes = timeframe_to_minutes(tf)
    
    # Special handling for 1h - process it last among 1m-based timeframes
    if tf == '1h':
        minutes = float('inf')
    
    # Return tuple for sorting:
    # 1. Base priority (grouping by base timeframe)
    # 2. Whether it's a base timeframe (bases come last in their group)
    # 3. Duration in minutes (for sorting within groups)
    return (-base_priority, is_base, minutes)

def print_execution_time(start_timestamp):
    """Print the total execution time in minutes and seconds"""
    execution_time = time.time() - start_timestamp
    minutes = int(execution_time // 60)
    seconds = int(execution_time % 60)
    print(f"\nTotal execution time: {minutes} minutes and {seconds} seconds")

def main():
    start_timestamp = time.time()  # Start timing
    
    # Set global flags
    global verbose, keep_incomplete, folder_path
    
    args = parse_args()
    
    verbose = args.verbose
    keep_incomplete = args.keep_incomplete if hasattr(args, 'keep_incomplete') else False
    
    # Return code: 0 = success, 1 = no data but ran successfully, 2 = error
    
    # Handle sample mode
    if args.sample:
        exchange_id = args.sample[0].lower()
        sample_time = args.sample[1]
        
        # Validate time format
        try:
            hour, minute = map(int, sample_time.split(':'))
            if not (0 <= hour < 24 and 0 <= minute < 60):
                raise ValueError()
        except ValueError:
            print(f"Error: Invalid time format '{sample_time}'. Please use HH:MM format (e.g., 14:30).")
            return 2  # Error code
        
        # Set output directory
        if args.directory:
            folder_path = args.directory
        else:
            # Create samples directory with the correct structure: Data/{symbol}-{exchange}/Candles/Samples
            folder_path = os.path.join(default_folder_path, 
                                   f"{args.symbol}-{exchange_id.upper()}", 
                                   "Candles",
                                   "Samples")
        
        # Create data folder if it doesn't exist
        os.makedirs(folder_path, exist_ok=True)
        
        # Format the date and time for the filename
        date_str = pd.to_datetime(args.start_date).strftime('%Y%m%d')
        time_str = sample_time.replace(':', '')
        
        # Generate output filename
        output_filename = f"{args.symbol}-{exchange_id.upper()}_sample_{date_str}_{time_str}_1s.csv"
        output_path = os.path.join(folder_path, output_filename)
        
        # Download the sample data
        df = download_sample_data(
            args.symbol,
            exchange_id,
            args.start_date,
            sample_time,
            folder_path
        )
        
        if df is not None:
            # Save the data
            df.to_csv(output_path, date_format='%Y-%m-%d %H:%M:%S')
            print(f"Sample data saved to {output_path}")
            print_execution_time(start_timestamp)
            return 0  # Success - data found and saved
        else:
            print_execution_time(start_timestamp)
            return 1  # No data found but process ran successfully
    
    # Set folder path for non-sample modes
    if args.directory:
        folder_path = args.directory
    else:
        # Create default path with subfolders
        folder_path = os.path.join(default_folder_path, 
                               f"{args.symbol}-{default_exchange.upper()}", 
                               "Candles")
    
    # Convert dates to UTC timestamps
    start_time = pd.to_datetime(args.start_date).tz_localize('UTC')
    end_time = pd.to_datetime(args.end_date).tz_localize('UTC') if args.end_date else pd.Timestamp.now(tz='UTC')
    
    print(f"End date for this run: {end_time}")
    
    # Create data folder if it doesn't exist
    os.makedirs(folder_path, exist_ok=True)
    
    print(f"Starting download of {args.symbol} {default_exchange}")
    
    # Get existing timeframe files
    existing_timeframes = get_existing_timeframes(folder_path)
    
    if args.some:
        print("Note: Download progress bars will not reach 100% if you specify a starting date before the market existed.")
        
        # Download all standard timeframes directly, largest to smallest
        for timeframe in reversed(STANDARD_TIMEFRAMES):
            filename = f"{args.symbol}_{default_exchange}_{timeframe}.csv"
            filepath = os.path.join(folder_path, filename)
            
            df = download_historical_data(
                args.symbol,
                default_exchange,
                timeframe,
                start_time,
                end_time,
                folder_path,
                args
            )
            
            if df is not None:
                df.to_csv(filepath, date_format='%Y-%m-%d %H:%M:%S')
                if not keep_incomplete:
                    truncate_future_candles(filepath, end_time)
        
        print("Download complete.")
        print_execution_time(start_timestamp)
        return 0  # Success - data found and saved
    
    # Handle 1m data first (for both --all and default modes)
    one_min_file = f"{args.symbol}_{default_exchange}_1m.csv"
    one_min_path = os.path.join(folder_path, one_min_file)
    
    if args.all or not os.path.exists(one_min_path):
        print("Note: Download progress bars will not reach 100% if you specify a starting date before the market existed.")
                # Download full 1m dataset
        if not args.all and args.verbose:
            print(f"No 1m candle data file found in {folder_path}")
            print(f"while looking for file: {one_min_file}")
        
        df = download_historical_data(
            args.symbol,
            default_exchange,
            '1m',
            start_time,
            end_time,
            folder_path,
            args
        )
        
        if df is not None:
            df.to_csv(one_min_path, date_format='%Y-%m-%d %H:%M:%S')
            if not keep_incomplete:
                truncate_future_candles(one_min_path, end_time)
    
    elif os.path.exists(one_min_path):
        # Update existing 1m file
        if args.verbose:
            print("Found 1m candle data file.")
        print("Downloading data for the 1m timeframe...")
        
        # Get third last row from existing file
        third_last_line = get_third_last_line(one_min_path)
        if third_last_line:
            # Extract date from third last line and ensure UTC
            third_last_date = pd.to_datetime(third_last_line.split(',')[0]).tz_localize('UTC')
            
            df = download_historical_data(
                args.symbol,
                default_exchange,
                '1m',
                third_last_date,
                end_time,
                folder_path,
                args
            )
            
            if df is not None:
                # Convert first row of new data to string format matching file
                first_new_row = df.iloc[0:1].to_csv(date_format='%Y-%m-%d %H:%M:%S', header=False).strip()
                
                # Compare with third last line
                if first_new_row == third_last_line:
                    # Read all lines except last two
                    with open(one_min_path, 'r') as f:
                        header = f.readline()  # Get header
                        lines = f.readlines()[:-2]  # Get all lines except last two, excluding header
                    
                    # Write back header and all lines except last two
                    with open(one_min_path, 'w', newline='') as f:
                        f.write(header)
                        f.writelines(lines)
                        
                        # Skip the first row of new data (it's the overlap row) and write the rest
                        new_data = df.iloc[1:].to_csv(date_format='%Y-%m-%d %H:%M:%S', header=False)
                        f.write(new_data)
                else:
                    print("Error: Data mismatch when trying to update 1m file")
                    if args.verbose:
                        print(f"Expected: {third_last_line}")
                        print(f"Got: {first_new_row}")
                    return 2  # Error code
                if not keep_incomplete:
                    truncate_future_candles(one_min_path, end_time)
    
    if args.all:
        # Launch the converter script
        print("Launching converter script...")
        converter_script = os.path.join(os.path.dirname(__file__), 'historical_data_TF_converter.py')
        subprocess.run(['python', converter_script, '--path', os.path.dirname(one_min_path)])
        
        if not keep_incomplete:
            print("\nChecking for incomplete candles...")
            # Get all timeframe files in the directory
            folder_path = os.path.dirname(one_min_path)
            # Get all timeframe files and extract their timeframes
            timeframe_files = []
            for file in os.listdir(folder_path):
                if file.endswith('.csv'):
                    # Extract timeframe from filename
                    tf = file.split('_')[-1].replace('.csv', '')
                    if tf != '1m':  # Skip 1m file as it's already handled
                        timeframe_files.append((tf, os.path.join(folder_path, file)))
            
            # Sort files by timeframe size using existing sort function
            timeframe_files.sort(key=lambda x: timeframe_sort_key(x[0]))
            
            # Process files in order
            for tf, file_path in timeframe_files:
                #print(f"Checking {tf}...", end='', flush=True)
                truncate_future_candles(file_path, end_time)
                
        
        print_execution_time(start_timestamp)
        return 0  # Success - data found and saved
    
    # Get list of all available timeframes (including 1m)
    all_timeframes = set(existing_timeframes.keys())
    if args.some:
        all_timeframes.update(STANDARD_TIMEFRAMES)
    all_timeframes.discard('1m')  # We handle 1m separately
    
    # Sort timeframes by their optimal processing order
    sorted_timeframes = sorted(all_timeframes, key=lambda x: timeframe_sort_key(x, all_timeframes))
    
    # We'll process timeframes in order from smallest to largest
    # and keep track of available bases as we go
    processed_timeframes = set()
    timeframe_bases = {}
    
    # Sort timeframes by duration in minutes
    sorted_by_mins = sorted(all_timeframes, key=timeframe_to_minutes)
    
    # First pass: assign default bases
    for tf in sorted_by_mins:
        tf_mins = timeframe_to_minutes(tf)
        
        # Get all possible bases (already processed timeframes that are factors)
        possible_bases = [b for b in processed_timeframes 
                         if tf_mins % timeframe_to_minutes(b) == 0]
        
        # Find the largest suitable base
        best_base = None
        best_base_mins = 0
        for base in possible_bases:
            base_mins = timeframe_to_minutes(base)
            if base_mins > best_base_mins:
                best_base = base
                best_base_mins = base_mins
        
        # If no suitable base found, use the default
        if best_base is None:
            best_base = _get_default_base_timeframe(tf)
        
        timeframe_bases[tf] = best_base
        processed_timeframes.add(tf)
    
    # Group timeframes by their base
    timeframes_by_base = {}
    for tf, base in timeframe_bases.items():
        if base not in timeframes_by_base:
            timeframes_by_base[base] = []
        timeframes_by_base[base].append(tf)
    
    # Process timeframes in order of increasing duration
    # This ensures that when we process a timeframe, all its potential bases are already processed
    for tf in sorted_by_mins:
        base = timeframe_bases[tf]
        
        # Check if this timeframe needs updating
        tf_file_path = os.path.join(folder_path, get_candle_filename(args.symbol, default_exchange, tf))
        
        # If we're keeping incomplete candles, always update
        # Otherwise, check if this timeframe needs updating
        should_update = True
        if not keep_incomplete:
            should_update = needs_update(tf_file_path, tf, end_time)
            
        if should_update:
            print(f"Updating {tf} TF using {base} as a base...")
            update_timeframe_from_base(args.symbol, default_exchange, tf, folder_path)
            if not keep_incomplete:
                truncate_future_candles(tf_file_path, end_time)
        else:
            print(f"Skipping {tf} update - no new candle needed since last run")
    
    # Truncate future candles in all generated timeframe files
    if not args.keep_incomplete:
        # Get all timeframe files including any newly created ones
        all_tf_files = []
        for file in os.listdir(folder_path):
            if file.endswith('.csv'):
                # Extract timeframe from filename
                parts = file.split('_')
                if len(parts) >= 3:
                    tf = parts[-1].replace('.csv', '')
                    all_tf_files.append((tf, os.path.join(folder_path, file)))
        
        # Sort by timeframe length
        all_tf_files.sort(key=lambda x: timeframe_to_minutes(x[0]))
        
        # Truncate each file
        for tf, file_path in all_tf_files:
            if tf != '1m':  # 1m is already handled
                truncate_future_candles(file_path, end_time)
    
    print("Download complete.")
    print_execution_time(start_timestamp)
    return 0  # Success - data found and saved

if __name__ == "__main__":
    # Run the main function and get its return code
    exit_code = main()
    # Exit with the appropriate code
    sys.exit(exit_code if exit_code is not None else 0)
