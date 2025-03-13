#!/usr/bin/env python
import pandas as pd
from datetime import datetime, timedelta
import os
import argparse

# This script converts 1-minute candle data from a CSV file into various custom timeframes.
# It prompts the user to input the folder path containing the 1-minute candle CSV file.
# The script then resamples the data into the specified custom timeframes and saves the resulting data as new CSV files in the same folder.

# Default settings
default_symbol = "SOLUSDT"
default_exchange = "BINANCE"
default_folder_path = os.path.join('..', '..', 'Data', f"{default_symbol}-{default_exchange}", 'Candles')

# Define the custom timeframes in order from largest to smallest
timeframes = [
    # Monthly timeframes (roll over at end of month)
    '1M', '2M',
    # Multi-day timeframes (roll over on Jan 1 UTC)
    '36D', '35D', '34D', '33D', '32D', '31D', '30D', '29D', '28D', '27D', '26D', '25D',
    '24D', '23D', '22D', '21D', '20D', '19D', '18D', '17D', '16D', '15D', '14D', '13D',
    '12D', '11D', '10D', '9D', '8D', '7D', '6D', '5D', '4D', '3D', '2D', '1D',
    # Weekly timeframes (never roll over)
    '1W', '2W', '3W', '4W', '5W', '6W',
    # Sub-daily timeframes (roll over at midnight UTC)
    '12h', '8h', '6h', '4h', '3h', '2h', '1h',
    '288m', '160m', '144m', '96m', '90m', '80m', '72m', '48m', '45m', '40m', '32m', '30m',
    '24m', '20m', '18m', '16m', '15m', '12m', '10m', '9m', '8m', '6m', '5m', '4m', '3m', '2m'
]

# **************************************************************************************************

def parse_args():
    parser = argparse.ArgumentParser(description='Convert 1-minute candle data to various timeframes')
    parser.add_argument('-p', '--path', type=str, help='Override the default folder path')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # If no path argument was provided, prompt for the folder path
    if args.path is None:
        folder_path = input(f"\n\rEnter the folder path containing the 1-minute candle CSV file (default: {default_folder_path}): ") or default_folder_path
    else:
        folder_path = args.path

    # Find the input file in the folder
    input_file = None
    for file in os.listdir(folder_path):
        if file.endswith("_1m.csv"):
            input_file = os.path.join(folder_path, file)
            break

    if input_file is None:
        raise FileNotFoundError("No file ending in '_1m.csv' found in the specified folder.")

    print("\nReading 1m file (this may take a while for large files)...")

    # Read the 1m data
    df = pd.read_csv(input_file, index_col='timestamp', parse_dates=True,
                     dtype={'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})

    # Get the base filename pattern (everything before _1m.csv)
    base_filename = os.path.basename(input_file).rsplit('_1m.csv', 1)[0]

    # Function to resample the data with proper rollover handling
    def resample_data(df, rule):
        df.index = pd.to_datetime(df.index)
        resampled = df.resample(rule, closed='left', label='left').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # Ensure float type for all numeric columns
        for col in ['open', 'high', 'low', 'close', 'volume']:
            resampled[col] = resampled[col].astype(float)
            
        return resampled

    # Function to handle year-end rollover for multi-day timeframes
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
        return pd.concat(result) if result else pd.DataFrame()

    # Function to handle midnight UTC rollover for sub-daily timeframes
    def handle_midnight_rollover(df, rule):
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

    # Function to handle weekly data (always start on Monday)
    def handle_weekly_rollover(df, rule):
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

    # Function to handle monthly data (always start at month begin)
    def handle_monthly_rollover(df, rule):
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
                    
            df = pd.concat(result) if result else pd.DataFrame()
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

    # Function to check if a timeframe divides evenly into a day
    def divides_evenly_into_day(tf):
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

    # Ensure the folder exists
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Resample and save the data for each custom timeframe
    for tf in timeframes:
        print(f"Processing timeframe {tf}...", end='', flush=True)
        
        # Convert timeframe to pandas resample rule
        rule = tf.replace('m', 'min').replace('h', 'h').replace('D', 'D').replace('M', 'M').replace('W', 'W')
        
        # Apply appropriate rollover handling based on timeframe type
        if tf.endswith('W'):  # Weekly timeframes - always start on Monday
            combined_df = handle_weekly_rollover(df, rule)
        elif tf.endswith('M'):  # Monthly timeframes - always start at month begin
            combined_df = handle_monthly_rollover(df, rule)
        elif tf.endswith('D'):  # Multi-day timeframes
            combined_df = handle_year_end_rollover(df, rule)
        elif (tf.endswith('m') or tf.endswith('h')) and not divides_evenly_into_day(tf):  
            # Only use midnight rollover for timeframes that don't divide evenly into a day
            combined_df = handle_midnight_rollover(df, rule)
        else:  # Timeframes that divide evenly into a day
            combined_df = resample_data(df, rule)

        if not combined_df.empty:
            # Save the combined data for each timeframe using consistent naming scheme
            # Convert monthly 'M' suffix to 'mo' in the filename to avoid NTFS case-insensitivity conflicts
            save_tf = tf.replace('M', 'mo') if tf.endswith('M') else tf
            file_name = f"{folder_path}/{base_filename}_{save_tf}.csv"
            combined_df.to_csv(file_name, index=True, date_format='%Y-%m-%d %H:%M:%S')
            print(f" saved to {file_name}")
        else:
            print(f" Warning: No data generated")

    print('Custom timeframe data creation complete!')

if __name__ == "__main__":
    main()