# This script produces a summary of instances by timeframe.  It calculates various metrics and saves the results to a CSV file.

import pandas as pd
import os
from datetime import datetime, timedelta

# Default paths (change these to your actual paths). You can put them here or enter them when prompted.
default_instances_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Processed', 'CompleteSet')
default_summary_file = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Processed', 'TF_Instance_Summary.csv')

# Use this to exclude recent instances.  For example, say you want to look at win rates historically; it makes sense to 
# exclude instances that are too recent to have been completed.  In this example, we take the cutoff date to be 60 days
# before the end of 2024. 
cutoff_date = datetime(2024, 12, 31) - timedelta(days=60)

# **************************************************************************************************
# Prompt for the input and output paths
instances_folder = input(f"Enter the folder path containing the instance CSV files (default: {default_instances_folder}): ") or default_instances_folder
summary_file = input(f"Enter the output file path for the summary CSV file (default: {default_summary_file}): ") or default_summary_file

# Function to calculate the time difference in "x days, y hours, z minutes"
def format_timedelta(td):
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{days} days, {hours} hours, {minutes} minutes"

def timeframe_to_minutes(timeframe):
    """
    Convert timeframe string to minutes for sorting
    Examples: '15m' -> 15, '1h' -> 60, '4h' -> 240, '1d/1D' -> 1440, 'multi-day' -> 10080, '1w/1W' -> 10080
    """
    # Handle multi-day as a special case, placing it after daily timeframes
    if timeframe.lower() == 'multi-day':
        # Place multi-day after 1d but before weekly timeframes
        return 6 * 1440  # 6 days * 1440 minutes per day (just under 1 week)
    
    # Convert timeframe to lowercase for case-insensitive comparison
    tf_lower = timeframe.lower()
    
    try:
        if tf_lower.endswith('m'):
            return int(tf_lower[:-1])
        elif tf_lower.endswith('h'):
            return int(tf_lower[:-1]) * 60
        elif tf_lower.endswith('d'):
            return int(tf_lower[:-1]) * 1440
        elif tf_lower.endswith('w'):
            return int(tf_lower[:-1]) * 10080  # 1 week = 7 days * 1440 minutes
        else:
            # Put unknown formats at the end
            return float('inf')
    except ValueError:
        # Put invalid formats at the end
        return float('inf')

# Function to create the summary
def create_summary():
    # Collect data for all timeframes
    timeframe_data = {}
    
    for filename in os.listdir(instances_folder):
        if filename.endswith('.csv'):
            instance_filepath = os.path.join(instances_folder, filename)
            df = pd.read_csv(instance_filepath, parse_dates=['confirm_date', 'Active Date', 'Completed Date', 'MaxDrawdown Date'])

            # Skip empty files or files with no data
            if df.empty:
                continue

            # Exclude instances without a completed date
            df_completed = df.dropna(subset=['Completed Date'])

            # Extract timeframe from filename
            timeframe = filename.split('_')[-1].replace('.csv', '')
            if timeframe == 'multi_day_timeframes':
                timeframe = 'multi-day'
                
            # Store the DataFrame for this timeframe
            timeframe_data[timeframe] = {
                'df': df,
                'df_completed': df_completed
            }
    
    # Create summary data in sorted order
    summary_data = []
    # Sort timeframes by converting to minutes first - this will ensure multi-day is at the end
    timeframes = sorted(timeframe_data.keys(), key=timeframe_to_minutes)
    
    for timeframe in timeframes:
        df = timeframe_data[timeframe]['df']
        df_completed = timeframe_data[timeframe]['df_completed']

        # Skip if no data for this timeframe
        if len(df) == 0:
            continue

        # Calculate required metrics
        total_count = len(df)
        count_longs = len(df[df['direction'] == 'long'])
        count_shorts = len(df[df['direction'] == 'short'])
        count_pending = len(df[df['Status'] == 'Pending'])
        count_active = len(df[df['Status'] == 'Active'])
        count_completed = len(df[df['Status'] == 'Completed'])

        # Win rates
        win_rate = (count_completed / (count_completed + count_active)) * 100 if (count_completed + count_active) > 0 else 0
        active_date_cutoff = df[df['Active Date'] < cutoff_date]
        adjusted_win_rate = (len(active_date_cutoff[active_date_cutoff['Status'] == 'Completed']) / len(active_date_cutoff)) * 100 if len(active_date_cutoff) > 0 else 0

        # Round win rates to 4 decimal places
        win_rate = round(win_rate, 4)
        adjusted_win_rate = round(adjusted_win_rate, 4)

        # Count of reached fib levels and their percentages
        fib_levels = ['0.5', '0.0', '-0.5', '-1.0']
        fib_counts = {f'Reached{level}': len(df_completed[df_completed[f'Reached{level}'] == 1]) for level in fib_levels}
        fib_percentages = {f'Reached{level}_Percent': (fib_counts[f'Reached{level}'] / len(df_completed)) * 100 if len(df_completed) > 0 else 0 for level in fib_levels}
        
        # Round fib percentages to 4 decimal places
        for key in fib_percentages:
            fib_percentages[key] = round(fib_percentages[key], 4)

        # Average times and formats
        time_confirm_to_active = df['Active Date'] - df['confirm_date']
        time_active_to_completed = df['Completed Date'] - df['Active Date']
        time_active_to_maxdrawdown = df['MaxDrawdown Date'] - df['Active Date']
        
        avg_time_confirm_to_active_hours = time_confirm_to_active.mean().total_seconds() / 3600 if not time_confirm_to_active.empty else 0
        avg_time_confirm_to_active_str = format_timedelta(time_confirm_to_active.mean()) if not time_confirm_to_active.empty else "0 days, 0 hours, 0 minutes"
        avg_time_active_to_completed_hours = time_active_to_completed.mean().total_seconds() / 3600 if not time_active_to_completed.empty else 0
        avg_time_active_to_completed_str = format_timedelta(time_active_to_completed.mean()) if not time_active_to_completed.empty else "0 days, 0 hours, 0 minutes"
        avg_time_active_to_maxdrawdown_hours = time_active_to_maxdrawdown.mean().total_seconds() / 3600 if not time_active_to_maxdrawdown.empty else 0
        avg_time_active_to_maxdrawdown_str = format_timedelta(time_active_to_maxdrawdown.mean()) if not time_active_to_maxdrawdown.empty else "0 days, 0 hours, 0 minutes"

        # Round time metrics to 4 decimal places
        avg_time_confirm_to_active_hours = round(avg_time_confirm_to_active_hours, 4)
        avg_time_active_to_completed_hours = round(avg_time_active_to_completed_hours, 4)
        avg_time_active_to_maxdrawdown_hours = round(avg_time_active_to_maxdrawdown_hours, 4)

        # Average MaxDrawdown and MaxFib
        avg_maxdrawdown = df['MaxDrawdown'].mean() if 'MaxDrawdown' in df.columns else 0
        avg_maxfib = df['MaxFib'].mean() if 'MaxFib' in df.columns else 0
        
        # Round avg_maxfib to 4 decimal places
        avg_maxfib = round(avg_maxfib, 4)

        # Compile summary data
        summary_data.append([
            timeframe, total_count, count_longs, count_shorts, count_pending, count_active, count_completed, 
            win_rate, adjusted_win_rate, 
            fib_counts['Reached0.5'], fib_percentages['Reached0.5_Percent'], 
            fib_counts['Reached0.0'], fib_percentages['Reached0.0_Percent'], 
            fib_counts['Reached-0.5'], fib_percentages['Reached-0.5_Percent'], 
            fib_counts['Reached-1.0'], fib_percentages['Reached-1.0_Percent'], 
            avg_time_confirm_to_active_hours, avg_time_confirm_to_active_str, 
            avg_time_active_to_completed_hours, avg_time_active_to_completed_str, 
            avg_maxdrawdown, avg_time_active_to_maxdrawdown_hours, avg_time_active_to_maxdrawdown_str, avg_maxfib
        ])

    # Create summary DataFrame
    summary_df = pd.DataFrame(summary_data, columns=[
        'timeframe', 'total_count', 'count_longs', 'count_shorts', 'count_pending', 'count_active', 'count_completed', 
        'winRate', 'adjusted_winRate', 
        'Reached0.5_Count', 'Reached0.5_Percent', 
        'Reached0.0_Count', 'Reached0.0_Percent', 
        'Reached-0.5_Count', 'Reached-0.5_Percent', 
        'Reached-1.0_Count', 'Reached-1.0_Percent', 
        'avg_time_confirm_to_active_hours', 'avg_time_confirm_to_active_str', 
        'avg_time_active_to_completed_hours', 'avg_time_active_to_completed_str', 
        'avg_maxdrawdown', 'avg_time_active_to_maxdrawdown_hours', 'avg_time_active_to_maxdrawdown_str', 'avg_maxfib'
    ])

    # Save the summary DataFrame to a CSV file
    summary_df.to_csv(summary_file, index=False)
    print(f'Summary saved to {summary_file}')

create_summary()
