# This script produces multiple summaries of instances by timeframe, with filtering based on group sizes.
# It calculates various metrics and saves the results to CSV files in a Summary folder.

import pandas as pd
import os
from datetime import datetime, timedelta

# Default paths (change these to your actual paths). You can put them here or enter them when prompted.
default_instances_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Processed', 'CompleteSet')

# Use this to exclude recent instances. For example, say you want to look at win rates historically; it makes sense to 
# exclude instances that are too recent to have been completed. In this example, we take the cutoff date to be 60 days
# before the end of 2024. 
cutoff_date = datetime(2024, 12, 31) - timedelta(days=60)

# Function to calculate the time difference in "x days, y hours, z minutes"
def format_timedelta(td):
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{days} days, {hours} hours, {minutes} minutes"

def filter_by_group_size(df, filter_type, group_sizes):
    """
    Filter DataFrame based on group size criteria using pre-loaded group sizes
    """
    # Check if we have a group_id column (new format) or tags column (old format)
    if 'group_id' in df.columns:
        # Create a helper function to convert group_id to group_tag format if needed
        def get_group_tag(group_id):
            if pd.isna(group_id) or group_id == 'NA':
                return None
            # If group_id is already in group_X format, keep it
            if isinstance(group_id, str) and group_id.startswith('group_'):
                return group_id
            # Otherwise convert it to group_X format
            try:
                return f"group_{int(group_id)}"
            except (ValueError, TypeError):
                return None
                
        # Add group_tag column for filtering
        df['group_tag'] = df['group_id'].apply(get_group_tag)
    else:
        # Extract group tags from the tags column (old format)
        if 'tags' not in df.columns:
            df['tags'] = ''
        
        # Create a helper function to get group tag from tags string
        def get_group_tag(tags_str):
            if not tags_str or pd.isna(tags_str):
                return None
            tags = tags_str.split(',')
            for tag in tags:
                if tag.startswith('group_'):
                    return tag
            return None
        
        # Add group_tag column for filtering
        df['group_tag'] = df['tags'].apply(get_group_tag)
    
    if filter_type == 'SINGLES':
        return df[df['group_tag'].isna()]
    elif filter_type == 'ALL_GROUPS':
        return df[df['group_tag'].notna()]
    elif filter_type == 'PAIRS':
        return df[df['group_tag'].isin(group_sizes[group_sizes == 2].index)]
    elif filter_type == 'TRIPLES':
        return df[df['group_tag'].isin(group_sizes[group_sizes == 3].index)]
    elif filter_type == 'QUADS':
        return df[df['group_tag'].isin(group_sizes[group_sizes == 4].index)]
    elif filter_type == 'FIVE_OR_MORE':
        return df[df['group_tag'].isin(group_sizes[group_sizes >= 5].index)]
    elif filter_type == 'FOUR_OR_MORE':
        return df[df['group_tag'].isin(group_sizes[group_sizes >= 4].index)]
    elif filter_type == 'THREE_OR_MORE':
        return df[df['group_tag'].isin(group_sizes[group_sizes >= 3].index)]
    else:  # ALL
        return df

def filter_by_move_size(df, filter_type):
    """
    Filter DataFrame based on move size criteria
    """
    if 'entry' not in df.columns or 'target' not in df.columns:
        print("Warning: entry or target columns missing, cannot filter by move size")
        return df
    
    move_sizes = calculate_move_size(df)
    
    if filter_type == 'UNDER_1_PERCENT':
        return df[move_sizes < 1]
    elif filter_type == 'ONE_TO_TWO_PERCENT':
        return df[(move_sizes >= 1) & (move_sizes < 2)]
    elif filter_type == 'TWO_TO_FIVE_PERCENT':
        return df[(move_sizes >= 2) & (move_sizes < 5)]
    elif filter_type == 'FIVE_OR_MORE_PERCENT':
        return df[move_sizes >= 5]
    else:  # ALL
        return df

def calculate_move_size(df):
    """
    Calculate the absolute percentage change between entry and target prices
    """
    return abs((df['target'] - df['entry']) / df['entry'] * 100)

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

def create_summary(df, output_file):
    """
    Create summary for the given DataFrame and save to output file
    """
    summary_data = []
    
    # Sort timeframes by converting to minutes first
    timeframes = sorted(df['timeframe'].unique(), key=timeframe_to_minutes)
    
    # Group the DataFrame by timeframe in the sorted order
    for timeframe in timeframes:
        timeframe_df = df[df['timeframe'] == timeframe]
        df_completed = timeframe_df.dropna(subset=['Completed Date'])

        # Calculate required metrics
        total_count = len(timeframe_df)
        count_longs = len(timeframe_df[timeframe_df['direction'] == 'long'])
        count_shorts = len(timeframe_df[timeframe_df['direction'] == 'short'])
        count_pending = len(timeframe_df[timeframe_df['Status'] == 'Pending'])
        count_active = len(timeframe_df[timeframe_df['Status'] == 'Active'])
        count_completed = len(timeframe_df[timeframe_df['Status'] == 'Completed'])

        # Win rates
        win_rate = (count_completed / (count_completed + count_active)) * 100 if (count_completed + count_active) > 0 else 0
        active_date_cutoff = timeframe_df[timeframe_df['Active Date'] < cutoff_date]
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
        time_confirm_to_active = timeframe_df['Active Date'] - timeframe_df['confirm_date']
        time_active_to_completed = timeframe_df['Completed Date'] - timeframe_df['Active Date']
        time_active_to_maxdrawdown = timeframe_df['MaxDrawdown Date'] - timeframe_df['Active Date']
        
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
        avg_maxdrawdown = timeframe_df['MaxDrawdown'].mean() if 'MaxDrawdown' in timeframe_df.columns else 0
        avg_maxfib = timeframe_df['MaxFib'].mean() if 'MaxFib' in timeframe_df.columns else 0
        
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
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    summary_df.to_csv(output_file, index=False)
    print(f'Summary saved to {output_file}')

def extract_checkpoint_date(folder_path):
    """
    Extract date from checkpoint folder name if present.
    Expected format: path/to/checkpoint_YYYYMMDD/
    Returns None if no date found or not a checkpoint folder.
    """
    folder_name = os.path.basename(folder_path)
    if 'checkpoint' in folder_name.lower():
        # Try to find YYYYMMDD pattern in the folder name
        import re
        date_match = re.search(r'(\d{8})', folder_name)
        if date_match:
            date_str = date_match.group(1)
            try:
                return datetime.strptime(date_str, '%Y%m%d')
            except ValueError:
                return None
    return None

def get_timestamped_folder(base_path):
    """Create a unique timestamped folder to avoid permission errors"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(base_path, f"Summary_{timestamp}")

def main():
    # Prompt for the input path
    instances_folder = input(f"Enter the folder path containing the instance CSV files (default: {default_instances_folder}): ") or default_instances_folder
    
    # Create unique timestamped Summary directory in the input folder to avoid permission issues
    summary_folder = get_timestamped_folder(instances_folder)
    try:
        os.makedirs(summary_folder, exist_ok=True)
    except PermissionError:
        print("Permission error creating summary folder. Please ensure you have write access to the directory.")
        return
    print(f"Summary files will be saved to: {summary_folder}")
    
    # Check if this is a checkpoint folder and get the cutoff date
    checkpoint_date = extract_checkpoint_date(instances_folder)
    if checkpoint_date:
        print(f"Checkpoint folder detected. Excluding instances after {checkpoint_date.strftime('%Y-%m-%d')}")
    
    # Load group statistics first
    group_stats_path = os.path.join(instances_folder, 'group_statistics.csv')
    if not os.path.exists(group_stats_path):
        print("Warning: group_statistics.csv not found in the input directory")
        print("Will continue without group filtering capabilities")
        group_sizes = pd.Series()
    else:
        # Read group statistics and ensure required columns exist
        try:
            group_stats = pd.read_csv(group_stats_path)
            # Check if we have the new format (group_tag) or old format
            if 'group_tag' in group_stats.columns and 'total_instances' in group_stats.columns:
                group_sizes = group_stats.set_index('group_tag')['total_instances']
                print(f"Found {len(group_sizes)} groups in group_statistics.csv")
            else:
                # Try alternative column names
                print("Standard column names not found in group_statistics.csv, trying alternatives...")
                if 'total_instances' not in group_stats.columns and 'instances' in group_stats.columns:
                    # Rename to expected column
                    group_stats['total_instances'] = group_stats['instances']
                
                # Check for group identifying column
                if 'group_id' in group_stats.columns:
                    # Convert to group_tag format if needed
                    group_stats['group_tag'] = group_stats['group_id'].apply(
                        lambda x: f"group_{x}" if not str(x).startswith('group_') else x
                    )
                    group_sizes = group_stats.set_index('group_tag')['total_instances']
                    print(f"Found {len(group_sizes)} groups using group_id column")
                else:
                    print("Could not find group identification column in group_statistics.csv")
                    group_sizes = pd.Series()
        except Exception as e:
            print(f"Error loading group_statistics.csv: {e}")
            print("Will continue without group filtering capabilities")
            group_sizes = pd.Series()
    
    # Get base filename for summaries
    base_filename = 'TF_Instance_Summary'
    
    # List of filter types and their corresponding suffixes
    filter_types = [
        ('ALL', '_ALL'),
        ('SINGLES', '_SINGLES'),
        ('ALL_GROUPS', '_ALL_GROUPS'),
        ('PAIRS', '_PAIRS'),
        ('TRIPLES', '_TRIPLES'),
        ('QUADS', '_QUADS'),
        ('FIVE_OR_MORE', '_FIVE_OR_MORE'),
        ('FOUR_OR_MORE', '_FOUR_OR_MORE'),
        ('THREE_OR_MORE', '_THREE_OR_MORE'),
        ('UNDER_1_PERCENT', '_UNDER_1_PERCENT'),
        ('ONE_TO_TWO_PERCENT', '_ONE_TO_TWO_PERCENT'),
        ('TWO_TO_FIVE_PERCENT', '_TWO_TO_FIVE_PERCENT'),
        ('FIVE_OR_MORE_PERCENT', '_FIVE_OR_MORE_PERCENT')
    ]
    
    # Read and combine all CSV files from the input directory
    all_data = []
    for filename in os.listdir(instances_folder):
        if filename.endswith('.csv') and filename != 'group_statistics.csv':
            file_path = os.path.join(instances_folder, filename)
            df = pd.read_csv(file_path, parse_dates=['confirm_date', 'Active Date', 'Completed Date', 'MaxDrawdown Date'])
            
            # Filter out instances after checkpoint date if applicable
            if checkpoint_date:
                df = df[df['confirm_date'] <= checkpoint_date]
                
            # Only include the DataFrame if it's not empty after filtering
            if not df.empty:
                # Extract timeframe from filename and add it as a column
                timeframe = filename.split('_')[-1].replace('.csv', '')
                if timeframe == 'multi_day_timeframes':
                    timeframe = 'multi-day'
                df['timeframe'] = timeframe
                
                all_data.append(df)
    
    if not all_data:
        print("No CSV files found in the input directory.")
        return
    
    # Combine all DataFrames
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Create summaries for each filter type
    for filter_type, suffix in filter_types:
        output_file = os.path.join(summary_folder, f"{base_filename}{suffix}.csv")
        
        # Apply appropriate filter based on type
        if filter_type in ['UNDER_1_PERCENT', 'ONE_TO_TWO_PERCENT', 'TWO_TO_FIVE_PERCENT', 'FIVE_OR_MORE_PERCENT']:
            filtered_df = filter_by_move_size(combined_df, filter_type)
        else:
            filtered_df = filter_by_group_size(combined_df, filter_type, group_sizes)
            
        if not filtered_df.empty:
            create_summary(filtered_df, output_file)
        else:
            print(f"No data for {filter_type} filter, skipping summary file {output_file}")

if __name__ == "__main__":
    main()
