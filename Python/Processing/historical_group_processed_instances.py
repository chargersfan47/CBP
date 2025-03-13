import pandas as pd
import os
import gc
import time
import sys
import traceback
import shutil
from datetime import datetime, timedelta
from tqdm import tqdm

# Constants for grouping
SIMILARITY_THRESHOLD = 0.983  # Minimum overlap percentage to consider instances similar
MIN_GROUP_SIZE = 2  # Minimum number of instances to form a group

# Configuration flags for different grouping approaches
BIDIRECTIONAL_GROUPING = False  # If True, requires mutual overlap from both instances' perspectives
IGNORE_TEMPORAL_CONSTRAINTS = False  # If True, allows grouping instances across all time periods

# Default paths
default_input_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Processed', 'CompleteSet')
# Dynamically create output folder name based on settings
output_folder_suffix = ""
if BIDIRECTIONAL_GROUPING:
    output_folder_suffix += "-bidirectional"
if IGNORE_TEMPORAL_CONSTRAINTS:
    output_folder_suffix += "-alltime"
default_output_folder = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Processed', 'Grouped' + output_folder_suffix + '-' + str(SIMILARITY_THRESHOLD))

# Memory optimization functions
def get_memory_usage():
    """Get memory usage of current process in MB"""
    import psutil
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return memory_info.rss / 1024 / 1024

def force_gc(message=""):
    """Force garbage collection and report memory usage"""
    pre_gc_mem = get_memory_usage()
    gc.collect()
    post_gc_mem = get_memory_usage()
    print(f"Memory {message}: {pre_gc_mem:.2f} MB -> {post_gc_mem:.2f} MB (freed: {pre_gc_mem - post_gc_mem:.2f} MB)")

def print_debug(*args, **kwargs):
    """Print debug info and flush immediately"""
    print(*args, **kwargs)
    sys.stdout.flush()

def calculate_overlap_percentage(price1, target1, price2, target2):
    """
    Calculate the percentage of overlap between two price ranges
    Returns a value between 0 and 1 representing the overlap percentage
    
    If BIDIRECTIONAL_GROUPING is True, returns the minimum overlap percentage from both perspectives
    Otherwise, returns the overlap percentage relative to the smaller range
    """
    try:
        # Convert inputs to float for calculation
        p1 = float(price1)
        t1 = float(target1)
        p2 = float(price2)
        t2 = float(target2)
        
        # Determine direction (long or short)
        long1 = t1 > p1
        long2 = t2 > p2
        
        # If directions differ, there's no overlap
        if long1 != long2:
            return 0.0
            
        # Ensure we're comparing low to high regardless of direction
        if not long1:  # For shorts
            p1, t1 = t1, p1
            p2, t2 = t2, p2
            
        # Calculate price ranges
        range1 = abs(t1 - p1)
        range2 = abs(t2 - p2)
        
        # Handle edge case of zero range
        if range1 <= 0 or range2 <= 0:
            return 0.0
            
        # Find overlap range
        overlap_start = max(p1, p2)
        overlap_end = min(t1, t2)
        
        # If ranges don't overlap
        if overlap_start >= overlap_end:
            return 0.0
            
        # Calculate overlap length
        overlap_length = overlap_end - overlap_start
        
        if BIDIRECTIONAL_GROUPING:
            # Calculate overlap as percentage of BOTH ranges for bidirectional grouping
            # This ensures we don't group a small range with a much larger one
            overlap_percentage1 = overlap_length / range1
            overlap_percentage2 = overlap_length / range2
            
            # Return the minimum overlap percentage (stricter requirement)
            return min(overlap_percentage1, overlap_percentage2)
        else:
            # Calculate overlap as percentage of the smaller range
            # We use the smaller range as the denominator to ensure we don't 
            # consider a small range inside a much larger one as low overlap
            smaller_range = min(range1, range2)
            overlap_percentage = overlap_length / smaller_range
            
            return overlap_percentage
        
    except Exception as e:
        print_debug(f"Error in overlap calculation: {e}")
        return 0.0

def load_instances_from_file(filepath):
    """
    Load instances CSV with memory optimizations
    """
    # print_debug(f"Loading instances from {filepath}...")
    
    try:
        # Read CSV file
        df = pd.read_csv(filepath)
        
        # Handle date columns properly - parse dates instead of trying to convert to float
        date_columns = ['confirm_date', 'Active Date', 'Completed Date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Apply memory optimizations for numeric columns
        for col in df.columns:
            if df[col].dtype == 'float64':
                df[col] = df[col].astype('float32')
            elif df[col].dtype == 'int64':
                df[col] = df[col].astype('int32')
            elif df[col].dtype == 'object' and col not in date_columns:
                # Convert string columns to category type for memory efficiency
                if df[col].nunique() < len(df) / 2:  # Only if cardinality is low
                    df[col] = df[col].astype('category')
        
        # Extract timeframe from filename
        filename = os.path.basename(filepath)
        timeframe = filename.split('_')[-1].split('.')[0]
        
        # Add timeframe column to DataFrame
        df['timeframe'] = timeframe
        
        # Add file path for reference
        df['source_file'] = filename
        
        # Convert DataFrame to list of dictionaries
        instances = df.to_dict('records')
        
        # Return the list of instances
        return instances
        
    except Exception as e:
        print_debug(f"Error loading {filepath}: {e}")
        return []  # Return empty list on error

def write_instance_to_file(instance, output_folder):
    """
    Write instance to the appropriate timeframe file in the output folder
    """
    timeframe = instance.get('timeframe', 'unknown')
    
    # Create output filename
    filename = f"grouped_instances_1v1_SOLUSDT_binance_{timeframe}.csv"
    filepath = os.path.join(output_folder, filename)
    
    # Create file if it doesn't exist and write header
    if not os.path.exists(filepath):
        # Get column names from instance dictionary
        # Skip 'index' column as it's internal for processing
        columns = [col for col in instance.keys() if col != 'index']
        
        with open(filepath, 'w', newline='') as f:
            header = ','.join(columns)
            f.write(header + '\n')
    
    # Append instance to file
    with open(filepath, 'a', newline='') as f:
        # Skip 'index' column as it's internal for processing
        values = [str(instance.get(col, '')) for col in instance.keys() if col != 'index']
        line = ','.join(values)
        f.write(line + '\n')

def write_group_stats(group_stats, output_folder, all_instances, instances_by_id, instance_to_group):
    """
    Write detailed group statistics to a CSV file in the output folder
    
    Parameters:
    group_stats (list): List of dictionaries containing basic group statistics
    output_folder (str): Folder to write the statistics file
    all_instances (list): List of all instances
    instances_by_id (dict): Dictionary mapping instance IDs to instance data
    instance_to_group (dict): Dictionary mapping instance IDs to group IDs
    """
    if not group_stats:
        print_debug("No group statistics to write.")
        return

    # Enhanced stats will contain all the detailed information for each group
    enhanced_stats = []
    
    # Process each group with a progress bar
    print_debug("Calculating detailed group statistics...")
    with tqdm(total=len(group_stats), desc="Calculating group stats") as progress_bar:
        for group_stat in group_stats:
            group_id = group_stat['group_id']
            direction = group_stat['direction']
            
            # Find all instances in this group
            group_instances = []
            instance_ids = []
            
            for instance_id, inst_group_id in instance_to_group.items():
                if inst_group_id == group_id:
                    instance = instances_by_id.get(instance_id)
                    if instance:
                        group_instances.append(instance)
                        instance_ids.append(instance_id)
            
            # Collect timeframes
            timeframes = set()
            for instance in group_instances:
                tf = instance.get('timeframe', '')
                if tf:
                    timeframes.add(tf)
            
            # Sort timeframes
            timeframes = sorted(list(timeframes))
            timeframes_str = '|'.join(timeframes)
            
            # Calculate dates and times
            activation_dates = []
            confirm_dates = []
            completion_dates = []
            
            for instance in group_instances:
                active_date = instance.get('Active Date')
                if pd.notna(active_date):
                    activation_dates.append(active_date)
                
                confirm_date = instance.get('confirm_date')
                if pd.notna(confirm_date):
                    confirm_dates.append(confirm_date)
                
                completed_date = instance.get('Completed Date')
                if pd.notna(completed_date):
                    completion_dates.append(completed_date)
            
            # Calculate group statistics based on dates
            if activation_dates:
                first_activation = min(activation_dates)
                last_activation = max(activation_dates)
            else:
                first_activation = pd.NaT
                last_activation = pd.NaT
            
            first_confirm_date = min(confirm_dates) if confirm_dates else pd.NaT
            first_timeframe = timeframes[0] if timeframes else ''
            
            # Determine if all instances are completed
            total_instances = len(group_instances)
            completed_instances = sum(1 for inst in group_instances if pd.notna(inst.get('Completed Date')))
            
            # Group status
            group_status = 'completed' if completed_instances == total_instances else 'active'
            
            # Completion date
            completion_date = max(completion_dates) if completion_dates else pd.NaT
            
            # Time durations
            if pd.notna(first_activation) and pd.notna(completion_date):
                first_active_2_completed = completion_date - first_activation
                first_active_2_completed_str = str(first_active_2_completed).split('.')[0]  # Format as HH:MM:SS
            else:
                first_active_2_completed_str = ''
            
            if pd.notna(first_confirm_date) and pd.notna(completion_date):
                first_conf_2_completed = completion_date - first_confirm_date
                first_conf_2_completed_str = str(first_conf_2_completed).split('.')[0]  # Format as HH:MM:SS
            else:
                first_conf_2_completed_str = ''
            
            if pd.notna(first_activation) and pd.notna(last_activation) and first_activation != last_activation:
                confirm_gap = last_activation - first_activation
                confirm_gap_str = str(confirm_gap).split('.')[0]  # Format as HH:MM:SS
            else:
                confirm_gap_str = '00:00:00'
            
            # Create instance IDs string (format: timestamp_timeframe_1v1_direction)
            formatted_instance_ids = []
            for instance in group_instances:
                instance_id = instance.get('instance_id', '')
                tf = instance.get('timeframe', '')
                dir_short = instance.get('direction', '')
                confirm = instance.get('confirm_date')
                
                if pd.notna(confirm):
                    confirm_str = confirm.strftime('%Y-%m-%d %H:%M:%S')
                    formatted_id = f"{confirm_str}_{tf}_1v1_{dir_short}"
                    formatted_instance_ids.append(formatted_id)
            
            instance_ids_str = '|'.join(formatted_instance_ids)
            
            # Format timestamps
            def format_timestamp(ts):
                return ts.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(ts) else ''
            
            # Create enhanced stats record
            enhanced_stat = {
                'group_tag': f"group_{group_id}",
                'direction': direction,
                'total_instances': total_instances,
                'completed_instances': completed_instances,
                'first_activation': format_timestamp(first_activation),
                'last_activation': format_timestamp(last_activation),
                'first_confirm_date': format_timestamp(first_confirm_date),
                'first_timeframe': first_timeframe,
                'timeframes': timeframes_str,
                'group_status': group_status,
                'completion_date': format_timestamp(completion_date),
                'first_active_2_completed': first_active_2_completed_str,
                'first_conf_2_completed': first_conf_2_completed_str,
                'confirm_gap': confirm_gap_str,
                'instance_ids': instance_ids_str
            }
            
            enhanced_stats.append(enhanced_stat)
            progress_bar.update(1)
    
    # Create DataFrame and write to CSV
    if enhanced_stats:
        stats_df = pd.DataFrame(enhanced_stats)
        output_path = os.path.join(output_folder, "group_statistics.csv")
        stats_df.to_csv(output_path, index=False)
        print_debug(f"Group statistics written to {output_path}")
    else:
        print_debug("No enhanced group statistics to write.")

def create_output_directory(input_folder):
    """
    Create a temporary output directory for grouped instances
    """
    # Use the default output folder path instead of a subfolder in the input directory
    output_folder = default_output_folder
    
    # Delete output directory if it already exists
    if os.path.exists(output_folder):
        print_debug(f"Clearing existing output folder: {output_folder}")
        try:
            shutil.rmtree(output_folder)
        except Exception as e:
            print_debug(f"Error removing existing output folder: {e}")
            # If we can't remove it, try to just clear its contents
            try:
                for filename in os.listdir(output_folder):
                    file_path = os.path.join(output_folder, filename)
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                print_debug("Successfully cleared contents of output folder")
            except Exception as inner_e:
                print_debug(f"Error clearing contents of output folder: {inner_e}")
                raise
    
    # Create folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print_debug(f"Created output folder: {output_folder}")
    
    return output_folder

def group_instances(input_folder, output_folder):
    """
    Group instances from a set of CSV files.
    
    Parameters:
    input_folder (str): Folder containing instance CSVs
    output_folder (str): Folder to write outputs
    """
    start_time = time.time()
    
    # Find all CSV files in the input folder
    print_debug(f"Searching for CSV files in: {input_folder}")
    csv_files = []
    for file in os.listdir(input_folder):
        if file.lower().endswith('.csv'):
            csv_files.append(os.path.join(input_folder, file))
    
    if not csv_files:
        print_debug("No CSV files found in input folder.")
        return 0
    
    print_debug(f"Found {len(csv_files)} CSV files.")
    
    # Load all instances from the CSV files
    all_instances = []
    for file in csv_files:
        instances = load_instances_from_file(file)
        all_instances.extend(instances)
        print_debug(f"Loaded {len(instances)} instances from {os.path.basename(file)}")
    
    if not all_instances:
        print_debug("No instances found in CSV files.")
        return 0
    
    print_debug(f"Loaded {len(all_instances)} total instances.")
    
    # Create a flat list of instances for easier processing
    all_instances_flat = all_instances
    
    # Sort instances by confirm_date
    print_debug("Sorting instances by confirm_date...")
    all_instances_flat.sort(key=lambda x: x.get('confirm_date', pd.Timestamp.min))
    
    # Create a dictionary for quick lookups using instance_id
    instances_by_id = {instance['instance_id']: instance for instance in all_instances_flat}
    
    # Create a date index for faster lookups
    print_debug("Building date index...")
    date_index = {}
    for i, inst in enumerate(all_instances_flat):
        confirm_date = inst.get('confirm_date')
        if pd.notna(confirm_date):
            # Convert Timestamp to datetime.date for the index
            date_key = confirm_date.date()
            if date_key not in date_index:
                date_index[date_key] = []
            date_index[date_key].append(i)
    
    print_debug(f"Built date index with {len(date_index)} unique dates")
    
    # Dictionary to store groups
    groups = {}
    group_id = 0
    
    # Dictionary to track which instances are already in a group
    instance_to_group = {}
    
    # Create file handles for each timeframe
    output_files = {}
    
    # Group statistics
    group_stats = []
    
    # Function to get the output file for a specific timeframe
    def get_output_file(timeframe):
        if timeframe not in output_files:
            filename = f"grouped_instances_1v1_SOLUSDT_binance_{timeframe}.csv"
            filepath = os.path.join(output_folder, filename)
            
            # Create file and write header if it doesn't exist
            if not os.path.exists(filepath):
                # Get column names from the first instance of this timeframe
                for inst in all_instances_flat:
                    if inst.get('timeframe') == timeframe:
                        # Add group_id to columns if not present
                        columns = list(inst.keys())
                        if 'group_id' not in columns:
                            columns.append('group_id')
                        
                        # Remove source_file column if present
                        if 'source_file' in columns:
                            columns.remove('source_file')
                        
                        with open(filepath, 'w', newline='') as f:
                            f.write(','.join(columns) + '\n')
                        break
            
            # Open file for appending
            output_files[timeframe] = open(filepath, 'a', newline='')
        
        return output_files[timeframe]
    
    # Function to write an instance to its output file
    def write_instance(instance, group_id=None):
        # Get timeframe and prepare output line
        timeframe = instance.get('timeframe', 'unknown')
        
        # Add group_id to instance if provided
        if group_id is not None:
            instance['group_id'] = group_id
        elif 'group_id' not in instance:
            instance['group_id'] = 'NA'  # Add placeholder for ungrouped instances
        
        # Get output file handle
        output_file = get_output_file(timeframe)
        
        # Get all columns including any new ones like group_id
        # First get a base set of columns from the file header
        first_instance = next(iter(all_instances_flat))
        columns = list(first_instance.keys())
        if 'group_id' not in columns:
            columns.append('group_id')
        
        # Remove source_file column if present
        if 'source_file' in columns:
            columns.remove('source_file')
        
        # Add any other columns present in this instance but not in columns
        for col in instance.keys():
            if col not in columns and col != 'source_file':
                columns.append(col)
        
        # Write instance to file, handling any missing columns and replacing NaN with empty string
        values = []
        for col in columns:
            val = instance.get(col, '')
            if pd.isna(val):
                values.append('')
            else:
                values.append(str(val))
                
        line = ','.join(values)
        output_file.write(line + '\n')
    
    # Track progress
    total_instances = len(all_instances_flat)
    processed_count = 0
    
    # For efficiency, don't check memory usage on every iteration
    last_memory_check = time.time()
    memory_check_interval = 1.0  # seconds
    current_memory = get_memory_usage()
    
    # Process all instances in chronological order
    with tqdm(total=total_instances, desc=f"Processing - Groups: 0 - Mem: {current_memory:.2f} MB") as progress_bar:
        for idx, instance in enumerate(all_instances_flat):
            instance_id = instance['instance_id']
            
            # Skip if it's already been marked as part of a group
            if instance_id in instance_to_group:
                # Write it with its group ID
                write_instance(instance, instance_to_group[instance_id])
                processed_count += 1
                progress_bar.update(1)
                
                # Update progress bar description with each instance
                if time.time() - last_memory_check > memory_check_interval:
                    current_memory = get_memory_usage()
                    last_memory_check = time.time()
                progress_bar.set_description(f"Processing - Groups: {group_id} - Mem: {current_memory:.2f} MB")
                continue
            
            # Set a timeout for grouping
            start_process_time = time.time()
            max_process_time = 60  # 60 seconds max per instance
            
            try:
                # Try to find similar instances to form a group
                similar_instances = self_find_more_group_members(instance, all_instances_flat, instance_to_group, date_index, idx, visited=None, depth=0)
                
                # Check if we've exceeded our time limit
                if time.time() - start_process_time > max_process_time:
                    print_debug(f"WARNING: Processing instance {instance_id} timed out after {max_process_time} seconds. Writing without grouping.")
                    write_instance(instance)
                else:
                    # If we found a group (including this instance)
                    if len(similar_instances) >= MIN_GROUP_SIZE:
                        # Create a new group
                        groups[group_id] = similar_instances
                        
                        # Create group statistics
                        group_direction = instance.get('direction', 'unknown')
                        group_entry_price = float(instance.get('entry', 0))
                        group_target_price = float(instance.get('target', 0))
                        group_date = instance.get('confirm_date', 'unknown')
                        group_timeframe = instance.get('timeframe', 'unknown')
                        
                        # Count outcomes
                        tp_count = 0
                        sl_count = 0
                        
                        for similar_id in similar_instances:
                            similar_instance = instances_by_id.get(similar_id)
                            if similar_instance:
                                status = similar_instance.get('Status', '')
                                if status == 'TP':
                                    tp_count += 1
                                elif status == 'SL':
                                    sl_count += 1
                        
                        # Calculate win rate
                        total_completed = tp_count + sl_count
                        win_rate = (tp_count / total_completed * 100) if total_completed > 0 else 0
                        
                        # Store group statistics
                        group_stats.append({
                            'group_id': group_id,
                            'direction': group_direction,
                            'entry_price': group_entry_price,
                            'target_price': group_target_price,
                            'confirm_date': group_date,
                            'timeframe': group_timeframe,
                            'instance_count': len(similar_instances),
                            'tp_count': tp_count,
                            'sl_count': sl_count,
                            'win_rate': win_rate
                        })
                        
                        # Process each instance in the group
                        for similar_id in similar_instances:
                            # Mark as being in this group
                            instance_to_group[similar_id] = group_id
                        
                        # Write the current instance
                        write_instance(instance, group_id)
                        
                        # Increment group counter
                        group_id += 1
                    else:
                        # Not in a group, write without group_id
                        write_instance(instance)
            except Exception as e:
                # Handle any exceptions during processing
                print_debug(f"ERROR processing instance {instance_id}: {str(e)}")
                # Write the instance without grouping so we can continue
                write_instance(instance)
            
            # Update progress count and bar
            processed_count += 1
            progress_bar.update(1)
            
            # Update memory usage periodically to avoid performance impact
            if time.time() - last_memory_check > memory_check_interval:
                current_memory = get_memory_usage()
                last_memory_check = time.time()
                
                # Force garbage collection periodically (but less frequently)
                if processed_count % 5000 == 0:
                    force_gc(f"After processing {processed_count} instances")
            
            # Update progress bar description with each instance
            progress_bar.set_description(f"Processing - Groups: {group_id} - Mem: {current_memory:.2f} MB")
    
    # Close all output files
    for file_handle in output_files.values():
        file_handle.close()
    
    # Count instances that are not in any group
    ungrouped_count = len(all_instances_flat) - len(instance_to_group)
    print_debug(f"Found {len(groups)} groups containing {len(instance_to_group)} instances")
    print_debug(f"{ungrouped_count} instances are not part of any group")
    print_debug(f"Total of {processed_count} instances processed")
    
    # Write group statistics
    if group_stats:
        write_group_stats(group_stats, output_folder, all_instances, instances_by_id, instance_to_group)
    
    # Calculate time taken
    end_time = time.time()
    print_debug(f"Grouping completed in {end_time - start_time:.2f} seconds")
    print_debug(f"Wrote output files to {output_folder}")
    
    return processed_count

def self_find_more_group_members(instance, all_instances, instance_to_group, date_index, current_index, visited=None, depth=0):
    """
    Find instances that form a group with the given instance.
    
    Parameters:
    instance (dict): The instance to find similar instances for
    all_instances (list): List of all instances
    instance_to_group (dict): Mapping of instance IDs to their group ID
    date_index (dict): Index of instances by confirm_date for faster lookups
    current_index (int): The index of the current instance in all_instances
    visited (set): Set of instance IDs already checked to prevent infinite recursion
    depth (int): Current recursion depth
    
    Returns:
    set: Set of instance IDs that are similar to the input instance
    """
    # Initialize visited set if None
    if visited is None:
        visited = set()
    
    # Start with just this instance
    instance_id = instance['instance_id']
    similar_instance_ids = {instance_id}
    
    # Skip processing if critical data is missing
    direction = instance.get('direction')
    entry = instance.get('entry')
    target = instance.get('target')
    confirm_date = instance.get('confirm_date')
    active_date = instance.get('Active Date')
    completed_date = instance.get('Completed Date')
    
    if pd.isna(direction) or pd.isna(entry) or pd.isna(target) or pd.isna(confirm_date) or pd.isna(active_date):
        return similar_instance_ids
    
    # Handle NaT for completed date (treat as still active)
    if pd.isna(completed_date):
        completed_date = pd.Timestamp.max
    
    # Prepare price range for comparison
    if direction == 'long':
        # For long trades, entry < target
        instance_min = float(entry)
        instance_max = float(target)
    else:  # short
        # For short trades, entry > target
        instance_min = float(target)
        instance_max = float(entry)
    
    instance_range = instance_max - instance_min
    
    # Create date window for temporal overlap check
    start_date = (confirm_date - timedelta(days=7)).date()
    end_date = (completed_date + timedelta(days=7)).date() if completed_date != pd.Timestamp.max else datetime.now().date()
    
    # Find all instances in the date window
    relevant_instances = []
    current_date = start_date
    while current_date <= end_date:
        if current_date in date_index:
            for idx in date_index[current_date]:
                # Skip:
                # 1. The current instance itself
                # 2. Instances already in a group
                other_instance = all_instances[idx]
                other_id = other_instance['instance_id']
                if (idx != current_index and
                    other_id not in instance_to_group):
                    relevant_instances.append(idx)
        current_date += timedelta(days=1)
    
    # Check each relevant instance for similarity
    # Keep track of already processed instances to avoid duplicates
    processed = {current_index}
    
    # Calculate price buffer based on similarity threshold
    price_buffer = (1 - SIMILARITY_THRESHOLD) * instance_range * 1.1  
    
    # Add the min/max price buffer for faster filtering
    search_min = instance_min - price_buffer
    search_max = instance_max + price_buffer
    
    for idx in relevant_instances:
        if idx in processed:
            continue
        
        processed.add(idx)
        other_instance = all_instances[idx]
        other_id = other_instance['instance_id']
        
        # Skip if not the same direction
        if other_instance.get('direction') != direction:
            continue
        
        # Quick check if price ranges are compatible
        other_entry = other_instance.get('entry')
        other_target = other_instance.get('target')
        
        if pd.isna(other_entry) or pd.isna(other_target):
            continue
            
        # Prepare price range
        if direction == 'long':
            other_min = float(other_entry)
            other_max = float(other_target)
        else:
            other_min = float(other_target)
            other_max = float(other_entry)
        
        # Fast range check before detailed overlap calculation
        if other_max < search_min or other_min > search_max:
            continue
        
        # Calculate actual overlap percentage
        overlap = calculate_overlap_percentage(
            entry, target,
            other_entry, other_target
        )
        
        if overlap < SIMILARITY_THRESHOLD:
            continue
        
        # Now check for temporal overlap
        other_confirm = other_instance.get('confirm_date')
        other_active = other_instance.get('Active Date')
        other_completed = other_instance.get('Completed Date')
        
        if pd.isna(other_confirm) or pd.isna(other_active):
            continue
            
        if pd.isna(other_completed):
            other_completed = pd.Timestamp.max
            
        # Check if the instances overlap temporally
        if not check_temporal_overlap(confirm_date, active_date, completed_date,
                                    other_confirm, other_active, other_completed):
            continue
        
        # Add to similar instances
        similar_instance_ids.add(other_id)
    
    return similar_instance_ids

def check_temporal_overlap(confirm1, active1, completed1, confirm2, active2, completed2):
    """
    Check if two instances overlap temporally
    An instance's active period is from its confirm date to its completed date
    
    Behavior is controlled by configuration flags:
    - If IGNORE_TEMPORAL_CONSTRAINTS is True, always returns True (all-time grouping)
    - If BIDIRECTIONAL_GROUPING is True, requires both instances to overlap with each other
    - Otherwise, requires only one instance to overlap with the other
    """
    # If temporal constraints are disabled, always return True
    if IGNORE_TEMPORAL_CONSTRAINTS:
        return True
        
    # Handle null values
    if pd.isna(completed1):
        completed1 = pd.Timestamp.max
    if pd.isna(completed2):
        completed2 = pd.Timestamp.max
        
    # Check if instance 1's active date falls within instance 2's active period
    instance1_in_instance2 = confirm2 <= active1 <= completed2
        
    # Check if instance 2's active date falls within instance 1's active period
    instance2_in_instance1 = confirm1 <= active2 <= completed1
        
    if BIDIRECTIONAL_GROUPING:
        # Bidirectional mode: both must overlap with each other
        return instance1_in_instance2 and instance2_in_instance1
    else:
        # Standard mode: either can overlap with the other
        return instance1_in_instance2 or instance2_in_instance1

def write_grouped_instances(all_instances, instances_by_id, instance_to_group, output_folder):
    """
    Write out each instance with its group ID
    """
    # Create output files for each group
    files_written = 0
    for group_id, group_instances in instance_to_group.items():
        # Create output filename
        filename = f"group_{group_id}.csv"
        filepath = os.path.join(output_folder, filename)
        
        # Create file if it doesn't exist and write header
        if not os.path.exists(filepath):
            # Get column names from instance dictionary
            columns = list(instances_by_id[group_instances[0]].keys())
            
            with open(filepath, 'w', newline='') as f:
                f.write(','.join(columns) + '\n')
        
        # Append instances to file
        with open(filepath, 'a', newline='') as f:
            for instance_id in group_instances:
                instance = instances_by_id[instance_id]
                values = [str(instance.get(col, '')) for col in columns]
                line = ','.join(values)
                f.write(line + '\n')
        
        files_written += 1
    
    return files_written

def similar_price_range(low1, high1, low2, high2):
    """
    Check if two price ranges are similar
    """
    # Calculate the overlap between the two ranges
    overlap = max(0, min(high1, high2) - max(low1, low2))
    
    # Calculate the total range
    total_range = max(high1, high2) - min(low1, low2)
    
    # If there's no overlap, return False
    if overlap == 0:
        return False
    
    # Calculate the overlap percentage
    overlap_percentage = overlap / total_range
    
    # If the overlap percentage is greater than or equal to the similarity threshold, return True
    if overlap_percentage >= SIMILARITY_THRESHOLD:
        return True
    
    # Otherwise, return False
    return False

# Main execution
if __name__ == "__main__":
    # Get input folder from command line or use default
    if len(sys.argv) > 1:
        input_folder = sys.argv[1]
    else:
        try:
            input_folder = input(f"Enter input folder path (or press Enter for default: {default_input_folder}): ")
            if not input_folder:
                input_folder = default_input_folder
        except KeyboardInterrupt:
            sys.exit("Interrupted by user")
    
    # Convert to absolute path
    input_folder = os.path.abspath(input_folder)
    
    # Check if input folder exists
    if not os.path.isdir(input_folder):
        sys.exit(f"Error: Input folder '{input_folder}' does not exist")
    
    # Output folder is set to the default output folder
    output_folder = default_output_folder
    
    start_time = time.time()
    try:
        # Create output directory
        try:
            output_folder = create_output_directory(input_folder)
        except Exception as e:
            sys.exit(f"Error creating output directory: {e}")
        
        # Group instances
        instances_written = group_instances(input_folder, output_folder)
        
        # Calculate and display processing stats
        end_time = time.time()
        print_debug(f"Processing completed in {end_time - start_time:.2f} seconds")
        
        if instances_written > 0:
            print_debug(f"Successfully wrote {instances_written} instances to output files")
            print_debug(f"Output files are available in: {output_folder}")
        else:
            print_debug("No instances were written. Check logs for errors.")
            
    except KeyboardInterrupt:
        print_debug("\nProcess was interrupted by user. Original files will NOT be modified.")
        print_debug(f"Memory usage after processing: {get_memory_usage():.2f} MB")
        print_debug("Processing did not complete successfully. Original files are preserved.")
        print_debug(f"Temporary results are available in: {output_folder}")
        
    except Exception as e:
        print_debug(f"Error during processing: {e}")
        print_debug(f"Memory usage after processing: {get_memory_usage():.2f} MB")
        print_debug("Processing did not complete successfully. Original files are preserved.")
        print_debug(f"Temporary results are available in: {output_folder}")