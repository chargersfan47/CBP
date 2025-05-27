import os
from datetime import datetime
from tqdm import tqdm
from config import *

def load_instances(instances_folder, start_date, end_date):
    import config
    
    instances_by_minute = {}
    filenames = [filename for filename in os.listdir(instances_folder) if filename.endswith('.csv')]
    
    with tqdm(filenames, desc='Loading instances data') as pbar:
        for filename in pbar:
            timeframe = filename.split('_')[-1].replace('.csv', '')
            try:
                with open(os.path.join(instances_folder, filename), 'r') as file:
                    lines = file.readlines()
                    if not lines: 
                        continue
                    headers = [h.strip() for h in lines[0].strip().split(',')]
                    data = [dict(zip(headers, [x.strip() for x in line.strip().split(',')])) for line in lines[1:] if line.strip()]
                    
                    for entry in data:
                        try:
                            # Parse dates
                            confirm_date_str = entry.get('confirm_date', '').strip()
                            if confirm_date_str:
                                date_format = '%Y-%m-%d %H:%M:%S' if ' ' in confirm_date_str else '%Y-%m-%d'
                                confirm_dt = datetime.strptime(confirm_date_str, date_format)
                                if date_format == '%Y-%m-%d':
                                    confirm_dt = confirm_dt.replace(hour=0, minute=0, second=0)
                                entry['confirm_date'] = confirm_dt
                            else:
                                entry['confirm_date'] = None

                            active_date_str = entry.get('Active Date', '').strip()
                            if active_date_str:
                                entry['Active Date'] = datetime.strptime(active_date_str, '%Y-%m-%d %H:%M:%S')
                            else:
                                entry['Active Date'] = None
                            
                            completed_date_str = entry.get('Completed Date', '').strip()
                            if completed_date_str:
                                entry['Completed Date'] = datetime.strptime(completed_date_str, '%Y-%m-%d %H:%M:%S')
                            else:
                                entry['Completed Date'] = None
                            
                            # Parse date fields
                            date_fields = [
                                'DateReached0.5', 'DateReached0.0', 
                                'DateReached-0.5', 'DateReached-1.0'
                            ]
                            for field in date_fields:
                                date_str = entry.get(field, '').strip()
                                if date_str:
                                    try:
                                        entry[field] = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                                    except (ValueError, TypeError):
                                        entry[field] = None
                                else:
                                    entry[field] = None

                            # Parse numeric fields
                            for key in ['Entry', 'target', 'entry', 'fib0.5', 'fib0.0', 'fib-0.5', 'fib-1.0']: 
                                value_str = entry.get(key, '').strip()
                                if value_str:
                                    try:
                                        entry[key] = float(value_str)
                                    except (ValueError, TypeError):
                                        entry[key] = None
                                else:
                                    entry[key] = None

                            entry['Timeframe'] = timeframe
                            
                            # Skip if we're avoiding groups and this entry has a group
                            if AVOID_GROUPS and entry.get('group_id', 'NA') != 'NA':
                                continue
                                
                            active_date = entry.get('Active Date')
                            
                            # If any of the FULL_INSTANCE_SET_FLAGS are True, we need to load all instances
                            # regardless of date. Otherwise, only load instances with active dates in our range
                            needs_full_set = any(getattr(config, flag, False) for flag in FULL_INSTANCE_SET_FLAGS)
                            if needs_full_set or (active_date and start_date <= active_date <= end_date):
                                activation_minute = active_date.replace(second=0, microsecond=0) if active_date else None
                                if activation_minute is not None:  # Only add if we have a valid activation minute
                                    if activation_minute not in instances_by_minute:
                                        instances_by_minute[activation_minute] = []
                                    instances_by_minute[activation_minute].append(entry)
                        except Exception as e:
                             print(f"\nWarning: Skipping entry due to error in file {filename}: {e}. Entry data: {entry}")
                             continue 

            except Exception as e:
                print(f"\nError processing file {filename}: {e}")
                continue 

    total_loaded_instances = sum(len(v) for v in instances_by_minute.values())
    print(f"Loaded {total_loaded_instances} instances into {len(instances_by_minute)} activation minutes after applying filters")
    return instances_by_minute 

def load_candles(file_path, start_date, end_date):
    candles = []
    try:
        total_lines = sum(1 for line in open(file_path, 'r')) -1 

        with open(file_path, 'r') as file:
             with tqdm(total=total_lines, desc='Loading candles data', unit='line') as pbar:
                headers = file.readline().strip().split(',')
                if not headers: 
                    print(f"Warning: Empty or invalid header in {file_path}")
                    return []

                for line in file:
                    pbar.update(1)
                    values = line.strip().split(',')
                    if len(values) != len(headers): 
                         print(f"Warning: Skipping malformed line in {file_path}: {line.strip()}")
                         continue
                         
                    try:
                        candle = {col: val for col, val in zip(headers, values)}
                        timestamp_str = candle.get('timestamp', '').strip()
                        if not timestamp_str:
                             print(f"Warning: Skipping candle with missing timestamp in {file_path}: {line.strip()}")
                             continue
                             
                        candle['timestamp'] = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        
                        if candle['timestamp'] > end_date:
                            pbar.n = pbar.total
                            pbar.refresh()
                            break 
                            
                        if start_date <= candle['timestamp'] <= end_date:
                            for key in ['open', 'high', 'low', 'close', 'volume']:
                                value_str = candle.get(key, '').strip()
                                if value_str:
                                    try:
                                        candle[key] = float(value_str)
                                    except (ValueError, TypeError):
                                        candle[key] = 0.0 
                                else:
                                    candle[key] = 0.0
                            candles.append(candle)
                    except Exception as e:
                        print(f"Warning: Error processing candle line in {file_path}: {e}. Line: {line.strip()}")
                        continue 

    except FileNotFoundError:
        print(f"Error: Candles file not found at {file_path}")
        return [] 
    except Exception as e:
        print(f"Error loading candles from {file_path}: {e}")
        return [] 

    print(f"Loaded {len(candles)} candles within the specified date range.")
    return candles

def load_state(output_folder):
    # Initialize default return values
    minute_log = []
    trade_log = []
    open_positions = []
    # Attempt to get starting_date from config, handle potential import issues gracefully
    try:
        from config import starting_date as config_starting_date
        current_month = config_starting_date.month
        latest_date = config_starting_date
    except ImportError:
        print("Warning: Could not import starting_date from config.py for load_state defaults.")
        # Set reasonable defaults or raise an error if config is critical
        current_month = datetime.now().month 
        latest_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    except AttributeError:
         print("Warning: starting_date not found in config.py for load_state defaults.")
         current_month = datetime.now().month 
         latest_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


    total_long_position = 0.0
    total_short_position = 0.0
    long_cost_basis = 0.0
    short_cost_basis = 0.0
    
    # Attempt to get starting_bankroll from config
    try:
        from config import starting_bankroll as config_starting_bankroll
        # --- Ensure the value from config is treated as a float --- 
        cash_on_hand = float(config_starting_bankroll)
        # -----------------------------------------------------------
    except ImportError:
        print("Warning: Could not import starting_bankroll from config.py for load_state defaults.")
        cash_on_hand = 0.0 # Default if not found
    except AttributeError:
        print("Warning: starting_bankroll not found in config.py for load_state defaults.")
        cash_on_hand = 0.0
    except ValueError: # Catch error if config value isn't a valid float string
        print(f"Warning: starting_bankroll in config.py ('{config_starting_bankroll}') is not a valid number. Using 0.0.")
        cash_on_hand = 0.0


    analysis_files = []
    trades_files = []
    open_positions_file = os.path.join(output_folder, 'open_positions.csv')
    
    state_loaded = False # Flag to indicate if state was successfully loaded

    if os.path.exists(output_folder):
        try:
            analysis_files = sorted([f for f in os.listdir(output_folder) if f.startswith('analysis_') and f.endswith('.csv')])
            trades_files = sorted([f for f in os.listdir(output_folder) if f.startswith('trades_') and f.endswith('.csv')]) # Assuming trades files exist

            # Load open positions first, as they don't depend on other logs
            if os.path.exists(open_positions_file):
                with open(open_positions_file, 'r') as file:
                    lines = file.readlines()
                    if len(lines) > 1: # Check if there's data beyond the header
                        headers = lines[0].strip().split(',')
                        # Use list comprehension for efficiency
                        open_positions = [dict(zip(headers, line.strip().split(','))) for line in lines[1:] if line.strip()]
                        # Process loaded open positions (convert types)
                        processed_positions = [] # Store successfully processed positions
                        for pos in open_positions:
                            try:
                                # Convert dates robustly
                                for date_key in ['trade_date', 'Completed Date', 'confirm_date', 'active_date']: # Added confirm/active
                                    date_str = pos.get(date_key, '').strip()
                                    if date_str:
                                         # More flexible date parsing if needed, assuming standard format for now
                                        pos[date_key] = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                                    else:
                                        pos[date_key] = None
                                # Convert numeric values robustly
                                numeric_fields = [
                                    'Position Size', 'Open Price', 'Target Price',
                                    'ampd_p_value', 'ampd_t_value'  # Add AMPD fields
                                ]
                                for num_key in numeric_fields:
                                    if num_key in pos:  # Only process if the key exists
                                        num_str = pos.get(num_key, '').strip()
                                        try:
                                            pos[num_key] = float(num_str) if num_str else 0.0
                                        except (ValueError, TypeError):
                                            pos[num_key] = 0.0  # Default to 0.0 if conversion fails
                                
                                # Ensure required keys exist after potential failures
                                if all(k in pos for k in headers): # Check if all original headers are still keys
                                     processed_positions.append(pos)
                                else:
                                     print(f"Warning: Skipping open position entry due to missing keys after processing. Data: {pos}")

                            except (ValueError, TypeError, KeyError) as e:
                                print(f"Warning: Error processing open position entry: {e}. Data: {pos}. Skipping entry.")
                                continue # Skip this problematic position
                        open_positions = processed_positions # Update with only valid positions


            # Load state from the latest analysis file if it exists
            if analysis_files:
                latest_analysis_file = os.path.join(output_folder, analysis_files[-1])
                with open(latest_analysis_file, 'r') as file:
                    lines = file.readlines()
                    if len(lines) > 1: # Check if there's data beyond the header
                         # Define expected headers based on simulation.py log format
                        analysis_headers = ['timestamp', 'total_bankroll', 'cash_on_hand', 
                                            'total_long_position', 'long_cost_basis', 'long_pnl', 
                                            'total_short_position', 'short_cost_basis', 'short_pnl', 'close']
                        last_line_values = lines[-1].strip().split(',')
                        
                        if len(last_line_values) == len(analysis_headers):
                            last_record_dict = dict(zip(analysis_headers, last_line_values))
                            
                            # Restore state variables robustly
                            try:
                                latest_date_str = last_record_dict.get('timestamp', '').strip()
                                if latest_date_str:
                                    latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d %H:%M:%S')
                                    current_month = latest_date.month # Update current_month from loaded date
                                else:
                                     raise ValueError("Missing or empty timestamp in last analysis record")
                                     
                                minute_log_entry = {
                                    'timestamp': latest_date, # This is a datetime object
                                    # --- Ensure numeric values are floats --- 
                                    'cash_on_hand': float(last_record_dict.get('cash_on_hand', cash_on_hand)), 
                                    'total_long_position': float(last_record_dict.get('total_long_position', 0.0)),
                                    'long_cost_basis': float(last_record_dict.get('long_cost_basis', 0.0)),        
                                    'total_short_position': float(last_record_dict.get('total_short_position', 0.0)),
                                    'short_cost_basis': float(last_record_dict.get('short_cost_basis', 0.0)),      
                                    'long_pnl': float(last_record_dict.get('long_pnl', 0.0)),                     
                                    'short_pnl': float(last_record_dict.get('short_pnl', 0.0)) 
                                    # --------------------------------------                     
                                }
                                minute_log = [minute_log_entry] # minute_log is a list containing this dict
                                cash_on_hand = minute_log_entry['cash_on_hand']
                                total_long_position = minute_log_entry['total_long_position']
                                long_cost_basis = minute_log_entry['long_cost_basis']
                                total_short_position = minute_log_entry['total_short_position']
                                short_cost_basis = minute_log_entry['short_cost_basis']
                                
                                state_loaded = True # State successfully loaded
                            except (ValueError, TypeError, KeyError) as e:
                                print(f"Warning: Error parsing last analysis record: {e}. Data: {last_line_values}")
                                # Reset relevant state variables if parsing fails?
                                # Or rely on defaults set earlier.

                        else: # Corrected indentation for this else block
                             print(f"Warning: Mismatched columns in last analysis record of {analysis_files[-1]}. Expected {len(analysis_headers)}, got {len(last_line_values)}. Cannot load state from this record.")

            # Corrected indentation for this block: Moved inside the main 'try'
            if state_loaded:
                 print(f"Loaded state from {analysis_files[-1]} with {len(open_positions)} open positions.")
                 print(f"State: latest_date={latest_date}, cash={cash_on_hand:.2f}, long_pos={total_long_position:.4f}, short_pos={total_short_position:.4f}")

        except Exception as e:
            print(f"Error loading state from {output_folder}: {e}")
            # Return None or default values to indicate failure
            return None # Or return the potentially partially loaded state if that's desired

    # Return loaded state only if successful, otherwise None or defaults
    if state_loaded:
         # Return as dictionary for clarity
         return {
            'minute_log': minute_log, # Contains only the last entry dict
            'trade_log': trade_log,   # Currently empty unless loaded above
            'open_positions': open_positions,
            'current_month': current_month,
            'current_date': latest_date, # Renamed from latest_date for consistency with main.py usage
            'total_long_position': total_long_position,
            'total_short_position': total_short_position,
            'long_cost_basis': long_cost_basis,
            'short_cost_basis': short_cost_basis,
            'cash_on_hand': cash_on_hand
        }
    else:
        print("No valid saved state found or error during loading.")
        return None # Indicate no state loaded

def initialize_trades_all(output_folder):
    trades_all_path = os.path.join(output_folder, 'trades_all.csv')
    trades_all = []

    if os.path.exists(trades_all_path):
        try:
            with open(trades_all_path, 'r') as file:
                lines = file.readlines()
                if len(lines) > 1: 
                    headers = lines[0].strip().split(',')
                    processed_trades = [] 
                    # --- Define numeric columns to convert --- 
                    # Define a mapping of source columns to their normalized names
                    # Only include columns that are actually used in calculations
                    numeric_columns = {
                        'Position Size': 'position_size',
                        'price': 'price',
                        'PnL': 'pnl',
                        'Fee': 'fee',
                        'units_traded': 'units_traded',
                        'realized_PnL': 'realized_pnl',
                        'total_long_position': 'total_long_position',
                        'total_short_position': 'total_short_position',
                        'long_cost_basis': 'long_cost_basis',
                        'short_cost_basis': 'short_cost_basis',
                        'balance': 'balance',
                        'ind_PnL': 'ind_pnl'
                    }
                    import csv
                    
                    for line in lines[1:]:
                        if not line.strip():
                            continue
                            
                        try:
                            reader = csv.reader([line])
                            values = next(reader)
                            
                            if len(values) > len(headers):
                                values = values[:len(headers)]
                            elif len(values) < len(headers):
                                values.extend([''] * (len(headers) - len(values)))
                            
                            trade_entry = dict(zip(headers, values))
                            
                            # Only convert columns we know should be numeric
                            for src_col, dest_col in numeric_columns.items():
                                if src_col in trade_entry and trade_entry[src_col]:
                                    try:
                                        if isinstance(trade_entry[src_col], str):
                                            value_str = trade_entry[src_col].strip()
                                            trade_entry[dest_col] = float(value_str) if value_str else 0.0
                                        elif isinstance(trade_entry[src_col], (int, float)):
                                            trade_entry[dest_col] = float(trade_entry[src_col])
                                        else:
                                            trade_entry[dest_col] = 0.0
                                    except (ValueError, TypeError) as e:
                                        # Only warn for columns we actually need
                                        if src_col in ['total_long_position', 'total_short_position', 'balance']:
                                            print(f"Warning: Could not convert {src_col} to number: {trade_entry[src_col]}. Using 0.0")
                                        trade_entry[dest_col] = 0.0
                            date_columns = [
                                'confirm_date', 'active_date', 'trade_date', 'completed_date',
                                'extreme_price_date', 'tt_confirm_date', 'tt_active_date', 'tt_completed_date',
                                'timestamp', 'date', 'time', 'entry_date', 'exit_date'
                            ]
                            for col in date_columns:
                                if col in trade_entry and trade_entry[col]:
                                    try:
                                        if isinstance(trade_entry[col], str):
                                            trade_entry[col] = datetime.strptime(trade_entry[col], '%Y-%m-%d %H:%M:%S')
                                    except (ValueError, TypeError):
                                        print(f"Warning: Could not convert date '{trade_entry[col]}' in column '{col}'. Keeping as string.")
                                        # Keep the original string value if conversion fails
                            
                            processed_trades.append(trade_entry)
                            
                        except Exception as e:
                            print(f"Warning: Error processing line in trades_all.csv: {e}\nLine: {line.strip()}")
                            continue
                    trades_all = processed_trades 

        except Exception as e:
            print(f"Error loading trades_all.csv from {output_folder}: {e}")
            return []

    return trades_all
