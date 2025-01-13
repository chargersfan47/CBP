import os
from datetime import datetime
from tqdm import tqdm
from config import *

def load_instances(instances_folder, start_date, end_date):
    instances = []
    filenames = [filename for filename in os.listdir(instances_folder) if filename.endswith('.csv')]
    for filename in tqdm(filenames, desc='Loading instances data'):
        timeframe = filename.split('_')[-1].replace('.csv', '')  # Extract timeframe from filename
        with open(os.path.join(instances_folder, filename), 'r') as file:
            lines = file.readlines()
            headers = lines[0].strip().split(',')
            data = [dict(zip(headers, line.strip().split(','))) for line in lines[1:]]
            for entry in data:
                # Handle confirm dates
                date_format = '%Y-%m-%d %H:%M:%S' if ' ' in entry['confirm_date'] else '%Y-%m-%d'
                entry['confirm_date'] = datetime.strptime(entry['confirm_date'], date_format)
                if date_format == '%Y-%m-%d':
                    entry['confirm_date'] = entry['confirm_date'].replace(hour=0, minute=0, second=0)
                
                # Handle Active Date
                if entry['Active Date']:
                    entry['Active Date'] = datetime.strptime(entry['Active Date'], '%Y-%m-%d %H:%M:%S')
                else:
                    entry['Active Date'] = None
                
                # Handle Completed Date
                if entry['Completed Date']:
                    entry['Completed Date'] = datetime.strptime(entry['Completed Date'], '%Y-%m-%d %H:%M:%S')
                else:
                    entry['Completed Date'] = None

                # Convert numerical values to float
                for key in ['Entry', 'target']:
                    if key in entry:
                        entry[key] = float(entry[key])

                entry['Timeframe'] = timeframe
                
                # Filter by date range
                if entry['Active Date'] and start_date <= entry['Active Date'] <= end_date:
                    instances.append(entry)
    return instances

def load_candles(file_path, start_date, end_date):
    candles = []

    # First, we need to find the first data point in the file
    with open(file_path, 'r') as file:
        first_line = file.readline()  # Read headers
        first_data_line = file.readline().strip().split(',')
        first_timestamp = datetime.strptime(first_data_line[0], '%Y-%m-%d %H:%M:%S')

    # Calculate the total number of minutes from the first timestamp to the end date (23:59)
    total_minutes = int((end_date - first_timestamp).total_seconds() // 60) + 1  # Add 1 to include the last minute

    # Initialize the progress bar
    pbar = tqdm(total=total_minutes, desc='Loading candles data', unit='minute')

    with open(file_path, 'r') as file:
        lines = file.readlines()
        headers = lines[0].strip().split(',')
        for line in lines[1:]:
            values = line.strip().split(',')
            candle = {col: val for col, val in zip(headers, values)}
            candle['timestamp'] = datetime.strptime(candle['timestamp'], '%Y-%m-%d %H:%M:%S')
            if candle['timestamp'] > end_date:
                break
            pbar.update(1)  # Update progress bar for every minute checked
            if start_date <= candle['timestamp'] <= end_date:
                candles.append(candle)

    pbar.close()  # Close the progress bar
    return candles

def load_state(output_folder):
    minute_log = []
    trade_log = []
    open_positions = []
    current_month = starting_date.month
    latest_date = starting_date
    total_long_position = 0
    total_short_position = 0
    long_cost_basis = 0
    short_cost_basis = 0
    cash_on_hand = starting_bankroll

    if os.path.exists(output_folder):
        try:
            # Load the latest files for analysis, trades, and open positions
            analysis_files = sorted([f for f in os.listdir(output_folder) if f.startswith('analysis_')])
            trades_files = sorted([f for f in os.listdir(output_folder) if f.startswith('trades_')])
            open_positions_file = os.path.join(output_folder, 'open_positions.csv')

            if analysis_files and trades_files:
                # Load the open positions
                if os.path.exists(open_positions_file):
                    with open(open_positions_file, 'r') as file:
                        lines = file.readlines()
                        headers = lines[0].strip().split(',')
                        open_positions = [dict(zip(headers, line.strip().split(','))) for line in lines[1:]]
                        for pos in open_positions:
                            if isinstance(pos['trade_date'], str) and pos['trade_date']:
                                pos['trade_date'] = datetime.strptime(pos['trade_date'], '%Y-%m-%d %H:%M:%S')
                            if isinstance(pos['Completed Date'], str) and pos['Completed Date']:
                                pos['Completed Date'] = datetime.strptime(pos['Completed Date'], '%Y-%m-%d %H:%M:%S')
                            pos['Position Size'] = float(pos['Position Size'])
                            pos['Open Price'] = float(pos['Open Price'])

                # Load the most recent values for minute_log and trade_log
                latest_analysis_file = analysis_files[-1]
                latest_trades_file = trades_files[-1]

                with open(os.path.join(output_folder, latest_analysis_file), 'r') as file:
                    lines = file.readlines()
                    if len(lines) > 1:
                        minute_log.append(lines[-1].strip().split(','))  # Ensure we don’t include the last record unnecessarily

                with open(os.path.join(output_folder, latest_trades_file), 'r') as file:
                    lines = file.readlines()
                    headers = lines[0].strip().split(',')
                    trade_log.append(dict(zip(headers, lines[-1].strip().split(','))))

                # Update current month and latest date
                current_month = int(latest_analysis_file.split('_')[1][:6])
                current_date_str = minute_log[-1][0]
                if current_date_str:
                    latest_date = datetime.strptime(current_date_str, '%Y-%m-%d %H:%M:%S')
                else:
                    raise ValueError("Empty timestamp in minute log")

                # Restore the positions and cash
                last_record = minute_log[-1]
                cash_on_hand = float(last_record[2])
                total_long_position = float(last_record[3])
                long_cost_basis = float(last_record[4])
                total_short_position = float(last_record[6])
                short_cost_basis = float(last_record[7])

                # Print state stats to console
                print(f"Loaded state with {len(open_positions)} open positions")
                print(f"Variables loaded: current_month={current_month}, latest_date={latest_date}, total_long_position={total_long_position}, total_short_position={total_short_position}, long_cost_basis={long_cost_basis}, short_cost_basis={short_cost_basis}, cash_on_hand={cash_on_hand}")

        except Exception as e:
            print(f"Error loading state: {e}")

            return None

    return (minute_log, trade_log, open_positions, current_month, latest_date,
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand)

def initialize_trades_all(output_folder):
    trades_all_path = os.path.join(output_folder, 'trades_all.csv')
    trades_all = []

    if os.path.exists(trades_all_path):
        with open(trades_all_path, 'r') as file:
            lines = file.readlines()
            headers = lines[0].strip().split(',')
            for line in lines[1:]:
                values = line.strip().split(',')
                trade_entry = {col: val for col, val in zip(headers, values)}
                trades_all.append(trade_entry)

    return trades_all
