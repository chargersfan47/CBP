import os
import csv
from datetime import datetime
import pandas as pd
import time

def write_log_entry(entry, filepath, columns):
    file_exists = os.path.isfile(filepath)
    with open(filepath, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        if not file_exists:
            writer.writeheader()  # file doesn't exist yet, write a header
        writer.writerow(entry)

def remove_log_entry(entry_id, filepath, columns):
    temp_filepath = filepath + '.tmp'
    try:
        # First try the normal operation
        df = pd.read_csv(filepath)
        df = df[df['trade_id'] != entry_id]
        df.to_csv(temp_filepath, index=False)
        os.replace(temp_filepath, filepath)
    except PermissionError:
        # If we get a permission error, wait and retry
        time.sleep(0.1)
        try:
            df = pd.read_csv(filepath)
            df = df[df['trade_id'] != entry_id]
            df.to_csv(temp_filepath, index=False)
            os.replace(temp_filepath, filepath)
        except PermissionError as e:
            print(f"Warning: Could not update {filepath} - {str(e)}")
            return False
    return True

def analyze_monthly_data(analysis_file, trades_file, open_positions_file, month, custom_order, timeframe_data):
    monthly_data = []
    opening_bankroll, closing_bankroll = 0.0, 0.0
    close_price = 0.0
    total_trades = 0
    open_long_trades, open_short_trades = 0, 0
    close_long_trades, close_short_trades = 0, 0
    sum_of_pnl = 0.0
    wins, losses = 0, 0
    current_longs, current_shorts = 0, 0
    closing_long_balance, closing_short_balance = 0.0, 0.0
    closing_balance = 0.0
    bankroll_high, bankroll_low = 0.0, 0.0
    br_high_date, br_low_date = '', ''

    # Extract data from analysis_file
    with open(analysis_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
            bankroll = float(row['total_bankroll'])
            if not opening_bankroll:
                opening_bankroll = bankroll
            closing_bankroll = bankroll
            close_price = float(row['close'])
            if bankroll > bankroll_high:
                bankroll_high = bankroll
                br_high_date = timestamp.strftime('%Y-%m-%d')
            if bankroll_low == 0 or bankroll < bankroll_low:
                bankroll_low = bankroll
                br_low_date = timestamp.strftime('%Y-%m-%d')
            closing_long_balance = float(row['total_long_position'])
            closing_short_balance = float(row['total_short_position'])
            closing_balance = closing_long_balance - closing_short_balance

    # Extract data from trades_file
    with open(trades_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trade_date = datetime.strptime(row['trade_date'], '%Y-%m-%d %H:%M:%S')
            if trade_date.strftime('%Y%m') == month:
                total_trades += 1
                if row['order_type'] == 'open long':
                    open_long_trades += 1
                elif row['order_type'] == 'open short':
                    open_short_trades += 1
                elif row['order_type'] == 'close long':
                    close_long_trades += 1
                elif row['order_type'] == 'close short':
                    close_short_trades += 1

                # Only count wins and losses for closed trades
                if row['order_type'] in ['close long', 'close short']:
                    pnl = float(row['ind_PnL']) if row['ind_PnL'] else 0.0
                    sum_of_pnl += pnl
                    if pnl > 0:
                        wins += 1
                    elif pnl < 0:
                        losses += 1

                # Update timeframe_data
                timeframe = row['timeframe']
                if timeframe not in timeframe_data:
                    timeframe_data[timeframe] = {'Wins': 0, 'Losses': 0, 'PnL_Sum': 0.0, 'ClosedTrades': 0}
                if row['order_type'] in ['close long', 'close short']:
                    timeframe_data[timeframe]['PnL_Sum'] += pnl
                    timeframe_data[timeframe]['ClosedTrades'] += 1
                    if pnl > 0:
                        timeframe_data[timeframe]['Wins'] += 1
                    elif pnl < 0:
                        timeframe_data[timeframe]['Losses'] += 1

    # Extract data from open_positions_file for current longs and shorts
    if os.path.exists(open_positions_file):
        with open(open_positions_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['Direction'] == 'long':
                    current_longs += 1
                elif row['Direction'] == 'short':
                    current_shorts += 1

    win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0.0

    monthly_data.append({
        'Month': month,
        'Opening Bankroll': round(opening_bankroll, 4),
        'Closing Bankroll': round(closing_bankroll, 4),
        'Close Price': round(close_price, 4),
        'Total Trades': total_trades,
        'Open Long Trades': open_long_trades,
        'Open Short Trades': open_short_trades,
        'Close Long Trades': close_long_trades,
        'Close Short Trades': close_short_trades,
        'Sum of PnL': round(sum_of_pnl, 4),
        'Wins': wins,
        'Losses': losses,
        'Win Rate': round(win_rate, 4),
        'Current Longs': current_longs,
        'Current Shorts': current_shorts,
        'Closing Long Balance': round(closing_long_balance, 4),
        'Closing Short Balance': round(closing_short_balance, 4),
        'Closing Balance': round(closing_balance, 4),
        'Bankroll High': round(bankroll_high, 4),
        'BR High Date': br_high_date,
        'Bankroll Low': round(bankroll_low, 4),
        'BR Low Date': br_low_date
    })

    return monthly_data, timeframe_data

def sort_timeframes(timeframe_data, custom_order):
    sorted_timeframes = sorted(timeframe_data.items(), key=lambda x: custom_order.index(x[0]) if x[0] in custom_order else len(custom_order))
    return sorted_timeframes
