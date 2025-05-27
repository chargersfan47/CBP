import os
import csv
import time
from datetime import datetime, timedelta
import pandas as pd
import time

def write_log_entry(entry, filepath, columns):
    file_exists = os.path.isfile(filepath)
    
    # Retry mechanism for file writing
    max_retries = 5
    retry_delay = 0.2  # seconds
    
    for attempt in range(max_retries):
        try:
            with open(filepath, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                if not file_exists:
                    writer.writeheader()  # file doesn't exist yet, write a header
                writer.writerow(entry)
            break  # Success, exit the retry loop
        except Exception as e:
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                time.sleep(retry_delay)
            else:
                print(f"Error writing to {filepath}: {e}")

def remove_log_entry(entry_id, filepath, columns):
    """Remove an entry from a CSV file by loading it entirely in memory and rewriting it"""
    max_retries = 5
    retry_delay = 0.2  # seconds
    
    # If file doesn't exist, nothing to do
    if not os.path.exists(filepath):
        return
        
    for attempt in range(max_retries):
        try:
            # Read all rows from the file
            rows = []
            try:
                with open(filepath, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    rows = [row for row in reader if row['trade_id'] != entry_id]
            except Exception as e:
                print(f"Error reading {filepath}: {e}")
                time.sleep(retry_delay)
                continue
                
            # Write all rows back to the file (except the one to be removed)
            try:
                with open(filepath, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=columns)
                    writer.writeheader()
                    writer.writerows(rows)
                break  # Success, exit the retry loop
            except Exception as e:
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    time.sleep(retry_delay)
                else:
                    print(f"Error writing back to {filepath}: {e}")
        except Exception as e:
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                time.sleep(retry_delay)
            else:
                print(f"Error in remove_log_entry for {filepath}: {e}")

def analyze_monthly_data(analysis_file, trades_file, open_positions_file, month, custom_order, timeframe_data):
    monthly_data = []
    opening_bankroll, closing_bankroll = 0.0, 0.0
    close_price = 0.0
    total_trades = 0
    open_long_trades, open_short_trades = 0, 0
    close_long_trades, close_short_trades = 0, 0
    realized_pnl = 0.0  # PnL from closed trades
    unrealized_pnl = 0.0  # PnL from positions still open at month end
    wins, losses = 0, 0
    current_longs, current_shorts = 0, 0
    closing_long_balance, closing_short_balance = 0.0, 0.0
    closing_balance = 0.0
    bankroll_high, bankroll_low = 0.0, 0.0
    br_high_date, br_low_date = '', ''
    fees_paid = 0.0

    # Extract data from analysis_file
    max_retries = 5
    retry_delay = 0.2  # seconds
    
    for attempt in range(max_retries):
        try:
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
            break  # Success, exit the retry loop
        except Exception as e:
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                time.sleep(retry_delay)
            else:
                print(f"Error reading {analysis_file}: {e}")

    # Extract data from trades_file
    for attempt in range(max_retries):
        try:
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

                        # Track fees for all trades
                        if 'trade_fee' in row and row['trade_fee']:
                            fees_paid += float(row['trade_fee'])

                        # Only count wins and losses for closed trades
                        if row['order_type'] in ['close long', 'close short']:
                            pnl = float(row['ind_PnL']) if row['ind_PnL'] else 0.0
                            realized_pnl += pnl  # Add to realized PnL
                            if pnl > 0:
                                wins += 1
                            elif pnl < 0:
                                losses += 1

                        # Update timeframe_data
                        timeframe = row['timeframe']
                        if timeframe not in timeframe_data:
                            timeframe_data[timeframe] = {
                                'Wins': 0, 
                                'Losses': 0, 
                                'PnL_Sum': 0.0,
                                'PnL_Sum_Profit': 0.0,  # Sum of winning PnLs
                                'PnL_Sum_Loss': 0.0,     # Sum of losing PnLs
                                'ClosedTrades': 0
                            }
                        if row['order_type'] in ['close long', 'close short']:
                            timeframe_data[timeframe]['PnL_Sum'] += pnl
                            timeframe_data[timeframe]['ClosedTrades'] += 1
                            if pnl > 0:
                                timeframe_data[timeframe]['Wins'] += 1
                                timeframe_data[timeframe]['PnL_Sum_Profit'] += pnl
                            elif pnl < 0:
                                timeframe_data[timeframe]['Losses'] += 1
                                timeframe_data[timeframe]['PnL_Sum_Loss'] += pnl
            break  # Success, exit the retry loop
        except Exception as e:
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                time.sleep(retry_delay)
            else:
                print(f"Error reading {trades_file}: {e}")

    # A much simpler approach to find positions that were open at the end of the month
    # We'll build a map of trade_id -> trade details for opened positions
    # and then remove any that were closed during or before this month
    open_positions_at_month_end = {}
    
    for attempt in range(max_retries):
        try:
            with open(trades_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    trade_date = datetime.strptime(row['trade_date'], '%Y-%m-%d %H:%M:%S')
                    trade_month = trade_date.strftime('%Y%m')
                    
                    # If this is an opening trade in this month or earlier
                    if row['order_type'] in ['open long', 'open short'] and trade_month <= month:
                        # Store the position details
                        open_positions_at_month_end[row['trade_id']] = {
                            'Direction': 'long' if row['order_type'] == 'open long' else 'short',
                            'Position Size': abs(float(row['units_traded'])) if row['units_traded'] else 0,
                            'Open Price': float(row['price']) if row['price'] else 0,
                            'Opening Fee': float(row['trade_fee']) if row['trade_fee'] else 0,
                            'Open Month': trade_month
                        }
                    
                    # If this is a closing trade in this month or earlier, remove it from open positions
                    elif row['order_type'] in ['close long', 'close short'] and trade_month <= month:
                        # Handle both regular trade_ids and those with _fib suffix
                        base_trade_id = row['trade_id'].split('_fib')[0] if '_fib' in row['trade_id'] else row['trade_id']
                        if base_trade_id in open_positions_at_month_end:
                            del open_positions_at_month_end[base_trade_id]
            break  # Success, exit the retry loop
        except Exception as e:
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                time.sleep(retry_delay)
            else:
                print(f"Error reading trades for unrealized PnL: {e}")
    
    # Calculate unrealized PnL for positions still open at month end
    # Use the close_price that was already determined earlier
    for trade_id, position in open_positions_at_month_end.items():
        position_size = position['Position Size']
        open_price = position['Open Price']
        opening_fee = position['Opening Fee']
        
        # Count the position in current longs/shorts
        if position['Direction'] == 'long':
            current_longs += 1
            price_pnl = (close_price - open_price) * position_size
        else:  # short
            current_shorts += 1
            price_pnl = (open_price - close_price) * position_size
        
        # Calculate unrealized PnL as price movement minus opening fee
        position_pnl = price_pnl - opening_fee
        unrealized_pnl += position_pnl

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

    # Calculate total PnL and add to the monthly_data entry
    if monthly_data:
        # Store metrics with proper rounding
        monthly_data[0]['Fees Paid'] = round(fees_paid, 4)
        monthly_data[0]['Realized PnL'] = round(realized_pnl, 4)
        monthly_data[0]['Unrealized PnL'] = round(unrealized_pnl, 4)
        
        # Calculate Net PnL as sum of Realized and Unrealized
        monthly_data[0]['Net PnL'] = round(realized_pnl + unrealized_pnl, 4)
                
    return monthly_data, timeframe_data

def sort_timeframes(timeframe_data, custom_order):
    sorted_timeframes = sorted(timeframe_data.items(), key=lambda x: custom_order.index(x[0]) if x[0] in custom_order else len(custom_order))
    return sorted_timeframes
