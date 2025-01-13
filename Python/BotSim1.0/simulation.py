from tqdm import tqdm
import os
from datetime import timedelta, datetime
from config import *
from sim_entries import sim_entries
from sim_exits import sim_exits
from log_utils import write_log_entry, remove_log_entry
from reporting import generate_summary_report
from initialization import load_state

# Define columns for open_positions
open_positions_columns = ['trade_id', 'confirm_date', 'trade_date', 'Completed Date', 'Target Price', 'Position Size', 'Direction', 'Open Price', 'Timeframe', 'Name']
analysis_columns = ['timestamp', 'total_bankroll', 'cash_on_hand', 'total_long_position', 'long_cost_basis', 'long_pnl', 'total_short_position', 'short_cost_basis', 'short_pnl', 'close']

def chunk_by_month(day_candles):
    chunks = {}
    for candle in day_candles:
        month = candle['timestamp'].strftime('%Y-%m')
        if month not in chunks:
            chunks[month] = []
        chunks[month].append(candle)
    return chunks

def run_simulation(instances, candles, starting_date, ending_date, output_folder, fee_rate, trades_all, minute_log, trade_log, open_positions):
    # Initialize variables to keep track of positions and balances
    total_long_position = 0
    total_short_position = 0
    long_cost_basis = 0
    short_cost_basis = 0
    long_pnl = 0
    short_pnl = 0
    cash_on_hand = starting_bankroll

    # Load state if continuing
    state = load_state(output_folder)
    if state:
        minute_log, trade_log, open_positions, current_month, latest_date, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = state

    # Create progress bar for the entire date range
    total_minutes = int((ending_date - starting_date).total_seconds() // 60)
    pbar_minutes = tqdm(total=total_minutes, desc=f'Processing {starting_date.strftime("%Y-%m-%d")}', unit='minute')

    # Split day_candles into monthly chunks
    day_candles = [c for c in candles if starting_date <= c['timestamp'] <= ending_date]
    monthly_chunks = chunk_by_month(day_candles)

    # Process each monthly chunk
    for month, candles_chunk in monthly_chunks.items():
        # Simulation for each month
        for minute_data in candles_chunk:
            # Ensure minute_data['timestamp'] is a datetime object
            if isinstance(minute_data['timestamp'], str):
                minute_data['timestamp'] = datetime.strptime(minute_data['timestamp'], '%Y-%m-%d %H:%M:%S')

            # Check for trades to take
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = sim_entries(
                minute_data, instances, float(fee_rate), trade_log, open_positions, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, output_folder)

            # Check for trades to close
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl = sim_exits(
                minute_data, trade_log, open_positions, float(fee_rate), total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, output_folder)

            # Update PnL values using the current minute's close price for analysis
            close_price = float(minute_data['close'])
            long_pnl = total_long_position * (close_price - long_cost_basis)
            short_pnl = total_short_position * (short_cost_basis - close_price)

            # Calculate total bankroll
            total_bankroll = cash_on_hand + long_pnl + short_pnl

            # Log minute data
            minute_log_entry = {
                'timestamp': minute_data['timestamp'],
                'total_bankroll': round(total_bankroll, 4),
                'cash_on_hand': round(cash_on_hand, 4),
                'total_long_position': round(total_long_position, 4),
                'long_cost_basis': round(long_cost_basis, 4),
                'long_pnl': round(long_pnl, 4),
                'total_short_position': round(total_short_position, 4),
                'short_cost_basis': round(short_cost_basis, 4),
                'short_pnl': round(short_pnl, 4),
                'close': round(close_price, 4)
            }
            minute_log.append(minute_log_entry)

            # Write analysis log to the correct monthly file
            if CREATE_ANALYSIS_ALL:
                write_log_entry(minute_log_entry, os.path.join(output_folder, 'analysis_all.csv'), analysis_columns)
            write_log_entry(minute_log_entry, os.path.join(output_folder, f'analysis_{minute_data["timestamp"].strftime("%Y%m")}.csv'), analysis_columns)

            # Update progress bar by 1 minute each iteration
            pbar_minutes.update(1)
            # Update the progress bar description with the current date
            pbar_minutes.set_description(f"Processing {minute_data['timestamp'].strftime('%Y-%m-%d')}")

    # Close progress bar with a newline
    pbar_minutes.close()
    print()  # Print a new line after progress bar completion

    # Generate the summary report at the end
    generate_summary_report(output_folder, starting_date, ending_date)
    print("Simulation complete!")
