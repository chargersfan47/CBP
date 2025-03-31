from tqdm import tqdm
from datetime import timedelta, datetime
from config import *
from sim_entries import sim_entries
from sim_exits import sim_exits
from log_utils import write_log_entry, remove_log_entry
from reporting import generate_summary_report
from initialization import load_state

# Define columns for open_positions
open_positions_columns = ['trade_id', 'confirm_date', 'active_date', 'trade_date', 'Completed Date', 'Target Price', 
                         'Position Size', 'Direction', 'Open Price', 'Timeframe', 'Name',
                         'DateReached0.5', 'DateReached0.0', 'DateReached-0.5', 'DateReached-1.0',
                         'fib0.5', 'fib0.0', 'fib-0.5', 'fib-1.0', 'instance_id']
analysis_columns = ['timestamp', 'total_bankroll', 'cash_on_hand', 'total_long_position', 'long_cost_basis', 'long_pnl', 'total_short_position', 'short_cost_basis', 'short_pnl', 'close']

def chunk_by_month(day_candles):
    chunks = {}
    for candle in day_candles:
        month = candle['timestamp'].strftime('%Y-%m')
        if month not in chunks:
            chunks[month] = []
        chunks[month].append(candle)
    return chunks

# --- Helper function to format seconds --- 
def _format_seconds(seconds):
    """Formats seconds into HH:MM:SS string."""
    if seconds is None or seconds < 0:
        return "?"
    try:
        td = timedelta(seconds=int(seconds))
        # str(td) gives HH:MM:SS or H:MM:SS - we want consistent HH:MM:SS
        parts = str(td).split(':')
        if len(parts) == 3:
            h, m, s = parts
            return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
        else: # Handle cases like H:MM:SS or just seconds
             return str(td) # Fallback to default timedelta string
    except (ValueError, TypeError):
        return "?"
# ----------------------------------------

def run_simulation(instances_by_minute, candles, starting_date, ending_date, output_folder, fee_rate, 
                   trades_all, trade_log, open_positions, 
                   initial_cash_on_hand, initial_total_long, initial_long_basis, 
                   initial_total_short, initial_short_basis, initial_long_pnl=0.0, initial_short_pnl=0.0):
    # Initialize variables to keep track of positions and balances
    total_long_position = float(initial_total_long)
    total_short_position = float(initial_total_short)
    long_cost_basis = float(initial_long_basis)
    short_cost_basis = float(initial_short_basis)
    cash_on_hand = float(initial_cash_on_hand)
    long_pnl = float(initial_long_pnl)
    short_pnl = float(initial_short_pnl)

    # Create progress bar for the entire date range
    total_minutes = int((ending_date - starting_date).total_seconds() // 60)
    pbar_minutes = tqdm(total=total_minutes, desc=f'Processing {starting_date.strftime("%Y-%m-%d")}', unit='minute')

    # Split day_candles into monthly chunks
    day_candles = [c for c in candles if starting_date <= c['timestamp'] <= ending_date]
    monthly_chunks = chunk_by_month(day_candles)

    # Process each monthly chunk
    minute_log = []
    for month, candles_chunk in monthly_chunks.items():
        # Simulation for each month
        for minute_data in candles_chunk:
            # Ensure minute_data['timestamp'] is a datetime object
            if isinstance(minute_data['timestamp'], str):
                minute_data['timestamp'] = datetime.strptime(minute_data['timestamp'], '%Y-%m-%d %H:%M:%S')

            # --- Optimization: Get instances only for the current minute ---
            current_minute_dt = minute_data['timestamp']
            # Key format should match the one used in load_instances
            minute_key = current_minute_dt # Assuming load_instances uses datetime objects as keys
            relevant_instances = instances_by_minute.get(minute_key, []) 
            # --------------------------------------------------------------

            # Check for trades to take
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = sim_entries(
                minute_data, relevant_instances, float(fee_rate), trade_log, open_positions, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, output_folder)

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
            
            fmt_dict = pbar_minutes.format_dict
            n = fmt_dict['n']
            total = fmt_dict['total']
            elapsed = fmt_dict['elapsed']
            rate_fmt = fmt_dict.get('rate_fmt') # Default rate (potentially smoothed)
            remaining_seconds = fmt_dict.get('remaining') # Default ETA seconds
            remaining_fmt = _format_seconds(remaining_seconds) # ETA formatted

            if elapsed > 0 and n > 0:
                avg_rate = n / elapsed # Overall average rate
                eta_stable_seconds = (elapsed / n * (total - n)) # ETA based on average rate
                # Construct the desired compact postfix string
                postfix_str = (
                    f"{rate_fmt} | " # Default rate
                    f"Avg: {_format_seconds(elapsed)}<{_format_seconds(eta_stable_seconds)}, {avg_rate:.2f}min/s" # Avg elapsed<stable ETA, Avg rate
                )
                pbar_minutes.set_postfix_str(postfix_str, refresh=False) # Update postfix without forcing refresh
            else:
                 pbar_minutes.set_postfix_str("Calculating...", refresh=False)

            # Update the progress bar description with the current date
            pbar_minutes.set_description(f"Processing {minute_data['timestamp'].strftime('%Y-%m-%d')}")

    # Close progress bar with a newline
    pbar_minutes.close()
    print()  # Print a new line after progress bar completion

    # Generate the summary report at the end
    generate_summary_report(output_folder, starting_date, ending_date)
    print("Simulation complete!")
