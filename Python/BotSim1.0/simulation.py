import os
import time
import csv
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from config import *
from sim_entries import sim_entries

def create_termination_marker(output_folder, reason, termination_time):
    """
    Create a termination marker file with the specified reason and timestamp.
    
    Args:
        output_folder (str): Path to the output folder
        reason (str): Reason for termination (will be used in filename and file content)
        termination_time (datetime): Timestamp when termination occurred
        
    Returns:
        str: Path to the created marker file
    """
    # Clean up the reason to be filesystem-safe
    safe_reason = "".join(c if c.isalnum() or c in ' -_' else '_' for c in reason)
    timestamp = termination_time.strftime('%Y%m%d_%H%M%S')
    filename = f"TERMINATED - {safe_reason}.txt"
    filepath = os.path.join(output_folder, filename)
    
    with open(filepath, 'w') as f:
        f.write(f"Simulation was terminated on {termination_time.isoformat()}\n")
        f.write(f"Reason: {reason}\n")
    
    return filepath

# Convert to set once for O(1) lookups
ALLOWED_SITUATIONS = set(ALLOWED_SITUATIONS)
from sim_exits import sim_exits
from log_utils import write_log_entry, remove_log_entry
from reporting import generate_summary_report
from initialization import load_state

# Define columns for open_positions
open_positions_columns = ['trade_id', 'confirm_date', 'active_date', 'trade_date', 'Completed Date', 'Target Price', 
                         'Position Size', 'Direction', 'Open Price', 'Timeframe', 'Name',
                         'DateReached0.5', 'DateReached0.0', 'DateReached-0.5', 'DateReached-1.0',
                         'fib0.5', 'fib0.0', 'fib-0.5', 'fib-1.0', 'instance_id',
                         'maxdrawdown', 'maxfib', 'maxdrawdown_date', 'max_position_drawdown',
                         'tt_instance_id', 'tt_confirm_date', 'tt_active_date', 'tt_completed_date', 'tt_entry_price',
                         'ampd_p_value', 'ampd_t_value']
analysis_columns = ['timestamp', 'total_bankroll', 'cash_on_hand', 'total_long_position', 'long_cost_basis', 'long_pnl', 'total_short_position', 'short_cost_basis', 'short_pnl', 'close']

def chunk_by_month(day_candles):
    """Group candles by month for more efficient processing."""
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

def check_monthly_trade_volume(month, output_folder, candles_chunk, total_bankroll, total_wins, total_losses):
    """
    Checks if the monthly trade volume is below the threshold.
    
    Args:
        month (str): Month to check in YYYY-MM format
        output_folder (str): Path to the output folder
        candles_chunk (list): List of candles for the current month
        total_bankroll (float): Current total bankroll
        total_wins (int): Total winning trades so far
        total_losses (int): Total losing trades so far
        
    Returns:
        tuple: (should_terminate, reason, termination_time)
    """
    # Extract year and month from month string (which is in YYYY-MM format)
    target_year, target_month = month.split('-')
    
    # Define paths to position files
    open_positions_path = os.path.join(output_folder, 'open_positions.csv')
    closed_positions_path = os.path.join(output_folder, 'closed_positions.csv')
    
    # Count open positions that were active during the month
    open_trades = 0
    if os.path.exists(open_positions_path):
        with open(open_positions_path, 'r') as f:
            reader = csv.DictReader(f)
            open_dates = []
            for position in reader:
                # Get the trade date from the position
                open_date_str = position.get('trade_date', '').strip()
                open_dates.append(open_date_str)
                if not open_date_str:
                    continue
                    
                try:
                    # Extract just the date part (YYYY-MM-DD) from the timestamp
                    date_part = open_date_str.split()[0] if ' ' in open_date_str else open_date_str
                    year, month, _ = date_part.split('-')
                    
                    # Format month with leading zero if needed (01, 02, etc.)
                    month = month.zfill(2)
                    
                    # Compare trade date with target month
                    if year == target_year and month == target_month:
                        open_trades += 1
                        
                except (ValueError, IndexError) as e:
                    pass  # Silently skip invalid dates
    else:
        tqdm.write(f"Warning: Open positions file does not exist yet")
    
    # Count closed positions from closed_positions.csv
    closed_trades = 0
    if os.path.exists(closed_positions_path):
        with open(closed_positions_path, 'r') as f:
            reader = csv.DictReader(f)
            closed_dates = []
            for position in reader:
                # Get the trade date from the position
                close_date_str = position.get('trade_date', '').strip()
                closed_dates.append(close_date_str)
                if not close_date_str:
                    continue
                    
                try:
                    # Extract just the date part (YYYY-MM-DD) from the timestamp
                    date_part = close_date_str.split()[0] if ' ' in close_date_str else close_date_str
                    year, month, _ = date_part.split('-')
                    
                    # Format month with leading zero if needed (01, 02, etc.)
                    month = month.zfill(2)
                    
                    # Compare trade date with target month
                    if year == target_year and month == target_month:
                        closed_trades += 1
                        
                except (ValueError, IndexError) as e:
                    pass  # Silently skip invalid dates
    else:
        tqdm.write(f"Warning: Closed positions file does not exist yet")
    
    # Total trades is the sum of open and closed trades
    trade_count = open_trades + closed_trades
    
    # Calculate win rate
    win_rate = total_wins / (total_wins + total_losses) if (total_wins + total_losses) > 0 else 0
    
    # Format the output message with bankroll and win/loss stats
    tqdm.write(f"{target_year}-{target_month}: {trade_count} trades (open: {open_trades}, closed: {closed_trades}) | "
               f"Bankroll: ${total_bankroll:,.2f} | "
               f"Trades: {total_wins} W / {total_losses} L ({win_rate:.0%})")
    
    if trade_count < LOW_VOLUME_THRESHOLD:
        # Format reason for console output
        console_reason = f"Low trading volume in {target_year}-{target_month}: {trade_count} trades (below threshold of {LOW_VOLUME_THRESHOLD})"
        # Simplified reason for filename
        filename_reason = f"Low Trading Volume - {trade_count} trades"
        # Use first timestamp of next month as termination time
        termination_time = min(candle['timestamp'] for candle in candles_chunk) if candles_chunk else datetime.now()
        tqdm.write(f"Low volume detected: {console_reason}")
        return True, filename_reason, termination_time
    
    return False, None, None

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
    
    # Initialize win/loss counters
    total_wins = 0
    total_losses = 0
    
    # If we're continuing a simulation, load the win/loss counts from the existing trade log
    if os.path.exists(os.path.join(output_folder, 'trades_all.csv')):
        with open(os.path.join(output_folder, 'trades_all.csv'), 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['order_type'] in ['close long', 'close short'] and 'ind_PnL' in row and row['ind_PnL']:
                    try:
                        pnl = float(row['ind_PnL'])
                        if pnl > 0:
                            total_wins += 1
                        elif pnl < 0:
                            total_losses += 1
                    except (ValueError, TypeError):
                        continue
    
    # Create progress bars for the entire date range
    total_minutes = int((ending_date - starting_date).total_seconds() // 60)
    
    # First line: Main progress bar with tqdm's built-in rate (responsive)
    pbar_main = tqdm(
        total=total_minutes,
        desc='',  # Empty desc as we'll handle the prefix manually
        unit='min',
        position=0,
        leave=True,
        bar_format='{elapsed!s:>8}<{remaining!s:>8}  {rate_fmt} | {desc} | {n_fmt}/{total_fmt} | {percentage:3.0f}% |{bar}|'
    )
    
    # Second line: Status line with statistics
    pbar_status = tqdm(
        total=0,  # We're not using the progress bar functionality here
        desc='',  # Empty desc as we'll handle the display manually
        position=1,
        leave=True,
        bar_format='{desc}'
    )

    # Split day_candles into monthly chunks
    day_candles = [c for c in candles if starting_date <= c['timestamp'] <= ending_date]
    monthly_chunks = chunk_by_month(day_candles)

    # Calculate max allowed positions based on leverage (only once at the start)
    if position_size_method == 3 and MAX_LEVERAGE is not None and MAX_LEVERAGE > 0:
        max_allowed_positions = int((MAX_LEVERAGE * 100) / position_size_percent)
    else:
        max_allowed_positions = float('inf')
    print(f"\n[INFO] Maximum allowed positions: {max_allowed_positions} (Leverage: {MAX_LEVERAGE}x, Position Size: {position_size_percent}%)")
    
    # Process each monthly chunk
    minute_log = []
    previous_month = None
    for month, candles_chunk in monthly_chunks.items():
        # Check if there's a previous month to analyze
        if previous_month is not None and USE_LOW_VOLUME_TERMINATION:
            # Check if we should terminate due to low trade volume
            should_terminate, reason, termination_time = check_monthly_trade_volume(
                previous_month, output_folder, candles_chunk, 
                cash_on_hand + total_long_position - abs(total_short_position), # total_bankroll 
                total_wins, total_losses)
            if should_terminate:
                create_termination_marker(output_folder, reason, termination_time)
                tqdm.write(f"Early termination triggered: {reason}")
                # Close progress bars and return to main loop
                pbar_main.close()
                pbar_status.close()
                return
        
        previous_month = month
        
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

            # Only process new entries if below max allowed positions
            if len(open_positions) < max_allowed_positions:
                total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = sim_entries(
                    minute_data, 
                    relevant_instances, 
                    float(fee_rate), 
                    trade_log, 
                    open_positions, 
                    total_long_position, 
                    total_short_position, 
                    long_cost_basis, 
                    short_cost_basis, 
                    cash_on_hand, 
                    output_folder, 
                    all_instances=instances_by_minute if tt_stf_any_inside_activation or tt_stf_within_x_candles or tt_stf_within_x_minutes else None
                )

            # Process exits for existing positions
            result = sim_exits(
                minute_data, trade_log, open_positions, fee_rate, total_long_position, total_short_position, 
                long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, output_folder
            )
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, \
            cash_on_hand, long_pnl, short_pnl, exit_wins, exit_losses = result
            
            # Update win/loss counters
            total_wins += exit_wins
            total_losses += exit_losses

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
            
            # Check for early termination due to low bankroll
            if USE_LOW_BANKROLL_TERMINATION and total_bankroll < (starting_bankroll * LOW_BANKROLL_THRESHOLD):
                reason = f"Bankroll {total_bankroll:.2f} dropped below {starting_bankroll * LOW_BANKROLL_THRESHOLD:.2f} ({(LOW_BANKROLL_THRESHOLD*100):.0f}% of starting bankroll)"
                create_termination_marker(output_folder, reason, minute_data['timestamp'])
                print(f"\nEarly termination triggered: {reason}")
                # Close progress bars and return to main loop
                pbar_main.close()
                pbar_status.close()
                return

            # Get progress bar formatting information
            fmt_dict = pbar_main.format_dict
            n = fmt_dict['n']
            total = fmt_dict['total']
            elapsed = fmt_dict['elapsed']
            rate_fmt = fmt_dict.get('rate_fmt', '0.00')  # Default rate (potentially smoothed)
            remaining_seconds = fmt_dict.get('remaining', 0)  # Default ETA seconds
            
            # Format times with consistent 8-character width (HH:MM:SS)
            elapsed_str = _format_seconds(elapsed).zfill(8)
            remaining_str = _format_seconds(remaining_seconds).zfill(8)
            
            if elapsed > 0 and n > 0:
                avg_rate = n / elapsed  # Overall average rate
                eta_stable_seconds = (elapsed / n * (total - n))  # ETA based on average rate
                
                # Format stable ETA with consistent 8-character width
                stable_remaining_str = _format_seconds(eta_stable_seconds).zfill(8)
                
                # Get open trades count and position values
                open_trades = len(open_positions)
                
                # Update the main progress bar with date and progress
                pbar_main.set_description_str(f"Processing {minute_data['timestamp'].strftime('%Y-%m-%d')}")
                
                # Use the running counters for wins/losses
                win_rate = total_wins / (total_wins + total_losses) if (total_wins + total_losses) > 0 else 0
                
                # Update the status line with statistics
                status_line = (
                    f"{elapsed_str}<{stable_remaining_str} "  # Use stable ETA for consistency
                    f"{avg_rate:>7.2f}m/s avg| "
                    f"Bankroll: ${total_bankroll:,.2f} | "
                    f"Trades: {total_wins} W / {total_losses} L ({win_rate:.0%}) | "
                    f"Pos: {open_trades} (L: {total_long_position:,.0f} |S: {abs(total_short_position):,.0f})"
                )
                pbar_status.set_description_str(status_line)
            else:
                status_line = (
                    f"{elapsed_str}<{remaining_str} "
                    f"{'0.00':>7}m/s avg| "
                    f"Bankroll: ${total_bankroll:,.2f} | "
                    f"Trades: 0 W / 0 L (0%) | "
                    f"Pos: 0 (L: 0|S: 0)"
                )
                pbar_main.set_description_str(f"Processing {minute_data['timestamp'].strftime('%Y-%m-%d')}")
                pbar_status.set_description_str(status_line)
                
            # Update the main progress bar
            pbar_main.update(1)

    # Close progress bars
    pbar_main.close()
    pbar_status.close()
    print()  # Extra newline after progress bars
    print()  # Print a new line after progress bar completion

    # Generate the summary report at the end
    generate_summary_report(output_folder, starting_date, ending_date)
    print("Simulation complete!")
