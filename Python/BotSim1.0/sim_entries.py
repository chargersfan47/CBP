import os
import uuid
from datetime import datetime
from log_utils import write_log_entry
from config import *
from position_size import calculate_position_size

def compare_timestamps_ignore_seconds(ts1, ts2):
    """
    Compare two timestamps ignoring seconds.
    Optimized for performance by minimizing string parsing and object creation.
    """
    # Quick None checks
    if ts1 is None or ts2 is None:
        return False
    
    # Handle string timestamps
    if isinstance(ts1, str):
        if not ts1: 
            return False
    if isinstance(ts2, str):
        if not ts2:
            return False
    
    # Extract year, month, day, hour, minute only - ignore conversion if possible
    try:
        # Get year, month, day, hour, minute for ts1
        if isinstance(ts1, datetime):
            y1, m1, d1, h1, min1 = ts1.year, ts1.month, ts1.day, ts1.hour, ts1.minute
        elif isinstance(ts1, str):
            # Fast string parsing - avoid full datetime conversion
            parts = ts1.split(' ')
            if len(parts) >= 2:  # Has date and time
                date_parts = parts[0].split('-')
                time_parts = parts[1].split(':')
                y1, m1, d1 = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
                h1, min1 = int(time_parts[0]), int(time_parts[1])
            else:  # Date only
                date_parts = parts[0].split('-')
                y1, m1, d1 = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
                h1, min1 = 0, 0
                
        # Get year, month, day, hour, minute for ts2
        if isinstance(ts2, datetime):
            y2, m2, d2, h2, min2 = ts2.year, ts2.month, ts2.day, ts2.hour, ts2.minute
        elif isinstance(ts2, str):
            # Fast string parsing - avoid full datetime conversion
            parts = ts2.split(' ')
            if len(parts) >= 2:  # Has date and time
                date_parts = parts[0].split('-')
                time_parts = parts[1].split(':')
                y2, m2, d2 = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
                h2, min2 = int(time_parts[0]), int(time_parts[1])
            else:  # Date only
                date_parts = parts[0].split('-')
                y2, m2, d2 = int(date_parts[0]), int(date_parts[1]), int(date_parts[2])
                h2, min2 = 0, 0
                
        # Direct comparison of components - no object creation
        return (y1 == y2 and m1 == m2 and d1 == d2 and h1 == h2 and min1 == min2)
        
    except (ValueError, IndexError) as e:
        # Fall back to original method only if parsing fails
        try:
            # Convert to datetime only if needed and only once
            if isinstance(ts1, str):
                ts1 = datetime.strptime(ts1, '%Y-%m-%d %H:%M:%S')
            if isinstance(ts2, str):
                ts2 = datetime.strptime(ts2, '%Y-%m-%d %H:%M:%S')
                
            # Compare only year, month, day, hour, minute
            return (ts1.year == ts2.year and 
                    ts1.month == ts2.month and 
                    ts1.day == ts2.day and 
                    ts1.hour == ts2.hour and 
                    ts1.minute == ts2.minute)
        except Exception:
            return False

def sim_entries(minute_data, instances, fee_rate, trade_log, open_positions, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, output_folder):
    # Check for regular trade entries first
    active_trades = [trade for trade in instances if trade['Active Date'] is not None and 
                     compare_timestamps_ignore_seconds(trade['Active Date'], minute_data['timestamp'])]
    for trade in active_trades:
        # Check if trade meets the minimum pending age requirement
        if USE_MIN_PENDING_AGE or USE_MAX_PENDING_AGE:
            confirm_date = datetime.strptime(trade['confirm_date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['confirm_date'], str) else trade['confirm_date']
            active_date = datetime.strptime(trade['Active Date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['Active Date'], str) else trade['Active Date']
            difference_minutes = (active_date - confirm_date).total_seconds() / 60
            if USE_MIN_PENDING_AGE and difference_minutes < MIN_PENDING_AGE:
                continue
            if USE_MAX_PENDING_AGE and difference_minutes > MAX_PENDING_AGE:
                continue
        
        # Group filtering is now done at load time, no need to check here

        trade_name = f"{trade['Timeframe']} {trade['direction']}({str(uuid.uuid4())[:4]}...{str(uuid.uuid4())[-4:]})"
        entry_price = float(trade['entry'])
        
        # Process regular entry and update position values
        total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
            trade, trade_name, entry_price, minute_data, trade_log, open_positions, 
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
            cash_on_hand, fee_rate, output_folder)
    
    # Check for fibonacci level entries based on open positions
    # This is more efficient as we only need to check positions that are already active
    
    # Process each fibonacci level separately
    if DD_on_fib0_5:
        # Filter only those open positions that have reached fib0.5 at the current timestamp
        # Make sure DateReached0.5 exists and is not empty/None/NaN
        fib_positions = [pos for pos in open_positions if 
                        'DateReached0.5' in pos and 
                        pos['DateReached0.5'] is not None and
                        pos['DateReached0.5'] != "" and
                        compare_timestamps_ignore_seconds(pos['DateReached0.5'], minute_data['timestamp']) and
                        pos.get('fib0.5') is not None]
        
        for position in fib_positions:
            # Use the original trade ID with the fib level appended
            fib_trade_id = f"{position['trade_id']}_fib0.5"
            fib_trade_name = f"{position['Timeframe']} {position['Direction']} Fib0.5"
            fib_entry_price = float(position['fib0.5'])
            
            # Pass the position directly to process_entry with the new trade ID
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
                position, fib_trade_name, fib_entry_price, minute_data, trade_log, open_positions, 
                total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                cash_on_hand, fee_rate, output_folder, trade_id=fib_trade_id)
            
    if DD_on_fib0_0:
        # Filter only those open positions that have reached fib0.0 at the current timestamp
        # Make sure DateReached0.0 exists and is not empty/None/NaN
        fib_positions = [pos for pos in open_positions if 
                        'DateReached0.0' in pos and 
                        pos['DateReached0.0'] is not None and
                        pos['DateReached0.0'] != "" and
                        compare_timestamps_ignore_seconds(pos['DateReached0.0'], minute_data['timestamp']) and
                        pos.get('fib0.0') is not None]
        
        for position in fib_positions:
            # Use the original trade ID with the fib level appended
            fib_trade_id = f"{position['trade_id']}_fib0.0"
            fib_trade_name = f"{position['Timeframe']} {position['Direction']} Fib0.0"
            fib_entry_price = float(position['fib0.0'])
            
            # Pass the position directly to process_entry with the new trade ID
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
                position, fib_trade_name, fib_entry_price, minute_data, trade_log, open_positions, 
                total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                cash_on_hand, fee_rate, output_folder, trade_id=fib_trade_id)
            
    if DD_on_fib_0_5:
        # Filter only those open positions that have reached fib-0.5 at the current timestamp
        # Make sure DateReached-0.5 exists and is not empty/None/NaN
        fib_positions = [pos for pos in open_positions if 
                        'DateReached-0.5' in pos and 
                        pos['DateReached-0.5'] is not None and
                        pos['DateReached-0.5'] != "" and
                        compare_timestamps_ignore_seconds(pos['DateReached-0.5'], minute_data['timestamp']) and
                        pos.get('fib-0.5') is not None]
        
        for position in fib_positions:
            # Use the original trade ID with the fib level appended
            fib_trade_id = f"{position['trade_id']}_fib-0.5"
            fib_trade_name = f"{position['Timeframe']} {position['Direction']} Fib-0.5"
            fib_entry_price = float(position['fib-0.5'])
            
            # Pass the position directly to process_entry with the new trade ID
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
                position, fib_trade_name, fib_entry_price, minute_data, trade_log, open_positions, 
                total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                cash_on_hand, fee_rate, output_folder, trade_id=fib_trade_id)
            
    if DD_on_fib_1_0:
        # Filter only those open positions that have reached fib-1.0 at the current timestamp
        # Make sure DateReached-1.0 exists and is not empty/None/NaN
        fib_positions = [pos for pos in open_positions if 
                        'DateReached-1.0' in pos and 
                        pos['DateReached-1.0'] is not None and
                        pos['DateReached-1.0'] != "" and
                        compare_timestamps_ignore_seconds(pos['DateReached-1.0'], minute_data['timestamp']) and
                        pos.get('fib-1.0') is not None]
        
        for position in fib_positions:
            # Use the original trade ID with the fib level appended
            fib_trade_id = f"{position['trade_id']}_fib-1.0"
            fib_trade_name = f"{position['Timeframe']} {position['Direction']} Fib-1.0"
            fib_entry_price = float(position['fib-1.0'])
            
            # Pass the position directly to process_entry with the new trade ID
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
                position, fib_trade_name, fib_entry_price, minute_data, trade_log, open_positions, 
                total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                cash_on_hand, fee_rate, output_folder, trade_id=fib_trade_id)

    return total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand

def process_entry(trade, trade_name, entry_price, minute_data, trade_log, open_positions, 
                 total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                 cash_on_hand, fee_rate, output_folder, trade_id=None):
    """Process a trade entry and calculate updated position values"""
    
    # Calculate position details
    position_size = calculate_position_size(entry_price, cash_on_hand)
    trade_cost = float(position_size) * float(entry_price)
    trade_fee = trade_cost * float(fee_rate)
    trade_id = trade_id if trade_id else str(uuid.uuid4())  # Generate unique ID for each trade

    # Determine direction and update position values
    direction_field = 'Direction' if 'Direction' in trade else 'direction'
    direction = trade[direction_field]
    
    if direction.lower() == 'long':
        old_cost_basis = float(long_cost_basis)
        total_long_position += float(position_size)
        long_cost_basis = (float(long_cost_basis) * (float(total_long_position) - float(position_size)) + trade_cost) / float(total_long_position) if total_long_position > 0 else 0
        cost_basis_change = f"{round(old_cost_basis, 4)} -> {round(long_cost_basis, 4)}"
        order_type = 'open long'
        units_traded = float(position_size)
    else:  # short
        old_cost_basis = float(short_cost_basis)
        total_short_position += float(position_size)
        short_cost_basis = (float(short_cost_basis) * (float(total_short_position) - float(position_size)) + trade_cost) / float(total_short_position) if total_short_position > 0 else 0
        cost_basis_change = f"{round(old_cost_basis, 4)} -> {round(short_cost_basis, 4)}"
        order_type = 'open short'
        units_traded = -float(position_size)

    cash_on_hand -= trade_fee  # Deduct fee only from cash, no trade_cost for futures

    # Get field names based on what's available in the trade dictionary
    target_field = 'Target Price' if 'Target Price' in trade else 'target'
    confirm_date_field = 'confirm_date' if 'confirm_date' in trade else 'Confirm Date'
    active_date_field = 'Active Date' if 'Active Date' in trade else 'active_date'
    completed_date_field = 'Completed Date' if 'Completed Date' in trade else 'completed_date'
    timeframe_field = 'Timeframe' if 'Timeframe' in trade else 'timeframe'

    # Create comprehensive trade log entry
    trade_entry_dict = {
        'trade_id': trade_id,
        'confirm_date': datetime.strptime(trade[confirm_date_field], '%Y-%m-%d %H:%M:%S') if isinstance(trade[confirm_date_field], str) else trade[confirm_date_field],
        'active_date': datetime.strptime(trade[active_date_field], '%Y-%m-%d %H:%M:%S') if isinstance(trade[active_date_field], str) else trade[active_date_field],
        'trade_date': minute_data['timestamp'],  # Current minute being processed
        'completed_date': datetime.strptime(trade[completed_date_field], '%Y-%m-%d %H:%M:%S') if isinstance(trade[completed_date_field], str) else trade[completed_date_field],
        'order_type': order_type,
        'trade_fee': round(trade_fee, 4),
        'price': round(entry_price, 4),
        'units_traded': round(units_traded, 4),
        'cost_basis_change': cost_basis_change,
        'realized_PnL': None,
        'total_long_position': round(total_long_position, 4),  # Include current position values
        'total_short_position': round(total_short_position, 4),  # Include current position values
        'balance': round(total_long_position - total_short_position, 4),
        'ind_PnL': 0,  # Set to 0 when entering trades
        'timeframe': trade[timeframe_field],
        'Name': trade_name,
        'winner': None,
        'loss_reason': None
    }
    trade_log.append(trade_entry_dict)

    # Add to open positions with all required fields
    open_position = {
        'trade_id': trade_id,
        'confirm_date': datetime.strptime(trade[confirm_date_field], '%Y-%m-%d %H:%M:%S') if isinstance(trade[confirm_date_field], str) else trade[confirm_date_field],
        'active_date': datetime.strptime(trade[active_date_field], '%Y-%m-%d %H:%M:%S') if isinstance(trade[active_date_field], str) else trade[active_date_field],
        'trade_date': minute_data['timestamp'],
        'Completed Date': datetime.strptime(trade[completed_date_field], '%Y-%m-%d %H:%M:%S') if isinstance(trade[completed_date_field], str) else trade[completed_date_field],
        'Target Price': float(trade[target_field]) if trade.get(target_field) is not None else None,
        'Position Size': float(position_size),
        'Direction': direction,
        'Open Price': entry_price,
        'Timeframe': trade[timeframe_field],
        'Name': trade_name,
        # Explicitly add Fibonacci data fields with proper date handling
        'DateReached0.5': datetime.strptime(trade['DateReached0.5'], '%Y-%m-%d %H:%M:%S') if 'DateReached0.5' in trade and isinstance(trade['DateReached0.5'], str) and trade['DateReached0.5'] else trade.get('DateReached0.5'),
        'DateReached0.0': datetime.strptime(trade['DateReached0.0'], '%Y-%m-%d %H:%M:%S') if 'DateReached0.0' in trade and isinstance(trade['DateReached0.0'], str) and trade['DateReached0.0'] else trade.get('DateReached0.0'),
        'DateReached-0.5': datetime.strptime(trade['DateReached-0.5'], '%Y-%m-%d %H:%M:%S') if 'DateReached-0.5' in trade and isinstance(trade['DateReached-0.5'], str) and trade['DateReached-0.5'] else trade.get('DateReached-0.5'),
        'DateReached-1.0': datetime.strptime(trade['DateReached-1.0'], '%Y-%m-%d %H:%M:%S') if 'DateReached-1.0' in trade and isinstance(trade['DateReached-1.0'], str) and trade['DateReached-1.0'] else trade.get('DateReached-1.0'),
        'fib0.5': float(trade['fib0.5']) if 'fib0.5' in trade and trade['fib0.5'] is not None else None,
        'fib0.0': float(trade['fib0.0']) if 'fib0.0' in trade and trade['fib0.0'] is not None else None,
        'fib-0.5': float(trade['fib-0.5']) if 'fib-0.5' in trade and trade['fib-0.5'] is not None else None,
        'fib-1.0': float(trade['fib-1.0']) if 'fib-1.0' in trade and trade['fib-1.0'] is not None else None
    }
    
    open_positions.append(open_position)
    
    # Write trade log entry to both trades_all.csv and trades_yyyymm.csv based on flag
    trades_columns = ['trade_id', 'confirm_date', 'active_date', 'trade_date', 'completed_date', 'order_type', 'trade_fee', 'price', 'units_traded', 'cost_basis_change', 'realized_PnL', 'total_long_position', 'total_short_position', 'balance', 'ind_PnL', 'timeframe', 'Name', 'winner', 'loss_reason']
    write_log_entry(trade_entry_dict, os.path.join(output_folder, 'trades_all.csv'), trades_columns)
    if CREATE_TRADES_BY_MONTH:
        write_log_entry(trade_entry_dict, os.path.join(output_folder, f'trades_{minute_data["timestamp"].strftime("%Y%m")}.csv'), trades_columns)

    # Write open positions log entry to open_positions.csv
    open_position_columns = list(open_position.keys())
    write_log_entry(open_position, os.path.join(output_folder, 'open_positions.csv'), open_position_columns)
    
    return total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand
