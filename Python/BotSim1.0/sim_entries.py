import os
import uuid
from datetime import datetime, timedelta
from log_utils import write_log_entry
from config import *
from simulation import ALLOWED_SITUATIONS
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

def timeframe_to_minutes(tf):
    """Convert a timeframe string (like '1h', '15m', '1d') to minutes"""
    if tf.endswith('m'):
        return int(tf[:-1])
    elif tf.endswith('h'):
        return int(tf[:-1]) * 60
    elif tf.endswith('d'):
        return int(tf[:-1]) * 1440
    return 0  # Default if format is unknown


def sim_entries(minute_data, relevant_instances, fee_rate, trade_log, open_positions, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, output_folder, all_instances=None):
    # Check for regular trade entries
    # Filter active trades by situation
    active_trades = [
        trade for trade in relevant_instances
        if (trade['Active Date'] is not None and 
            compare_timestamps_ignore_seconds(trade['Active Date'], minute_data['timestamp']) and
            trade.get('situation', '1v1') in ALLOWED_SITUATIONS)
    ]

    for trade in active_trades:
        # Check if trade meets the minimum/maximum pending age requirements
        confirm_date = datetime.strptime(trade['confirm_date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['confirm_date'], str) else trade['confirm_date']
        active_date = datetime.strptime(trade['Active Date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['Active Date'], str) else trade['Active Date']
        
        # Time-based pending age checks
        if USE_MIN_PENDING_AGE or USE_MAX_PENDING_AGE:
            difference_minutes = (active_date - confirm_date).total_seconds() / 60
            if USE_MIN_PENDING_AGE and difference_minutes < MIN_PENDING_AGE:
                continue
            if USE_MAX_PENDING_AGE and difference_minutes > MAX_PENDING_AGE:
                continue
                
        # Candle-based pending age checks
        if USE_MIN_PENDING_CANDLES or USE_MAX_PENDING_CANDLES:
            # Get the timeframe in minutes
            tf_minutes = timeframe_to_minutes(trade['Timeframe'])
            if tf_minutes == 0:  # Invalid timeframe, skip candle-based checks
                continue
                
            # Calculate number of candles between confirm and active dates
            time_diff = active_date - confirm_date
            total_minutes = time_diff.total_seconds() / 60
            candle_count = total_minutes / tf_minutes
            
            # Apply min/max candle count checks
            if USE_MIN_PENDING_CANDLES and candle_count < MIN_PENDING_CANDLES:
                continue
            if USE_MAX_PENDING_CANDLES and candle_count > MAX_PENDING_CANDLES:
                continue

        # Check if trade meets the trigger trade requirements.  Checking the global flag is part of the sub-function.
        has_trigger, trigger_trade = check_for_trigger_trades(trade, relevant_instances, all_instances)
        if not has_trigger:
            continue
                
        # Group filtering is now done at load time, no need to check here

        trade_name = f"{trade['Timeframe']} {trade['direction']}({str(uuid.uuid4())[:4]}...{str(uuid.uuid4())[-4:]})"
        entry_price = float(trade['entry'])
        
        # Process regular entry and update position values
        total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
            trade, trade_name, entry_price, minute_data, trade_log, open_positions, 
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
            cash_on_hand, fee_rate, output_folder, trigger_trade=trigger_trade)
    
    # Check for fibonacci level entries based on open positions
    # This is more efficient as we only need to check positions that are already active
    
    # Process each fibonacci level separately
    if DD_on_fib0_5:
        # Filter only those open positions that have reached fib0.5 at the current timestamp
        # Make sure DateReached0.5 exists and is not empty/None/NaN
        # Only include original trades (those that don't have 'fib' in the trade_id)
        fib_positions = [pos for pos in open_positions if 
                        'DateReached0.5' in pos and 
                        pos['DateReached0.5'] is not None and
                        pos['DateReached0.5'] != "" and
                        compare_timestamps_ignore_seconds(pos['DateReached0.5'], minute_data['timestamp']) and
                        pos.get('fib0.5') is not None and
                        'fib' not in str(pos.get('trade_id', ''))]
        
        for position in fib_positions:
            # Use the original trade ID with the fib level appended
            fib_trade_id = f"{position['trade_id']}_fib0.5"
            fib_trade_name = f"{position['Timeframe']} {position['Direction']} Fib0.5"
            fib_entry_price = float(position['fib0.5'])
            
            # Pass the position directly to process_entry with the new trade ID and fib_level
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
                position, fib_trade_name, fib_entry_price, minute_data, trade_log, open_positions, 
                total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                cash_on_hand, fee_rate, output_folder, trade_id=fib_trade_id, fib_level="0.5")
            
    if DD_on_fib0_0:
        # Filter only those open positions that have reached fib0.0 at the current timestamp
        # Make sure DateReached0.0 exists and is not empty/None/NaN
        # Only include original trades (those that don't have 'fib' in the trade_id)
        fib_positions = [pos for pos in open_positions if 
                        'DateReached0.0' in pos and 
                        pos['DateReached0.0'] is not None and
                        pos['DateReached0.0'] != "" and
                        compare_timestamps_ignore_seconds(pos['DateReached0.0'], minute_data['timestamp']) and
                        pos.get('fib0.0') is not None and
                        'fib' not in str(pos.get('trade_id', ''))]
        
        for position in fib_positions:
            # Use the original trade ID with the fib level appended
            fib_trade_id = f"{position['trade_id']}_fib0.0"
            fib_trade_name = f"{position['Timeframe']} {position['Direction']} Fib0.0"
            fib_entry_price = float(position['fib0.0'])
            
            # Pass the position directly to process_entry with the new trade ID and fib_level
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
                position, fib_trade_name, fib_entry_price, minute_data, trade_log, open_positions, 
                total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                cash_on_hand, fee_rate, output_folder, trade_id=fib_trade_id, fib_level="0.0")
    
    if DD_on_fib_0_5:
        # Filter only those open positions that have reached fib-0.5 at the current timestamp
        # Make sure DateReached-0.5 exists and is not empty/None/NaN
        # Only include original trades (those that don't have 'fib' in the trade_id)
        fib_positions = [pos for pos in open_positions if 
                        'DateReached-0.5' in pos and 
                        pos['DateReached-0.5'] is not None and
                        pos['DateReached-0.5'] != "" and
                        compare_timestamps_ignore_seconds(pos['DateReached-0.5'], minute_data['timestamp']) and
                        pos.get('fib-0.5') is not None and
                        'fib' not in str(pos.get('trade_id', ''))]
        
        for position in fib_positions:
            # Use the original trade ID with the fib level appended
            fib_trade_id = f"{position['trade_id']}_fib-0.5"
            fib_trade_name = f"{position['Timeframe']} {position['Direction']} Fib-0.5"
            fib_entry_price = float(position['fib-0.5'])
            
            # Pass the position directly to process_entry with the new trade ID and fib_level
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
                position, fib_trade_name, fib_entry_price, minute_data, trade_log, open_positions, 
                total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                cash_on_hand, fee_rate, output_folder, trade_id=fib_trade_id, fib_level="-0.5")
    
    if DD_on_fib_1_0:
        # Filter only those open positions that have reached fib-1.0 at the current timestamp
        # Make sure DateReached-1.0 exists and is not empty/None/NaN
        # Only include original trades (those that don't have 'fib' in the trade_id)
        fib_positions = [pos for pos in open_positions if 
                        'DateReached-1.0' in pos and 
                        pos['DateReached-1.0'] is not None and
                        pos['DateReached-1.0'] != "" and
                        compare_timestamps_ignore_seconds(pos['DateReached-1.0'], minute_data['timestamp']) and
                        pos.get('fib-1.0') is not None and
                        'fib' not in str(pos.get('trade_id', ''))]
        
        for position in fib_positions:
            # Use the original trade ID with the fib level appended
            fib_trade_id = f"{position['trade_id']}_fib-1.0"
            fib_trade_name = f"{position['Timeframe']} {position['Direction']} Fib-1.0"
            fib_entry_price = float(position['fib-1.0'])
            
            # Pass the position directly to process_entry with the new trade ID and fib_level
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand = process_entry(
                position, fib_trade_name, fib_entry_price, minute_data, trade_log, open_positions, 
                total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                cash_on_hand, fee_rate, output_folder, trade_id=fib_trade_id, fib_level="-1.0")
    
    return total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand


def check_for_trigger_trades(trade, relevant_instances, all_instances=None):
    """
    Check if a trade meets the trigger trade criteria based on enabled flags.
    Returns (bool, dict): 
        - First value is True if the trade should be taken, False otherwise
        - Second value is the trigger trade information if a valid trigger is found, None otherwise
    """
    
    # If none of the trigger trade flags are enabled, take all trades
    if not (tt_stf_any_inside_activation or tt_stf_same_minute or tt_stf_within_x_candles or tt_stf_within_x_minutes):
        return True, None
    
    # Check if we have the required dates
    confirm_date = trade.get('confirm_date')
    active_date = trade.get('Active Date')
    if confirm_date is None or active_date is None:
        return False  # Skip trades without proper dates
    
    # Convert to datetime if they're strings
    if isinstance(confirm_date, str):
        confirm_date = datetime.strptime(confirm_date, '%Y-%m-%d %H:%M:%S')
    if isinstance(active_date, str):
        active_date = datetime.strptime(active_date, '%Y-%m-%d %H:%M:%S')
    
    timeframe = trade['Timeframe']
    direction = trade['direction']
    
    # Check tt_stf_any_inside_activation (previously wait_for_seconds)
    if tt_stf_any_inside_activation and all_instances is not None:
        found_trigger = False
        
        # Check all instances between confirm_date and active_date
        current_minute = confirm_date.replace(second=0, microsecond=0)
        end_minute = active_date.replace(second=0, microsecond=0)
        
        # Check each minute in the range
        while current_minute <= end_minute and not found_trigger:
            if current_minute in all_instances:
                for other_trade in all_instances[current_minute]:
                    # Skip the current trade and check same timeframe/direction
                    if (other_trade is not trade and 
                        other_trade.get('Timeframe') == timeframe and 
                        other_trade.get('direction') == direction and
                        other_trade.get('confirm_date') is not None and
                        other_trade.get('Active Date') is not None):
                        
                        other_confirm = other_trade['confirm_date']
                        other_active = other_trade['Active Date']
                        
                        # Convert to datetime if needed
                        if isinstance(other_confirm, str):
                            other_confirm = datetime.strptime(other_confirm, '%Y-%m-%d %H:%M:%S')
                        if isinstance(other_active, str):
                            other_active = datetime.strptime(other_active, '%Y-%m-%d %H:%M:%S')
                        
                        # Get the other trade's completed date if it exists
                        other_completed = other_trade.get('Completed Date')
                        if isinstance(other_completed, str) and other_completed.strip():
                            other_completed = datetime.strptime(other_completed, '%Y-%m-%d %H:%M:%S')
                        
                        # Check if the other trade's confirm_date and active_date are between this trade's confirm_date and active_date
                        # AND either there is no completed date OR it's outside our window
                        if (confirm_date < other_confirm < active_date and 
                            confirm_date < other_active < active_date and
                            (other_completed is None or 
                             not (confirm_date < other_completed < active_date))):
                            found_trigger = True
                            # Return the trigger trade information
                            return True, {
                                'trade': other_trade,
                                'confirm_date': other_confirm,
                                'active_date': other_active,
                                'completed_date': other_completed,
                                'entry': other_trade.get('entry')
                            }
            
            # Move to the next minute
            current_minute += timedelta(minutes=1)
        
        # If we checked this flag and no other flags are enabled, return False
        if not (tt_stf_same_minute or tt_stf_within_x_candles):
            return False, None
    
    # Check tt_stf_same_minute (previously take_older_double_activation)
    if tt_stf_same_minute:
        found_trigger = False
        
        # Only check instances in the same minute as the current trade
        for other_trade in relevant_instances:
            if (other_trade is not trade and 
                other_trade.get('Timeframe') == timeframe and 
                other_trade.get('direction') == direction and
                other_trade.get('confirm_date') is not None and
                other_trade.get('Active Date') is not None):
                
                other_confirm = other_trade['confirm_date']
                other_active = other_trade['Active Date']
                
                # Convert to datetime if needed
                if isinstance(other_confirm, str):
                    other_confirm = datetime.strptime(other_confirm, '%Y-%m-%d %H:%M:%S')
                if isinstance(other_active, str):
                    other_active = datetime.strptime(other_active, '%Y-%m-%d %H:%M:%S')
                
                # Get the other trade's completed date if it exists
                other_completed = other_trade.get('Completed Date')
                if isinstance(other_completed, str) and other_completed.strip():
                    other_completed = datetime.strptime(other_completed, '%Y-%m-%d %H:%M:%S')
                
                # Check if the other trade's confirm_date and active_date are between this trade's confirm_date and active_date
                if (confirm_date < other_confirm < active_date and 
                    confirm_date < other_active < active_date):
                    # Return the trigger trade information
                    return True, {
                        'trade': other_trade,
                        'confirm_date': other_confirm,
                        'active_date': other_active,
                        'completed_date': other_completed,
                        'entry': other_trade.get('entry')
                    }
        
        # If we checked this flag and tt_stf_within_x_candles is not enabled, return False
        if not tt_stf_within_x_candles:
            return False, None
    
    # Check tt_stf_within_x_candles (previously take_older_activated_within_x_candles)
    if tt_stf_within_x_candles and all_instances is not None:
        # Create a cache for the tt_stf_within_x_candles checks if it doesn't exist
        if 'trigger_trades_cache' not in globals():
            global trigger_trades_cache
            trigger_trades_cache = {}
        
        # Calculate the active date window based on timeframe multiples
        tf_minutes = timeframe_to_minutes(timeframe)
        window_minutes = tf_minutes * tt_stf_within_x
        active_window_start = active_date - timedelta(minutes=window_minutes)
        active_minute = active_date.replace(second=0, microsecond=0)
        
        # Create a key for the cache based on timeframe and direction
        cache_key = f"{timeframe}_{direction}"
        
        # Build the cache if needed
        if cache_key not in trigger_trades_cache:
            trigger_trades_cache[cache_key] = {}
            
            # First pass: collect all trades with matching timeframe and direction
            for minute, minute_trades in all_instances.items():
                for t in minute_trades:
                    if (t.get('Timeframe') == timeframe and 
                        t.get('direction') == direction and 
                        t.get('confirm_date') is not None and 
                        t.get('Active Date') is not None):
                        
                        # Get and convert dates
                        t_confirm = t['confirm_date']
                        t_active = t['Active Date']
                        
                        if isinstance(t_confirm, str):
                            t_confirm = datetime.strptime(t_confirm, '%Y-%m-%d %H:%M:%S')
                        if isinstance(t_active, str):
                            t_active = datetime.strptime(t_active, '%Y-%m-%d %H:%M:%S')
                        
                        # Get completed date if it exists
                        t_completed = t.get('Completed Date')
                        if isinstance(t_completed, str) and t_completed.strip():
                            t_completed = datetime.strptime(t_completed, '%Y-%m-%d %H:%M:%S')
                        else:
                            t_completed = None
                        
                        # Store the trade with all its datetime objects, indexed by activation minute
                        t_active_minute = t_active.replace(second=0, microsecond=0)
                        if t_active_minute not in trigger_trades_cache[cache_key]:
                            trigger_trades_cache[cache_key][t_active_minute] = []
                        
                        trigger_trades_cache[cache_key][t_active_minute].append({
                            'trade': t,
                            'confirm_date': t_confirm,
                            'active_date': t_active,
                            'completed_date': t_completed
                        })
        
        # Check if we have a matching trigger trade
        if cache_key in trigger_trades_cache:
            # Check minutes in our window
            window_start_minute = active_window_start.replace(second=0, microsecond=0)
            
            for minute, potential_triggers in trigger_trades_cache[cache_key].items():
                # Skip minutes outside our window
                if minute < window_start_minute or minute >= active_minute:
                    continue
                
                # Check trades in this minute
                for trigger in potential_triggers:
                    # Skip self comparison
                    if trigger['trade'] is trade:
                        continue
                    
                    # Check conditions using pre-converted datetime objects
                    if (confirm_date < trigger['confirm_date'] < active_date and 
                        active_window_start <= trigger['active_date'] < active_date and 
                        (trigger['completed_date'] is None or trigger['completed_date'] > active_date)):
                        
                        # Return the trigger trade information
                        return True, {
                            'trade': trigger['trade'],
                            'confirm_date': trigger['confirm_date'],
                            'active_date': trigger['active_date'],
                            'completed_date': trigger['completed_date'],
                            'entry': trigger['trade'].get('entry')
                        }
    
    # Check tt_stf_within_x_minutes - look for triggers within a fixed number of minutes
    if tt_stf_within_x_minutes and all_instances is not None and tt_stf_within_minutes > 0:
        # Create a cache for the tt_stf_within_x_minutes checks if it doesn't exist
        if 'trigger_trades_minutes_cache' not in globals():
            global trigger_trades_minutes_cache
            trigger_trades_minutes_cache = {}
        
        # Calculate the active date window based on fixed minutes
        active_window_start = active_date - timedelta(minutes=tt_stf_within_minutes)
        active_minute = active_date.replace(second=0, microsecond=0)
        
        # Create a key for the cache based on timeframe and direction
        cache_key = f"{timeframe}_{direction}"
        
        # Build the cache if needed
        if cache_key not in trigger_trades_minutes_cache:
            trigger_trades_minutes_cache[cache_key] = {}
            
            # First pass: collect all trades with matching timeframe and direction
            for minute, minute_trades in all_instances.items():
                for t in minute_trades:
                    if (t.get('Timeframe') == timeframe and 
                        t.get('direction') == direction and 
                        t.get('confirm_date') is not None and 
                        t.get('Active Date') is not None):
                        
                        # Get and convert dates
                        t_confirm = t['confirm_date']
                        t_active = t['Active Date']
                        
                        if isinstance(t_confirm, str):
                            t_confirm = datetime.strptime(t_confirm, '%Y-%m-%d %H:%M:%S')
                        if isinstance(t_active, str):
                            t_active = datetime.strptime(t_active, '%Y-%m-%d %H:%M:%S')
                        
                        # Get completed date if it exists
                        t_completed = t.get('Completed Date')
                        if isinstance(t_completed, str) and t_completed.strip():
                            t_completed = datetime.strptime(t_completed, '%Y-%m-%d %H:%M:%S')
                        else:
                            t_completed = None
                        
                        # Store the trade with all its datetime objects, indexed by activation minute
                        t_active_minute = t_active.replace(second=0, microsecond=0)
                        if t_active_minute not in trigger_trades_minutes_cache[cache_key]:
                            trigger_trades_minutes_cache[cache_key][t_active_minute] = []
                        
                        trigger_trades_minutes_cache[cache_key][t_active_minute].append({
                            'trade': t,
                            'confirm_date': t_confirm,
                            'active_date': t_active,
                            'completed_date': t_completed
                        })
        
        # Check if we have a matching trigger trade
        if cache_key in trigger_trades_minutes_cache:
            # Check minutes in our window
            window_start_minute = active_window_start.replace(second=0, microsecond=0)
            
            for minute, potential_triggers in trigger_trades_minutes_cache[cache_key].items():
                # Skip minutes outside our window
                if minute < window_start_minute or minute >= active_minute:
                    continue
                
                # Check trades in this minute
                for trigger in potential_triggers:
                    # Skip self comparison
                    if trigger['trade'] is trade:
                        continue
                    
                    # Check conditions using pre-converted datetime objects
                    if (confirm_date < trigger['confirm_date'] < active_date and 
                        active_window_start <= trigger['active_date'] < active_date and 
                        (trigger['completed_date'] is None or trigger['completed_date'] > active_date)):
                        
                        # Return the trigger trade information
                        return True, {
                            'trade': trigger['trade'],
                            'confirm_date': trigger['confirm_date'],
                            'active_date': trigger['active_date'],
                            'completed_date': trigger['completed_date'],
                            'entry': trigger['trade'].get('entry')
                        }
    
    # If we reach here, no trigger trades were found with any enabled flag
    return False, None

def process_entry(trade, trade_name, entry_price, minute_data, trade_log, open_positions, 
                 total_long_position, total_short_position, long_cost_basis, short_cost_basis, 
                 cash_on_hand, fee_rate, output_folder, trade_id=None, fib_level=None, trigger_trade=None):
    """Process a trade entry and calculate updated position values"""
    
    # Check position limits based on leverage and position size
    if position_size_method == 3 and MAX_LEVERAGE is not None and MAX_LEVERAGE > 0:
        max_allowed_positions = int((MAX_LEVERAGE * 100) / position_size_percent)
        if max_allowed_positions > 0 and len(open_positions) >= max_allowed_positions:
            return total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand
    
    # Calculate AMPD values (always calculate these, regardless of use_ampd_percent)
    ampd_p_value = 0.0
    ampd_t_value = 0.0
    
    # Get the confirm and active dates from the trade
    confirm_date = trade.get('confirm_date')
    active_date = trade.get('Active Date')
    
    # Initialize AMPD values
    ampd_p_value = 0.0
    ampd_t_value = 0.0
    
    # Calculate pending time factor (p_value)
    if confirm_date is not None and active_date is not None:
        if isinstance(confirm_date, str):
            confirm_date = datetime.strptime(confirm_date, '%Y-%m-%d %H:%M:%S')
        if isinstance(active_date, str):
            active_date = datetime.strptime(active_date, '%Y-%m-%d %H:%M:%S')
            
        if ampd_use_pending_time:
            pending_days = (active_date - confirm_date).total_seconds() / (24 * 3600)
            # Normalize to 0-1 range based on ampd_pending_time_high and round to 4 decimal places
            ampd_p_value = round(min(pending_days / ampd_pending_time_high, 1.0), 4) if ampd_pending_time_high > 0 else 0.0
    
    # Calculate trigger time factor (t_value)
    if ampd_use_trigger_time and trigger_trade and 'trade' in trigger_trade and 'Active Date' in trade:
        # Get the trigger trade's active date from the nested structure
        trigger_trade_data = trigger_trade['trade']
        trigger_active_date = trigger_trade_data.get('Active Date')
        
        # Parse dates if they're strings
        if isinstance(trigger_active_date, str):
            trigger_active_date = datetime.strptime(trigger_active_date, '%Y-%m-%d %H:%M:%S')
        if isinstance(active_date, str):
            active_date = datetime.strptime(active_date, '%Y-%m-%d %H:%M:%S')
            
        # Make sure we have valid dates before proceeding
        if trigger_active_date is None or active_date is None:
            return
        
        # Calculate time difference in minutes
        time_diff_minutes = (active_date - trigger_active_date).total_seconds() / 60
        
        # Apply the formula: (ampd_trigger_time_high - time_diff) / ampd_trigger_time_high
        # Clamp the result between 0 and 1
        if ampd_trigger_time_high > 0:
            ampd_t_value = (ampd_trigger_time_high - time_diff_minutes) / ampd_trigger_time_high
            ampd_t_value = max(0.0, min(1.0, ampd_t_value))  # Clamp between 0 and 1
            ampd_t_value = round(ampd_t_value, 4)  # Round to 4 decimal places
    
    # Calculate position details
    position_size = calculate_position_size(entry_price, cash_on_hand, ampd_p_value=ampd_p_value, ampd_t_value=ampd_t_value)
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

    # Store the opening fee in the position instead of deducting from cash
    # The fee will be accounted for when the position is closed
    opening_fee = trade_fee

    # Get field names based on what's available in the trade dictionary
    target_field = 'Target Price' if 'Target Price' in trade else 'target'
    confirm_date_field = 'confirm_date' if 'confirm_date' in trade else 'Confirm Date'
    active_date_field = 'Active Date' if 'Active Date' in trade else 'active_date'
    completed_date_field = 'Completed Date' if 'Completed Date' in trade else 'completed_date'
    timeframe_field = 'Timeframe' if 'Timeframe' in trade else 'timeframe'
    
    # Determine the appropriate trade date based on whether this is a fib DD trade
    trade_date = minute_data['timestamp']  # Default to current minute
    if fib_level:  # If this is a fib level trade, use the date when the fib level was reached
        date_field_name = f'DateReached{fib_level}'
        if date_field_name in trade and trade[date_field_name] is not None and trade[date_field_name] != "":
            trade_date = datetime.strptime(trade[date_field_name], '%Y-%m-%d %H:%M:%S') if isinstance(trade[date_field_name], str) else trade[date_field_name]

    # Create comprehensive trade log entry
    trade_entry_dict = {
        'trade_id': trade_id,
        'confirm_date': datetime.strptime(trade[confirm_date_field], '%Y-%m-%d %H:%M:%S') if isinstance(trade[confirm_date_field], str) else trade[confirm_date_field],
        'active_date': datetime.strptime(trade[active_date_field], '%Y-%m-%d %H:%M:%S') if isinstance(trade[active_date_field], str) else trade[active_date_field],
        'trade_date': trade_date,  # Use the appropriate date for the trade
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
        'opening_fee': opening_fee,  # Store the opening fee for PnL calculation on close
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
        'ampd_p_value': float(ampd_p_value),  # Store pre-calculated pending time factor (ensure it's a float)
        'ampd_t_value': float(ampd_t_value),  # Store pre-calculated trigger time factor (ensure it's a float)
        # Explicitly add Fibonacci data fields with proper date handling
        'DateReached0.5': datetime.strptime(trade['DateReached0.5'], '%Y-%m-%d %H:%M:%S') if 'DateReached0.5' in trade and isinstance(trade['DateReached0.5'], str) and trade['DateReached0.5'] else trade.get('DateReached0.5'),
        'DateReached0.0': datetime.strptime(trade['DateReached0.0'], '%Y-%m-%d %H:%M:%S') if 'DateReached0.0' in trade and isinstance(trade['DateReached0.0'], str) and trade['DateReached0.0'] else trade.get('DateReached0.0'),
        'DateReached-0.5': datetime.strptime(trade['DateReached-0.5'], '%Y-%m-%d %H:%M:%S') if 'DateReached-0.5' in trade and isinstance(trade['DateReached-0.5'], str) and trade['DateReached-0.5'] else trade.get('DateReached-0.5'),
        'DateReached-1.0': datetime.strptime(trade['DateReached-1.0'], '%Y-%m-%d %H:%M:%S') if 'DateReached-1.0' in trade and isinstance(trade['DateReached-1.0'], str) and trade['DateReached-1.0'] else trade.get('DateReached-1.0'),
        'fib0.5': float(trade['fib0.5']) if 'fib0.5' in trade and trade['fib0.5'] is not None else None,
        'fib0.0': float(trade['fib0.0']) if 'fib0.0' in trade and trade['fib0.0'] is not None else None,
        'fib-0.5': float(trade['fib-0.5']) if 'fib-0.5' in trade and trade['fib-0.5'] is not None else None,
        'fib-1.0': float(trade['fib-1.0']) if 'fib-1.0' in trade and trade['fib-1.0'] is not None else None,
        # Instance metadata
        'instance_id': trade.get('instance_id', None),
        # Store MaxFib value from instance data
        'maxfib': float(trade['MaxFib']) if 'MaxFib' in trade and trade['MaxFib'] is not None and trade['MaxFib'] != '' and trade['MaxFib'] != 'None' else None,
        # Store extreme_price value from instance data
        'extreme_price': float(trade['extreme_price']) if 'extreme_price' in trade and trade['extreme_price'] is not None and trade['extreme_price'] != '' and trade['extreme_price'] != 'None' else None,
        # Trigger trade information
        'tt_instance_id': trigger_trade['trade'].get('instance_id') if trigger_trade and trigger_trade.get('trade') else None,
        'tt_confirm_date': trigger_trade.get('confirm_date') if trigger_trade else None,
        'tt_active_date': trigger_trade.get('active_date') if trigger_trade else None,
        'tt_completed_date': trigger_trade.get('completed_date') if trigger_trade else None,
        'tt_entry_price': float(trigger_trade['trade'].get('entry')) if trigger_trade and trigger_trade.get('trade') and trigger_trade['trade'].get('entry') is not None else None
    }
    
    # Store extreme_price_date from instance data
    if 'extreme_price_date' in trade and trade['extreme_price_date'] is not None and trade['extreme_price_date'] and trade['extreme_price_date'] != 'None':
        try:
            if isinstance(trade['extreme_price_date'], str) and trade['extreme_price_date'].strip():
                open_position['extreme_price_date'] = datetime.strptime(trade['extreme_price_date'], '%Y-%m-%d %H:%M:%S')
            else:
                open_position['extreme_price_date'] = trade['extreme_price_date']
        except (ValueError, TypeError):
            open_position['extreme_price_date'] = None
    else:
        open_position['extreme_price_date'] = None
    
    # Calculate max_position_drawdown as negative absolute value of (trade open price - extreme price) * position size
    max_position_drawdown = None
    if open_position['extreme_price'] is not None and entry_price is not None:
        # Calculate price difference based on direction
        if direction.lower() == 'long':
            price_diff = entry_price - float(open_position['extreme_price'])
        else:  # short
            price_diff = float(open_position['extreme_price']) - entry_price
            
        # Calculate absolute value and make it negative (drawdown is always a loss)
        max_position_drawdown = -abs(price_diff * float(position_size))
    
    open_position['max_position_drawdown'] = max_position_drawdown
    
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
