import os
from datetime import datetime, timedelta
from config import *
from log_utils import write_log_entry, remove_log_entry
from sim_entries import compare_timestamps_ignore_seconds  # Import the function for timestamp comparison

def close_trade(open_position, close_price, trade_log, open_positions, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, output_folder, minute_data, loss_reason=None):
    """Close a trade and return updated position information.
    
    Returns:
        tuple: (total_long_position, total_short_position, long_cost_basis, short_cost_basis,
               cash_on_hand, long_pnl, short_pnl, is_win, is_loss)
    """
    position_size = float(open_position['Position Size'])
    closing_fee = float(close_price) * position_size * float(fee_rate)
    opening_fee = float(open_position.get('opening_fee', 0))  # Get the stored opening fee
    open_price = float(open_position['Open Price'])
    
    # Individual PnL (trade in isolation, including both opening and closing fees)
    price_pnl = (float(close_price) - open_price) * position_size if open_position['Direction'] == 'long' else (open_price - float(close_price)) * position_size
    ind_PnL = price_pnl - opening_fee - closing_fee  # Include both fees in PnL

    if open_position['Direction'] == 'long':
        # Calculate realized PnL based on cost basis (portfolio impact)
        # Include both opening and closing fees in the PnL calculation
        realized_price_pnl = (float(close_price) - float(long_cost_basis)) * position_size
        realized_PnL = realized_price_pnl - opening_fee - closing_fee
        total_long_position -= position_size
        # Update cash on hand with the net PnL (already includes both fees)
        cash_on_hand += realized_PnL
        order_type = 'close long'
        units_traded = -position_size
        if total_long_position == 0:
            long_cost_basis = 0
            long_pnl = 0  # Reset PnL when all positions are closed
    elif open_position['Direction'] == 'short':
        # Calculate realized PnL based on cost basis (portfolio impact)
        # Include both opening and closing fees in the PnL calculation
        realized_price_pnl = (float(short_cost_basis) - float(close_price)) * position_size
        realized_PnL = realized_price_pnl - opening_fee - closing_fee
        total_short_position -= position_size
        # Update cash on hand with the net PnL (already includes both fees)
        cash_on_hand += realized_PnL
        order_type = 'close short'
        units_traded = position_size
        if total_short_position == 0:
            short_cost_basis = 0
            short_pnl = 0  # Reset PnL when all positions are closed

    # Fee is already accounted for in net_trade_value calculation
    # No need to deduct it again here

    # Directly use the provided open_position
    timeframe = open_position.get('Timeframe', '')

    # We're now using the instance data values directly from open_position
    # These fields (extreme_price, maxfib, extreme_price_date, max_position_drawdown) are now
    # set in sim_entries.py when the trade is first opened
    
    # Calculate financial impact based on close price and entry price
    close_trade_impact = 0
    if 'extreme_price' in open_position and open_position['extreme_price'] is not None:
        open_price = float(open_position['Open Price'])
        extreme_price = float(open_position['extreme_price'])
        
        # For a long position, drawdown happens when price falls below entry
        # For a short position, drawdown happens when price rises above entry
        if open_position['Direction'] == 'long':
            # Only count if extreme price is worse than entry price
            if extreme_price < open_price:
                close_trade_impact = (open_price - extreme_price) * position_size
        else:  # short
            # Only count if extreme price is worse than entry price
            if extreme_price > open_price:
                close_trade_impact = (extreme_price - open_price) * position_size

    # Add trade to trade log as a dictionary
    trade_entry_dict = {
        'trade_id': open_position['trade_id'],
        'confirm_date': datetime.strptime(open_position['confirm_date'], '%Y-%m-%d %H:%M:%S') if isinstance(open_position['confirm_date'], str) else open_position['confirm_date'],
        'active_date': datetime.strptime(open_position['active_date'], '%Y-%m-%d %H:%M:%S') if isinstance(open_position['active_date'], str) and open_position['active_date'] else '',
        'entry_date': open_position.get('entry_date', minute_data['timestamp']),  # Use stored entry_date
        'exit_date': minute_data['timestamp'],  # Current timestamp as the exit date
        'completed_date': open_position['Completed Date'],
        'order_type': order_type,
        'trade_fee': round(opening_fee + closing_fee, 4),  # Include both opening and closing fees
        'price': round(float(close_price), 4),
        'units_traded': round(units_traded, 4),
        'cost_basis_change': None,
        'realized_PnL': round(realized_PnL, 4),
        'total_long_position': round(total_long_position, 4),
        'total_short_position': round(total_short_position, 4),
        'balance': round(total_long_position - total_short_position, 4),
        'ind_PnL': round(ind_PnL, 4),
        'timeframe': timeframe,
        'Name': open_position['Name'],
        'winner': 1 if ind_PnL > 0 else 0,
        'loss_reason': loss_reason,  # Update this as needed
        'maxfib': open_position.get('maxfib'),  # MaxFib from instance data
        'extreme_price': open_position.get('extreme_price'),  # Extreme price from instance data
        'extreme_price_date': open_position.get('extreme_price_date'),  # Extreme price date from instance data
        'max_position_drawdown': round(float(open_position.get('max_position_drawdown', 0)), 4) if open_position.get('max_position_drawdown') is not None else None,  # Max position drawdown with rounding
        'instance_id': open_position.get('instance_id'),  # Include instance_id for exit trades
        'ampd_p_value': open_position.get('ampd_p_value'),  # Include ampd_p_value for exit trades
        'ampd_t_value': open_position.get('ampd_t_value'),  # Include ampd_t_value for exit trades
        'tt_instance_id': open_position.get('tt_instance_id'),
        'tt_confirm_date': open_position.get('tt_confirm_date'),
        'tt_active_date': open_position.get('tt_active_date'),
        'tt_completed_date': open_position.get('tt_completed_date'),
        'tt_entry_price': open_position.get('tt_entry_price')
    }

    trade_log.append(trade_entry_dict)

    # Write trade log entry to both trades_all.csv and trades_yyyymm.csv based on flag
    trades_columns = ['trade_id', 'confirm_date', 'active_date', 'entry_date', 'exit_date', 'completed_date', 'order_type', 'trade_fee', 'price', 'units_traded', 'cost_basis_change', 'realized_PnL', 'total_long_position', 'total_short_position', 'balance', 'ind_PnL', 'timeframe', 'Name', 'winner', 'loss_reason', 'maxfib', 'extreme_price', 'extreme_price_date', 'max_position_drawdown',
                     'instance_id', 'ampd_p_value', 'ampd_t_value', 'tt_instance_id', 'tt_confirm_date', 'tt_active_date', 'tt_completed_date', 'tt_entry_price']
    write_log_entry(trade_entry_dict, os.path.join(output_folder, 'trades_all.csv'), trades_columns)
    if CREATE_TRADES_BY_MONTH:
        write_log_entry(trade_entry_dict, os.path.join(output_folder, f'trades_{minute_data["timestamp"].strftime("%Y%m")}.csv'), trades_columns)

    # Remove the position from open_positions.csv
    # Get the open_positions_columns from simulation.py since that's the source of truth
    from simulation import open_positions_columns
    remove_log_entry(open_position['trade_id'], os.path.join(output_folder, 'open_positions.csv'), open_positions_columns)

    # Create closed position entry from open position
    closed_position_entry = {}
    # Add the base fields (those that were in original open_positions)
    base_fields = ['trade_id', 'confirm_date', 'active_date', 'Completed Date', 'Target Price', 
                   'Position Size', 'Direction', 'Open Price', 'Timeframe', 'Name',
                   'extreme_price', 'maxfib', 'extreme_price_date', 'max_position_drawdown', 'ampd_p_value', 'ampd_t_value',
                   'instance_id',
                   'tt_instance_id', 'tt_confirm_date', 'tt_active_date', 'tt_completed_date', 'tt_entry_price'] 
    
    # Copy all relevant fields from open position
    for field in base_fields:
        if field in open_position:
            closed_position_entry[field] = open_position[field]
    
    # Note: close_trade_impact was already calculated earlier in the function
    
    # Add the additional fields for closed positions
    closed_position_entry.update({
        'entry_date': open_position.get('trade_date', minute_data['timestamp']),  # Rename trade_date to entry_date
        'exit_date': minute_data['timestamp'],  # Add exit_date as current timestamp
        'ind_PnL': round(ind_PnL, 4),
        'winner': 1 if ind_PnL > 0 else 0,
        'loss_reason': loss_reason
        # Note: extreme_price, maxfib, extreme_price_date, and max_position_drawdown are already included
        # from the open_position when copying fields above
    })
    
    # Define columns for closed positions (original open position columns + indicators of trade result)
    closed_positions_columns = ['trade_id', 'confirm_date', 'active_date', 'Completed Date', 'entry_date', 'exit_date', 'Target Price', 'Position Size', 'Direction', 'Open Price', 'Timeframe', 'Name', 'ind_PnL', 'winner', 'loss_reason', 'extreme_price', 'maxfib', 'extreme_price_date', 'max_position_drawdown', 'instance_id',
                              'tt_instance_id', 'tt_confirm_date', 'tt_active_date', 'tt_completed_date', 'tt_entry_price',
                              'ampd_p_value', 'ampd_t_value']  # Added AMPD values
    
    write_log_entry(closed_position_entry, os.path.join(output_folder, 'closed_positions.csv'), closed_positions_columns)

    # Remove the closed position from the open_positions list in memory
    open_positions.remove(open_position)

    # Determine if this was a win or loss
    is_win = ind_PnL > 0
    is_loss = ind_PnL < 0

    return total_long_position, total_short_position, long_cost_basis, short_cost_basis, \
           cash_on_hand, long_pnl, short_pnl, is_win, is_loss

def check_advanced_max_position_drawdown(open_position, current_price, current_timestamp=None):
    """
    Check if a position has triggered the advanced max position drawdown based on bankroll percentage.
    Uses a sliding scale for the drawdown percentage based on pending time and/or trigger time.
    Only works with position_size_method = 3.
    
    Args:
        open_position: Dictionary containing position details
        current_price: Dictionary with current price data
        current_timestamp: Current timestamp (datetime object)
        
    Returns:
        tuple: (should_close, exit_price, reason)
    """
    if not use_ampd_percent or position_size_method != 3:
        return False, None, None
        
    # Get pre-calculated values from the open position and ensure they're floats
    ampd_p_value = float(open_position.get('ampd_p_value', 0.0))  # Pending time factor (0-1)
    ampd_t_value = float(open_position.get('ampd_t_value', 0.0))  # Trigger time factor (0-1)
    
    # Calculate the advanced max position drawdown percentage using the pre-calculated values
    scale_factor = 0.0
    
    if ampd_use_pending_time and ampd_use_trigger_time:
        # If both factors are used, use weighted average
        # Convert weights to fractions of 1.0
        pending_weight = float(ampd_pending_weight) / 100.0
        trigger_weight = 1.0 - pending_weight
        
        # Calculate weighted average of the two factors
        scale_factor = (ampd_p_value * pending_weight) + (ampd_t_value * trigger_weight)
    elif ampd_use_pending_time:
        scale_factor = ampd_p_value
    elif ampd_use_trigger_time:
        scale_factor = ampd_t_value
    else:
        # If neither is enabled, just use the base percentage
        scale_factor = 0.0
    
    # Calculate the final percentage using the scale factor
    # Scale the percentage between ampd_percent_base and ampd_percent_max
    # scale_factor should be between 0 and 1
    ampd_range = ampd_percent_max - ampd_percent_base
    ampd_percent = ampd_percent_base + (scale_factor * ampd_range)
    
    # Ensure the value is within bounds
    ampd_percent = max(ampd_percent_base, min(ampd_percent_max, ampd_percent))
    
    # The rest of the function is similar to check_max_position_drawdown but uses ampd_percent
    position_size = float(open_position['Position Size'])
    entry_price = float(open_position['Open Price'])
    direction = open_position['Direction'].lower()
    
    # Get current price based on direction (use low for long, high for short)
    current_price_value = float(current_price['low'] if direction == 'long' else current_price['high'])
    
    # Check if we're in the same minute as the trade activation
    if current_timestamp is not None and 'trade_date' in open_position:
        trade_date = open_position['trade_date']
        if isinstance(trade_date, str):
            trade_date = datetime.strptime(trade_date, '%Y-%m-%d %H:%M:%S')
        
        # Remove seconds and microseconds for minute-level comparison
        current_minute = current_timestamp.replace(second=0, microsecond=0)
        trade_minute = trade_date.replace(second=0, microsecond=0)
        
        # If current minute is the same as trade minute
        if current_minute == trade_minute:
            # Check if completed date exists and is in the same minute
            if 'Completed Date' in open_position and open_position['Completed Date'] is not None:
                completed_date = open_position['Completed Date']
                if isinstance(completed_date, str):
                    completed_date = datetime.strptime(completed_date, '%Y-%m-%d %H:%M:%S')
                completed_minute = completed_date.replace(second=0, microsecond=0)
                
                # If completed in the same minute, check max drawdown using extreme_price
                if completed_minute == trade_minute and 'extreme_price' in open_position and open_position['extreme_price'] is not None:
                    extreme_price = float(open_position['extreme_price'])
                    
                    # Calculate drawdown based on extreme price
                    if direction == 'long':
                        extreme_drawdown_pct = (entry_price - extreme_price) / entry_price * 100
                    else:  # short
                        extreme_drawdown_pct = (extreme_price - entry_price) / entry_price * 100
                    
                    # Calculate entry bankroll and max allowed loss
                    entry_bankroll = (position_size * entry_price) / (position_size_percent / 100.0)
                    max_allowed_loss = entry_bankroll * (ampd_percent / 100.0)
                    
                    # Check if extreme drawdown exceeds our threshold
                    if extreme_drawdown_pct > ampd_percent:
                        # Calculate exit price at max allowed loss
                        if direction == 'long':
                            exit_price = entry_price * (1 - ampd_percent / 100.0)
                        else:  # short
                            exit_price = entry_price * (1 + ampd_percent / 100.0)
                        
                        if debug_show_ampd_output:
                            from tqdm import tqdm
                            
                            confirm_date = open_position.get('confirm_date', 'N/A')
                            active_date = open_position.get('active_date', 'N/A')
                            current_date = current_timestamp.strftime('%Y-%m-%d %H:%M:%S') if current_timestamp else 'N/A'
                            
                            tqdm.write("\n=== AMPD Triggered (Same Minute) ===")
                            tqdm.write(f"Confirm: {confirm_date} | Active: {active_date} | Current: {current_date}")
                            tqdm.write(f"MPD: {ampd_percent:>5.4f}% (Base: {ampd_percent_base:>3.1f}% | Max: {ampd_percent_max:>3.1f}% | P: {ampd_p_value:>4.2f} | T: {ampd_t_value:>4.2f})")
                            tqdm.write(f"Price: Entry={entry_price:>10.8f} | Exit={exit_price:>10.8f} | Extreme={extreme_price:>10.8f}")
                            tqdm.write(f"Drawdown: Current={extreme_drawdown_pct:>6.4f}% | Max Allowed={ampd_percent:>6.4f}%")
                        
                        return True, exit_price, f'same-minute advanced max position drawdown ({ampd_percent:.2f}% of bankroll)'
                    
                    # If we get here, the extreme price didn't trigger max drawdown
                    return False, None, None
            
            # If we're in the same minute but no completion or no extreme price
            return False, None, None
    
    # Calculate current PnL in the position's currency
    if direction == 'long':
        current_pnl = (current_price_value - entry_price) * position_size
        current_drawdown_pct = (entry_price - current_price_value) / entry_price * 100
    else:  # short
        current_pnl = (entry_price - current_price_value) * position_size
        current_drawdown_pct = (current_price_value - entry_price) / entry_price * 100
    
    # Calculate entry bankroll from position size and percentage
    entry_bankroll = (position_size * entry_price) / (position_size_percent / 100.0)
    
    # Calculate max allowed loss in position's currency (based on ampd_percent of entry bankroll)
    max_allowed_loss = entry_bankroll * (ampd_percent / 100.0)
    
    # Get the instance's max drawdown if available
    instance_max_drawdown = open_position.get('max_position_drawdown')
    if instance_max_drawdown is not None:
        try:
            instance_max_drawdown = float(instance_max_drawdown)
            # If instance's max drawdown is not worse than our threshold, don't close
            if current_drawdown_pct <= instance_max_drawdown:
                return False, None, None
        except (ValueError, TypeError):
            pass  # If there's an error parsing, continue with normal logic
    
    # Check if current loss exceeds max allowed loss
    if current_pnl <= -max_allowed_loss:
        # Calculate the price at which we would hit exactly the max allowed loss
        if direction == 'long':
            exit_price = entry_price - (max_allowed_loss / position_size)
        else:  # short
            exit_price = entry_price + (max_allowed_loss / position_size)
        
        if debug_show_ampd_output:
            from tqdm import tqdm
            
            confirm_date = open_position.get('confirm_date', 'N/A')
            active_date = open_position.get('active_date', 'N/A')
            current_date = current_timestamp.strftime('%Y-%m-%d %H:%M:%S') if current_timestamp else 'N/A'
            
            tqdm.write("\n=== AMPD Triggered ===")
            tqdm.write(f"Confirm: {confirm_date} | Active: {active_date} | Current: {current_date}")
            tqdm.write(f"MPD: {ampd_percent:>5.4f}% (Base: {ampd_percent_base:>3.1f}% | Max: {ampd_percent_max:>3.1f}% | P: {ampd_p_value:>4.2f} | T: {ampd_t_value:>4.2f})")
            tqdm.write(f"Price: Entry={entry_price:>10.8f} | Exit={exit_price:>10.8f} | Current={current_price_value:>10.8f}")
            tqdm.write(f"PnL: {current_pnl:>10.8f} | Max Loss: {-max_allowed_loss:>10.8f} | Size: {position_size:>10.8f}")
            tqdm.write(f"Drawdown: Current={current_drawdown_pct:>6.4f}% | Max Allowed={ampd_percent:>6.4f}% | Instance Max={instance_max_drawdown if instance_max_drawdown is not None else 'N/A':>6.4f}%")
        
        return True, exit_price, f'advanced max position drawdown ({ampd_percent:.2f}% of bankroll)'
    
    return False, None, None


def check_max_position_drawdown(open_position, current_price, current_timestamp=None):
    """
    Check if a position has triggered the max position drawdown based on bankroll percentage.
    Only works with position_size_method = 3.
    
    Args:
        open_position: Dictionary containing position details
        current_price: Dictionary with current price data
        current_timestamp: Current timestamp (datetime object)
        
    Returns:
        tuple: (should_close, exit_price, reason)
    """
    if not use_mpd_percent or position_size_method != 3:
        return False, None, None
        
    position_size = float(open_position['Position Size'])
    entry_price = float(open_position['Open Price'])
    direction = open_position['Direction'].lower()
    
    # Get current price based on direction (use low for long, high for short)
    current_price_value = float(current_price['low'] if direction == 'long' else current_price['high'])
    
    # Check if we're in the same minute as the trade activation
    if current_timestamp is not None and 'trade_date' in open_position:
        trade_date = open_position['trade_date']
        if isinstance(trade_date, str):
            from datetime import datetime
            trade_date = datetime.strptime(trade_date, '%Y-%m-%d %H:%M:%S')
        
        # Remove seconds and microseconds for minute-level comparison
        current_minute = current_timestamp.replace(second=0, microsecond=0)
        trade_minute = trade_date.replace(second=0, microsecond=0)
        
        # If current minute is the same as trade minute
        if current_minute == trade_minute:
            # Check if completed date exists and is in the same minute
            if 'Completed Date' in open_position and open_position['Completed Date'] is not None:
                completed_date = open_position['Completed Date']
                if isinstance(completed_date, str):
                    completed_date = datetime.strptime(completed_date, '%Y-%m-%d %H:%M:%S')
                completed_minute = completed_date.replace(second=0, microsecond=0)
                
                # If completed in the same minute, check max drawdown using extreme_price
                if completed_minute == trade_minute and 'extreme_price' in open_position and open_position['extreme_price'] is not None:
                    extreme_price = float(open_position['extreme_price'])
                    
                    # Calculate drawdown based on extreme price
                    if direction == 'long':
                        extreme_drawdown_pct = (entry_price - extreme_price) / entry_price * 100
                    else:  # short
                        extreme_drawdown_pct = (extreme_price - entry_price) / entry_price * 100
                    
                    # Calculate entry bankroll and max allowed loss
                    entry_bankroll = (position_size * entry_price) / (position_size_percent / 100.0)
                    max_allowed_loss = entry_bankroll * (mpd_percent / 100.0)
                    
                    # Check if extreme drawdown exceeds our threshold
                    if extreme_drawdown_pct > mpd_percent:
                        # Calculate exit price at max allowed loss
                        if direction == 'long':
                            exit_price = entry_price * (1 - mpd_percent / 100.0)
                        else:  # short
                            exit_price = entry_price * (1 + mpd_percent / 100.0)
                        
                        if debug_show_mpd_output:
                            from tqdm import tqdm
                            tqdm.write("\n=== DEBUG: Same-Minute Max Drawdown Triggered ===")
                            tqdm.write(f"Position ID: {open_position.get('trade_id', 'N/A')}")
                            tqdm.write(f"Extreme Drawdown: {extreme_drawdown_pct:.4f}%")
                            tqdm.write(f"Max Allowed: {mpd_percent:.4f}%")
                            tqdm.write(f"Exit Price: {exit_price:.8f}")
                            tqdm.write(f"Extreme Price: {extreme_price:.8f}")
                            tqdm.write(f"Entry Price: {entry_price:.8f}")
                            tqdm.write("")
                        
                        return True, exit_price, f'same-minute max position drawdown ({mpd_percent}% of bankroll)'
                    
                    # If we get here, the extreme price didn't trigger max drawdown
                    return False, None, None
            
            # If we're in the same minute but no completion or no extreme price
            return False, None, None
    
    # Calculate current PnL in the position's currency
    if direction == 'long':
        current_pnl = (current_price_value - entry_price) * position_size
        current_drawdown_pct = (entry_price - current_price_value) / entry_price * 100
    else:  # short
        current_pnl = (entry_price - current_price_value) * position_size
        current_drawdown_pct = (current_price_value - entry_price) / entry_price * 100
    
    # Calculate entry bankroll from position size and percentage
    entry_bankroll = (position_size * entry_price) / (position_size_percent / 100.0)
    
    # Calculate max allowed loss in position's currency (5% of entry bankroll)
    max_allowed_loss = entry_bankroll * (mpd_percent / 100.0)
    
    # Get the instance's max drawdown if available
    instance_max_drawdown = open_position.get('max_position_drawdown')
    if instance_max_drawdown is not None:
        try:
            instance_max_drawdown = float(instance_max_drawdown)
            # If instance's max drawdown is not worse than our threshold, don't close
            if current_drawdown_pct <= instance_max_drawdown:
                return False, None, None
        except (ValueError, TypeError):
            pass  # If there's an error parsing, continue with normal logic
    
    # Check if current loss exceeds max allowed loss
    if current_pnl <= -max_allowed_loss:
        # Calculate the price at which we would hit exactly the max allowed loss
        if direction == 'long':
            exit_price = entry_price - (max_allowed_loss / position_size)
        else:  # short
            exit_price = entry_price + (max_allowed_loss / position_size)
        
        if debug_show_mpd_output:
            from tqdm import tqdm
            tqdm.write("\n=== DEBUG: Max Drawdown Triggered ===")
            tqdm.write(f"Position ID: {open_position.get('trade_id', 'N/A')}")
            tqdm.write(f"Current PnL: {current_pnl:.8f}")
            tqdm.write(f"Current Drawdown: {current_drawdown_pct:.4f}%")
            if instance_max_drawdown is not None:
                tqdm.write(f"Instance Max Drawdown: {instance_max_drawdown:.4f}%")
            tqdm.write(f"Max Allowed Loss: {-max_allowed_loss:.8f}")
            tqdm.write(f"Exit Price: {exit_price:.8f}")
            tqdm.write(f"Current Price: {current_price_value:.8f}")
            tqdm.write(f"Entry Price: {entry_price:.8f}")
            tqdm.write(f"Position Size: {position_size:.8f}")
            tqdm.write("")
        
        return True, exit_price, f'max position drawdown ({mpd_percent}% of bankroll)'
    
    return False, None, None

def check_fib_levels(minute_data, open_position):
    """Check if a trade should exit at a fibonacci level based on the DateReached timestamps"""
    # Check if this is the time to exit based on the DateReached timestamps
    if (SL_on_fib0_5 and 
        'DateReached0.5' in open_position and 
        open_position['DateReached0.5'] is not None and 
        open_position['DateReached0.5'] != "" and
        compare_timestamps_ignore_seconds(open_position['DateReached0.5'], minute_data['timestamp']) and 
        open_position.get('fib0.5') is not None):
        return True, float(open_position['fib0.5']), 'fib0.5_exit'
        
    if (SL_on_fib0_0 and 
        'DateReached0.0' in open_position and 
        open_position['DateReached0.0'] is not None and 
        open_position['DateReached0.0'] != "" and
        compare_timestamps_ignore_seconds(open_position['DateReached0.0'], minute_data['timestamp']) and 
        open_position.get('fib0.0') is not None):
        return True, float(open_position['fib0.0']), 'fib0.0_exit'
        
    if (SL_on_fib_0_5 and 
        'DateReached-0.5' in open_position and 
        open_position['DateReached-0.5'] is not None and 
        open_position['DateReached-0.5'] != "" and
        compare_timestamps_ignore_seconds(open_position['DateReached-0.5'], minute_data['timestamp']) and 
        open_position.get('fib-0.5') is not None):
        return True, float(open_position['fib-0.5']), 'fib-0.5_exit'
        
    if (SL_on_fib_1_0 and 
        'DateReached-1.0' in open_position and 
        open_position['DateReached-1.0'] is not None and 
        open_position['DateReached-1.0'] != "" and
        compare_timestamps_ignore_seconds(open_position['DateReached-1.0'], minute_data['timestamp']) and 
        open_position.get('fib-1.0') is not None):
        return True, float(open_position['fib-1.0']), 'fib-1.0_exit'
    
    return False, None, None

def sim_exits(minute_data, trade_log, open_positions, fee_rate, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, output_folder, all_instances=None):
    # Initialize win/loss counters
    wins = 0
    losses = 0
    
    close_price = float(minute_data['close'])
    
    # Check for completed trades
    for open_position in open_positions[:]:  # Create a copy of the list to iterate over
        # Ensure 'trade_date' is a datetime object
        if isinstance(open_position['trade_date'], str):
            open_position['trade_date'] = datetime.strptime(open_position['trade_date'], '%Y-%m-%d %H:%M:%S')
        
        # We no longer need to convert 'Completed Date' here as the compare_timestamps_ignore_seconds function handles all conversions
        
        should_close = False
        exit_price = None
        loss_reason = None
        
        # Check advanced max position drawdown (bankroll percentage with sliding scale)
        if use_ampd_percent and not should_close:
            should_close, exit_price, loss_reason = check_advanced_max_position_drawdown(
                open_position, minute_data, minute_data['timestamp']
            )
        
        # Check regular max position drawdown (bankroll percentage)
        if use_mpd_percent and not should_close:
            should_close, exit_price, loss_reason = check_max_position_drawdown(
                open_position, minute_data, minute_data['timestamp']
            )
        
        # Check if the trade should be closed due to reaching a Fibonacci level
        if not should_close:
            fib_exit, fib_price, fib_reason = check_fib_levels(minute_data, open_position)
            if fib_exit:
                should_close = True
                close_price = fib_price
                loss_reason = fib_reason
        
        # Check for static time capitulation if enabled
        if not should_close and USE_STATIC_TIME_CAPIT:
            active_duration = minute_data['timestamp'] - open_position['trade_date']
            if active_duration >= timedelta(hours=STATIC_TIME_CAPIT_DURATION):
                should_close = True
                close_price = float(minute_data['close'])
                loss_reason = 'static time capit'
                
        # Check for successful completion (independent of other conditions)
        if not should_close and compare_timestamps_ignore_seconds(open_position['Completed Date'], minute_data['timestamp']):
            should_close = True
            close_price = float(open_position['Target Price'])
            loss_reason = None  # Clear any previous loss reason

        if should_close:
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, is_win, is_loss = close_trade(
                open_position, close_price, trade_log, open_positions, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, output_folder, minute_data, loss_reason=loss_reason
            )
            if is_win:
                wins += 1
            elif is_loss:
                losses += 1

    return total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, wins, losses
