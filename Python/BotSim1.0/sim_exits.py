import os
from datetime import datetime, timedelta
from config import USE_STATIC_TIME_CAPIT, STATIC_TIME_CAPIT_DURATION, fee_rate, CREATE_TRADES_BY_MONTH
from log_utils import write_log_entry, remove_log_entry

def close_trade(open_position, close_price, trade_log, open_positions, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, output_folder, minute_data, loss_reason=None):
    position_size = float(open_position['Position Size'])
    trade_fee = float(close_price) * position_size * float(fee_rate)
    net_trade_value = float(close_price) * position_size - trade_fee
    open_price = float(open_position['Open Price'])
    ind_PnL = (float(close_price) - open_price) * position_size if open_position['Direction'] == 'long' else (open_price - float(close_price)) * position_size

    if open_position['Direction'] == 'long':
        realized_PnL = (float(close_price) - float(long_cost_basis)) * position_size
        total_long_position -= position_size
        PnL_change = net_trade_value - float(long_cost_basis) * position_size
        long_pnl -= PnL_change
        cash_on_hand += PnL_change  # Move the PnL to cash on hand
        order_type = 'close long'
        units_traded = -position_size
        if total_long_position == 0:
            long_cost_basis = 0
    elif open_position['Direction'] == 'short':
        realized_PnL = (float(short_cost_basis) - float(close_price)) * position_size
        total_short_position -= position_size
        PnL_change = float(short_cost_basis) * position_size - net_trade_value
        short_pnl -= PnL_change
        cash_on_hand += PnL_change  # Move the PnL to cash on hand
        order_type = 'close short'
        units_traded = position_size
        if total_short_position == 0:
            short_cost_basis = 0

    # Deduct the trading fee from cash on hand
    cash_on_hand -= trade_fee

    # Directly use the provided open_position
    timeframe = open_position.get('Timeframe', '')

    # Add trade to trade log as a dictionary
    trade_entry_dict = {
        'trade_id': open_position['trade_id'],
        'confirm_date': datetime.strptime(open_position['confirm_date'], '%Y-%m-%d %H:%M:%S') if isinstance(open_position['confirm_date'], str) else open_position['confirm_date'],
        'active_date': datetime.strptime(open_position['active_date'], '%Y-%m-%d %H:%M:%S') if isinstance(open_position['active_date'], str) and open_position['active_date'] else '',
        'trade_date': minute_data['timestamp'],  # Current minute being processed
        'completed_date': open_position['Completed Date'],
        'order_type': order_type,
        'trade_fee': round(trade_fee, 4),
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
        'loss_reason': loss_reason  # Update this as needed
    }

    trade_log.append(trade_entry_dict)

    # Write trade log entry to both trades_all.csv and trades_yyyymm.csv based on flag
    trades_columns = ['trade_id', 'confirm_date', 'active_date', 'trade_date', 'completed_date', 'order_type', 'trade_fee', 'price', 'units_traded', 'cost_basis_change', 'realized_PnL', 'total_long_position', 'total_short_position', 'balance', 'ind_PnL', 'timeframe', 'Name', 'winner', 'loss_reason']
    write_log_entry(trade_entry_dict, os.path.join(output_folder, 'trades_all.csv'), trades_columns)
    if CREATE_TRADES_BY_MONTH:
        write_log_entry(trade_entry_dict, os.path.join(output_folder, f'trades_{minute_data["timestamp"].strftime("%Y%m")}.csv'), trades_columns)

    # Remove the position from open_positions.csv and add to closed_positions.csv
    open_positions_columns = ['trade_id', 'confirm_date', 'active_date', 'trade_date', 'Completed Date', 'Target Price', 'Position Size', 'Direction', 'Open Price', 'Timeframe', 'Name']
    remove_log_entry(open_position['trade_id'], os.path.join(output_folder, 'open_positions.csv'), open_positions_columns)

    # Add additional columns to closed_positions.csv
    closed_positions_columns = open_positions_columns + ['ind_PnL', 'winner', 'loss_reason']
    closed_position_entry = open_position.copy()
    closed_position_entry.update({
        'ind_PnL': round(ind_PnL, 4),
        'winner': 1 if ind_PnL > 0 else 0,
        'loss_reason': loss_reason
    })
    write_log_entry(closed_position_entry, os.path.join(output_folder, 'closed_positions.csv'), closed_positions_columns)

    # Remove the closed position from the open_positions list in memory
    open_positions.remove(open_position)

    return total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl

def sim_exits(minute_data, trade_log, open_positions, fee_rate, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, output_folder):
    for open_position in open_positions[:]:  # Create a copy of the list to iterate over
        # Ensure 'trade_date' is a datetime object
        if isinstance(open_position['trade_date'], str):
            open_position['trade_date'] = datetime.strptime(open_position['trade_date'], '%Y-%m-%d %H:%M:%S')

        should_close = False
        close_price = None
        loss_reason = None

        # Check if the trade should be closed due to completed date
        if open_position['Completed Date'] == minute_data['timestamp']:
            should_close = True
            close_price = float(open_position['Target Price'])

        # Check if the trade should be closed due to static time capitulation
        elif USE_STATIC_TIME_CAPIT:
            active_duration = minute_data['timestamp'] - open_position['trade_date']
            if active_duration >= timedelta(hours=STATIC_TIME_CAPIT_DURATION):
                should_close = True
                close_price = float(minute_data['close'])
                loss_reason = 'static time capit'

        if should_close:
            total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl = close_trade(
                open_position, close_price, trade_log, open_positions, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl, output_folder, minute_data, loss_reason=loss_reason
            )

    return total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, long_pnl, short_pnl
