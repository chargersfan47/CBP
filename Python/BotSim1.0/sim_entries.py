import os
import uuid
from datetime import datetime
from log_utils import write_log_entry
from config import CREATE_TRADES_BY_MONTH, USE_MIN_PENDING_AGE, MIN_PENDING_AGE, USE_MAX_PENDING_AGE, MAX_PENDING_AGE
from position_size import calculate_position_size

def sim_entries(minute_data, instances, fee_rate, trade_log, open_positions, total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand, output_folder):
    active_trades = [trade for trade in instances if trade['Active Date'] == minute_data['timestamp']]
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

        trade_name = f"{trade['Timeframe']} {trade['direction']}({str(uuid.uuid4())[:4]}...{str(uuid.uuid4())[-4:]})"
        entry_price = float(trade['entry'])
        
        # Calculate position size based on selected method
        position_size = calculate_position_size(entry_price, cash_on_hand)
        
        trade_cost = float(position_size) * entry_price
        trade_fee = trade_cost * float(fee_rate)
        trade_id = str(uuid.uuid4())  # Generate unique ID for each trade

        if trade['direction'] == 'long':
            old_cost_basis = float(long_cost_basis)
            total_long_position += float(position_size)
            long_cost_basis = (old_cost_basis * (total_long_position - float(position_size)) + trade_cost) / total_long_position
            cost_basis_change = f"{round(old_cost_basis, 4)} -> {round(long_cost_basis, 4)}"
            order_type = 'open long'
            units_traded = float(position_size)
        elif trade['direction'] == 'short':
            old_cost_basis = float(short_cost_basis)
            total_short_position += float(position_size)
            short_cost_basis = (old_cost_basis * (total_short_position - float(position_size)) + trade_cost) / total_short_position
            cost_basis_change = f"{round(old_cost_basis, 4)} -> {round(short_cost_basis, 4)}"
            order_type = 'open short'
            units_traded = -float(position_size)

        cash_on_hand -= trade_fee

        trade_entry_dict = {
            'trade_id': trade_id,
            'confirm_date': datetime.strptime(trade['confirm_date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['confirm_date'], str) else trade['confirm_date'],
            'active_date': datetime.strptime(trade['Active Date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['Active Date'], str) else trade['Active Date'],
            'trade_date': minute_data['timestamp'],  # Current minute being processed
            'completed_date': datetime.strptime(trade['Completed Date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['Completed Date'], str) else trade['Completed Date'],
            'order_type': order_type,
            'trade_fee': round(trade_fee, 4),
            'price': round(entry_price, 4),
            'units_traded': round(units_traded, 4),
            'cost_basis_change': cost_basis_change,
            'realized_PnL': None,
            'total_long_position': round(total_long_position, 4),
            'total_short_position': round(total_short_position, 4),
            'balance': round(total_long_position - total_short_position, 4),
            'ind_PnL': 0,  # Set to 0 when entering trades
            'timeframe': trade['Timeframe'],
            'Name': trade_name,
            'winner': None,
            'loss_reason': None
        }
        trade_log.append(trade_entry_dict)

        open_position_dict = {
            'trade_id': trade_id,
            'confirm_date': datetime.strptime(trade['confirm_date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['confirm_date'], str) else trade['confirm_date'],
            'active_date': datetime.strptime(trade['Active Date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['Active Date'], str) else trade['Active Date'],  # Add active_date
            'trade_date': minute_data['timestamp'],
            'Completed Date': datetime.strptime(trade['Completed Date'], '%Y-%m-%d %H:%M:%S') if isinstance(trade['Completed Date'], str) else trade['Completed Date'],
            'Target Price': float(trade['target']),
            'Position Size': float(position_size),
            'Direction': trade['direction'],
            'Open Price': entry_price,
            'Timeframe': trade['Timeframe'],
            'Name': trade_name
        }
        open_positions.append(open_position_dict)

        # Write trade log entry to both trades_all.csv and trades_yyyymm.csv based on flag
        trades_columns = ['trade_id', 'confirm_date', 'active_date', 'trade_date', 'completed_date', 'order_type', 'trade_fee', 'price', 'units_traded', 'cost_basis_change', 'realized_PnL', 'total_long_position', 'total_short_position', 'balance', 'ind_PnL', 'timeframe', 'Name', 'winner', 'loss_reason']
        write_log_entry(trade_entry_dict, os.path.join(output_folder, 'trades_all.csv'), trades_columns)
        if CREATE_TRADES_BY_MONTH:
            write_log_entry(trade_entry_dict, os.path.join(output_folder, f'trades_{minute_data["timestamp"].strftime("%Y%m")}.csv'), trades_columns)

        # Write open positions log entry to open_positions.csv
        open_positions_columns = ['trade_id', 'confirm_date', 'active_date', 'trade_date', 'Completed Date', 'Target Price', 'Position Size', 'Direction', 'Open Price', 'Timeframe', 'Name']
        write_log_entry(open_position_dict, os.path.join(output_folder, 'open_positions.csv'), open_positions_columns)

    return total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand
