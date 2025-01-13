# position_size.py

from config import position_size_method, position_size_qty, position_size_amount, position_size_percent

def calculate_position_size(entry_price, bankroll):
    if position_size_method == 1:
        return position_size_qty
    elif position_size_method == 2:
        return position_size_amount / entry_price
    elif position_size_method == 3:
        return (position_size_percent / 100) * bankroll / entry_price
    else:
        raise ValueError("Invalid position_size_method")
