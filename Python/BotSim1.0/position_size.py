# position_size.py

from config import position_size_method, position_size_qty, position_size_amount, position_size_percent

def calculate_position_size(entry_price, bankroll, ampd_p_value=None, ampd_t_value=None):
    """
    Calculates the position size based on entry price and bankroll.
    
    Args:
        entry_price: The entry price for the position
        bankroll: The current available bankroll
        ampd_p_value: The pending time factor (0-1) for advanced max position drawdown
        ampd_t_value: The trigger time factor (0-1) for advanced max position drawdown
        
    Returns:
        float: The calculated position size
    """
    if position_size_method == 1:
        return position_size_qty
    elif position_size_method == 2:
        return position_size_amount / entry_price
    elif position_size_method == 3:
        # For now, we're just passing through the values without using them
        # They can be used in the future for dynamic position sizing
        return (float(position_size_percent) / 100) * bankroll / entry_price
    else:
        raise ValueError("Invalid position_size_method")
