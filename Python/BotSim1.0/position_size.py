# position_size.py

from config import (position_size_method, position_size_qty, position_size_amount, 
                  position_size_percent, starting_bankroll, 
                  USE_POSITION_DESCALING, POSITION_DESCALING_FACTOR)

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
        if USE_POSITION_DESCALING:
            # Calculate position size based on current bankroll
            current_position_size = (float(position_size_percent) / 100) * bankroll / entry_price
            
            # Calculate position size based on starting bankroll
            starting_position_size = (float(position_size_percent) / 100) * starting_bankroll / entry_price
            
            # Calculate weighted average based on descalation factor
            # Higher POSITION_DESCALING_FACTOR gives more weight to starting bankroll
            weighted_size = (starting_position_size * POSITION_DESCALING_FACTOR + 
                           current_position_size * (1 - POSITION_DESCALING_FACTOR))
            
            return weighted_size
        else:
            return (float(position_size_percent) / 100) * bankroll / entry_price
    else:
        raise ValueError("Invalid position_size_method")
