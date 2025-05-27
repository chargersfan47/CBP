from datetime import datetime
from operator import truediv
import os

# Variables
starting_bankroll = 10000
position_size_method = 3   # 1 = fixed quantity, 2 = fixed dollar amount, 3 = percentage of bankroll
position_size_qty = 0.2    # Used if position_size_method = 1
position_size_amount = 50  # Used if position_size_method = 2
position_size_percent = 70  # Used if position_size_method = 3 (1 = 1%; no need to divide by 100)

starting_date = datetime(2022, 1, 1)
ending_date = datetime(2024, 6, 30)
fee_rate = 0.0003   # 0.03% fee rate (this one you do need to divide by 100)

#Entries:
# List of allowed trade situations (e.g., ['1v1', '2v1', '1v1+1'])
# Only trades with a situation in this list will be considered for entry
ALLOWED_SITUATIONS = ['1v1']

# Trigger Trade Logic - All options require same timeframe (stf) and direction
    # If True, only take trades if another instance with same timeframe/direction confirmed and activated between this trade's confirm and active dates
    # Note: When enabled, all instances will be loaded regardless of date range to ensure proper trigger checking
tt_stf_any_inside_activation = False
    # If True, only take trades if another instance with same timeframe/direction activated in the same minute
tt_stf_same_minute = False
    # If True, only take trades if another instance with same timeframe/direction activated within X multiples of the timeframe
    # For example, within 3 hours for a 1h timeframe trade
tt_stf_within_x_candles = False
tt_stf_within_x = 1  # Number of timeframe multiples to look back
    # If True, only take trades if another instance with same timeframe/direction activated within X minutes
    # For example, within 60 minutes regardless of timeframe
tt_stf_within_x_minutes = False
tt_stf_within_minutes = 60  # Number of minutes to look back

# List of flags that require loading the full set of instances (not filtered by date)
FULL_INSTANCE_SET_FLAGS = [
    'tt_stf_any_inside_activation',
    'tt_stf_within_x_candles',
    'tt_stf_within_x_minutes'
    # Add any future flags that require full instance set here
]

# Enter trades that are pending for a certain number of hours by turning on the following four variables:
USE_MIN_PENDING_AGE = False
MIN_PENDING_AGE = 72
USE_MAX_PENDING_AGE = False
MAX_PENDING_AGE = 2500

# Enter trades at Fibonacci levels
DD_on_fib0_5 = False  # Enter at fib 0.5 level
DD_on_fib0_0 = False  # Enter at fib 0.0 level
DD_on_fib_0_5 = False  # Enter at fib -0.5 level
DD_on_fib_1_0 = False  # Enter at fib -1.0 level

# Groups filtering
AVOID_GROUPS = False  # If True, only take trades where group_id is 'NA' (if group_id exists in data)

#Exits:
# Exit trades after a certain number of hours by turning on the following two variables:
USE_STATIC_TIME_CAPIT = False
STATIC_TIME_CAPIT_DURATION = 1.5  # Or any other suitable number of hours

# Exit trades at Fibonacci levels
SL_on_fib0_5 = False  # Exit at fib 0.5 level
SL_on_fib0_0 = False  # Exit at fib 0.0 level
SL_on_fib_0_5 = False  # Exit at fib -0.5 level
SL_on_fib_1_0 = False  # Exit at fib -1.0 level

# Position drawdown settings (only works with position_size_method = 3)
# Standard max position drawdown percent (mpd)
# If True, will exit trades when the total position drawdown exceeds the specified percentage
# of the total bankroll at the time the trade was opened.
use_mpd_percent = False  # Enable/disable max position drawdown check
mpd_percent = 3.6  # Maximum allowed drawdown as percentage of original bankroll
# Advanced max position drawdown percent (ampd)
use_ampd_percent = False
ampd_percent_base = 3
ampd_percent_max = 8
ampd_use_pending_time = True
ampd_use_trigger_time = True
# if both are true, specify a weighting (1 to 100) for pending time; trigger distance will use the remaining amount.
ampd_pending_weight = 50
ampd_pending_time_high = 100 # days, default 100.  This means a pending instance 67/100 days old will get a 67% allowance towards max drawdown.  
ampd_trigger_time_high = 60 # minutes, default 60.  This means a trigger instance 57 minutes before activation time will get a (60-57)/60 allowance towards max drawdown.

# Debug settings
debug_show_ampd_output = False  # Set to True to show detailed AMPD debug output

# Update these paths to match your environment.  The defaults are just placeholders.
prompt_for_paths = False # basically deprecated; there's an option in the main menu for this anyway.

# candles_file_default will need the path to your 1m.csv file (not just the folder!)
candles_file_default = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Candles', 'SOLUSDT_binance_1m.csv') 

# I recommend using the "Subset" folder for instances; you can have the bot simulate trades across
# all instances by pointing this at "CompleteSet", or just a subset by placing copies of your instance
# files in the subset folder.
instances_folder_default = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Instances', '1v1', 'Processed', 'SubSet')

# For output_folder_default, you should create a new folder for each run, named something meaningful (see the README file for suggestions).
# The name of the output folder will be used in the name of the summary file for reference.
output_folder_default = os.path.join('..', '..', 'Data', 'SOLUSDT-BINANCE', 'Simulations', 'NewFolderHere')

# logging settings.  Turning these on produce a lot of extra files in the output folder that aren't really required.
# I was just using them to debug some issues with monthly rollovers.  Probably best to leave them off.
CREATE_TRADES_BY_MONTH = False
CREATE_ANALYSIS_ALL = False
