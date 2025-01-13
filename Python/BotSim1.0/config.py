from datetime import datetime
import os

# Variables
starting_bankroll = 10000
position_size_method = 3   # 1 = fixed quantity, 2 = fixed dollar amount, 3 = percentage of bankroll
position_size_qty = 0.2    # Used if position_size_method = 1
position_size_amount = 50  # Used if position_size_method = 2
position_size_percent = 1  # Used if position_size_method = 3 (1 = 1%; no need to divide by 100)
starting_date = datetime(2022, 1, 1)
ending_date = datetime(2022, 12, 31)
fee_rate = 0.0004   # 0.04% fee rate (this one you do need to divide by 100)

#Entries:
# Enter trades that are pending for a certain number of hours by turning on the following four variables:
USE_MIN_PENDING_AGE = False
MIN_PENDING_AGE = 720
USE_MAX_PENDING_AGE = False
MAX_PENDING_AGE = 2500

#Exits:
# Exit trades after a certain number of hours by turning on the following two variables:
USE_STATIC_TIME_CAPIT = False
STATIC_TIME_CAPIT_DURATION = 60  # Or any other suitable number of hours

# Update these paths to match your environment.  The defaults are just placeholders.
prompt_for_paths = False # basically deprecated; there's an option in the main menu for this now anyway.

# candles_file_default will need the path to your 1m.csv file (not just the folder!)
candles_file_default = os.path.join('..', '..', 'Data', 'SOLUSDT', 'Candles', 'fill in the rest of your path to your candle data 1m.csv') 

# I recommend using the "Subset" folder for instances; you can have the bot simulate trades across
# all instances by pointing this at "CompleteSet", or just a subset by placing copies of your instance
# files in the subset folder.
instances_folder_default = os.path.join('..', '..', 'Data', 'SOLUSDT', 'Instances', '1v1', 'Processed', 'SubSet')

# For output_folder_default, you should create a new folder for each run, named something meaningful (see the README file for suggestions).
# The name of the output folder will be used in the name of the summary file for reference.
output_folder_default = os.path.join('..', '..', 'Data', 'SOLUSDT', 'Simulations', 'NewFolderHere')

# logging settings.  Turning these on produce a lot of extra files in the output folder that aren't really required.
# I was just using them to debug some issues with monthly rollovers.  Probably best to leave them off.
CREATE_TRADES_BY_MONTH = False
CREATE_ANALYSIS_ALL = False
