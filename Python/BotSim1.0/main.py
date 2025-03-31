import os
from datetime import datetime, timedelta
from tqdm import tqdm
from initialization import load_instances, load_candles, load_state, initialize_trades_all
from simulation import run_simulation
from reporting import generate_summary_report
from config import *

def prompt_paths():
    global instances_folder, candles_file, output_folder
    
    # Show current path and get new input, keeping existing if empty
    new_instances_folder = input(f"Enter the folder path containing the instance CSV files [{instances_folder}]: ")
    instances_folder = new_instances_folder if new_instances_folder else instances_folder
    
    new_candles_file = input(f"Enter the path to the 1m candles data file [{candles_file}]: ")
    candles_file = new_candles_file if new_candles_file else candles_file
    
    new_output_folder = input(f"Enter the folder path for output files [{output_folder}]: ")
    output_folder = new_output_folder if new_output_folder else output_folder

def prompt_dates():
    global starting_date, ending_date
    while True:
        try:
            start_date_input = input("Enter the start date (yyyy-mm-dd or yyyymmdd): ")
            end_date_input = input("Enter the end date (yyyy-mm-dd or yyyymmdd): ")
            if '-' in start_date_input:
                starting_date = datetime.strptime(start_date_input, "%Y-%m-%d")
            else:
                starting_date = datetime.strptime(start_date_input, "%Y%m%d")
            if '-' in end_date_input:
                ending_date = datetime.strptime(end_date_input, "%Y-%m-%d")
            else:
                ending_date = datetime.strptime(end_date_input, "%Y%m%d")
            break
        except ValueError:
            print("Invalid date format. Please try again.")

# Prompt for input paths if flag is set
if prompt_for_paths:
    prompt_paths()
else:
    instances_folder = instances_folder_default
    candles_file = candles_file_default
    output_folder = output_folder_default

print("\n")  # Add CR/LF before prompting for user choice

if __name__ == "__main__":
    while True:
        # Print current paths and dates
        print(f"Current paths selected:\nInstances folder: {instances_folder}\nCandles file: {candles_file}\nOutput folder: {output_folder}")
        if starting_date and ending_date:
            print(f"Current dates selected:\nStart date: {starting_date.strftime('%Y-%m-%d')}\nEnd date: {ending_date.strftime('%Y-%m-%d')}\n")
        
        # Display a menu for the user to choose an option
        print("Please select an option:")
        print("N - Start a new simulation")
        print("C - Continue from the last saved state")
        print("S - Generate a summary report from CSV files")
        print("P - Prompt for paths")
        print("D - Set start and end dates")
        user_choice = input("Enter your choice (N/C/S/P/D): ").strip().upper()

        if user_choice == 'N':
            # Initialize for a new run
            minute_log = []
            trade_log = []
            open_positions = []
            trades_all = []
            cash_on_hand = starting_bankroll
            total_long_position = 0
            total_short_position = 0
            long_cost_basis = 0
            short_cost_basis = 0
            current_date = starting_date.replace(hour=0, minute=0, second=0)  # Set start time to 00:00
            current_month = starting_date.month

            # Set the end date to 23:59:59
            ending_date = ending_date.replace(hour=23, minute=59, second=59)

            # Load data with progress bar
            # print("Loading instances data...")
            instances_by_minute = load_instances(instances_folder, current_date, ending_date)
            # print("\n")  # Add CR/LF after progress bar completes

            print("Loading candles data...")
            candles = load_candles(candles_file, current_date, ending_date)

            # Run the simulation for a new run
            run_simulation(instances_by_minute, candles, current_date, ending_date, 
                           output_folder, fee_rate, trades_all, trade_log, open_positions,
                           initial_cash_on_hand=starting_bankroll, # Use starting bankroll from config
                           initial_total_long=0.0,
                           initial_long_basis=0.0,
                           initial_total_short=0.0,
                           initial_short_basis=0.0)

            break

        if user_choice == 'C':
            trades_all = initialize_trades_all(output_folder)
            state = load_state(output_folder)
            if state:
                # Correctly unpack the state dictionary using keys
                # minute_log is intentionally not unpacked here as run_simulation no longer uses it
                trade_log = state['trade_log'] 
                open_positions = state['open_positions']
                current_month = state['current_month']
                current_date = state['current_date']
                total_long_position = state['total_long_position']
                total_short_position = state['total_short_position']
                long_cost_basis = state['long_cost_basis']
                short_cost_basis = state['short_cost_basis']
                cash_on_hand = state['cash_on_hand']

                # Prompt for new end date FIRST
                new_end_date_str = input(f"Enter new end date (YYYY-MM-DD or YYYYMMDD) [current: {ending_date.strftime('%Y-%m-%d')}]: ")
                if new_end_date_str:
                    # Handle different date formats
                    try:
                        if '-' in new_end_date_str:
                            ending_date = datetime.strptime(new_end_date_str, '%Y-%m-%d')
                        else:
                            ending_date = datetime.strptime(new_end_date_str, '%Y%m%d')
                        ending_date = ending_date.replace(hour=23, minute=59, second=59)
                    except ValueError:
                        print("Invalid date format. Please try again.")
                        continue
                # If no new date entered, ending_date remains as loaded from config or previous state
                
                # Now process trade history AFTER getting the new end date
                print("Processing trade history for PnL...")
                trade_log_set = set(trade['trade_id'] for trade in trade_log) # Use set for O(1) lookup
                for trade in tqdm(trades_all, desc='Processing historical trades'):
                    if trade['trade_id'] not in trade_log_set:
                        trade_log.append(trade)
                        trade_log_set.add(trade['trade_id']) # Keep the set updated

                # Reload instances and candles for the new date range
                print("Reloading instances data for the specified range...")
                instances_by_minute = load_instances(instances_folder, current_date, ending_date)
                print("Reloading candles data for the specified range...")
                candles = load_candles(candles_file, current_date, ending_date)

                # Run the simulation with the loaded state
                run_simulation(instances_by_minute, candles, current_date, ending_date, 
                               output_folder, fee_rate, trades_all, trade_log, open_positions, 
                               initial_cash_on_hand=cash_on_hand, 
                               initial_total_long=total_long_position, 
                               initial_long_basis=long_cost_basis, 
                               initial_total_short=total_short_position, 
                               initial_short_basis=short_cost_basis)
            else:
                print("Could not load saved state. Starting fresh.")
                # Fallback to starting fresh or handle error as needed
                # Resetting variables needed for a fresh start
                current_date = starting_date
                open_positions = []
                trade_log = []
                trades_all = []
                minute_log = []
                cash_on_hand = starting_bankroll 
                total_long_position = 0.0
                long_cost_basis = 0.0
                total_short_position = 0.0
                short_cost_basis = 0.0
                # Ensure trades_all is initialized if needed for fresh start
                # trades_all = initialize_trades_all(output_folder) # Or leave empty
                run_simulation(instances_by_minute, candles, current_date, ending_date, 
                               output_folder, fee_rate, trades_all, trade_log, open_positions,
                               initial_cash_on_hand=cash_on_hand, 
                               initial_total_long=total_long_position, 
                               initial_long_basis=long_cost_basis, 
                               initial_total_short=total_short_position, 
                               initial_short_basis=short_cost_basis)

            break # Exit loop after processing 'C'

        elif user_choice == 'S':
            # Generate summary report directly from the CSV files
            print("Generating summary report...")
            generate_summary_report(output_folder, starting_date, ending_date)
            print("Summary report generated successfully.")
            break

        elif user_choice == 'P':
            # Prompt for paths
            prompt_paths()

        elif user_choice == 'D':
            # Prompt for dates
            prompt_dates()

        else:
            print("Invalid input. Please enter 'N', 'C', 'S', 'P', or 'D.'")
