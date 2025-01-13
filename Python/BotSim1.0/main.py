import os
from datetime import datetime, timedelta
from tqdm import tqdm
from initialization import load_instances, load_candles, load_state, initialize_trades_all
from simulation import run_simulation
from reporting import generate_summary_report
from config import *

def prompt_paths():
    global instances_folder, candles_file, output_folder
    instances_folder = input("Enter the folder path containing the instance CSV files: ")
    candles_file = input("Enter the path to the 1m candles data file: ")
    output_folder = input("Enter the folder path for output files: ")

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
            instances = load_instances(instances_folder, current_date, ending_date)
            # print("\n")  # Add CR/LF after progress bar completes

            # print("Loading candles data...")
            candles = load_candles(candles_file, current_date, ending_date)
            # print("\n")  # Add CR/LF after progress bar completes

            # Run the simulation
            run_simulation(instances, candles, current_date, ending_date, output_folder, fee_rate, trades_all, minute_log, trade_log, open_positions)

            break

        if user_choice == 'C':
            trades_all = initialize_trades_all(output_folder)
            state = load_state(output_folder)
            if state:
                (minute_log, trade_log, open_positions, current_month, current_date, 
                 total_long_position, total_short_position, long_cost_basis, short_cost_basis, cash_on_hand) = state

                # Ensure that all loaded trades are part of the trade_log
                for trade in trades_all:
                    if trade not in trade_log:
                        trade_log.append(trade)
    
                # Prompt for new end date
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

                # Increment the current_date to start processing from the next day at 00:00
                current_date = (current_date + timedelta(days=1)).replace(hour=0, minute=0, second=0)

                # Load data with progress bar
                print("Loading instances data...")
                instances = load_instances(instances_folder, current_date, ending_date)
    
                print("Loading candles data...")
                candles = load_candles(candles_file, current_date, ending_date)
    
                # Run the simulation
                run_simulation(instances, candles, current_date, ending_date, output_folder, fee_rate, trades_all, minute_log, trade_log, open_positions)

                break
            else:
                print("No saved state found. Starting a new simulation.")
                continue

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
            print("Invalid input. Please enter 'N', 'C', 'S', 'P', or 'D'.")
