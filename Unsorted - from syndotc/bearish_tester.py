import pandas as pd
import os
from datetime import datetime

# Directory containing the chart data CSV files
directory_path = "./chart_data/"
lower_timeframe_file = "combined.csv"  # The 15-minute timeframe file for the second pass

# Function to read and parse data from CSV files
def load_data_from_csv(file_path, start_time=None, end_time=None):
    try:
        df = pd.read_csv(file_path)

        # Convert the 'time' column from UNIX to datetime format
        df['time'] = pd.to_datetime(df['time'], unit='s')

        # Filter data based on start and end time, if specified
        if start_time and end_time:
            # Use the correct format string for input
            start_timestamp = datetime.strptime(start_time, '%d/%m/%Y %H:%M:%S')
            end_timestamp = datetime.strptime(end_time, '%d/%m/%Y %H:%M:%S')
            df = df[(df['time'] >= start_timestamp) & (df['time'] <= end_timestamp)]

        # Convert time back to string format for display
        df['time'] = df['time'].dt.strftime('%d/%m/%Y %H:%M:%S')
        return df
    except Exception as e:
        print(f"Error loading file {file_path}: {e}")
        return None

# Function to analyze data and identify potential trades based on the current file
def analyse_data_bearish(data, file_name):
    if data is None or len(data) < 3:
        print(f"Insufficient data for analysis in {file_name}.")
        return []

    potential_trades = []  # Store potential trades in the first pass

    # First Pass: Identify Potential Entries for Bearish Breaks
    for i in range(2, len(data)):
        current_candle = data.iloc[i]
        previous_candle = data.iloc[i - 1]

        is_bullish_prev = previous_candle['close'] > previous_candle['open']
        is_bearish_current = current_candle['close'] < previous_candle['open']
        bearish_break_below_bullish = is_bullish_prev and is_bearish_current and current_candle['close'] < previous_candle['open']

        if bearish_break_below_bullish:
            open_prev = round(previous_candle['open'], 3)
            high_prev = previous_candle['high']

            # Calculate Fibonacci targets and entry price
            target1618 = round(open_prev - (high_prev - open_prev) * 0.618, 3)
            entry_price = round(open_prev - (10 * 0.001), 3)  # Adjust entry price by subtracting 10 ticks

            # Calculate stop-loss for the short position
            stop_loss = round(entry_price * 2, 3)

            # Store potential trade information
            trade_info = {
                'File': file_name,  # New column to track the source file
                'Entry Price': entry_price,
                'Target': target1618,
                'Stop Loss': stop_loss,
                'Found Timestamp': current_candle['time'],
                'Entry Timestamp': None,
                'Exit Timestamp': None,
                'Status': 'Pending',
                'Time in Trade': None  # New column for time in trade duration
            }

            potential_trades.append(trade_info)

    return potential_trades

# Function to validate trades using a lower timeframe file
# Function to validate trades using a lower timeframe file with max drawdown adjustment for short trades
def validate_trades_with_lower_timeframe(trades, lower_timeframe_data, higher_timeframe_data):
    if len(trades) == 0 or lower_timeframe_data is None or len(higher_timeframe_data) < 2:
        return []

    validated_trades = []  # Store validated trades after the second pass

    # Ensure 'time' in higher timeframe data is in datetime format with day-first format
    higher_timeframe_data['time'] = pd.to_datetime(higher_timeframe_data['time'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)

    # Determine the interval of the higher timeframe in minutes
    interval_seconds = (higher_timeframe_data['time'].iloc[1] - higher_timeframe_data['time'].iloc[0]).total_seconds()
    interval_minutes = interval_seconds / 60

    # Convert 'Found Timestamp' in potential trades to datetime format for proper comparison
    for trade in trades:
        found_time = datetime.strptime(trade['Found Timestamp'], '%d/%m/%Y %H:%M:%S')
        # Set the Validation Start Time based on the calculated interval
        trade['Validation Start Time'] = found_time + pd.Timedelta(minutes=interval_minutes)

    # Convert 'time' in lower timeframe data to datetime format for comparison
    lower_timeframe_data['time'] = pd.to_datetime(lower_timeframe_data['time'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)

    # Second Pass: Validate trades using lower timeframe data
    for trade in trades:
        # Filter lower timeframe data to start checking from the validation start timestamp
        relevant_data = lower_timeframe_data[lower_timeframe_data['time'] >= trade['Validation Start Time']]

        entry_found = False
        max_drawdown = 0  # Initialize max drawdown for this trade

        for i, subsequent_candle in relevant_data.iterrows():
            # Check if the trade is entered based on the entry price
            if subsequent_candle['low'] <= trade['Entry Price'] <= subsequent_candle['high']:
                trade['Entry Timestamp'] = subsequent_candle['time']
                entry_found = True
                highest_price = trade['Entry Price']  # Track the highest price reached since entry for shorts

                # Validate if TP or SL is hit in subsequent candles after entry
                for j, validation_candle in relevant_data.loc[i:].iterrows():
                    # Update the highest price since entry to calculate drawdown for shorts
                    if validation_candle['high'] > highest_price:
                        highest_price = validation_candle['high']
                        max_drawdown = round(((highest_price - trade['Entry Price']) / trade['Entry Price']) * 100, 2)

                    if validation_candle['low'] <= trade['Target']:
                        trade['Status'] = 'Win'
                        trade['Exit Timestamp'] = validation_candle['time']
                        break
                    if validation_candle['high'] >= trade['Stop Loss']:
                        trade['Status'] = 'Loss'
                        trade['Exit Timestamp'] = validation_candle['time']
                        break

                # If no TP or SL hit, mark it as Incomplete
                if trade['Status'] == 'Pending':
                    trade['Status'] = 'Incomplete'
                    trade['Exit Timestamp'] = 'Incomplete (End of Data)'
                    
                    # For incomplete trades, calculate drawdown from entry to the highest observed price
                    highest_observed_price = relevant_data['high'].max()
                    max_drawdown = round(((highest_observed_price - trade['Entry Price']) / trade['Entry Price']) * 100, 2)

                # Calculate the time in trade (in minutes)
                if trade['Entry Timestamp'] and trade['Exit Timestamp'] != 'Incomplete (End of Data)':
                    entry_time = pd.to_datetime(trade['Entry Timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
                    exit_time = pd.to_datetime(trade['Exit Timestamp'], format='%d/%m/%Y %H:%M:%S', dayfirst=True)
                    trade['Time in Trade'] = (exit_time - entry_time).total_seconds() / 60  # Time in minutes

                trade['Max Drawdown'] = max_drawdown  # Add max drawdown to the trade
                validated_trades.append(trade)
                break

        # If no entry found, mark as incomplete for further validation
        if not entry_found:
            trade['Status'] = 'Incomplete'
            trade['Entry Timestamp'] = 'No Entry (End of Data)'
            trade['Max Drawdown'] = 'N/A'  # No drawdown if trade never entered
            validated_trades.append(trade)

    return validated_trades

# Function to iterate through and analyze all CSV files in the directory
def analyze_csv_files(start_time, end_time, output_id):
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv') and f != lower_timeframe_file]
    all_validated_trades = []
    win_rate_summary = {}  # Store win rate for each timeframe (CSV file)

    # Load the lower timeframe data for the second pass validation
    lower_timeframe_path = os.path.join(directory_path, lower_timeframe_file)
    lower_timeframe_data = load_data_from_csv(lower_timeframe_path, start_time, end_time)

    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        print(f"\nAnalyzing file: {file_path}")
        market_data = load_data_from_csv(file_path, start_time, end_time)

        if market_data is not None:
            potential_trades = analyse_data_bearish(market_data, csv_file)  # First Pass with file name
            validated_trades = validate_trades_with_lower_timeframe(potential_trades, lower_timeframe_data, market_data)  # Second Pass
            all_validated_trades.extend(validated_trades)

            # Calculate win rate for this file/timeframe
            total_trades = len(validated_trades)
            winning_trades = sum(1 for trade in validated_trades if trade['Status'] == 'Win')
            losing_trades = sum(1 for trade in validated_trades if trade['Status'] == 'Loss')
            incomplete_trades = sum(1 for trade in validated_trades if trade['Status'] == 'Incomplete')

            # Calculate win rate for this timeframe
            win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
            win_rate_summary[csv_file] = {
                'Total Trades': total_trades,
                'Winning Trades': winning_trades,
                'Losing Trades': losing_trades,
                'Incomplete Trades': incomplete_trades,
                'Win Rate': win_rate
            }

    # Save all validated trades to a CSV file
    if all_validated_trades:
        trades_df = pd.DataFrame(all_validated_trades)
        # Output file path
        output_file = os.path.join(f'validated_bearish_trades_with_time_range_{output_id}.csv')
        trades_df.to_csv(output_file, index=False)
        print(f"\nAll validated trades have been saved to {output_file}")

    # Display overall summary for each timeframe
    print("\n=== Summary of Bearish Win Rates by Timeframe ===")
    for timeframe, stats in win_rate_summary.items():
        # Initialize list to store drawdowns
        drawdowns = [trade['Max Drawdown'] for trade in all_validated_trades if trade['File'] == timeframe and isinstance(trade['Max Drawdown'], (int, float))]
        
        # Calculate average max drawdown
        avg_max_drawdown = sum(drawdowns) / len(drawdowns) if drawdowns else 0
        
        print(f"Timeframe: {timeframe}")
        print(f"  - Total Trades: {stats['Total Trades']}")
        print(f"  - Winning Trades: {stats['Winning Trades']}")
        print(f"  - Losing Trades: {stats['Losing Trades']}")
        print(f"  - Incomplete Trades: {stats['Incomplete Trades']}")
        print(f"  - Win Rate: {stats['Win Rate']:.2f}%")
        print(f"  - Average Max Drawdown: {avg_max_drawdown:.2f}%")  # Display average max drawdown
        print("====================================")

# Main function to run the strategy using the CSV file analysis
def run_strategy():
    start_time = input("Enter Start Date & Time (DD/MM/YYYY HH:MM:SS): ")
    end_time = input("Enter End Date & Time (DD/MM/YYYY HH:MM:SS): ")
    output_name = input("Enter Output Identifier: ")
    analyze_csv_files(start_time, end_time, output_name)

# Run the strategy
run_strategy()
