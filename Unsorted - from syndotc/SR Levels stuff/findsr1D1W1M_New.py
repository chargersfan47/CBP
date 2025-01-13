import pandas as pd
import os
from datetime import datetime
import requests
import re


def load_chart_data(data_file):
    # Load the candlestick data
    data = pd.read_csv(data_file)

    # Debugging: Print column names to verify structure
    print(f"Loaded data columns: {list(data.columns)} from {data_file}")

    # Handle various possible names for the time column
    time_column = None
    for col in data.columns:
        if col.lower() in ['time', 'timestamp', 'date']:  # Possible column names for time
            time_column = col
            break

    if not time_column:
        raise KeyError("The dataset is missing a 'time' column or equivalent (e.g., 'timestamp').")

    # Convert the time column to datetime
    data['time'] = pd.to_datetime(data[time_column], unit='s')

    if data.empty:
        raise ValueError("The dataset is empty after loading. Please check the input file.")

    return data


def align_to_timeframe(data, timeframe, start_date, file_name_prefix):
    """
    Construct a new timeframe from the data for a specific year.
    """
    if data is None or len(data) < 1:
        print("No data available for timeframe construction.")
        return None

    # Ensure the 'time' column is recognized and converted to datetime
    if 'time' not in data.columns:
        raise KeyError("The dataset is missing the 'time' column.")

    # Set 'time' as the index without losing the column
    data = data.copy()
    data.set_index('time', inplace=True)

    # Generate the custom frequency rule
    frequency = None
    if timeframe.endswith('D'):
        days = int(timeframe[:-1])
        frequency = f'{days}D'
    elif timeframe.endswith('W'):
        weeks = int(timeframe[:-1])
        frequency = f'{weeks * 7}D'  # Weekly intervals as multiples of 7 days
    elif timeframe.endswith('M'):
        months = int(timeframe[:-1])
        frequency = f'{months}MS'

    if not frequency:
        print(f"Unsupported timeframe: {timeframe}")
        return None

    # Define aggregation mapping dynamically based on existing columns
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
    }
    if 'volume' in data.columns:
        agg_dict['volume'] = 'sum'

    # Resample data to the specified timeframe
    try:
        resampled_data = data.resample(frequency, origin=start_date).agg(agg_dict).dropna()
        resampled_data.reset_index(inplace=True)  # Ensure 'time' is restored as a column

    except Exception as e:
        print(f"Error during resampling for timeframe {timeframe}: {e}")
        return None

    return resampled_data


def analyze_all_charts_year_by_year(folder_path, symbol="ETHUSDT24"):
    current_price = get_current_price(symbol)
    if current_price is None:
        print("Unable to fetch current price. Exiting analysis.")
        return

    print(f"Current Price for {symbol}: {current_price}\n")

    all_potential_trades = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            print(f"\nAnalyzing chart data from: {file_name}")

            # Load the chart data
            data = load_chart_data(file_path)
            file_name_prefix = os.path.splitext(file_name)[0]  # Get the file name without extension

            # Split data into year ranges
            data['year'] = data['time'].dt.year
            years = sorted(data['year'].unique())

            for year in years:
                if year + 1 in years:
                    year_data = data[(data['year'] >= year) & (data['year'] < year + 1)]
                else:
                    year_data = data[data['year'] == year]

                print(f"Processing year: {year} ({len(year_data)} rows)")

                # Determine the base timeframe from the filename
                if "1D" in file_name:
                    #timeframes = ["1D", "2D", "3D", "4D", "5D"]
                    timeframes = [f"{i}D" for i in range(1, 182)]
                elif "1W" in file_name:
                    #timeframes = ["1W", "2W", "3W", "4W", "5W"]
                    timeframes = [f"{i}W" for i in range(1, 26)]
                elif "1M" in file_name:
                    #timeframes = ["1M", "2M", "3M", "4M", "5M"]
                    timeframes = [f"{i}M" for i in range(1, 6)]
                else:
                    print(f"Unrecognized timeframe in file name: {file_name}. Skipping.")
                    continue

                # Process each timeframe for the year
                for tf in timeframes:
                    aligned_data = align_to_timeframe(year_data, tf, f"{year}-01-01", file_name_prefix)
                    if aligned_data is not None:
                        trades = analyse_data(aligned_data, tf, current_price)
                        print(f"Found {len(trades)} potential trades in {tf} timeframe for {year}.")
                        all_potential_trades.extend(trades)

    # Save the trades to a CSV
    if all_potential_trades:
        trades_df = pd.DataFrame(all_potential_trades)
        trades_df = trades_df.sort_values(by=['Timeframe', 'Found Timestamp'], key=lambda col: col.str.extract(r'(\d+)', expand=False).astype(float) if col.name == 'Timeframe' else col)
        output_file = "support_and_resistance_levels.csv"
        trades_df.to_csv(output_file, index=False)
        print(f"\nAll potential trades have been saved to {output_file}")

    print("\n=== Summary of Trades ===")
    for trade in all_potential_trades:
        print(trade)


def get_current_price(symbol="ETHUSDM24"):
    # Fetch current price from Bitget API
    url = f"https://api.bitget.com/api/v2/mix/market/ticker?productType=USDT-FUTURES&symbol={symbol}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data.get("code") == "00000":
            current_price = float(data["data"][0]["lastPr"])
            return current_price
        else:
            print(f"Error fetching current price: {data.get('msg')}")
            return None
    except Exception as e:
        print(f"Error fetching current price: {e}")
        return None


def analyse_data(data, timeframe_name, current_price):
    if data is None or len(data) < 3:
        print(f"Insufficient data for analysis in {timeframe_name}.")
        return []

    potential_trades = []

    for i in range(2, len(data)):
        current_candle = data.iloc[i]
        previous_candle = data.iloc[i - 1]

        # Analyze bearish break below bullish
        is_bullish_prev = previous_candle['close'] > previous_candle['open']
        is_bearish_current = current_candle['close'] < previous_candle['open']
        bearish_break = (
            is_bullish_prev
            and is_bearish_current
            and current_candle['close'] < previous_candle['open']
            and current_candle['high'] > previous_candle['high']
        )
        if bearish_break:
            level = previous_candle['high']
            level_type = "Resistance" if current_price < level else "Support"
            trade_info = {
                'Timeframe': timeframe_name,
                'Level Type': level_type,
                'Level': level,
                'Found Timestamp': current_candle['time'],
            }
            potential_trades.append(trade_info)

        # Analyze bullish break above bearish
        is_bearish_prev = previous_candle['close'] < previous_candle['open']
        is_bullish_current = current_candle['close'] > previous_candle['open']
        bullish_break = (
            is_bearish_prev
            and is_bullish_current
            and current_candle['close'] > previous_candle['open']
            and current_candle['low'] < previous_candle['low']
        )
        if bullish_break:
            level = previous_candle['low']
            level_type = "Support" if current_price > level else "Resistance"
            trade_info = {
                'Timeframe': timeframe_name,
                'Level Type': level_type,
                'Level': level,
                'Found Timestamp': current_candle['time'],
            }
            potential_trades.append(trade_info)

    return potential_trades


# Directory containing chart data
chart_data_folder = 'chart_data/'  # Change to your actual folder path
analyze_all_charts_year_by_year(chart_data_folder, symbol="SOLUSDT")