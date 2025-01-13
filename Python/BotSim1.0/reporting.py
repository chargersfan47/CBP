import os
import csv
from datetime import datetime, timedelta
from log_utils import analyze_monthly_data, sort_timeframes

def calculate_current_positions(trades_file, end_date):
    current_longs, current_shorts = 0, 0

    with open(trades_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trade_date = datetime.strptime(row['trade_date'], '%Y-%m-%d %H:%M:%S')
            if trade_date <= end_date:
                if row['order_type'] == 'open long':
                    current_longs += 1
                elif row['order_type'] == 'close long':
                    current_longs -= 1
                elif row['order_type'] == 'open short':
                    current_shorts += 1
                elif row['order_type'] == 'close short':
                    current_shorts -= 1

    return current_longs, current_shorts

def count_trades_by_month(trades_file, month):
    total_trades, open_long_trades, open_short_trades = 0, 0, 0
    close_long_trades, close_short_trades = 0, 0
    sum_of_pnl, wins, losses = 0.0, 0, 0

    with open(trades_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trade_date = datetime.strptime(row['trade_date'], '%Y-%m-%d %H:%M:%S')
            if trade_date.strftime('%Y%m') == month:
                total_trades += 1
                if row['order_type'] == 'open long':
                    open_long_trades += 1
                elif row['order_type'] == 'open short':
                    open_short_trades += 1
                elif row['order_type'] == 'close long':
                    close_long_trades += 1
                elif row['order_type'] == 'close short':
                    close_short_trades += 1
                pnl = float(row['ind_PnL']) if row['ind_PnL'] else 0.0
                sum_of_pnl += pnl
                if row['order_type'] in ['close long', 'close short']:
                    if pnl > 0:
                        wins += 1
                    elif pnl < 0:
                        losses += 1

    return total_trades, open_long_trades, open_short_trades, close_long_trades, close_short_trades, sum_of_pnl, wins, losses

def generate_summary_report(output_folder, starting_date, ending_date):
    # Get the direct folder name
    folder_name = os.path.basename(output_folder)

    existing_files = os.listdir(output_folder)
    analysis_files = [file for file in existing_files if file.startswith('analysis_')]
    months = sorted({file.split('_')[1].split('.')[0] for file in analysis_files if file.split('_')[1].split('.')[0].isdigit()})

    if not months:
        print("No monthly files found for the summary report.")
        return

    # Determine the first and last month from the available files
    first_month = months[0]
    last_month = months[-1]

    # Construct the summary file name using the folder name and the first and last month
    summary_file = os.path.join(output_folder, f'{folder_name}_{first_month}_{last_month}.csv')

    all_monthly_data = []
    timeframe_data = {}

    custom_order = [
        "1m", "2m", "3m", "4m", "5m", "6m", "8m", "9m", "10m", "12m", "15m", "16m", "18m", "20m", "24m", "30m",
        "32m", "40m", "45m", "48m", "1h", "72m", "80m", "90m", "96m", "2h", "144m", "160m", "3h", "4h", "288m",
        "6h", "8h", "12h", "1d", "2d", "3d", "1w", "multi-day"
    ]

    trades_file = os.path.join(output_folder, 'trades_all.csv')

    cumulative_longs, cumulative_shorts = 0, 0

    for month in months:
        analysis_file = os.path.join(output_folder, f'analysis_{month}.csv')
        open_positions_file = os.path.join(output_folder, 'open_positions.csv')
        
        if os.path.exists(analysis_file):
            monthly_data, timeframe_data = analyze_monthly_data(analysis_file, trades_file, open_positions_file, month, custom_order, timeframe_data)
            all_monthly_data.extend(monthly_data)

        total_trades, open_long_trades, open_short_trades, close_long_trades, close_short_trades, sum_of_pnl, wins, losses = count_trades_by_month(trades_file, month)

        # Update cumulative positions
        cumulative_longs += open_long_trades - close_long_trades
        cumulative_shorts += open_short_trades - close_short_trades

        # Update monthly data for current positions
        for entry in all_monthly_data:
            if entry['Month'] == month:
                entry.update({
                    'Total Trades': total_trades,
                    'Open Long Trades': open_long_trades,
                    'Open Short Trades': open_short_trades,
                    'Close Long Trades': close_long_trades,
                    'Close Short Trades': close_short_trades,
                    'Sum of PnL': round(sum_of_pnl, 4),
                    'Wins': wins,
                    'Losses': losses,
                    'Current Longs': cumulative_longs,
                    'Current Shorts': cumulative_shorts
                })

    monthly_summary_columns = [
        'Month', 'Opening Bankroll', 'Closing Bankroll', 'Close Price', 'Total Trades', 'Open Long Trades',
        'Open Short Trades', 'Close Long Trades', 'Close Short Trades', 'Sum of PnL', 'Wins', 'Losses',
        'Win Rate', 'Current Longs', 'Current Shorts', 'Closing Long Balance',
        'Closing Short Balance', 'Closing Balance', 'Bankroll High', 'BR High Date', 'Bankroll Low', 'BR Low Date'
    ]

    timeframe_summary_columns = [
        'Timeframe', 'Wins', 'Losses', 'Average PnL', 'Win Rate'
    ]

    with open(summary_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=monthly_summary_columns)
        writer.writeheader()
        for entry in all_monthly_data:
            writer.writerow(entry)

        writer.writerow({})  # Add a blank line between the tables

        writer = csv.DictWriter(f, fieldnames=timeframe_summary_columns)
        writer.writeheader()
        sorted_timeframes = sort_timeframes(timeframe_data, custom_order)
        for tf, data in sorted_timeframes:
            average_pnl = round(data['PnL_Sum'] / data['ClosedTrades'], 4) if data['ClosedTrades'] > 0 else 0.0
            win_rate = round(data['Wins'] / (data['Wins'] + data['Losses']), 4) if (data['Wins'] + data['Losses']) > 0 else 0.0
            writer.writerow({
                'Timeframe': tf,
                'Wins': data['Wins'],
                'Losses': data['Losses'],
                'Average PnL': average_pnl,
                'Win Rate': win_rate
            })

    print(f"Summary report saved as {summary_file}")
