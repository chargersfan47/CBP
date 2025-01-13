# Candle Breaks Project

This project is, at least initially, a Backtesting Starter Kit for the Candle Breaks method.

## Project Structure

The Python folder is organized into:
- **Processing**: Contains scripts for fetching historical candle data, converting timeframes, finding instances, and processing instance data.
- **Analysis**: A place to contain scripts for analyzing instances, with a sample script.
- **BotSim1.0**: Contains a program for simulating trading strategies based on processed instances.
- **Airtable**: Contains the Airtable Updater script.
- Plotter tool is not ready for this release and will be coming soon.

## Required Python Libraries
- `ccxt`
- `pandas`
- `tqdm`

You can install all the required libraries using the following pip command:

```sh
pip install ccxt pandas tqdm
```

## A Quick Note on Paths

The Github comes with an empty set of folders in the Data folder as a suggested structure to hold historical data and all of the output of the various scripts.  Nearly all of these scripts have one or more input and output folder paths that by default make use of the suggested structure.  You can specify paths by changing the default listed near the top of the script (and just press enter when prompted), or you can copy/paste the path into the program when it prompts you to do so.  This is what one of the default path variables look like:
```sh
default_folder_path = os.path.join("..", "..", "Data")
```
Using os.path.join allows us to create a path that works on both Windows (where \ is used) or Linux/Mac (where / is used).  You can string together as many folders as you want by adding more commas and folder names in quotation marks inside the brackets.

We're also using relative paths here, where ".." means up one level from where the script is run.  In most cases we go up two levels to find the Data folder.

The first script downloads historical data and creates a folder named after the end date of the data you download.  The other scripts don't know that date, so they guess today:
```sh
default_input_path = os.path.join("..", "..", "Data", "SOLUSDT", "Candles", datetime.now().strftime('%Y-%m-%d'))
```
So, unless you process everything on the same day, you will likely need to change the defaults.

## Processing Folder

The scripts in the Processing folder are used for fetching historical candle data, converting timeframes, finding instances, and processing instance data. 

### Step 1:  Download historical data.  Run `download_binance_historical_data.py`
   - This script downloads historical candle data for any trading pair (default: SOLUSDT) from Binance Futures and saves it in CSV format.
   - By default it will download a standard set of TFs but if you want to deal with a wide variety of custom timeframes, I recommend only downloading the 1m candle data and proceeding to step 2.

### (Optional) Step 2:  Create custom timeframes.  Run `historical_data_TF_converter.py`
   - This script resamples the 1m candle data into specified custom timeframes and saves the resulting data as new CSV files.
   - The default list of custom TFs was provided by syndotc.  The intra-day TFs are all factors of 1440, or in other words, divide evenly into a day.  The multi-day TFs run up to 36D.

### Step 3: Find 1v1 instances.  Run `historical_opps_finder_1v1.py`
   - This script processes historical candle data to find instances of 1v1 candle breaks.
   - It calculates some Fibonacci retrace levels for further analysis and saves the results to CSV files.

### (Optional) Step 4: Find XvY instances.  Run `historical_opps_finder_XvY.py`
   - This script processes historical candle data to find instances of XvY candle breaks.
   - It calculates some Fibonacci retrace levels for further analysis and saves the results to CSV files.

### Step 5: Process instances.
####  Run `historical_process_status_of_instances.py`
   - This script processes instances using the 1m candle data as a price reference.
   - It calculates whether specific price targets or Fibonacci levels are reached and updates the instance's status accordingly.  It also records timestamps of the various fib levels being reached.
   - The processed files are saved to an output folder, and the original files can be optionally deleted.
   - This "single core" version of the file will do all processing in one thread, which may take a long time.

#### Or

#### Run `historical_process_status_of_instances_multicore.py`
   - This script does the same processing as the single-core version above.
   - This "multicore" version will split the work into chunks, one for each processor core you have, and process each chunk at the same time. This will be much faster but may strain your system.

## Analysis Folder

The Analysis folder contains scripts for analyzing processed instances.

### `TF_Instances_Summarizer.py`
   - This script produces a summary of instances by timeframe.
   - It calculates various metrics such as counts of longs/shorts, win rates, and average times for different stages of fib levels.

## BotSim1.0

The BotSim1.0 folder contains a program for simulating trading strategies based on processed instances.  It produces the following output:
- A minute-by-minute tracking of total bankroll, long / short position size, unrealized PnL, and cost basis.  This is split into monthly files named **analysis_yyyymm.csv**
- A list of currently open positions in **open_positions.csv**.  As trades are closed, they are removed from this file and moved to...
- A list of closed positions in **closed_positions.csv**.  This file contains instance performance, because each row represents an open trade and a close trade.
- A list of all trades made in four types: open long, close long, open short, close short.  This file is called **trades_all.csv**.
- A performance summary report, named **(folder name)_(start_date)_(end_date).csv**.  It produces a monthly performance summary, and a report on results by timeframe. 

Brief description of the files:
- **config.py** - Contains the configurable settings for a simulator run.
- **main.py** - Launch this to start the program.  Contains the main menu.
- **initialization.py** - Contains code to load state data and resume a previously started run.
- **log_utils.py / reporting.py** - Contains logging utilities and generates the summary data file.
- **sim_entries.py** - Contains logic for executing entries during the sim.
- **sim_exits.py** - Contains logic for executing exits during the sim.
- **position_size.py** - Contains logic for determining the position size of an entry. 
- **simulation.py** - Contains the main structure of the simulation processing loop.

To use it:

1.  Create a new folder for the output of the simulation.  Suggested location is (root)\Data\{asset}\Simulations
    Name it something meaningful; its name will be used in the creation of the summary file.  See suggested naming scheme below.
2.  Update the configuration settings in `config.py` to define how you want the simulated bot to function, and set the input / output folder paths.
3.  Run `main.py` in terminal to start the simulator.

### Prompt Options in `main.py`

When you run `main.py`, you will be presented with the following main menu options:

- **N - Start a new simulation**: Initialize and start a new simulation based on the parameters provided in config.py.
- **C - Continue from the last saved state**: Continue a simulation from the last saved state.  You will be prompted for a new end date, and the simulation will load previous data from its last stopping point and continue from there until the new end date.  Please note that this is only tested for use on simulation runs that completed successfully; no guarantees it would work on a run that stopped because it was halted.  Also note that resuming a run will continue the simulation with the variables currently set in config.py; in other words, a completed simulation doesn't save its config.py settings for use when continuing a completed run.
- **S - Generate a summary report from CSV files**: Generate a summary report directly from the data in a simulation run's output folder.  Useful if you're working on changing the format of the summary report.
- **P - Prompt for paths**: Prompt for the paths to the instances folder, candle data file, and output folder, in case you want to change paths while the program is running instead of specifying them in config.py.
- **D - Set start and end dates**: Set the start and end dates for the simulation.  Useful if you launched the program without editing config.py and want to make a change to the dates.

### Suggested Simulation Naming Scheme

Example:  30mo eLTF 36hTC 720pen2500 1ppU

- **30mo** - Length of time the simulation covers.
- **eLTF** - Extra-low timeframes.  In this case it was 3m - 20m.
- **36hTC** - 36 hour time capitulation factor was used.
- **720pen2500** - A min/max time spent pending was used, 720 minutes to 2500 minutes.
- **1ppU** - 1 percentage point (of total bankroll) units were used for each trade.

## Airtable

Version 1 of the Airtable Opportunities Updater script lives here.  For more on tracking opportunities using Airtable, please visit our #tracking-with-airtable channel on the discord server.  If you are already tracking with Airtable, the updater script here might be useful to you.

Version 2 of the script is unfinished, so I did not post it to github. If anyone wants to complete it, please let me know.

### How it works
Airtable has API rate limits; you can only make so many calls to their system per account tier.  For the first paid version, it's 100,000 / month, which sounds like a lot, but it works out to about 2.2 / minute.  The next tier up is unlimited (but no more than 10/second or something like that).

Version 1 of the script fetches fresh data from Airtable (1 API call) every x seconds (I have it at 180 by default).  Then it monitors the price feed from the Binance websocket, and every time it changes, it checks all the records it knows about and sends updates (1 API call per record) to Airtable as statuses change. 
It works, and updates are nearly realtime, but there's not much control over how many API calls it makes.  It also has the downside of not knowing about new opportunities sent to Airtable by TradingView for up to x seconds, and if said opps activate in those x seconds, the updater wouldn't know about it and would leave them as pending.

Version 2 also pulls fresh Airtable data every x seconds (I was testing it at 30 this time).  Instead of constantly checking for real-time updates from the binance websocket, it collects data from it for y seconds (i was testing at 10) and just makes note of the highest and lowest price in those y seconds.  Then it checks all the records for changes (once every y seconds), and makes a list of them, and then submits them all at once in a batch (Airtable lets you update 10 records in 1 API call this way).
As long as x = y, the script would never miss an activation.  The larger the value of y, the less API calls would be made by record changes.  But updates to Airtable might take up to y seconds to actually appear on your screen.

With all that said, it has been suggested in the Airtable forums that they're not actually enforcing API call limits... 🤷‍♂️

### More info
- This script uses data from the Binance Websockets API and specifically the SOLUSDT(futures) pair.  It is strongly recommended that you set your TV alerts to the same pair for consistency of data (or edit the script to use whatever pair you want)
- This script will only track opportunities as they happen; it can't look at past prices and figure out what happened to your opps in the past.  So I recommend manually updating the status of your opps before you start to use the script.  (Or just delete them and start fresh)
- You will need to have this script running 24/7 on a computer or server for it to be able to do its thing.
- In the first few lines of code you'll see a place you need to put your Airtable account info.  Go into your Airtable account and create a Personal Access Token (and give it some permissions), and then copy and paste it into the script.  There's also a place where you have to put your Airtable Base ID, which you can find in the Airtable API docs.

## The "Unsorted - from syndotc" folder
... is a collection of scripts and stuff that syndotc has given me that I haven't had the time to look at just yet.  What's in there?  How do I use it?  I have no idea yet.  All I know is that the S/R stuff has a script to generate S/R levels and another one to insert them into your chart... I think.  You figure that one out.

## Support the Project

I only include this because I was asked to by people who intend to use it.  If you find this project helpful and would like to support its development, consider making a donation. Your support is absolutely not necessary but greatly appreciated!

**Crypto Wallet Address:**

Bitcoin (BTC): 
```sh
bc1q0fexm6y2zp92frcg7evekagdhr4jahv3cmd3jg
```
Solana (SOL):
```sh
9QeFeWkoXSveDLk8Ryp8UsaoTEekTaudmS5tccCPdG25
```
Ethereum (ETH):
```sh
0x0665cdD88E305A299B91294f9C9D11746A4688e7
```

