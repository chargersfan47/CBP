import websocket
import json
from datetime import datetime
import pytz
import requests
import signal
import sys
import threading
import time

# Configurable Options:
AT_REFRESH_INTERVAL = 180 # Number of seconds between each refreshing of the Airtable data
WAIT_ON_FAILED_CONNECTIONS = 30  # Wait time between retries in seconds, both Airtable and Binance
# Airtable API settings
PERSONAL_ACCESS_TOKEN = 'your_PAT_here'  # Replace with your Airtable personal access token
BASE_ID = 'your_baseID_here'  # Replace with your Airtable Base ID
TABLE_NAME = 'Opportunities'  # Replace with your Airtable Table name
AIRTABLE_URL = f'https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}'
TIME_ZONE = 'UTC'  # Replace with your desired time zone, e.g., 'UTC', 'America/New_York'

# Airtable Fields Used by the Script:
# - Name
# - Status
# - Direction
# - Entry Target
# - TP Target
# - Active Date
# - Completed Date

# **************************************************************************************************
ws = None

# var to store refresh timer:
refresh_timer = None

def signal_handler(sig, frame):
    print("Closing WebSocket Connection...")
    ws.close()
    if refresh_timer is not None:
        refresh_timer.cancel()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Variable to store the last known price
last_price = None

# Variable to store Airtable data
airtable_data = []

# Function to fetch all data from Airtable with pagination and status filtering
def fetch_airtable_data():
    headers = {
        'Authorization': f'Bearer {PERSONAL_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    params = {
        'filterByFormula': "OR(Status = 'Active', Status = 'Pending')"
    }
    
    all_records = []  # List to hold all retrieved records
    offset = None  # Initialize offset
    retry_count = 0  # Retry counter

    while True:
        # If there's an offset, include it in the params
        if offset:
            params['offset'] = offset

        try:
            response = requests.get(AIRTABLE_URL, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()
                records = data.get('records', [])
                all_records.extend(records)  # Append retrieved records to the list
                print(f"Fetched {len(records)} records from Airtable (total so far: {len(all_records)}).")

                # Check if there's more data to fetch
                offset = data.get('offset')
                if not offset:
                    break  # Exit the loop if there's no more data

                retry_count = 0  # Reset retry count on successful request

            else:
                print(f"Error fetching data: {response.status_code} {response.text}")
                time.sleep(WAIT_ON_FAILED_CONNECTIONS)  # Wait before retrying

        except requests.exceptions.RequestException as e:
            print(f"Connection error: {e}")
            time.sleep(WAIT_ON_FAILED_CONNECTIONS)  # Wait before retrying

    return all_records  # Return the complete list of records

# Function to refresh Airtable data
def refresh_airtable_data():
    global airtable_data
    airtable_data = fetch_airtable_data()  # Fetch and store the data

# Function to log updates to a text file
def log_update_to_file(timestamp, record_name, new_status):
    with open('AT_Update.txt', 'a') as log_file:
        log_file.write(f"{timestamp} - {record_name} status changed to {new_status}\n")

# Function to log Airtable responses to a separate file
def log_airtable_response(record_id, response):
    with open('AT_Update_Responses.txt', 'a') as response_log_file:
        current_time = datetime.now(pytz.timezone(TIME_ZONE)).isoformat()
        response_log_file.write(f"{current_time} - Record {record_id} response: {json.dumps(response)}\n")

def log_debug_to_file(record_id, fields):
    with open('AT_Update.txt', 'a') as log_file:
        current_time = datetime.now(pytz.timezone(TIME_ZONE)).isoformat()
        log_file.write(f"Debug: Record ID {record_id} has no name in fields: {fields}\n")

def update_airtable_record(record_id, fields):
    headers = {
            'Authorization': f'Bearer {PERSONAL_ACCESS_TOKEN}',
            'Content-Type': 'application/json'
        }

    # Fetch the current record to verify if an update is necessary
    current_record = next((record for record in airtable_data if record['id'] == record_id), None)
    if current_record is None:
        print(f"Record with ID {record_id} not found in local data.")
        return  # Early exit if record not found

    # Check if the new fields match the current local data to prevent duplicate updates
    if all(current_record['fields'].get(key) == value for key, value in fields.items()):
        print(f"No changes detected for record '{current_record['fields'].get('Name', 'Unknown')}' (ID: {record_id}). Update skipped.")
        return  # Exit if no actual change

    record_name = current_record['fields'].get('Name', 'Unknown Record')
        
    # Log debug details if the record name is missing
    if record_name == 'Unknown Record':
        log_debug_to_file(record_id, current_record['fields'])  # Log to file

    # Prepare the update request
    update_url = f'{AIRTABLE_URL}/{record_id}'
    data = {
        'fields': fields
    }

    # Send the update request
    response = requests.patch(update_url, headers=headers, json=data)
    if response.status_code == 200:
        response_data = response.json()
        print(f"Record '{record_name}' (ID: {record_id}) updated successfully.")
        # Log the update
        log_update_to_file(datetime.now(pytz.timezone(TIME_ZONE)).isoformat(), record_name, fields.get('Status', 'Unknown'))
        log_airtable_response(record_id, response_data)  # Log the response

        # Update the local airtable_data to reflect the new changes
        for record in airtable_data:
            if record['id'] == record_id:
                record['fields'].update(fields)  # Apply new fields to local record
                break
    else:
        print(f"Error updating record {record_id}: {response.status_code} {response.text}")
        log_airtable_response(record_id, response.json())  # Log the error response

# Modify the handle_price_change function to include record counting for logging
def handle_price_change(new_price):
    # Get the current time in the configured time zone
    current_time = datetime.now(pytz.timezone(TIME_ZONE)).isoformat()
    
    # Initialize counters for records checked and updated
    records_checked = 0
    records_updated = 0
    
    # Prepare the initial message for price change
    output = f"[{current_time}] Price changed: {new_price} - "

    # Use the existing Airtable data
    for record in airtable_data:
        records_checked += 1  # Increment count of records checked
        status = record['fields'].get('Status')
        direction = record['fields'].get('Direction')
        record_name = record['fields'].get('Name', 'Unnamed Record')
        
        # Safely convert to float, raising an error if conversion fails
        try:
            entry_target = float(record['fields'].get('Entry Target'))
            tp_target = float(record['fields'].get('TP Target'))
        except (ValueError, TypeError) as e:
            print(f"Error converting Entry Target or TP Target for record '{record_name}' (ID: {record['id']}): {e}. Exiting subroutine.")
            continue  # Skip this record if conversion fails

        # Determine if an update is needed and apply it only if necessary
        target_status = None
        update_fields = {}

        # Check for Pending Long
        if status == 'Pending' and direction == 'Long' and entry_target >= new_price:
            target_status = 'Active'
            update_fields = {'Status': target_status, 'Active Date': current_time}

        # Check for Pending Short
        elif status == 'Pending' and direction == 'Short' and entry_target <= new_price:
            target_status = 'Active'
            update_fields = {'Status': target_status, 'Active Date': current_time}

        # Check for Active Long
        elif status == 'Active' and direction == 'Long' and tp_target <= new_price:
            target_status = 'Completed'
            update_fields = {'Status': target_status, 'Completed Date': current_time}

        # Check for Active Short
        elif status == 'Active' and direction == 'Short' and tp_target >= new_price:
            target_status = 'Completed'
            update_fields = {'Status': target_status, 'Completed Date': current_time}

        # If an update is required and status has changed, proceed
        if target_status and status != target_status:
            update_airtable_record(record['id'], update_fields)
            log_update_to_file(current_time, record_name, target_status)
            records_updated += 1

    # Log the total records checked and updated after processing
    output += f"Checked {records_checked} records"
    if records_updated > 0:
        output += f"; {records_updated} records updated."
    
    print(output)

# Define the WebSocket callback functions
def on_message(ws, message):
    global last_price
    try:
        data = json.loads(message)

        # Check for error messages
        if 'error' in data:
            print(f"Error from server: {data['error']['msg']}")
            return  # Exit the function on error messages

        # Check if the message is a valid trade message
        if 'e' in data:  # Check if the message has an event type
            if data['e'] == 'trade':  # Make sure it's a trade event
                if 'p' in data:  # Check for price key
                    price = float(data['p'])
                    
                    # Check if the price has changed
                    if last_price is None or price != last_price:
                        handle_price_change(price)
                        last_price = price
            else:
                print("Received a non-trade event:", data)
        else:
            print("Received message without event type:", data)
    except Exception as e:
        print(f"Error processing message: {e}")

def on_ping(ws, message):
    print("Received ping; the library will automatically respond with pong.")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed with status {close_status_code} and message: {close_msg}")

def on_open(ws):
    print("Connection opened")

def periodic_refresh():
    global refresh_timer
    refresh_airtable_data()  # Refresh data from Airtable
    refresh_timer = threading.Timer(AT_REFRESH_INTERVAL, periodic_refresh)
    refresh_timer.start()  # Schedule the next refresh

# Start the periodic refresh
periodic_refresh()

def connect_to_binance():
    retry_count = 0
    # WebSocket URL for the SOL/USDT ticker
    url = "wss://stream.binance.com:9443/ws/solusdt@trade"

    while True:
        try:

            # Create the WebSocket application
            global ws
            ws = websocket.WebSocketApp(url,
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close,
                            on_ping=on_ping)

            # Run the WebSocket
            ws.run_forever()
            retry_count = 0
        except Exception as e:
            retry_count += 1
            print(f"Binance WebSocket connection failed (attempt {retry_count}). Retrying in {WAIT_ON_FAILED_CONNECTIONS} seconds...")
            time.sleep(WAIT_ON_FAILED_CONNECTIONS)

connect_to_binance()
