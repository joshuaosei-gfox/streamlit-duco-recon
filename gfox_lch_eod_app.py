import os
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import yaml
import requests
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from collections import OrderedDict
import csv
import json
import time

# -------------------------- Global Variables --------------------------

# Database connection parameters
DB_PARAMS = {
    "dbname": "gfox",
    "user": "gfox",
    "password": "gfox1234",
    "host": "10.16.1.10",
    "port": "5432"
}

# Directory paths
GFOX_BASE_DIRECTORY = r"Z:\DUCO LCH Recon App\PROD\File Creation\API\GFOX EOD Files"
LCH_BASE_DIRECTORY = r"Z:\DUCO LCH Recon App\PROD\File Creation\API\LCH EOD Files"
LCH_OUTBOUND_URL = "http://10.16.1.13:20201/outbound/"

# DUCO API variables
DUCO_SUBMISSION_URL = "https://gfo-x.duco-app.com/api/submissions"
DUCO_API_HEADERS = {
    "Authorization": "token 202e642811a164533ebdda6cc8f258bfNDc",
    "Accept": "application/vnd.duco-cube.v3+json"
}
DUCO_PROCESSES_URL = "https://gfo-x.duco-app.com/processes"

# File patterns for submissions
FILE_PATTERNS = {
    "TRADES_LONG": {"GFOX": r"LCH EOD Trades Ingest Long_\d{4}-\d{2}-\d{2}\.csv", 
                    "LCH": r"\d{8}_RECO_GFOX_LCHC_EOD_PRD_\d{14}_LONG_DUCO\.csv"},

    "TRADES_SHORT": {"GFOX": r"LCH EOD Trades Ingest Short_\d{4}-\d{2}-\d{2}\.csv",
                     "LCH": r"\d{8}_RECO_GFOX_LCHC_EOD_PRD_\d{14}_SHORT_DUCO\.csv"},

    "PRICES": {"GFOX": r"LCH EOD Prices Ingest_\d{4}-\d{2}-\d{2}\.csv",
               "LCH": r"PRICE_GFOX_PRD_\d{8}_DUCO\.csv"},

    "INSTRUMENTS": {"GFOX": r"LCH EOD Instruments Ingest_\d{4}-\d{2}-\d{2}\.csv",
                    "LCH": r"INSTRUMENT_GFOX_PRD_\d{8}\.csv"}
}

# -------------------------- Helper Functions --------------------------

# Configure logging
logging.basicConfig(filename='query_log.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

# Function to log queries
def log_query(query):
    logging.info(query)

# Streamlit logger to update in the app interface
def streamlit_logger(log_message, log_widget):
    """Update the Streamlit log widget with new log messages."""
    if log_widget:
        current_logs = log_widget
        current_logs += f"{log_message}\n"
        return current_logs
    return f"{log_message}\n"

# -------------------------- App 1: GFOX EOD File Extraction --------------------------

def gfox_eod_file_extraction():
    st.title("GFOX EOD File Extraction")

    # Load the queries from YAML file
    def load_queries_from_yaml(filepath):
        with open(filepath, 'r') as file:
            queries = yaml.safe_load(file)
        return queries

    queries = load_queries_from_yaml('queries.yaml')

    # Function to check database connection and provide feedback to the user
    def check_db_connection():
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            conn.close()
            st.success(f"Successfully connected to the database at {DB_PARAMS['host']}:{DB_PARAMS['port']}")
            return True
        except Exception as e:
            st.error(f"Failed to connect to the database: {str(e)}")
            return False

    # Function to validate the input date format
    def validate_date_input(date_input):
        try:
            trade_date = datetime.strptime(date_input, '%Y-%m-%d')
            if trade_date > datetime.now():
                st.error("The date cannot be in the future. Please enter a valid past or current date.")
                return False
            return True
        except ValueError:
            st.error("Please enter the date in the correct format (YYYY-MM-DD).")
            return False

    # Function to execute query and export data to CSV
    def export_data(query, filename, save_directory, log_widget):
        try:
            log_query(query)
            conn = psycopg2.connect(**DB_PARAMS)
            cur = conn.cursor()
            cur.execute(query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(data, columns=columns)

            # Ensure the save directory exists
            if not os.path.exists(save_directory):
                os.makedirs(save_directory)

            save_path = os.path.join(save_directory, filename)

            # Check if the file already exists and notify
            if os.path.exists(save_path):
                log_widget = streamlit_logger(f"  - Overwriting {filename}", log_widget)
            else:
                log_widget = streamlit_logger(f"  - Creating {filename}", log_widget)

            df.to_csv(save_path, index=False)
            logging.info(f"File saved: {save_path}")
            conn.close()

        except Exception as e:
            log_widget = streamlit_logger(f"Error: {str(e)}", log_widget)
            logging.error(f"Error saving file: {str(e)}")

        return log_widget

    # Function to generate and export all queries
    def generate_and_export_all(date_input, log_widget):
        try:
            trade_date = datetime.strptime(date_input, '%Y-%m-%d')
            trade_date_str = trade_date.strftime('%Y-%m-%d')
            folder_date_str = trade_date.strftime('%Y%m%d')

            # Define the base save directory
            save_directory = os.path.join(GFOX_BASE_DIRECTORY, folder_date_str)

            if trade_date.weekday() == 4:
                t_plus_1_date = trade_date + timedelta(days=3)
            else:
                t_plus_1_date = trade_date + timedelta(days=1)

            t_plus_1_date_str = t_plus_1_date.strftime('%Y-%m-%d')

            log_widget = streamlit_logger(f"\nExporting Files to: {save_directory}\n", log_widget)

            for query_type, query_template in queries.items():
                # Handle instrument queries that use both trade_date_str and t_plus_1_date_str
                if query_type == "LCH_EOD_instruments_ingest":
                    query = query_template.format(trade_date_str=trade_date_str, t_plus_1_date_str=t_plus_1_date_str)
                else:
                    query = query_template.format(trade_date_str=trade_date_str)

                # Define the custom filenames based on query_type
                if query_type == "LCH_EOD_instruments_ingest":
                    filename = f"LCH EOD Instruments Ingest_{trade_date_str}.csv"
                elif query_type == "LCH_EOD_prices_ingest":
                    filename = f"LCH EOD Prices Ingest_{trade_date_str}.csv"
                elif query_type == "LCH_EOD_trades_ingest_long":
                    filename = f"LCH EOD Trades Ingest Long_{trade_date_str}.csv"
                elif query_type == "LCH_EOD_trades_ingest_short":
                    filename = f"LCH EOD Trades Ingest Short_{trade_date_str}.csv"
                else:
                    filename = f"{query_type}_{trade_date_str}.csv"  # Fallback for any other queries

                log_widget = export_data(query, filename, save_directory, log_widget)

            log_widget = streamlit_logger("\nAll queries exported successfully.\n", log_widget)

        except Exception as e:
            log_widget = streamlit_logger(f"Failed to export all queries: {e}", log_widget)
            logging.error(f"Failed to export all queries: {e}")

        return log_widget

    # Create a text input for the date with validation for format and future dates
    date_input = st.text_input('Enter Today\'s Date (YYYY-MM-DD):', label_visibility='visible', max_chars=10)

    # Text area for log display
    log_widget = st.empty()
    log_text = ""

    # Prevent the user from interacting with buttons before entering a valid date
    if not date_input:
        st.warning("Please enter the date before proceeding.")
        return

    if not validate_date_input(date_input):
        return

    # Check database connection before proceeding
    if not check_db_connection():
        return

    trade_date = datetime.strptime(date_input, '%Y-%m-%d')
    folder_date_str = trade_date.strftime('%Y%m%d')
    save_directory = os.path.join(GFOX_BASE_DIRECTORY, folder_date_str)

    # Add GFOX EOD File Extraction Button
    if st.button('GFOX EOD File Extraction'):
        log_text = generate_and_export_all(date_input, log_text)
        log_widget.text_area("Logs", log_text, height=250)

    # Additional buttons for individual file extraction
    if st.button('Extract GFOX Trades Long Only'):
        log_text = streamlit_logger(f"Extracting GFOX Trades for {date_input}", log_text)
        query = queries['LCH_EOD_trades_ingest_long'].format(trade_date_str=date_input)
        log_text = export_data(query, f"LCH EOD Trades Ingest Long_{date_input}.csv", save_directory, log_text)
        log_widget.text_area("Logs", log_text, height=250)

    if st.button('Extract GFOX Trades Short Only'):
        log_text = streamlit_logger(f"Extracting GFOX Trades for {date_input}", log_text)
        query = queries['LCH_EOD_trades_ingest_short'].format(trade_date_str=date_input)
        log_text = export_data(query, f"LCH EOD Trades Ingest Short_{date_input}.csv", save_directory, log_text)
        log_widget.text_area("Logs", log_text, height=250)

    if st.button('Extract GFOX Prices Only'):
        log_text = streamlit_logger(f"Extracting GFOX Prices for {date_input}", log_text)
        query = queries['LCH_EOD_prices_ingest'].format(trade_date_str=date_input)
        log_text = export_data(query, f"LCH EOD Prices Ingest_{date_input}.csv", save_directory, log_text)
        log_widget.text_area("Logs", log_text, height=250)

    # Fix: Add t_plus_1_date_str calculation here for instruments
    if st.button('Extract GFOX Instruments Only'):
        log_text = streamlit_logger(f"Extracting GFOX Instruments for {date_input}", log_text)

        if trade_date.weekday() == 4:
            t_plus_1_date = trade_date + timedelta(days=3)
        else:
            t_plus_1_date = trade_date + timedelta(days=1)
        t_plus_1_date_str = t_plus_1_date.strftime('%Y-%m-%d')

        query = queries['LCH_EOD_instruments_ingest'].format(trade_date_str=date_input, t_plus_1_date_str=t_plus_1_date_str)
        log_text = export_data(query, f"LCH EOD Instruments Ingest_{date_input}.csv", save_directory, log_text)
        log_widget.text_area("Logs", log_text, height=250)

# -------------------------- App 2: LCH EOD File Extraction --------------------------

def lch_eod_file_extraction():
    st.title("LCH EOD File Extraction")

    # Function to check URL connection and provide feedback
    def check_url_connection(url):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                container = st.empty()
                container.success("Successfully connected to the URL!", icon="✅")  # Green success dialog box
                time.sleep(0.5)
                container.empty()
                st.session_state.url_connection_status = True
            else:
                st.error(f"Failed to connect to the URL. Status code: {response.status_code}", icon="⛔")  # Red error dialog box
                st.session_state.url_connection_status = False
        except Exception as e:
            st.error(f"Error connecting to the URL: {str(e)}")  # Red error dialog box for exceptions
            st.session_state.url_connection_status = False

    # Function to validate the input date format
    def validate_date_input(date_input):
        try:
            trade_date = datetime.strptime(date_input, '%Y-%m-%d')
            if trade_date > datetime.now():
                st.error("The date cannot be in the future. Please enter a valid past or current date.")
                return False
            return True
        except ValueError:
            st.error("Please enter the date in the correct format (YYYY-MM-DD).")
            return False

    # Initialize session state to avoid rerunning buttons
    if 'url_connection_status' not in st.session_state:
        st.session_state.url_connection_status = False
    if 'buttons_rendered' not in st.session_state:
        st.session_state.buttons_rendered = False
    
    # Limit input to max 10 characters, which is the length of YYYY-MM-DD
    date_input = st.text_input('Enter the Trade Date (YYYY-MM-DD):', label_visibility='visible', max_chars=10)

    # Base URL for LCH EOD files
    url = LCH_OUTBOUND_URL  # Use the global variable for the LCH URL

    if date_input:
        if not validate_date_input(date_input):
            return

        # Add an additional button to check URL connection
        if st.button('Check URL Connection') and not st.session_state.url_connection_status:
            check_url_connection(url)
            if st.session_state.url_connection_status:
                st.rerun()  # Rerun the app to refresh the state

    # Display a message based on URL connection status
    if st.session_state.url_connection_status:
        st.write("Connection established, ready to process files.")
        if not st.session_state.buttons_rendered:
            st.session_state.buttons_rendered = True
    else:
        st.warning("Connection not established yet. Please check the URL.")

    # Render buttons only after connection is established and only once
    if st.session_state.url_connection_status and st.session_state.buttons_rendered:
        try:
            # Parse the user input date
            date_obj = datetime.strptime(date_input, '%Y-%m-%d')
            date_str = date_obj.strftime('%Y%m%d')  # Convert to yyyymmdd format

            # Define the base directory
            base_directory = LCH_BASE_DIRECTORY
            download_directory = os.path.join(base_directory, date_str)

            # Check if the directory exists for the entered date, if not, create it
            if not os.path.exists(download_directory):
                os.makedirs(download_directory)
                st.info(f"Directory created: {download_directory}")
            else:
                st.info(f"Directory already exists: {download_directory}")

            # Define the regex patterns based on user input date
            regex_patterns = {
                "TRADES": rf"{date_str}_RECO_GFOX_LCHC_EOD_PRD_\d{{14}}.dat",
                "PRICE": rf"PRICE_GFOX_PRD_{date_str}\.csv",
                "INSTRUMENTS": rf"INSTRUMENT_GFOX_PRD_{date_str}\.csv"
            }

        except ValueError:
            st.error("Please enter a valid date in the format YYYY-MM-DD.")
            return

        # Function to log messages with clear sections
        def log_message(message, log_text):
            log_text += f"{message}\n"
            return log_text

        # Function to update the log widget
        def update_log_widget(log_text, log_widget):
            log_widget.text_area("Logs", log_text, height=600)

        # Function to process individual files and handle logs
        def process_individual_files(file_type, date_input, url, download_directory, log_text, log_widget):
            log_text = log_message(f"Starting {file_type} extraction for {date_input}...", log_text)
            files_info, log_text = list_files_in_directory(url, log_text)

            if not files_info:
                log_text = log_message(f"No {file_type.lower()} files found for the date {date_input}.", log_text)
            else:
                # Filter files based on the type
                filtered_files = [f for f in files_info if f['type'] == file_type.upper()]
                if filtered_files:
                    original_files, parsed_files, all_files, lch_eod_files, log_text = process_files(
                        filtered_files, url, download_directory, log_text
                    )
                    # Log the processed files
                    log_text = log_message(f"\n---\nProcessed {file_type} Files:", log_text)
                    for file in lch_eod_files:
                        log_text = log_message(f"  - {file}", log_text)
                else:
                    log_text = log_message(f"No {file_type.lower()} files available for {date_input}.", log_text)

            # Update the log widget with the updated text
            update_log_widget(log_text, log_widget)

        # Text area for log display
        log_widget = st.empty()
        log_text = ""

        # Function to list files from the directory
        def list_files_in_directory(url, log_widget):
            log_widget = streamlit_logger(f"Accessing URL: {url}\n", log_widget)
            response = requests.get(url)

            if response.status_code != 200:
                log_widget = streamlit_logger(f"Failed to access {url}. Status code: {response.status_code}", log_widget)
                return {"error": f"Failed to access {url}. Status code: {response.status_code}"}, log_widget

            soup = BeautifulSoup(response.text, 'html.parser')
            files_info = []

            for link in soup.find_all('a'):
                href = link.get('href')
                if href and not href.startswith('/'):
                    file_name = os.path.basename(href)

                    for file_type, pattern in regex_patterns.items():
                        if re.match(pattern, file_name):
                            files_info.append({
                                "name": file_name,
                                "type": file_type
                            })
                            break

            if not files_info:
                log_widget = streamlit_logger("No files found for the specified date.\n", log_widget)
                return None, log_widget

            return files_info, log_widget

        # Trade-specific parsing logic
        def parse_fix_message(message):
            parts = message.split('')
            data = OrderedDict()  # Use OrderedDict to maintain the order of tags

            # Track repeating groups
            repeating_group_count = 0
            repeating_group_data = []

            for part in parts:
                if '=' in part:
                    tag, value = part.split('=')
                    tag = int(tag)
                    if tag == 552:
                        repeating_group_count = int(value)
                        data[tag] = value
                    elif tag in [54, 453, 448, 447, 452, 581, 576, 577, 77, 58, 37, 11]:
                        if repeating_group_count > 0:
                            repeating_group_data.append((tag, value))
                    else:
                        data[tag] = value

            # Handle the repeating group
            for i in range(repeating_group_count):
                offset = i * 12  # Number of tags in each repeating group
                for j, tag in enumerate([54, 453, 448, 447, 452, 581, 576, 577, 77, 58, 37, 11]):
                    data[f'552_{i+1}_{tag}'] = repeating_group_data[offset + j][1]

            return data

        # Function to process trade file
        def process_trade_file(input_file, output_file_long, output_file_short, log_widget):
            with open(input_file, 'r') as file:
                lines = file.readlines()

            records = []
            column_order = OrderedDict()  # Store column order

            for line in lines:
                if line.startswith("8="):
                    data = parse_fix_message(line.strip())

                    # Insert tags 201 and 202 after tag 552 if they are missing
                    if 552 in data:
                        if 201 not in data:
                            data[201] = ''  # Insert empty tag 201
                        if 202 not in data:
                            data[202] = ''  # Insert empty tag 202

                    records.append(data)

                    # Update column order based on this message
                    for key in data.keys():
                        if key not in column_order:
                            column_order[key] = None

            # Convert the list of dictionaries to a DataFrame
            df = pd.DataFrame(records)

            # Ensure the columns are ordered correctly
            ordered_columns = []
            repeating_columns = []
            end_columns = [570, 571, 828, 10]

            for key in column_order.keys():
                if any(str(key).startswith(f'552_{i+1}_') for i in range(2)):
                    repeating_columns.append(key)
                elif key in end_columns:
                    continue  # Skip end columns for now
                else:
                    ordered_columns.append(key)

            # Insert the repeating columns and then the end columns
            ordered_columns.extend(repeating_columns)
            ordered_columns.extend(end_columns)

            # Reorder the DataFrame
            df = df[ordered_columns]

            # Save the DataFrame to both LONG and SHORT CSV files
            df.to_csv(output_file_long, index=False)
            df.to_csv(output_file_short, index=False)

            log_widget = streamlit_logger(f"Processed Trade File: {os.path.basename(input_file)}", log_widget)
            log_widget = streamlit_logger(f"  - Created: {os.path.basename(output_file_long)}", log_widget)
            log_widget = streamlit_logger(f"  - Created: {os.path.basename(output_file_short)}", log_widget)

            return log_widget

        # Price-specific parsing logic
        def parse_fix_messages(file_path):
            parsed_messages = []

            with open(file_path, 'r') as file:
                for line in file:
                    message = line.strip().split('')
                    message_dict = OrderedDict()
                    for field in message:
                        if '=' in field:
                            tag, value = field.split('=', 1)
                            message_dict[tag] = value
                    parsed_messages.append(message_dict)

            return parsed_messages

        # Function to save parsed messages to CSV
        def save_to_csv(parsed_messages, file_path):
            if not parsed_messages:
                raise ValueError("No messages to save.")

            fieldnames = []
            for message in parsed_messages:
                for key in message.keys():
                    if key not in fieldnames:
                        fieldnames.append(key)

            # Ensure mandatory tags are present
            for tag in ['201', '202', "527"]:
                if tag not in fieldnames:
                    fieldnames.append(tag)

            with open(file_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for message in parsed_messages:
                    for tag in ['201', '202', "527"]:
                        if tag not in message:
                            message[tag] = ''
                    writer.writerow(message)

        # Function to parse price file
        def parse_price_file(file_path, output_file, log_widget):
            try:
                log_widget = streamlit_logger(f"Processing Price File: {os.path.basename(file_path)}", log_widget)
                parsed_messages = parse_fix_messages(file_path)
                save_to_csv(parsed_messages, output_file)
                log_widget = streamlit_logger(f"  - Created: {os.path.basename(output_file)}", log_widget)

            except Exception as e:
                log_widget = streamlit_logger(f"Error processing price file: {str(e)}", log_widget)

            return log_widget

        # Function to process files (Keep original functionality)
        def process_files(files_info, base_url, download_directory, log_widget):
            original_files = []
            parsed_files = []
            all_files = []
            lch_eod_files = []  # Track the LCH EOD files

            log_widget = log_message(f"Processing files in: {download_directory}\n", log_widget)

            if not os.path.exists(download_directory):
                os.makedirs(download_directory)

            for file in files_info:
                file_url = urljoin(base_url, file['name'])
                local_file_path = os.path.join(download_directory, file['name'])

                # Download the file
                with requests.get(file_url, stream=True) as r:
                    with open(local_file_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

                original_files.append(file['name'])
                all_files.append(local_file_path)

                if file['type'] == 'TRADES':
                    output_file_long = os.path.join(download_directory, file['name'].replace(".dat", "_LONG_DUCO.csv"))
                    output_file_short = os.path.join(download_directory, file['name'].replace(".dat", "_SHORT_DUCO.csv"))
                    log_widget = process_trade_file(local_file_path, output_file_long, output_file_short, log_widget)
                    parsed_files.append(os.path.basename(output_file_long))
                    parsed_files.append(os.path.basename(output_file_short))
                    all_files.append(output_file_long)
                    all_files.append(output_file_short)
                    lch_eod_files.append(os.path.basename(output_file_long))
                    lch_eod_files.append(os.path.basename(output_file_short))

                elif file['type'] == 'PRICE':
                    output_file = os.path.join(download_directory, file['name'].replace(".csv", "_DUCO.csv"))
                    log_widget = parse_price_file(local_file_path, output_file, log_widget)
                    parsed_files.append(os.path.basename(output_file))
                    all_files.append(output_file)
                    lch_eod_files.append(os.path.basename(output_file))

            log_widget = log_message("All files processed and saved successfully.\n", log_widget)
            return original_files, parsed_files, all_files, lch_eod_files, log_widget

        # Button to extract and process all LCH EOD Files
        st.header("LCH EOD File Extraction")
        if st.button('Extract and Process All LCH EOD Files'):
            try:
                if not date_input:
                    st.error("Please enter a valid date before starting the extraction.")
                    return

                # Access and process the files
                log_text = log_message("Starting LCH EOD File Extraction...", log_text)
                files_info, log_text = list_files_in_directory(url, log_text)

                if files_info is None:
                    log_text = log_message(f"No files found for the date {date_input}.", log_text)
                else:
                    original_files, parsed_files, all_files, lch_eod_files, log_text = process_files(
                        files_info, url, download_directory, log_text
                    )

                    # Clean up logs for better readability
                    log_text = log_message("\n---\nOriginal Files:", log_text)
                    for file in original_files:
                        log_text = log_message(f"  - {file}", log_text)

                    log_text = log_message("\nParsed Files:", log_text)
                    for file in parsed_files:
                        log_text = log_message(f"  - {file}", log_text)

                    log_text = log_message("\nDownloaded Files:", log_text)
                    for file in all_files:
                        log_text = log_message(f"  - {file}", log_text)

                    log_text = log_message("\nLCH EOD Files:", log_text)
                    for file in lch_eod_files:
                        log_text = log_message(f"  - {file}", log_text)

                # Update log display with larger text area
                log_widget.text_area("Logs", log_text, height=600)

            except Exception as e:
                st.error(f"Error occurred: {str(e)}")
                log_text = log_message(f"Error occurred: {str(e)}", log_text)
                log_widget.text_area("Logs", log_text, height=600)

        # Group individual file extraction buttons under a subheader
        st.subheader("Individual LCH File Extractions")
        
        # Process trades only
        if st.button('Extract LCH Trades Only'):
            process_individual_files("TRADES", date_input, url, download_directory, log_text, log_widget)

        # Process prices only
        if st.button('Extract LCH Prices Only'):
            process_individual_files("PRICE", date_input, url, download_directory, log_text, log_widget)

        # Process instruments only
        if st.button('Extract LCH Instruments Only'):
            process_individual_files("INSTRUMENTS", date_input, url, download_directory, log_text, log_widget)

# -------------------------- App 3: File Submission to DUCO --------------------------

def file_submission_to_duco():
    st.title("File Submission to DUCO")

    def validate_date_input(date_input):
        if len(date_input) != 10:
            st.error("Date must be exactly 10 characters long (YYYY-MM-DD).")
            return False

        date_regex = r'^\d{4}-\d{2}-\d{2}$'
        if not re.match(date_regex, date_input):
            st.error("Please enter the date in the correct format (YYYY-MM-DD).")
            return False

        try:
            trade_date = datetime.strptime(date_input, '%Y-%m-%d')
            if trade_date > datetime.now():
                st.error("The date cannot be in the future. Please enter a valid past or current date.")
                return False
            return True
        except ValueError:
            st.error("Invalid date. Please enter a real date in the correct format (YYYY-MM-DD).")
            return False

    date_input = st.text_input('Enter the File Date (YYYY-MM-DD):', max_chars=10)

    if date_input:
        if validate_date_input(date_input):
            try:
                date_obj = datetime.strptime(date_input, '%Y-%m-%d')
                date_str = date_obj.strftime('%Y%m%d')

                lch_directory = os.path.join(LCH_BASE_DIRECTORY, date_str)
                gfox_directory = os.path.join(GFOX_BASE_DIRECTORY, date_str)

            except ValueError:
                st.error("Please enter a valid date in the format YYYY-MM-DD.")
                return
    else:
        st.error("Please enter a date to proceed.")
        return

    def find_file(directory, pattern, status_container):
        status_container.update(label=f"Searching for files in {directory}...", state="running")
        for filename in os.listdir(directory):
            if re.match(pattern, filename):
                status_container.update(label=f"File found: {filename}", state="complete")
                return os.path.join(directory, filename)
        status_container.update(label="No file found", state="error")
        return None

    def submit_file(file_path, status_container):
        try:
            status_container.update(label=f"Submitting {os.path.basename(file_path)} to DUCO...", state="running")
            with open(file_path, "rb") as file:
                files = {"file": file}
                response = requests.post(DUCO_SUBMISSION_URL, headers=DUCO_API_HEADERS, files=files)

            if response and response.status_code == 200:
                response_data = response.json()

                submission_id = response_data.get('id', 'N/A')
                name = response_data.get('name', 'N/A')
                submission_time = response_data.get('submission_time', 'N/A')
                upload_method = response_data.get('upload_method', 'N/A')
                md5sum = response_data.get('md5sum', 'N/A')
                size = response_data.get('size', 'N/A')
                runs_triggered = response_data.get('runs_triggered', [])
                processes_awaiting_input = response_data.get('processes_awaiting_input', [])

                detailed_info = (
                    f"**File submitted successfully!**\n\n"
                    f"**ID**: {submission_id}\n\n"
                    f"**Name**: {name}\n\n"
                    f"**Submission Time**: {submission_time}\n\n"
                    f"**Upload Method**: {upload_method}\n\n"
                    f"**MD5 Sum**: {md5sum}\n\n"
                    f"**Size**: {size}\n\n"
                    f"**Runs Triggered**: {len(runs_triggered)}\n\n"
                )

                if runs_triggered:
                    detailed_info += "\n**Runs Triggered**:\n\n"
                    for run in runs_triggered:
                        detailed_info += (
                            f"- **Run Number**: {run.get('run_number', 'N/A')}\n\n"
                            f"  **Code**: {run.get('code', 'N/A')}\n\n"
                            f"  **Input Name**: {run.get('input_name', 'N/A')}\n\n"
                        )

                if processes_awaiting_input:
                    detailed_info += "\n**Processes Awaiting Input**:\n\n"
                    for process in processes_awaiting_input:
                        awaiting = process.get('awaiting', {})
                        detailed_info += (
                            f"- **Input Name**: {process.get('input_name', 'N/A')}\n\n"
                            f"- **File Name Pattern**: {awaiting.get('file_name_pattern', 'N/A')}\n\n"
                        )

                status_container.update(label=detailed_info, state="complete")

            else:
                status_container.update(label=f"Failed to submit file. Status code: {response.status_code}", state="error")
            return response
        except Exception as e:
            status_container.update(label=f"Error submitting file: {str(e)}", state="error")
            return None

    def process_file_submission(file_type, source, directory, pattern):
        with st.status(label=f"Processing {file_type} {source} files...", state="running") as status_container:
            file_path = find_file(directory, pattern, status_container)
            if file_path:
                submit_file(file_path, status_container)

    if st.button('Submit Files to DUCO'):
        for file_type, patterns in FILE_PATTERNS.items():
            process_file_submission(file_type, "GFOX", gfox_directory, patterns["GFOX"])
            process_file_submission(file_type, "LCH", lch_directory, patterns["LCH"])

    if st.button('Submit TRADES_LONG to DUCO'):
        process_file_submission('TRADES_LONG', "GFOX", gfox_directory, FILE_PATTERNS['TRADES_LONG']['GFOX'])
        process_file_submission('TRADES_LONG', "LCH", lch_directory, FILE_PATTERNS['TRADES_LONG']['LCH'])

    if st.button('Submit TRADES_SHORT to DUCO'):
        process_file_submission('TRADES_SHORT', "GFOX", gfox_directory, FILE_PATTERNS['TRADES_SHORT']['GFOX'])
        process_file_submission('TRADES_SHORT', "LCH", lch_directory, FILE_PATTERNS['TRADES_SHORT']['LCH'])

    if st.button('Submit PRICES to DUCO'):
        process_file_submission('PRICES', "GFOX", gfox_directory, FILE_PATTERNS['PRICES']['GFOX'])
        process_file_submission('PRICES', "LCH", lch_directory, FILE_PATTERNS['PRICES']['LCH'])

    if st.button('Submit INSTRUMENTS to DUCO'):
        process_file_submission('INSTRUMENTS', "GFOX", gfox_directory, FILE_PATTERNS['INSTRUMENTS']['GFOX'])
        process_file_submission('INSTRUMENTS', "LCH", lch_directory, FILE_PATTERNS['INSTRUMENTS']['LCH'])

# -------------------------- Main Application --------------------------

def main():
    st.sidebar.title("LCH EOD DUCO Reconciliation")

    st.sidebar.markdown(f"[Go to DUCO Processes]({DUCO_PROCESSES_URL})")

    app_choice = st.sidebar.radio("Choose a process to run:", ('GFOX EOD File Extraction', 'LCH EOD File Extraction', 'File Submission to DUCO'))

    if app_choice == 'GFOX EOD File Extraction':
        gfox_eod_file_extraction()
    elif app_choice == 'LCH EOD File Extraction':
        lch_eod_file_extraction()
    elif app_choice == 'File Submission to DUCO':
        file_submission_to_duco()

if __name__ == "__main__":
    main()
