import csv
import json
import re
from datetime import datetime

# Define the paths to your log files
log_file_path = '/mnt/tgriggs-disk/ycsb-rocksdb-data/LOG'

# Output CSV file path
output_csv_path = 'output.csv'

# Regular expression for matching the "Stalling writes" line format
stall_pattern = re.compile(r'(\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}.\d{6}) \d+ \[WARN\] \[/column_family.cc:\d+\] \[default\] Stalling writes because we have \d+ level-0 files rate (\d+)')
stall_pattern2 = re.compile(r"(\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6}).*rate (\d+)")

def timestamp_to_micros(timestamp_str):
    # Define the format of the timestamp in the log
    timestamp_format = '%Y/%m/%d-%H:%M:%S.%f'
    # Convert string to datetime object
    dt = datetime.strptime(timestamp_str, timestamp_format)
    # Convert datetime object to microseconds since the Unix epoch
    epoch = datetime(1970, 1, 1)
    micros_since_epoch = int((dt - epoch).total_seconds() * 1000000)
    return micros_since_epoch

with open(output_csv_path, 'w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    
    with open(log_file_path, 'r') as log_file_1:
        for line in log_file_1:
            if 'EVENT_LOG_v1' in line:
                try:
                    json_str = line.split('EVENT_LOG_v1 ')[1]
                    data = json.loads(json_str)
                    event = data.get('event')
                    if event == 'compaction_finished':
                        csv_writer.writerow(['compaction_finished', data.get('time_micros'), data.get('compaction_time_micros'), data.get('total_output_size')])
                    elif event == 'flush_started':
                        csv_writer.writerow(['flush_started', data.get('total_data_size'), data.get('time_micros')])
                except Exception as e:
                    print(f"Error processing line for events: {line}. Error: {e}")
    
    with open(log_file_path, 'r') as log_file_2:
        for line in log_file_2:
            match = stall_pattern.search(line)
            if match:
                timestamp_str, rate = match.groups()
                timestamp_micros = timestamp_to_micros(timestamp_str)
                csv_writer.writerow(['stall_started', timestamp_micros, rate])

            match = stall_pattern2.search(line)
            if match:
                timestamp_str, rate = match.groups()
                timestamp_micros = timestamp_to_micros(timestamp_str)
                csv_writer.writerow(['stall_started', timestamp_micros, rate])
print("Finished processing the log files and writing to the CSV file.")

def add_client_lines(input_csv, output_csv):
    client_counter = 1  # Start with client1

    with open(input_csv, 'r') as csvfile:
        reader = csv.reader(csvfile)
        
        with open(output_csv, 'a', newline='') as outputfile:  # Append mode
            writer = csv.writer(outputfile)

            for row in reader:
                client_id = f'client{client_counter}'  # Create client ID
                writer.writerow([client_id] + row)  # Prepend client ID to row data
                client_counter += 1  # Increment for next client ID

# Call the function with your file paths
add_client_lines("client_progress.csv", output_csv_path)

print("Finished adding client lines to the output CSV file.")