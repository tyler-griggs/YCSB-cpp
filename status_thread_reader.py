import re
import csv

# Define the file paths
input_file_path = 'status_thread.txt'
output_file_path = 'client_stats.csv'

# Initialize a dictionary to hold the data
client_data = {}

# Compile the regular expressions for matching lines
client_regex = re.compile(r'client(\d+) stats:')
data_regex = re.compile(r'(\d+) operations; \[(READ|UPDATE): Count=(\d+) Max=\d+\.\d+ Min=\d+\.\d+ Avg=(\d+\.\d+) 90=\d+\.\d+ 99=(\d+\.\d+) 99.9=\d+\.\d+ 99.99=\d+\.\d+\]')
# Open and read the input file
with open(input_file_path, 'r') as file:
    lines = file.readlines()

    for i, line in enumerate(lines):
        client_match = client_regex.match(line)
        if client_match:
            client_id = client_match.group(1)
            # Check if the next line contains the data
            if i + 1 < len(lines):
                data_match = data_regex.match(lines[i + 1])
                if data_match:
                    # Extract the relevant data
                    _, count, avg, p99 = data_match.group(2, 3, 4, 5)
                    # Check if the client ID already exists in the dictionary
                    if client_id not in client_data:
                        client_data[client_id] = {'Count': [], 'Avg': [], '99': []}
                    # Append the data to the lists in the dictionary
                    client_data[client_id]['Count'].append(count)
                    client_data[client_id]['Avg'].append(avg)
                    client_data[client_id]['99'].append(p99)
print(client_data)
# Writing to CSV can be challenging due to varying lengths of lists
# Here, we handle it by finding the longest list to determine the row count
max_length = max(len(data['Count']) for data in client_data.values())

with open(output_file_path, 'w', newline='') as csvfile:
    fieldnames = ['ClientID', 'TimeIndex', 'Count', 'Avg', '99']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for client_id, stats in client_data.items():
        for i in range(max_length):
            row = {
                'ClientID': 'client' + client_id,
                'TimeIndex': i + 1,
                'Count': stats['Count'][i] if i < len(stats['Count']) else '',
                'Avg': stats['Avg'][i] if i < len(stats['Avg']) else '',
                '99': stats['99'][i] if i < len(stats['99']) else ''
            }
            writer.writerow(row)

print("Data extraction and writing to CSV complete with timelines.")