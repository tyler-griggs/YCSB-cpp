import csv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

# Path to your final output CSV file
csv_file_path = 'output.csv'

# Function to convert micros since epoch to datetime object
def micros_since_epoch_to_datetime(micros):
    return datetime(1970, 1, 1) + timedelta(microseconds=int(micros))

# Initialize dictionaries to hold data for each client and compaction_finished events
client_data = {}
compaction_events = []
stall_events = []

# Read the CSV file
with open(csv_file_path, 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        # Process client data
        if row[0].startswith('client'):
            client_id = row[0]
            start_time_micros = micros_since_epoch_to_datetime(row[1])
            operation_counts = [int(count) for count in row[2:]]
            print(operation_counts)
            for i in range(len(operation_counts)-1, 0, -1):
              operation_counts[i] -= operation_counts[i-1] 
            for i in range(len(operation_counts)):
              operation_counts[i] = operation_counts[i] / 2 * 16 / 1024 # MB/s
              # IF read AND update --> get rid of reads
              # operation_counts[i] = operation_counts[i] / 2 * 16 / 1024 /2 # MB/s
            print(operation_counts)
            print()
            client_data[client_id] = {
                'start_time': start_time_micros,
                'operation_counts': operation_counts
            }
        # Process compaction_finished events
        elif row[0] == 'compaction_finished':
          end_time_micros = int(row[1])
          compaction_time_micros = int(row[2])
          start_time_micros = end_time_micros - compaction_time_micros
          total_output_size = int(row[3])
          # Convert times to datetime for plotting
          start_time = micros_since_epoch_to_datetime(start_time_micros)
          end_time = micros_since_epoch_to_datetime(end_time_micros)
          # Calculate y-value
          duration_seconds = (end_time_micros - start_time_micros) / 1e6
          y_value = total_output_size / (1024 * 1024 * duration_seconds)
          compaction_events.append((start_time, end_time, y_value))

        elif row[0] == 'stall_started':
          stall_time_micros = int(row[1])
          rate_bytes_per_sec = int(row[2])  # Assuming 'rate' is in bytes per second
          # Convert timestamp to datetime for plotting
          stall_time = micros_since_epoch_to_datetime(stall_time_micros)
          # Calculate y-value in MB/s
          y_value = rate_bytes_per_sec / (1024 * 1024)
          stall_events.append((stall_time, y_value))

# Plotting
fig, ax = plt.subplots()

# Plot client data
for client_id, data in client_data.items():
    operation_counts = data['operation_counts']
    time_offsets = [2 * i for i in range(len(operation_counts))]
    times = [mdates.date2num(data['start_time'] + timedelta(seconds=offset)) for offset in time_offsets]
    ax.plot_date(times, operation_counts, '-', label=client_id)

for start_time, end_time, y_value in compaction_events:
    ax.hlines(y=y_value, xmin=mdates.date2num(start_time), xmax=mdates.date2num(end_time), colors='r', linestyles='solid', label='Compaction' if compaction_events.index((start_time, end_time, y_value)) == 0 else "")

for stall_time, y_value in stall_events:
    ax.scatter(mdates.date2num(stall_time), y_value, color='g', label='Stall Started' if stall_events.index((stall_time, y_value)) == 0 else "")
# Format the x-axis to display time correctly
ax.xaxis.set_major_locator(mdates.AutoDateLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
plt.xticks(rotation=45)
plt.xlabel('Time (H:M:S)')
plt.ylabel('MB/s')
plt.title('Read-only Workload: 1 client')
plt.legend()
plt.tight_layout()
plt.show()
