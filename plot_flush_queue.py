import pandas as pd
import matplotlib.pyplot as plt
import re
from collections import defaultdict
import numpy as np

def parse_flush_queue_log(log_line):
    parts = log_line.strip().split(',', 4)
    if len(parts) < 5 or parts[0] != 'flush_queue':
        return None

    try:
        timestamp_ms = int(parts[2])
    except ValueError:
        # Handle cases where timestamp might not be an integer (e.g., header)
        return None

    queue_details_str = parts[4]

    # Extract requests from the queue_details string
    requests_str = queue_details_str[1:-1] # Remove outer brackets
    if not requests_str:
        return timestamp_ms, {}

    reason_counts = defaultdict(int)
    total_cfds = 0

    # Regex to find Reason and CFDs for each request
    # Handles cases like {Reason:X, CFDs:[Y]} and {Reason:X, CFDs:[]}
    # Updated regex to be more robust against variations like extra spaces
    request_pattern = re.compile(r"\{Reason:\s*([^,]+),\s*CFDs:\s*\[([^\]]*)\]\}")

    for match in request_pattern.finditer(requests_str):
        reason = match.group(1).strip()
        cfds_str = match.group(2).strip()

        if not cfds_str: # Handle empty CFDs list
            cfd_count_in_req = 0
        else:
            # Count CFD pairs by splitting on '|'
            cfd_pairs = cfds_str.split('|')
            # Filter out potentially empty strings if the format is like '[id:val|]'
            cfd_count_in_req = len([pair for pair in cfd_pairs if pair])

        reason_counts[reason] += cfd_count_in_req
        total_cfds += cfd_count_in_req

    return timestamp_ms, reason_counts

def plot_flush_queue_stats(output_file, axs, start_time_s, xlim, fig_loc):
    log_file = 'logs/WAL_logs.csv'
    data = []
    all_reasons = set()

    try:
        with open(log_file, 'r') as f:
            # Skip header if it exists
            first_line = f.readline()
            if not parse_flush_queue_log(first_line):
                # Assuming the first line was a header or non-data, continue from next
                pass
            else:
                # If the first line was data, process it and reset file pointer
                f.seek(0)

            for line in f:
                parsed = parse_flush_queue_log(line)
                if parsed:
                    timestamp_ms, reason_counts = parsed
                    total_cfds = sum(reason_counts.values())
                    # Store data per reason for stacked plot
                    if reason_counts:
                        for reason, count in reason_counts.items():
                            data.append({'timestamp_s': timestamp_ms / 1000.0, 'reason': reason, 'count': count})
                            all_reasons.add(reason)
                    else: # Log an entry even if the queue is empty at this timestamp
                         data.append({'timestamp_s': timestamp_ms / 1000.0, 'reason': 'Empty', 'count': 0})
                         all_reasons.add('Empty')

    except FileNotFoundError:
        print(f"Warning: {log_file} not found. Skipping flush queue plot.")
        # Add a dummy plot to avoid errors if axs expects something
        ax = axs[fig_loc[0]]
        ax.text(0.5, 0.5, 'WAL_logs.csv not found', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)
        ax.set_title('Flush Queue State (Error)')
        return
    except Exception as e:
        print(f"Error processing {log_file}: {e}. Skipping flush queue plot.")
        # Add a dummy plot
        ax = axs[fig_loc[0]]
        ax.text(0.5, 0.5, f'Error: {e}', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)
        ax.set_title('Flush Queue State (Error)')
        return


    if not data:
        print("Warning: No 'flush_queue' data found in log. Skipping plot.")
         # Add a dummy plot
        ax = axs[fig_loc[0]]
        ax.text(0.5, 0.5, 'No flush_queue data found', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)
        ax.set_title('Flush Queue State (No Data)')
        return

    df = pd.DataFrame(data)
    df['time_rel_s'] = df['timestamp_s'] - start_time_s

    # Ensure all reasons are present in the pivot table, fill missing with 0
    pivot_df = df.pivot_table(index='time_rel_s', columns='reason', values='count', fill_value=0)
    # Add missing reason columns filled with 0
    for reason in all_reasons:
         if reason not in pivot_df.columns:
             pivot_df[reason] = 0

    # Ensure chronological order for plotting
    pivot_df = pivot_df.sort_index()

    # Calculate total CFDs at each unique timestamp *after* pivoting to include all timestamps
    total_cfds_series = pivot_df.sum(axis=1)

    # Remove 'Empty' if it exists and only contains zeros (avoids plotting it)
    if 'Empty' in pivot_df.columns and pivot_df['Empty'].sum() == 0:
        pivot_df = pivot_df.drop(columns=['Empty'])

    reasons_to_plot = pivot_df.columns
    values_to_plot = pivot_df.values.T # Transpose for stackplot: rows are reasons, columns are time

    # --- Plotting ---
    ax = axs[fig_loc[0]] # Use the specified axes

    # Stacked area plot for reasons
    if not pivot_df.empty and len(reasons_to_plot) > 0 :
        colors = plt.cm.viridis(np.linspace(0, 1, len(reasons_to_plot)))
        ax.stackplot(pivot_df.index, values_to_plot, labels=reasons_to_plot, alpha=0.7, colors=colors)

    # Line plot for total CFDs
    ax.plot(total_cfds_series.index, total_cfds_series.values, label='Total CFDs in Queue', color='black', linewidth=1.5, linestyle='--')

    ax.set_title('Flush Queue State Over Time')
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Number of CFDs')
    ax.legend(loc='upper left', fontsize='small')
    ax.grid(True)
    ax.set_xlim(xlim)
    ax.set_ylim(bottom=0) # Ensure y-axis starts at 0

    # Save data to output file
    with open(output_file, 'a') as outfile:
        outfile.write(f"metric-flush_queue_total\\n")
        outfile.write(f"time_points:{total_cfds_series.index.tolist()}\\n")
        outfile.write(f"data_points:{total_cfds_series.values.tolist()}\\n")
        if not pivot_df.empty:
             for reason in reasons_to_plot:
                 outfile.write(f"metric-flush_queue_reason-{reason}\\n")
                 outfile.write(f"time_points:{pivot_df.index.tolist()}\\n")
                 outfile.write(f"data_points:{pivot_df[reason].tolist()}\\n") 