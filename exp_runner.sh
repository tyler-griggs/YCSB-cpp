#!/bin/bash

# Cleanup function to kill background processes and process iostat output
cleanup() {
    kill $iostat_pid 2>/dev/null
    kill $ycsb_pid 2>/dev/null
    process_iostat_output
    # rm iostat_output.txt  # Clean up temporary file
}

# Function to process iostat's output and write to CSV
process_iostat_output() {
    echo "Timestamp,rMB/s,wMB/s" > iostat_results.csv
    current_timestamp=""
    while IFS= read -r line; do
        if [[ $line == *"Time:"* ]]; then
            # Extract timestamp
            current_timestamp=$(echo $line | awk '{print $2, $3}')
        elif [[ $line == *nvme0n1* ]]; then
            # Write the current timestamp along with the iostat metrics
            echo "$line" | awk -v ts="$current_timestamp" '{print ts,",",$3,",",$9}' >> iostat_results.csv
        fi
    done < iostat_output.txt
}

# Setup trap for cleanup on script exit
trap cleanup EXIT

# Start iostat in the background, appending a timestamp to each interval, and redirecting output to a file
(echo "Time: $(date +'%Y-%m-%d %H:%M:%S.%3N')"; iostat -xdm /dev/nvme0n1 1;) > iostat_output.txt &
iostat_pid=$!






# Start ycsb process in the background
./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p rocksdb.dbname=/mnt/tgriggs-disk/ycsb-rocksdb-data -s -p operationcount=30000 -p recordcount=1562500 -p updateproportion=1 -p insertproportion=0 -p readproportion=0 -threads 1 -target_rates 1000 -p zipfian_const=0.99 -p requestdistribution=zipfian | tee status_thread.txt &



ycsb_pid=$!

# Wait for the ycsb process to finish
wait $ycsb_pid

# The script exits here, triggering the cleanup function