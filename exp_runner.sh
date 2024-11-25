#!/bin/bash

# Cleanup function to kill background processes and process iostat output
cleanup() {
    kill $iostat_pid 2>/dev/null
    kill $mpstat_pid 2>/dev/null
    kill $ycsb_pid 2>/dev/null
    process_iostat_output
    process_mpstat_output
    # rm iostat_output.txt  # Clean up temporary file
}

# Function to process iostat's output and write to CSV
process_iostat_output() {
    echo "Timestamp,r/s,rMB/s,r_await,rareq-sz,w/s,wMB/s,w_await,wareq-sz" > iostat_results.csv
    current_timestamp=""
    while IFS= read -r line; do
        if [[ $line == *"Time:"* ]]; then
            # Extract timestamp
            current_timestamp=$(echo $line | awk '{print $2, $3}')
        elif [[ $line == *md0* ]]; then
            # Write the current timestamp along with the iostat metrics
            echo "$line" | awk -v ts="$current_timestamp" '{print ts,",",$2,",",$3,",",$6,",",$7,",",$8,",",$9,",",$12,",",$13}' >> iostat_results.csv
        fi
    done < iostat_output.txt
}

process_mpstat_output() {
  echo "Timestamp,core,usr,sys,iowait,soft,idle" > mpstat_results.csv
  current_timestamp=""
  while IFS= read -r line; do
      if [[ $line == *"Time:"* ]]; then
          # Extract timestamp
          current_timestamp=$(echo $line | awk '{print $2, $3}')
      else
          # Write the current timestamp along with the iostat metrics
          echo "$line" | awk -v ts="$current_timestamp" '{print ts,",",$2,",",$3,",",$5,",",$6,",",$8,",",$12}' >> mpstat_results.csv
      fi
  done < mpstat_output.txt
}

# Setup trap for cleanup on script exit
trap cleanup EXIT

# Start iostat in the background, appending a timestamp to each interval, and redirecting output to a file
(echo "Time: $(date +'%Y-%m-%d %H:%M:%S.%3N')"; iostat -xdm /dev/md0 1;) > iostat_output.txt &
iostat_pid=$!

(echo "Time: $(date +'%Y-%m-%d %H:%M:%S.%3N')"; mpstat -P ALL 1;) > mpstat_output.txt &
mpstat_pid=$!


#   -p client_to_op_map="RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT" \
#   -p client_to_op_map="READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ" \


#   -threads 16 \
#   -p rocksdb.num_cfs=16 \
#   -p client_to_cf_map="default,cf1,cf2,cf3,cf4,cf5,cf6,cf7,cf8,cf9,cf10,cf11,cf12,cf13,cf14,cf15" \
#   -p client_to_op_map="READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ,READ" \
#   -target_rates "380,420,395,405,401,399,409,415,385,382,418,412,391,383,392,400" \
#   -p wbm_limits="256,256,256,256,256,256,256,256,256,256,256,256,256,256,256,256" \
#   -p status.interval_ms=500 \
#   -p burst_gap_s=30 \
#   -p burst_size_ops=1 \
#   -p rate_limits="10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000" \
#   -p read_rate_limits="10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000" \

#   -p client_to_op_map="READ,READ,READ,READ" \
#   -p client_to_op_map="RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT" \

# Start ycsb process in the background
./ycsb -run -db rocksdb -P rocksdb/rocksdb.properties \
  -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data \
  -p request_rate_json=sample.json \
  -p scale_factor=1 \
  -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
  -p readallfields=true \
  -p requestdistribution=zipfian \
  -s -p operationcount=35000000 \
  -p recordcount=3125000 \
  -p fieldcount=1 \
  -p fieldlength=1024 \
  -p updateproportion=0 \
  -p insertproportion=0 \
  -p readproportion=1 \
  -p scanproportion=0 \
  -p randominsertproportion=0 \
  -p real_op_mode=false \
  -threads 4 \
  -p rocksdb.num_cfs=4 \
  -p client_to_cf_map="default,cf1,cf2,cf3" \
  -p client_to_op_map="RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT,RANDOM_INSERT" \
  -target_rates "8000,8000,8000,8000" \
  -p wbm_size="300" \
  -p wbm_limits="512,512,512,512" \
  -p status.interval_ms=500 \
  -p burst_gap_s=0 \
  -p burst_size_ops=1 \
  -p rate_limits="10000,10000,10000,10000" \
  -p read_rate_limits="10000,10000,10000,10000" \
  -p refill_period=50 \
  -p rsched=false \
  -p rsched_interval_ms=10 \
  -p lookback_intervals=30 \
  -p rsched_rampup_multiplier=1.2 \
  -p io_read_capacity_kbps=$((9000 * 1024)) \
  -p io_write_capacity_kbps=$((4500 * 1024)) \
  -p memtable_capacity_kb=$((512 * 1024)) \
  -p min_memtable_count=$((16)) \
  -p max_memtable_size_kb=$((64 * 1024)) \
  -p min_memtable_size_kb=$((64 * 1024)) \
  | tee status_thread.txt &

# To add:
# rocksdb parameters for memtable size, etc.

# Multi column family
# ./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p rocksdb.dbname=/mnt/multi-cf2/ycsb-rocksdb-data \
#   -s -p operationcount=35000000 \
#   -p recordcount=1562500 \
#   -p updateproportion=0 \
#   -p insertproportion=0 \
#   -p readproportion=1 \
#   -p scanproportion=0 \
#   -p randominsertproportion=0 \
#   -threads 2 \
#   -p op_mode=real \
#   -target_rates "900,900" \
#   -p requestdistribution=uniform \
#   | tee status_thread.txt &

# Multi column family
# ./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p rocksdb.dbname=/mnt/multi-cf2/ycsb-rocksdb-data-2 \
#   -s -p operationcount=35000000 \
#   -p recordcount=1562500 \
#   -p updateproportion=0 \
#   -p insertproportion=0 \
#   -p readproportion=1 \
#   -p scanproportion=0 \
#   -p randominsertproportion=0 \
#   -threads 2 \
#   -p op_mode=fake \
#   -target_rates "200,900" \
#   -p rate_limits=60 \
#   -p read_rate_limits=60 \
#   -p refill_period=10 \
#   -p requestdistribution=uniform \
#   | tee status_thread2.txt &

ycsb_pid=$!
echo "YCSB pid: ${ycsb_pid}"
# Wait for the ycsb process to finish
wait $ycsb_pid

# The script exits here, triggering the cleanup function

# GDB format
# gdb --args ./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data \
#   -s -p operationcount=35000000 \
#   -p recordcount=1562500 \
#   -p updateproportion=1 \
#   -p insertproportion=0 \
#   -p readproportion=0 \
#   -p scanproportion=0 \
#   -p randominsertproportion=0 \
#   -threads 4 \
#   -p rate_limits=200 \
#   -p requestdistribution=uniform \
#   | tee status_thread.txt

# nohup ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
#   -p recordcount=3125000 \
#   -p fieldcount=16 \
#   -p fieldlength=1024 \
#   -p table=default \
#   -threads 1 \
#   -p op_mode=real \
#   -p requestdistribution=uniform \
#   -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data -s | tee status_thread.txt &

# nohup ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
#   -p recordcount=3125000 \
#   -p fieldcount=16 \
#   -p fieldlength=1024 \
#   -p table=cf2 \
#   -threads 1 \
#   -p op_mode=real \
#   -p requestdistribution=uniform \
#   -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data -s | tee status_thread.txt &

#   nohup ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
#   -p recordcount=3125000 \
#   -p fieldcount=16 \
#   -p fieldlength=1024 \
#   -p table=cf3 \
#   -threads 1 \
#   -p op_mode=real \
#   -p requestdistribution=uniform \
#   -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data -s | tee status_thread.txt &

#   nohup ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
#   -p recordcount=3125000 \
#   -p fieldcount=16 \
#   -p fieldlength=1024 \
#   -p table=cf4 \
#   -threads 1 \
#   -p op_mode=real \
#   -p requestdistribution=uniform \
#   -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data -s | tee status_thread.txt &
