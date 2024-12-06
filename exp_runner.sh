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
            echo "$line" | awk -v ts="$current_timestamp" '{print ts ",",$2 ",",$3 ",",$6 ",",$7 ",",$8 ",",$9 ",",$12 ",",$13}' >> iostat_results.csv
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
          # Write the current timestamp along with the mpstat metrics
          echo "$line" | awk -v ts="$current_timestamp" '{print ts ",",$2 ",",$3 ",",$5 ",",$6 ",",$8 ",",$12}' >> mpstat_results.csv
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

# Default values for YCSB parameters if not set by the environment
: "${CONFIG:=examples/tg_read.yaml}"
: "${FIELDCOUNT:=1}"
: "${FIELDLENGTH:=1024}"
: "${TPOOL_THREADS:=16}"
: "${ROCKSDB_NUM_CFS:=16}"
: "${FAIRDB_USE_POOLED:=true}"
: "${ROCKSDB_CACHE_SIZE:=1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576}"
: "${CACHE_NUM_SHARD_BITS:=8}"
: "${WBM_SIZE:=850}"
: "${WBM_STEADY_RES_SIZE:=650}"
: "${WBM_LIMITS:=750,750,750,750,750,750,750,750,750,750,750,750,750,750,750,750}"

./ycsb -run -db rocksdb -P rocksdb/rocksdb.properties -s \
  -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data \
  -p workload=com.yahoo.ycsb.workloads.CoreWorkload \
  -p config="$CONFIG" \
  -p readallfields=true \
  -p fieldcount="$FIELDCOUNT" \
  -p fieldlength="$FIELDLENGTH" \
  -p tpool_threads="$TPOOL_THREADS" \
  -p rocksdb.num_cfs="$ROCKSDB_NUM_CFS" \
  -p fairdb_use_pooled="$FAIRDB_USE_POOLED" \
  -p rocksdb.cache_size="$ROCKSDB_CACHE_SIZE" \
  -p cache_num_shard_bits="$CACHE_NUM_SHARD_BITS" \
  -p fairdb_cache_rad="$CACHE_RAD_MICROSECONDS" \
  -p wbm_size="$WBM_SIZE" \
  -p wbm_steady_res_size="$WBM_STEADY_RES_SIZE" \
  -p wbm_limits="$WBM_LIMITS" \
  -p status.interval_ms=100 \
  -p rate_limits="10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000," \
  -p read_rate_limits="10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000,10000," \
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

ycsb_pid=$!
echo "YCSB pid: ${ycsb_pid}"
# Wait for the ycsb process to finish
wait $ycsb_pid
