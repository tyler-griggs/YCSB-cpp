#!/usr/bin/env python3
import math
import os
import sys
import signal
import atexit
import subprocess
import asyncio
import time
from pathlib import Path

# Global process handles
iostat_proc = None
mpstat_proc = None
ycsb_proc = None

def current_milliseconds():
    """Get current time in milliseconds since epoch."""
    return int(time.time() * 1000)

async def monitor_folder(folder_path: str, log_file: str):
    """
    Asynchronously monitors a folder for .log files, logs changes, and 
    tracks the total size of all .log files with timestamps in milliseconds.

    Args:
        folder_path (str): The folder to monitor.
        log_file (str): The path to the log file where changes will be recorded.
    """
    folder = Path(folder_path)
    previous_files = set()

    with open(log_file, "w") as log:
        log.write(f"--- Monitoring started at {current_milliseconds()} ---\n")
        print(f"Monitoring folder: {folder_path}")
        
    try:
        while True:
            current_files = {f for f in folder.glob("*.log") if f.is_file()}

            # Detect new and removed files
            new_files = current_files - previous_files
            removed_files = previous_files - current_files

            # Calculate total size of .log files
            total_size = sum(f.stat().st_size for f in current_files)

            # Log changes with timestamps
            timestamp = current_milliseconds()
            with open(log_file, "a") as log:
                for f in new_files:
                    log.write(f"[{timestamp}] [+] {f.name} appeared ({f.stat().st_size} bytes)\n")
                
                for f in removed_files:
                    log.write(f"[{timestamp}] [-] {f.name} disappeared\n")

                # Log the total size with a timestamp
                log.write(f"[{timestamp}] [=] Total .log file size: {total_size} bytes\n")

            previous_files = current_files

            # Sleep before the next check
            await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        with open(log_file, "a") as log:
            log.write(f"\n--- Monitoring stopped at {current_milliseconds()} ---\n")
        print("Folder monitoring stopped.")

async def stream_output(stream, log_file, prefix=""):
    """
    Asynchronously stream output to the terminal and log file.
    """
    with open(log_file, "w") as log:
        while True:
            line = await stream.readline()
            if not line:
                break
            # Write to terminal and log file
            sys.stdout.write(f"{prefix}{line.decode()}")
            sys.stdout.flush()
            log.write(line.decode())

async def run_ycsb(monitor_task, ycsb_cmd, log_file):
    """
    Run the YCSB benchmark process asynchronously while the monitoring task runs.
    
    Args:
        monitor_task: The asyncio task running the folder monitor.
        ycsb_cmd (str): The full YCSB command to run.
        log_file (str): The log file to save the YCSB output.
    """
    print(f"Executing YCSB command:\n{ycsb_cmd}")

    # Create the subprocess with streaming output
    process = await asyncio.create_subprocess_shell(
        ycsb_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    print(f"YCSB pid: {process.pid}")

    # Stream both stdout and stderr concurrently
    await asyncio.gather(
        stream_output(process.stdout, log_file, ""),
        stream_output(process.stderr, log_file, "")
    )

    # Wait for the YCSB process to finish
    await process.wait()
    
    print("YCSB process finished.")

    # Cancel the monitoring task after YCSB finishes
    monitor_task.cancel()
    await monitor_task


def cleanup():
  """Kill background processes and process output files."""
  print("Cleaning up...")
  global iostat_proc, mpstat_proc, ycsb_proc
  for proc, name in [(iostat_proc, "iostat"), (mpstat_proc, "mpstat"), (ycsb_proc, "ycsb")]:
    if proc is not None and proc.poll() is None:
      try:
        proc.kill()
        print(f"Killed {name} process (pid {proc.pid})")
      except Exception as e:
        print(f"Error killing {name} process: {e}", file=sys.stderr)
  process_iostat_output()
  process_mpstat_output()
  # Optionally remove temporary files:
  # os.remove("iostat_output.txt")

def process_iostat_output():
  """Process iostat_output.txt into iostat_results.csv."""
  header = "Timestamp,r/s,rMB/s,r_await,rareq-sz,w/s,wMB/s,w_await,wareq-sz\n"
  try:
    with open("iostat_results.csv", "w") as csv_out:
      csv_out.write(header)
      current_timestamp = ""
      with open("iostat_output.txt", "r") as f:
        for line in f:
          line = line.strip()
          if "Time:" in line:
            parts = line.split()
            if len(parts) >= 3:
              # Concatenate 2nd and 3rd tokens for the timestamp
              current_timestamp = parts[1] + " " + parts[2]
          elif "nvme0n1" in line:
            parts = line.split()
            # Ensure we have at least 13 fields (awk uses fields 2,3,6,7,8,9,12,13)
            if len(parts) >= 13:
              # Convert from 1-indexed (awk) to 0-indexed (python):
              # fields: $2->parts[1], $3->parts[2], $6->parts[5], $7->parts[6],
              # $8->parts[7], $9->parts[8], $12->parts[11], $13->parts[12]
              row = f"{current_timestamp},{parts[1]},{parts[2]},{parts[5]},{parts[6]},{parts[7]},{parts[8]},{parts[11]},{parts[12]}\n"
              csv_out.write(row)
    print("iostat output processed into iostat_results.csv")
  except Exception as e:
    print(f"Error processing iostat output: {e}", file=sys.stderr)

def process_mpstat_output():
  """Process mpstat_output.txt into mpstat_results.csv."""
  header = "Timestamp,core,usr,sys,iowait,soft,idle\n"
  try:
    with open("mpstat_results.csv", "w") as csv_out:
      csv_out.write(header)
      current_timestamp = ""
      with open("mpstat_output.txt", "r") as f:
        for line in f:
          line = line.strip()
          if "Time:" in line:
            parts = line.split()
            if len(parts) >= 3:
              current_timestamp = parts[1] + " " + parts[2]
          else:
            parts = line.split()
            # awk: uses fields $2,$3,$5,$6,$8,$12 (i.e. indices 1,2,4,5,7,11)
            if len(parts) >= 12:
              row = f"{current_timestamp},{parts[1]},{parts[2]},{parts[4]},{parts[5]},{parts[7]},{parts[11]}\n"
              csv_out.write(row)
    print("mpstat output processed into mpstat_results.csv")
  except Exception as e:
    print(f"Error processing mpstat output: {e}", file=sys.stderr)

async def main():
  global iostat_proc, mpstat_proc, ycsb_proc

  # Register cleanup to be called on exit.
  atexit.register(cleanup)
  # Also catch SIGINT and SIGTERM to exit gracefully.
  def signal_handler(signum, frame):
    sys.exit(0)
  signal.signal(signal.SIGINT, signal_handler)
  signal.signal(signal.SIGTERM, signal_handler)

  # Start iostat in the background.
  # The command prepends a timestamp and then runs "iostat -xdm /dev/nvme0n1 1".
  with open("iostat_output.txt", "w") as iostat_file:
    iostat_cmd = "(echo \"Time: $(date +'%Y-%m-%d %H:%M:%S.%3N')\"; iostat -xdm /dev/nvme0n1 1)"
    iostat_proc = subprocess.Popen(iostat_cmd, shell=True, stdout=iostat_file, stderr=subprocess.STDOUT)
    print(f"Started iostat (pid {iostat_proc.pid})")

  # Start mpstat in the background.
  with open("mpstat_output.txt", "w") as mpstat_file:
    mpstat_cmd = "(echo \"Time: $(date +'%Y-%m-%d %H:%M:%S.%3N')\"; mpstat -P ALL 1)"
    mpstat_proc = subprocess.Popen(mpstat_cmd, shell=True, stdout=mpstat_file, stderr=subprocess.STDOUT)
    print(f"Started mpstat (pid {mpstat_proc.pid})")
    
    
  ############################ Parameters ######################################### 
    
  # Dataset Parameters -- should be set based on loaded data
  fieldcount = "1"
  RECORD_SIZE = 8*1024
  fieldlength = str(RECORD_SIZE)
  NUM_CFS = 16
  rocksdb_num_cfs = str(NUM_CFS)
    
  # Workload Parameters
  client_config = "wal-experiments/16client_ramp_up.yaml"

  # RocksDB Parameters
  tpool_threads = "8"
  status_interval_ms = "100"
  max_background_jobs = "4"
  max_background_flushes = "3"  # Subset of jobs
  max_subcompactions = "1" # Multiplier on max_background_jobs
  
  # Cache Parameters
  CACHE_SIZE = 0  # Vanilla RocksDB: Set to 0 to disable
  NUM_RECORDS_PER_SHARD = 256
  CACHE_SHARD_BITS_CALC = lambda size: int(math.log2(size // (RECORD_SIZE * NUM_RECORDS_PER_SHARD)))
  CACHE_SHARD_BITS_POOLED = 0 if CACHE_SIZE == 0 else CACHE_SHARD_BITS_CALC(CACHE_SIZE * NUM_CFS)
  CACHE_SHARD_BITS_ISOLATED = 0 if CACHE_SIZE == 0 else CACHE_SHARD_BITS_CALC(CACHE_SIZE)
  fairdb_use_pooled = "true"
  if fairdb_use_pooled == "true":
    cache_num_shard_bits = str(CACHE_SHARD_BITS_POOLED)
  else:
    cache_num_shard_bits = str(CACHE_SHARD_BITS_ISOLATED)
  rocksdb_cache_size = ",".join([str(CACHE_SIZE)] * NUM_CFS)
  cache_rad_microseconds = str(0) # 0s --> isolated, reduces to vanilla rocksdb
  # cache_rad_microseconds = str(10 * 1000 * 1000) # 10s

  # Write Buffer Parameters
  wbm_size = "0" # Vanilla RocksDB: Set to 0 to disable
  wbm_steady_res_size = "650"
  wbm_limits =  ",".join(["750"] * NUM_CFS)
  write_buffer_size = ",".join(["67108864"] * NUM_CFS)
  max_write_buffer_number = ",".join(["2"] * NUM_CFS)
  
  # I/O Bandwidth Parameters
  enable_rate_limiter = "false" # Vanilla RocksDB: Set to false to disable
  write_rate_limits_mbps = ",".join(["10000"] * NUM_CFS)
  read_rate_limits_mbps = ",".join(["10000"] * NUM_CFS)
  
  # Resource Scheduler Parameters
  rsched = "false" # Vanilla RocksDB: Set to false to disable
  refill_period_ms = "50"
  rsched_interval_ms = "10"
  lookback_intervals = "30"
  rsched_rampup_multiplier = "1.2"
  # Note: only need to set these if rsched is true
  io_read_capacity = 9000 * 1024
  io_write_capacity = 4500 * 1024
  memtable_capacity = 512 * 1024
  min_memtable_count = 16
  max_memtable_size = 64 * 1024
  min_memtable_size = 64 * 1024

  # WAL Parameters
  max_total_wal_size = 1024 * max_memtable_size * NUM_CFS * 2
  wal_steady_res_size = 0 # Vanilla RocksDB: Set to 0 to disable

  rocksdb_folder = "/mnt/rocksdb/ycsb-rocksdb-data"
  log_file_path = "./logs/disc_log.txt"
  tmplog = "./tmplog.txt"

  cf_deltas = ",".join([str(-1)] * (NUM_CFS - 1) + [str(1000)])
  cf_weights = ",".join([str(1)] * NUM_CFS)
  
  flush_thread_lmax = "800"
  flush_thread_k = "1"


  ############################ End Parameters ######################################### 


  # Construct the YCSB command string.
  ycsb_cmd = (
      "./ycsb -run -db rocksdb -P rocksdb/rocksdb.properties -s "
      f"-p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data "
      f"-p workload=com.yahoo.ycsb.workloads.CoreWorkload "
      f"-p config={client_config} "
      "-p readallfields=true "
      f"-p fieldcount={fieldcount} "
      f"-p fieldlength={fieldlength} "
      f"-p tpool_threads={tpool_threads} "
      f"-p fairdb_use_pooled={fairdb_use_pooled} "
      f"-p rocksdb.num_cfs={rocksdb_num_cfs} "
      f"-p rocksdb.cache_size={rocksdb_cache_size} "
      f"-p rocksdb.write_buffer_size={write_buffer_size} "
      f"-p rocksdb.max_write_buffer_number={max_write_buffer_number} "
      f"-p rocksdb.max_background_jobs={max_background_jobs} "
      f"-p rocksdb.max_background_flushes={max_background_flushes} "
      f"-p rocksdb.max_subcompactions={max_subcompactions} "
      f"-p cache_num_shard_bits={cache_num_shard_bits} "
      f"-p fairdb_cache_rad={cache_rad_microseconds} "
      f"-p wbm_size={wbm_size} "
      f"-p wbm_steady_res_size={wbm_steady_res_size} "
      f"-p wbm_limits={wbm_limits} "
      f"-p status.interval_ms={status_interval_ms} "
      f"-p enable_rate_limiter={enable_rate_limiter} "
      f"-p write_rate_limits={write_rate_limits_mbps} "
      f"-p read_rate_limits={read_rate_limits_mbps} "
      f"-p refill_period_ms={refill_period_ms} "
      f"-p rsched={rsched} "
      f"-p rsched_interval_ms={rsched_interval_ms} "
      f"-p lookback_intervals={lookback_intervals} "
      f"-p rsched_rampup_multiplier={rsched_rampup_multiplier} "
      f"-p io_read_capacity_kbps={io_read_capacity} "
      f"-p io_write_capacity_kbps={io_write_capacity} "
      f"-p memtable_capacity_kb={memtable_capacity} "
      f"-p min_memtable_count={min_memtable_count} "
      f"-p max_memtable_size_kb={max_memtable_size} "
      f"-p min_memtable_size_kb={min_memtable_size} "
      f"-p rocksdb.max_total_wal_size={max_total_wal_size} "
      f"-p rocksdb.wal_steady_res_size={wal_steady_res_size} "
      f"-p cf_deltas={cf_deltas} "
      f"-p cf_weights={cf_weights} "
      f"-p flush_thread_lmax={flush_thread_lmax} "
      f"-p flush_thread_k={flush_thread_k} "
  )

  # Log the command (equivalent to bash set -x)
  print("Executing YCSB command:")
  print(ycsb_cmd)

  # Start the YCSB process. Its output is piped to tee so that output is both logged and saved.
  # Using shell=True allows the pipe to work as expected.
  ycsb_full_cmd = ycsb_cmd + " | tee status_thread.txt"


  process = await asyncio.create_subprocess_shell(
      ycsb_cmd,
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.PIPE
  )

  print(f"YCSB pid: {process.pid}")

  log_file = "templog.txt"

  await asyncio.gather(
      stream_output(process.stdout, log_file, ""),
      stream_output(process.stderr, log_file, "")
  )


if __name__ == "__main__":
  asyncio.run(main())
