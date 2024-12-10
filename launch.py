import math
import os
import subprocess

# TODO: Set client labels (WBM steady/aggressive)

# CACHE_SIZE = 128 * (1024**2)
# CACHE_SIZE = 2 * (1024**3)
CACHE_SIZE = 0
NUM_RECORDS_PER_SHARD = 256
RECORD_SIZE=4096
NUM_CFS = 16
CACHE_SHARD_BITS_CALC = lambda size: int(math.log2(size // (RECORD_SIZE * NUM_RECORDS_PER_SHARD)))
CACHE_SHARD_BITS_POOLED = 0 if CACHE_SIZE == 0 else CACHE_SHARD_BITS_CALC(CACHE_SIZE * NUM_CFS)
CACHE_SHARD_BITS_ISOLATED = 0 if CACHE_SIZE == 0 else CACHE_SHARD_BITS_CALC(CACHE_SIZE)
CACHE_RAD_MICROSECONDS = 10 * 1000 * 1000  # 10s

# Set the environment variables for the parameters you want to override
os.environ["CONFIG"] = "examples/tg_read.yaml"
os.environ["FIELDCOUNT"] = "1"
os.environ["FIELDLENGTH"] = str(RECORD_SIZE)
os.environ["TPOOL_THREADS"] = "200"
os.environ["ROCKSDB_NUM_CFS"] = str(NUM_CFS)
os.environ["FAIRDB_USE_POOLED"] = "true"
os.environ["ROCKSDB_CACHE_SIZE"] = ",".join([str(CACHE_SIZE)] * NUM_CFS)
os.environ["CACHE_NUM_SHARD_BITS"] = str(CACHE_SHARD_BITS_POOLED)
os.environ["CACHE_RAD_MICROSECONDS"]  = str(CACHE_RAD_MICROSECONDS)
os.environ["WBM_SIZE"] = "850"
os.environ["WBM_STEADY_RES_SIZE"] = "650"
os.environ["WBM_LIMITS"] =  ",".join(["750"] * NUM_CFS)

# print("Environment variables:")
# for key, value in os.environ.items():
#     print(f"{key}={value}")

# Run the bash script
subprocess.run(["/bin/bash", "exp_runner.sh"], check=True)
