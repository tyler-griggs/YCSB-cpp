import math
import os
import subprocess

CACHE_SIZE = 1 * (1024**3)  # 1GB
NUM_RECORDS_PER_SHARD = 64
RECORD_SIZE=1024
NUM_CFS = 16
CACHE_SHARD_BITS_CALC = lambda size: int(math.log2(size // (RECORD_SIZE * NUM_RECORDS_PER_SHARD)))
CACHE_SHARD_BITS_POOLED = CACHE_SHARD_BITS_CALC(CACHE_SIZE * NUM_CFS)
CACHE_SHARD_BITS_ISOLATED = CACHE_SHARD_BITS_CALC(CACHE_SIZE)
CACHE_RAD_MICROSECONDS = 10 * 1000 * 1000  # 10s

# Set the environment variables for the parameters you want to override
os.environ["CONFIG"] = "examples/tg_read.yaml"
os.environ["FIELDCOUNT"] = "1"
os.environ["FIELDLENGTH"] = str(RECORD_SIZE)
os.environ["TPOOL_THREADS"] = str(NUM_CFS)
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
