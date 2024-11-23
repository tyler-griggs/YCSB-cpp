#!/bin/bash

if [ $# -ne 4 ]; then
    echo "Usage: $0 <num_column_families> <field_count> <field_length> <record_count>"
    exit 1
fi

# Args
num_column_families=$1
field_count=$2
field_length=$3
record_count=$4

# Loop through the number of column families
for (( i=0; i<num_column_families; i++ ))
do
    if [ $i -eq 0 ]; then
        table_name="default"
    else
        table_name="cf${i}"
    fi

    echo "Loading data into column family: $table_name"

    ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
      -p rocksdb.num_cfs=${num_column_families} \
      -p recordcount=${record_count} \
      -p fieldcount=${field_count} \
      -p fieldlength=${field_length} \
      -p table=${table_name} \
      -threads 1 \
      -p op_mode=real \
      -p requestdistribution=uniform \
      -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data -s
done

# ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#       -p rocksdb.num_cfs=16 \
#       -p recordcount=3125000 \
#       -p fieldcount=16 \
#       -p fieldlength=1024 \
#       -p table=cf1 \
#       -threads 1 \
#       -p op_mode=real \
#       -p requestdistribution=uniform \
#       -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data -s