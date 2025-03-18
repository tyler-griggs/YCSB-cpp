#!/bin/bash
if [ $# -ne 4 ]; then
    echo "Usage: $0 <num_column_families> <field_count> <field_length> <record_count>, must have <num_column_families> = 16"
    exit 1
fi

name=$(whoami)
num_fams="$1"
num_cols="$2"
data_size="$3"
num_entries="$4"

git stash
git checkout 53ae10c5cf0d6c254c0567d0f7cdc6249ea12031

# Delete lines 470-475
sed -i '470,475d' ./rocksdb/rocksdb_db.cc

# File to modify
file="./rocksdb/rocksdb.properties"

# Change config
sed -i '23s/^# //' "$file"
sed -i '24s/^/# /' "$file"
sed -i '25s/^# //' "$file"
sed -i '26s/^/# /' "$file"
sed -i '39s/^/# /' "$file"
sed -i '41s/^# //' "$file"

sed -i "s/tylergriggs/$name/g" Makefile

mkdir logs
mkdir results

make clean
make

bash ycsb_loader.sh $num_fams $num_cols $data_size $num_entries

git checkout -f master

git stash pop

make clean
make