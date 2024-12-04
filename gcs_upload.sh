
# First, authenticate to get access to buckets
gcloud auth login

BUCKET_NAME="gs://rocksdb-backups"

# IMPORTANT: update this to a new file path for each backup
# BACKUP_FILE_PATH_IN_BUCKET="16_client-1kb_values-3125000_records"
BACKUP_FILE_PATH_IN_BUCKET="[FILL THIS IN]"

# Upload *without* compression
gsutil -m cp -r /mnt/rocksdb/ycsb-rocksdb-data ${BUCKET_NAME}/${BACKUP_FILE_PATH_IN_BUCKET}/

# Download *without* compression
gsutil -m cp -r ${BUCKET_NAME}/${BACKUP_FILE_PATH_IN_BUCKET}/ycsb-rocksdb-data /mnt/rocksdb/
