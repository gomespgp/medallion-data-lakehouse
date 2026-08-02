#!/bin/sh
# Wait for MinIO service to start
sleep 5;

# Configure minio client alias
/usr/bin/mc alias set localminio http://minio:9000 minioadmin minioadmin;

# Create the data lake bucket if it doesn't exist
/usr/bin/mc mb localminio/data-lake --ignore-existing;

echo "Bucket 'data-lake' created successfully!";