#!/bin/bash

# run: docker cp local_data_dir master:/tmp on your host machine

# Set the source directory containing the files to upload
source_dir="/tmp/data"

# Set the destination directory in HDFS
destination_dir="/data"

# Log file path
log_file="/tmp/hadoop_logs/data_log"
if [ ! -f "$log_file" ]; then
        # Check if log file directory exists
        log_dir=$(dirname "$log_file")
        if [ ! -d "$log_dir" ]; then
            # Create log file directory
            mkdir -p "$log_dir"
        fi

        # Create log file
        touch "$log_file"
fi

# Function to log messages
log_message() {
    echo "$(date "+%Y-%m-%d %H:%M:%S") $1" >> "$log_file"
}

# Function to upload files to HDFS
upload_to_hdfs() {
    local source_file="$1"
    local destination="$2"

    # Check if the file exists in HDFS
    hdfs dfs -test -e "$destination"

    if [ $? -eq 0 ]; then
        log_message "File $source_file already exists in HDFS. Skipping."
    else
        # Copy the file to HDFS
        hdfs dfs -mkdir -p $destination_dir
        hdfs dfs -copyFromLocal "$source_file" "$destination"
        if [ $? -eq 0 ]; then
            output_file=$(hdfs dfs -du -h "$destination")
            log_message "Successfully uploaded from $source_file to HDFS. Output file: $output_file"
        else
            log_message "Failed to upload $source_file to HDFS at $destination"
        fi
    fi
}

# Iterate through files in the source directory
for file in "$source_dir"/*; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        upload_to_hdfs "$file" "$destination_dir/$filename"
    fi
done