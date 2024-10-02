#!/bin/bash

# Set root dir
root_dir="data/iotdb-data/datanode/data/sequence/root.test.g_0"

# Enumerate each folder in root
for dir in "$root_dir"/*; do
    if [ -d "$dir" ]; then
        if [ -d "$dir/19712" ]; then
            # Delete 19712
            echo "Deleting directory: $dir/19712"
            rm -rf "$dir/19712"
        fi
    fi
done

echo "Done."
