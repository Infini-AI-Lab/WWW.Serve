#!/bin/bash

for i in {1..5}
do
    echo "Starting node$i ..."
    bash ./scripts/sglang_node$i.sh
    sleep 6
done

echo "All 5 nodes started."
