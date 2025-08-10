#!/bin/bash

for i in {1..5}
do
    echo "Starting node$i ..."
    bash node$i.sh
done

echo "All 5 nodes started."
