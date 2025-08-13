#!/usr/bin/env python3
import pandas as pd

# ==== Parameters ====
file_path = "AzureLLMInferenceTrace_conv_1week.csv"  # input
output_path = "node_workloads.csv"                   # output CSV

number_of_nodes = 5         # total nodes
scale = 30                 # pick 1 row out of every 'scale' rows
time_difference = 3 * 3600  # seconds between start times of consecutive nodes
starting_time = 0           # start offset in seconds from the dataset's earliest time
interval_length = 3600      # seconds of workload to take for each node


# ==== 1. Load dataset ====
df = pd.read_csv(file_path)
df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='mixed')

# ==== 2. Prepare result container ====
all_nodes = []

# Earliest timestamp in dataset
dataset_start = df['TIMESTAMP'].min()

for node_idx in range(number_of_nodes):
    # Compute time window for this node
    node_start_time = dataset_start + pd.Timedelta(seconds=starting_time + node_idx * time_difference)
    node_end_time = node_start_time + pd.Timedelta(seconds=interval_length)

    # Filter rows within this interval
    sub_df = df[(df['TIMESTAMP'] >= node_start_time) & (df['TIMESTAMP'] < node_end_time)].copy()

    if sub_df.empty:
        print(f"[warn] Node {node_idx}: no data in [{node_start_time}, {node_end_time})")
        continue

    # Downsample by scale
    if scale > 1:
        sub_df = sub_df.iloc[::scale, :]

    # Compute relative timestamp starting from 0 for this node
    sub_df['TIMESTAMP'] = (sub_df['TIMESTAMP'] - node_start_time).dt.total_seconds()

    # Assign node index
    sub_df['node_index'] = node_idx

    # Keep only needed columns
    sub_df = sub_df[['TIMESTAMP', 'ContextTokens', 'GeneratedTokens', 'node_index']]

    all_nodes.append(sub_df)

# ==== 3. Merge all nodes and sort by timestamp ====
if all_nodes:
    merged_df = pd.concat(all_nodes, ignore_index=True)
    merged_df = merged_df.sort_values(by='TIMESTAMP', ascending=True)
    output_path = f"node_workloads_{number_of_nodes}_nodes_{scale}_scale_{time_difference}_time_{starting_time}_start_{interval_length}_interval.csv"
    merged_df.to_csv(output_path, index=False)
    print(f"[ok] Saved workload to {output_path} with {len(merged_df)} rows")
else:
    print("[error] No data found for any node!")
