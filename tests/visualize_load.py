import json
import matplotlib.pyplot as plt
import pandas as pd
from scipy.signal import savgol_filter


json_files = [
    "/home/hywang/Reasoning/Decentralized-Agents/datasets/test_load/test_1_node_1.json",
    "/home/hywang/Reasoning/Decentralized-Agents/datasets/test_load/test_1_node_2.json",
    "/home/hywang/Reasoning/Decentralized-Agents/datasets/test_load/test_1_node_3.json",
    "/home/hywang/Reasoning/Decentralized-Agents/datasets/test_load/test_1_node_4.json",
    "/home/hywang/Reasoning/Decentralized-Agents/datasets/test_load/test_1_node_5.json",
]

plt.figure(figsize=(14, 6))

start_time = 0

for idx, filepath in enumerate(json_files, start=1):
    with open(filepath, "r", encoding="utf-8") as f:
        data_dict = json.load(f)

    data = list(data_dict.values())[0]

    if idx == 1:
        start_time = data[0]['timestamp']

    df = pd.DataFrame(data)

    df['relative_time'] = df['timestamp'] - start_time

    # df['smoothed_token_usage'] = df['token_usage'].rolling(window=20, center=True).mean()
    # df['smoothed_token_usage'] = savgol_filter(df['token_usage'], window_length=11, polyorder=2)

    plt.plot(df['relative_time'], df['num_running_reqs'], label=f'Node {idx}', lw=0.5)

plt.xlabel("Time")
plt.ylabel("Running Requests")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("token_usage_over_time.png")
