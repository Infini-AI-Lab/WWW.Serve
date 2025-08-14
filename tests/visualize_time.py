import json
import matplotlib.pyplot as plt
import numpy as np


def get_latency(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    latency_list = []
    for item in data:
        latency = item["timestamp_list"][-1] - item["timestamp_list"][0]
        latency_list.append(latency)

    return latency_list


json_path_single = "/home/hywang/Reasoning/Decentralized-Agents/results/test4/test_4_result.json"
json_path_network = "/home/hywang/Reasoning/Decentralized-Agents/results/test3/test_3_result.json"

latency_single = get_latency(json_path_single)
latency_network = get_latency(json_path_network)

print(len(latency_single), len(latency_network))

n = len(latency_single)
idx = np.arange(n)
bar_width = 0.4

plt.figure(figsize=(12, 6))

plt.bar(idx - bar_width/2, latency_single, width=bar_width, 
        label="Single", color="#1f77b4", alpha=0.8)

plt.bar(idx + bar_width/2, latency_network, width=bar_width, 
        label="DeServe", color="#ff7f0e", alpha=0.8)

plt.xlabel("Requests")
plt.ylabel("Latency (s)")
plt.legend()
plt.grid(axis="y", linestyle="--", alpha=0.7)

plt.tight_layout()
plt.savefig("time.png")